#!/usr/bin/env bash
# Gateway-host test: clients on a host that runs no cluster member, across the loss of the member they
# read through.
#
# A three-node cluster, plus a gateway host: node 3, its own Aeron directory and archive, running
# ReplayerServer with replayer.archiveEndpoints. Its relay reads the leader's tap recording over UDP and
# republishes it onto the host's own tap. A ClusterProbe confirm producer on node 3 submits over UDP ingress
# and checks its own frames on that tap; mid-stream the leader is killed, which is both a leader failover
# and the relay's source going away. The gateway host is then restarted, which relays the log again from
# globalSeqNo 1 into a second local recording, and a fresh ClusterProbe follow on node 3 cold-starts through
# the host's Replayer across that two-recording chain.
#
# PASS iff the producer saw every frame exactly once, in order, the relay moved to another member, and
# the fresh follower caught up after the restart.
#
# Java only. One machine stands in for four hosts: node 3 differs from a member only in running no
# SequencerServer.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
LOG_DIR="logs/gateway-host"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqgh"
CLUSTER_MEMBERS="$(cluster_members_string 3)"
NODE=3

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}"/seqeron-seq-aeron-{0,1,2,$NODE} 2>/dev/null

declare -a SEQ_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "Running" 30 || { echo "seq $m not up"; exit 1; }
done
echo "cluster up"

declare -a REPLAYER_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.seqeron.replayer.server.ReplayerServer > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
done

sleep 2  # let tenure-1 LeadershipChanged replicate to followers
L1=$(grep -h "isLeader=true" "$LOG_DIR"/seq-*.log | grep -oE 'SequencerService/[0-9]+' | head -1 | cut -d/ -f2)
echo "leader = member $L1"

# The leader first, so the kill takes away the member the relay is reading.
ENDPOINTS="localhost:$(archive_port "$L1")"
for m in 0 1 2; do
  [[ "$m" == "$L1" ]] || ENDPOINTS+=",localhost:$(archive_port "$m")"
done
HOST_LOG="$LOG_DIR/gateway-host.log"
java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$NODE" -Dreplayer.baseDir="$BASE_DIR" \
     -Dreplayer.archiveEndpoints="$ENDPOINTS" -cp "$JAR" \
     org.limitless.seqeron.replayer.server.ReplayerServer > "$HOST_LOG" 2>&1 &
HOST_PID=$!
wait_for_log "$HOST_LOG" "ready —" 30 || {
  echo "gateway host not serving"; cat "$HOST_LOG"
  kill "$HOST_PID" "${REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}" 2>/dev/null; exit 1
}
echo "gateway host (node $NODE) serving, member archives $ENDPOINTS"

java "${JAVA_OPTS[@]}" -Dprobe.memberId="$NODE" -Dprobe.clientId=11 -Dprobe.count="${CONFIRM_COUNT:-20000}" \
     -Dprobe.pacingMicros="${CONFIRM_PACING_MICROS:-100}" -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe confirm > "$LOG_DIR/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "$LOG_DIR/confirm.log" "confirm: sending" 30 || echo "producer never started"
echo "confirm producer streaming on node $NODE"
sleep 1  # mid-stream when the leader dies

kill "${SEQ_PIDS[$L1]}" 2>/dev/null
echo "killed member $L1: the leader, and the relay's source"

wait "$CONFIRM_PID"; CONFIRM_RC=$?

SWITCHED=0
grep -q "left member archive localhost:$(archive_port "$L1")" "$HOST_LOG" &&
  [[ $(grep -c "following member archive" "$HOST_LOG") -ge 2 ]] && SWITCHED=1

kill "$HOST_PID" 2>/dev/null; wait "$HOST_PID" 2>/dev/null
RESTART_LOG="$LOG_DIR/gateway-host-restart.log"
java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$NODE" -Dreplayer.baseDir="$BASE_DIR" \
     -Dreplayer.archiveEndpoints="$ENDPOINTS" -cp "$JAR" \
     org.limitless.seqeron.replayer.server.ReplayerServer > "$RESTART_LOG" 2>&1 &
HOST_PID=$!
wait_for_log "$RESTART_LOG" "ready —" 30 || echo "restarted gateway host not serving"
echo "gateway host restarted: $(grep -o '[0-9]*-recording chain' "$RESTART_LOG")"

FRESH_LOG="$LOG_DIR/fresh-follower.log"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$NODE" -Dprobe.clientId=9 -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe follow > "$FRESH_LOG" 2>&1 &
FRESH_PID=$!
CAUGHT=0; wait_for_log "$FRESH_LOG" "following live" 30 && CAUGHT=1

echo ""
echo "=== RESULT ==="
echo "--- relay ---"
grep -E "following member archive|left member archive|FATAL" "$HOST_LOG" || echo "(nothing)"
echo "producer on node $NODE       : $(grep -h 'confirm:' "$LOG_DIR/confirm.log" | tail -1)"
echo "relay moved to another member : $SWITCHED"
echo "restarted host's chain        : $(grep -o '[0-9]*-recording chain' "$RESTART_LOG")"
echo "fresh follower caught up      : $CAUGHT"

kill "$FRESH_PID" "$HOST_PID" "${REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}" 2>/dev/null; wait 2>/dev/null
if [[ "$CONFIRM_RC" == "0" && "$SWITCHED" == "1" && "$CAUGHT" == "1" ]]; then
  echo "GATEWAY-HOST TEST: PASS"; exit 0
else
  echo "GATEWAY-HOST TEST: FAIL"; exit 1
fi
