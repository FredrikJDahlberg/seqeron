#!/usr/bin/env bash
# Cross-failover cold-start replay test.
#
# Forces a leader failover (two leader tenures), then cold-starts a fresh ClusterProbe follower
# co-located with the surviving new leader and verifies it catches up on full history. Since every node records
# its own node-local tap continuously — the tap publication is never re-created on a leadership change
# — the new leader's node holds ONE continuous recording spanning both tenures (pre- and post-failover),
# so the cold-start walk catches up from that single recording. This validates that the tap recording on
# a node that was a follower during tenure 1 still contains tenure-1 history after it becomes leader.
#
# It also proves failover loss recovery end to end: two ClusterProbe confirm producers, co-located with a
# member that survives, stream ProbeMarkers across the kill. The one sending through PendingSends must see
# every frame on its tap exactly once, in order. The control sends untracked; its loss count is what the kill
# had in flight, and is reported rather than asserted, since a kill can land between two frames.
#
# PASS iff the fresh client prints "following live", its ReplayerService served it >= 1 replay segment, and
# the PendingSends producer came out exact.
#
# Java only — no C++ binary is built or launched. The client is the
# edge-neutral probe, which attaches to the member's own media driver, so this script needs no
# standalone aeronmd either.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
LOG_DIR="logs/failover"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}/seqeron-seq-aeron-0" "${TMP_DIR}/seqeron-seq-aeron-1" \
       "${TMP_DIR}/seqeron-seq-aeron-2" 2>/dev/null

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
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/replayer-$m.log" "serving replay" 30 || true
done
echo "replayers serving"

sleep 2  # let tenure-1 LeadershipChanged replicate to followers
L1=$(grep -h "isLeader=true" "$LOG_DIR"/seq-*.log | grep -oE 'SequencerService/[0-9]+' | head -1 | cut -d/ -f2)
echo "tenure-1 leader = member $L1"

P=$(( (L1 + 1) % 3 ))
PRODUCER_OPTS=(-Dprobe.memberId="$P" -Dprobe.count="${CONFIRM_COUNT:-20000}"
               -Dprobe.pacingMicros="${CONFIRM_PACING_MICROS:-100}")
java "${JAVA_OPTS[@]}" "${PRODUCER_OPTS[@]}" -Dprobe.clientId=11 -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe confirm > "$LOG_DIR/confirm.log" 2>&1 &
CONFIRM_PID=$!
java "${JAVA_OPTS[@]}" "${PRODUCER_OPTS[@]}" -Dprobe.clientId=12 -Dprobe.pendingSends=false -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe confirm > "$LOG_DIR/control.log" 2>&1 &
CONTROL_PID=$!
for f in confirm control; do
  wait_for_log "$LOG_DIR/$f.log" "confirm: sending" 30 || echo "$f producer never started"
done
echo "confirm + control producers streaming on member $P"
sleep 1  # both mid-stream when the leader dies

kill "${SEQ_PIDS[$L1]}" 2>/dev/null
echo "killed tenure-1 leader (member $L1) -> forcing failover"

W=0; NEWLEADER=""
until [[ -n "$NEWLEADER" ]]; do
  sleep 0.5; W=$((W+1))
  for m in 0 1 2; do
    [[ "$m" == "$L1" ]] && continue
    grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { NEWLEADER="$m"; break; }
  done
  ((W>120)) && { echo "no new leader emerged"; break; }
done
echo "tenure-2 leader = member $NEWLEADER"
sleep 3  # let the new leader settle (its tap recording is continuous across the failover)

FRESH_LOG="$LOG_DIR/fresh-follower.log"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$NEWLEADER" -Dprobe.clientId=9 -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe follow > "$FRESH_LOG" 2>&1 &
FRESH_PID=$!
echo "started fresh cold probe follower (client 9) co-located with new leader member $NEWLEADER"

CAUGHT=0; wait_for_log "$FRESH_LOG" "following live" 30 && CAUGHT=1

# Each producer judges itself once its stream has drained; its own drain timeout bounds the wait.
wait "$CONFIRM_PID"; CONFIRM_RC=$?
wait "$CONTROL_PID"; CONTROL_RC=$?

echo ""
echo "=== RESULT ==="
echo "--- Replayer (member $NEWLEADER) replay decisions for fresh client 9 ---"
grep "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log" || echo "(none)"
SEGMENTS=$(grep -c "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log")
echo "segments served to client 9 : $SEGMENTS"
echo "fresh client caught up      : $CAUGHT"
echo "PendingSends producer       : $(grep -h 'confirm:' "$LOG_DIR/confirm.log" | tail -1)"
echo "untracked control           : $(grep -h 'confirm:' "$LOG_DIR/control.log" | tail -1)"
((CONTROL_RC != 0)) || echo "  (the control lost nothing: the kill had no frame in flight, so this run proved no recovery)"

kill "$FRESH_PID" "${REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}" 2>/dev/null; wait 2>/dev/null
if [[ "$CAUGHT" == "1" && "$SEGMENTS" -ge 1 && "$CONFIRM_RC" == "0" ]]; then
  echo "CROSS-FAILOVER TEST: PASS"; exit 0
else
  echo "CROSS-FAILOVER TEST: FAIL"; exit 1
fi
