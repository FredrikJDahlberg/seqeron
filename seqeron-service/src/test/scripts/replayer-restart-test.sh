#!/usr/bin/env bash
# replayer-restart-test.sh — an app running through its own node's restart, and a resume/walk crossing a
# genuine recording rotation (where findings 2, 4 and 5 live). failover-test.sh only cold-starts a FRESH
# client after a leader kill against a node whose own tap recording was never rotated; gap-recovery-test.sh
# deliberately keeps its consumer's own member (0) alive throughout. Neither restarts the consumer's OWN
# node. This script does, against real Aeron/Archive processes — not fabricated frames (that
# precision-testing already lives in ReplayerStreamReceiverTest.cpp's unit suite).
#
# It targets member 0, started last after members 1/2 elect a leader between themselves, so member 0 is
# never the leader and this stays clear of Raft election behaviour — that is failover-test.sh's/
# chaos-runner.sh's job, not this one's.
#
# A backlog is flooded onto cluster ingress and a client (ClusterProbe follow, clientId 9) catches up on
# member 0. Member 0's SequencerServer — its Replayer with it — is then killed. The client shares its
# embedded media driver and must fail fast (die within the driver-loss grace window) rather than spin forever
# against a dead driver — the same invariant chaos-runner.sh's assert_died_on_driver_loss checks, asserted
# here directly rather than as one random outcome among many. Member 0 is restarted: with no snapshots,
# recovery is a full-log replay onto a BRAND NEW tap publication/recording, leaving the pre-restart recording
# as a real, stopped, cold-start-walk segment rather than the current active one. A fresh client (same
# clientId) must then walk that real 2-recording chain — segment 0 the old recording, segment 1 the new one —
# the live boundary case ReplayerService.serveReplay/ReplayRecordings.stitch and the 2026-08-10
# recordingId-echo hardening exist for, exercised here for real rather than via hand-fabricated Replaying
# replies.
#
# Java only — no C++ binary is built or launched: the backlog is
# ClusterProbe submit and the client is ClusterProbe follow, which attaches to the member's own media
# driver, so this script needs no standalone aeronmd either.
#
# PASS iff the first client exits within the driver-loss grace window when its node dies, the node comes
# back, and the fresh client's cold-start walk is served (at least) two segments naming two distinct
# recordingIds.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
LOG_DIR="logs/replayer-restart"
FLOOD_FRAMES=1000
CN=0            # restart target — member 0, never the leader (see header)
CLIENT_ID=9     # matches gap-recovery-test.sh/chaos-runner.sh's dedicated test-consumer clientId/port

NODE_START_TIMEOUT_SECS=30
APP_CATCHUP_TIMEOUT_SECS=30
PROC_EXIT_TIMEOUT_SECS=10
DRIVER_LOSS_GRACE_SECS=15   # must exceed Aeron's DEFAULT_MEDIA_DRIVER_TIMEOUT_MS (10s)

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

start_seq() {  # start_seq <memberId> <logfile>
  local m="$1" log="$2"
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$log" 2>&1 &
  SEQ_PIDS[$m]=$!
}
start_client() {  # start_client <logfile>
  local log="$1"
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -Dprobe.clientId="$CLIENT_ID" -cp "$JAR" \
       org.limitless.seqeron.tools.ClusterProbe follow > "$log" 2>&1 &
  CLIENT_PID=$!
}
wait_for() {  # wait_for <pattern> <logfile> <timeout_iters (x0.5s)> <description>
  wait_for_log "$2" "$1" $(( $3 / 2 )) || { echo "TIMEOUT waiting for $4 (see $2)"; return 1; }
}
wait_for_exit() {  # wait_for_exit <pid> <timeout_iters (x0.5s)>
  local pid="$1" timeout="$2" W=0
  while kill -0 "$pid" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>timeout)) && return 1; done
  return 0
}

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}/seqeron-seq-aeron-0" "${TMP_DIR}/seqeron-seq-aeron-1" \
       "${TMP_DIR}/seqeron-seq-aeron-2" 2>/dev/null

CLIENT_PID=""
declare -a SEQ_PIDS
cleanup() {
  kill "${CLIENT_PID:-0}" "${SEQ_PIDS[@]:-}" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── Bring-up: members 1 & 2 first -> leader is one of them; member 0 (our target) joins as a follower ──
start_seq 1 "$LOG_DIR/seq-1.log"; start_seq 2 "$LOG_DIR/seq-2.log"
for m in 1 2; do wait_for "Running" "$LOG_DIR/seq-$m.log" 120 "seq $m up" || exit 1; done
W=0; LEADER=""
until [[ -n "$LEADER" ]]; do
  sleep 0.5; W=$((W+1))
  for m in 1 2; do grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { LEADER="$m"; break; }; done
  ((W>120)) && { echo "no tenure-1 leader among members 1,2"; exit 1; }
done
start_seq 0 "$LOG_DIR/seq-0.log"
wait_for "Running" "$LOG_DIR/seq-0.log" 120 "seq 0 up" || exit 1
echo "cluster up; tenure-1 leader = member $LEADER ; restart target = member $CN (never the leader)"

for m in 0 1 2; do wait_for "serving replay" "$LOG_DIR/seq-$m.log" 120 "replayer $m serving" || exit 1; done
echo "replayers serving"
sleep 2

PASS=0

echo "flooding $FLOOD_FRAMES frames onto cluster ingress to give the cold-start walks real work"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -Dprobe.count="$FLOOD_FRAMES" -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe submit > "$LOG_DIR/flood.log" 2>&1 || true
sleep 1   # let the flood replicate before the client's cold walk starts racing it

start_client "$LOG_DIR/client-before.log"
wait_for "following live" "$LOG_DIR/client-before.log" $((APP_CATCHUP_TIMEOUT_SECS * 2)) \
  "client $CLIENT_ID to catch up on member $CN" || exit 1
echo "client $CLIENT_ID caught up on member $CN"

echo "killing member $CN's SequencerServer (its co-located client must fail fast)"
kill "${SEQ_PIDS[$CN]}" 2>/dev/null
SEQ_DIED=0; CLIENT_DIED=0
wait_for_exit "${SEQ_PIDS[$CN]}" $((PROC_EXIT_TIMEOUT_SECS * 2)) && SEQ_DIED=1
wait_for_exit "$CLIENT_PID" $((DRIVER_LOSS_GRACE_SECS * 2)) && CLIENT_DIED=1
echo "  member $CN SequencerServer exited : $SEQ_DIED"
echo "  co-located client failed fast   : $CLIENT_DIED"

if [[ "$SEQ_DIED" != "1" || "$CLIENT_DIED" != "1" ]]; then
  echo "FAIL: a co-located process outlived its node's media driver (should fail fast, not spin)"
else
  RESTART_LOG="$LOG_DIR/seq-$CN-restarted.log"
  start_seq "$CN" "$RESTART_LOG"
  if wait_for "serving replay" "$RESTART_LOG" $((NODE_START_TIMEOUT_SECS * 2)) "member $CN restart"; then
    sleep 2   # let the restored member finish its own full-log replay and settle
    CLIENT_LOG="$LOG_DIR/client-after.log"
    start_client "$CLIENT_LOG"
    echo "started fresh client $CLIENT_ID (cold start) after member $CN's own restart"
    if wait_for "following live" "$CLIENT_LOG" $((APP_CATCHUP_TIMEOUT_SECS * 2)) \
         "fresh client $CLIENT_ID to catch up across the rotated chain"; then
      SEGMENTS=$(grep -c "replay for client $CLIENT_ID: segment" "$RESTART_LOG")
      RECORDINGS=$(grep "replay for client $CLIENT_ID: segment" "$RESTART_LOG" \
        | grep -oE "recording [0-9]+" | sort -u | wc -l | tr -d ' ')
      echo "  segments served to client $CLIENT_ID : $SEGMENTS"
      echo "  distinct recordingIds among them    : $RECORDINGS"
      if [[ "$SEGMENTS" -ge 2 && "$RECORDINGS" -ge 2 ]]; then
        echo "fresh client $CLIENT_ID walked a genuine multi-recording chain and converged"
        PASS=1
      else
        echo "FAIL: walk did not cross a real 2-recording chain (segments=$SEGMENTS recordings=$RECORDINGS)"
      fi
    else
      echo "FAIL: fresh client $CLIENT_ID never caught up"
    fi
  else
    echo "FAIL: member $CN never came back up"
  fi
fi

echo ""
if [[ "$PASS" == "1" ]]; then
  echo "REPLAYER-RESTART TEST: PASS"; exit 0
else
  echo "REPLAYER-RESTART TEST: FAIL"; exit 1
fi
