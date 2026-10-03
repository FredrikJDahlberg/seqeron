#!/usr/bin/env bash
# Paused-subscriber test: a tap subscriber that stops polling must not take its member down.
#
# A co-located app subscribes to the tap untethered. Until its driver evicts it, a stopped one still holds
# the tap's window, and a full window behind it back-pressures the sequencer, which terminates after 1 s of
# back-pressure with no recording progress (exit 70). seqeron's drivers evict well inside that second
# (doc/ops.md, "Untethered subscribers"); at Aeron's own defaults the eviction takes ~10 s and this fails.
#
# Sequence:
#   1. A three-node cluster; a ClusterProbe follow on member 0 catches up.
#   2. SIGSTOP the follower, then flood more than two tap windows of frames through member 1.
#   3. Hold for PAUSE_SECS, under the driver's 10 s client liveness timeout, so the follower's own Aeron
#      client survives the pause.
#   4. SIGCONT the follower, wait out the 10 s an evicted subscriber rests before it may rejoin the tap, and
#      send a few frames more.
#
# PASS iff every member is still up, member 0 logged no tap-stall fatal, and the follower healed the hole
# its eviction left and re-converged.
#
# Java only, on the members' embedded drivers.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
LOG_DIR="logs/paused-subscriber"
FLOOD_FRAMES=2000
FLOOD_FILLER_BYTES=8800   # ~17.6 MB: more than two 8 MiB windows of the default 16 MiB IPC term
PAUSE_SECS="${PAUSE_SECS:-4}"
SN=0   # the follower's member

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqps"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}"/seqeron-seq-aeron-{0,1,2} 2>/dev/null

FOLLOWER_PID=""
declare -a SEQ_PIDS
cleanup() {
  [[ -n "$FOLLOWER_PID" ]] && kill -CONT "$FOLLOWER_PID" 2>/dev/null
  kill "$FOLLOWER_PID" "${SEQ_PIDS[@]:-}" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "Running" 30 || { echo "seq $m not up"; exit 1; }
  wait_for_log "$LOG_DIR/seq-$m.log" "serving replay" 30 || true
done
echo "cluster up"

FOLLOWER_LOG="$LOG_DIR/follower.log"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$SN" -Dprobe.clientId=9 -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe follow > "$FOLLOWER_LOG" 2>&1 &
FOLLOWER_PID=$!
wait_for_log "$FOLLOWER_LOG" "following live" 30 || { echo "follower never caught up"; exit 1; }

kill -STOP "$FOLLOWER_PID"
echo "follower on member $SN stopped; flooding $FLOOD_FRAMES frames of $FLOOD_FILLER_BYTES bytes of filler"
java "${JAVA_OPTS[@]}" -Dprobe.memberId=1 -Dprobe.count="$FLOOD_FRAMES" -Dprobe.fillerBytes="$FLOOD_FILLER_BYTES" \
     -cp "$JAR" org.limitless.seqeron.tools.ClusterProbe submit > "$LOG_DIR/flood.log" 2>&1 || true
sleep "$PAUSE_SECS"

kill -CONT "$FOLLOWER_PID"
echo "follower resumed; waiting out the eviction's resting timeout"
sleep 12
java "${JAVA_OPTS[@]}" -Dprobe.memberId=1 -Dprobe.count=100 -Dprobe.pacingMicros=10000 -cp "$JAR" \
     org.limitless.seqeron.tools.ClusterProbe submit > "$LOG_DIR/after.log" 2>&1 || true
sleep 3

# ── Assertions ────────────────────────────────────────────────────────────────
DOWN=""
for m in 0 1 2; do kill -0 "${SEQ_PIDS[$m]}" 2>/dev/null || DOWN+=" $m"; done
FATAL=$(grep -c "FATAL: the tap recording made no progress" "$LOG_DIR/seq-$SN.log" 2>/dev/null)
HEALED=$(grep -c "re-converged at globalSeqNo" "$FOLLOWER_LOG" 2>/dev/null)

echo ""
echo "=== RESULT ==="
echo "  members down                        : ${DOWN:- none}"
echo "  tap-stall fatals on member $SN        : $FATAL  (expected 0)"
echo "  follower re-convergences            : $HEALED  (expected at least 1)"
grep -E "tap gap|re-converged" "$FOLLOWER_LOG" 2>/dev/null | tail -4 | sed 's/^/    /'

if [[ -z "$DOWN" && "$FATAL" -eq 0 && "$HEALED" -ge 1 ]]; then
  echo "PAUSED-SUBSCRIBER TEST: PASS — the stopped follower was evicted and healed; no member went down"
  exit 0
else
  echo "PAUSED-SUBSCRIBER TEST: FAIL — down:${DOWN:- none}, fatals=$FATAL, re-convergences=$HEALED"
  exit 1
fi
