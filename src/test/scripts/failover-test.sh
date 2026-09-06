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
# PASS iff the fresh client prints "following live" AND its ReplayerService served it >= 1 replay segment.
#
# Java only — no C++ binary is built or launched (doc/future-arch.md §11 step 5). The client is the
# edge-neutral probe, which attaches to the member's own media driver, so this script needs no
# standalone aeronmd either.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"

JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/failover"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMP_DIR}/phixeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}/phixeron-seq-aeron-0" "${TMP_DIR}/phixeron-seq-aeron-1" \
       "${TMP_DIR}/phixeron-seq-aeron-2" 2>/dev/null

declare -a SEQ_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
for m in 0 1 2; do
  W=0; until grep -q "Running" "$LOG_DIR/seq-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "seq $m not up"; exit 1; }; done
done
echo "cluster up"

declare -a REPLAYER_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.server.ReplayerServer > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
done
for m in 0 1 2; do
  W=0; until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done
done
echo "replayers serving"

sleep 2  # let tenure-1 LeadershipChanged replicate to followers
L1=$(grep -h "isLeader=true" "$LOG_DIR"/seq-*.log | grep -oE 'SequencerService/[0-9]+' | head -1 | cut -d/ -f2)
echo "tenure-1 leader = member $L1"

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
     org.limitless.phixeron.tools.ClusterProbe follow > "$FRESH_LOG" 2>&1 &
FRESH_PID=$!
echo "started fresh cold probe follower (client 9) co-located with new leader member $NEWLEADER"

W=0; until grep -q "following live" "$FRESH_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done
CAUGHT=0; grep -q "following live" "$FRESH_LOG" && CAUGHT=1

echo ""
echo "=== RESULT ==="
echo "--- Replayer (member $NEWLEADER) replay decisions for fresh client 9 ---"
grep "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log" || echo "(none)"
SEGMENTS=$(grep -c "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log")
echo "segments served to client 9 : $SEGMENTS"
echo "fresh client caught up      : $CAUGHT"

kill "$FRESH_PID" "${REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}" 2>/dev/null; wait 2>/dev/null
if [[ "$CAUGHT" == "1" && "$SEGMENTS" -ge 1 ]]; then
  echo "CROSS-FAILOVER TEST: PASS"; exit 0
else
  echo "CROSS-FAILOVER TEST: FAIL"; exit 1
fi
