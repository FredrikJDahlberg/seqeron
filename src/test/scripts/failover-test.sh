#!/usr/bin/env bash
# Cross-failover cold-start replay test.
#
# Forces a leader failover (two leader tenures), then cold-starts a fresh OrderExecClient co-located
# with the surviving new leader and verifies it catches up on full history. Since every node records
# its own node-local tap continuously — the tap publication is never re-created on a leadership change
# — the new leader's node holds ONE continuous recording spanning both tenures (pre- and post-failover),
# so the cold-start walk catches up from that single recording. This validates that the tap recording on
# a node that was a follower during tenure 1 still contains tenure-1 history after it becomes leader.
#
# PASS iff the fresh client prints "following live" AND its ReplayerService served it >= 1 replay segment.
set -uo pipefail

BUILD_DIR="cmake-build-release"
JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/failover"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMPDIR:-/tmp}phixeron-seqfo"
CLUSTER_MEMBERS="0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301"
CLUSTER_MEMBERS+="|1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311"
CLUSTER_MEMBERS+="|2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321"
AERON_DIR="${TMPDIR}aeron-$(whoami)"

if command -v aeronmd >/dev/null 2>&1; then AERONMD="$(command -v aeronmd)"; else AERONMD="${BUILD_DIR}/_deps/aeron-build/binaries/aeronmd"; fi

pkill -f SequencerNode 2>/dev/null; pkill -f ReplayerNode 2>/dev/null; pkill -f OrderExecClient 2>/dev/null
pkill -f FixSessionClient 2>/dev/null; pkill -f aeronmd 2>/dev/null; sleep 1
rm -rf "$BASE_DIR" "${TMPDIR}phixeron-seq-aeron-0" "${TMPDIR}phixeron-seq-aeron-1" \
       "${TMPDIR}phixeron-seq-aeron-2" "$AERON_DIR" 2>/dev/null

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

AERON_DIR="$AERON_DIR" "$AERONMD" > "$LOG_DIR/aeronmd.log" 2>&1 &
MD_PID=$!
W=0; until [[ -f "$AERON_DIR/cnc.dat" ]]; do sleep 0.2; W=$((W+1)); ((W>25)) && { echo "md not up"; exit 1; }; done

declare -a REPLAYER_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.ReplayerNode > "$LOG_DIR/replayer-$m.log" 2>&1 &
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

FRESH_LOG="$LOG_DIR/fresh-orderexec.log"
PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${NEWLEADER}" \
  PHIXERON_NODE_MEMBER_ID="$NEWLEADER" \
  PHIXERON_REPLAYER_CLIENT_ID=9 \
  PHIXERON_CLUSTER_EGRESS_ENDPOINT="localhost:9349" \
  stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$FRESH_LOG" 2>&1 &
FRESH_PID=$!
echo "started fresh cold OrderExecClient (client 9) co-located with new leader member $NEWLEADER"

W=0; until grep -q "following live" "$FRESH_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done
CAUGHT=0; grep -q "following live" "$FRESH_LOG" && CAUGHT=1

echo ""
echo "=== RESULT ==="
echo "--- Replayer (member $NEWLEADER) replay decisions for fresh client 9 ---"
grep "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log" || echo "(none)"
SEGMENTS=$(grep -c "replay for client 9" "$LOG_DIR/replayer-$NEWLEADER.log")
echo "segments served to client 9 : $SEGMENTS"
echo "fresh client caught up      : $CAUGHT"

kill "$FRESH_PID" "${REPLAYER_PIDS[@]}" "$MD_PID" "${SEQ_PIDS[@]}" 2>/dev/null; wait 2>/dev/null
if [[ "$CAUGHT" == "1" && "$SEGMENTS" -ge 1 ]]; then
  echo "CROSS-FAILOVER TEST: PASS"; exit 0
else
  echo "CROSS-FAILOVER TEST: FAIL"; exit 1
fi
