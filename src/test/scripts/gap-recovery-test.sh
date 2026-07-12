#!/usr/bin/env bash
# Steady-state gap-recovery test (RouterClient globalSeqNo re-walk).
#
# Verifies that a consumer which is CAUGHT UP (following live via the Router fan-out) heals a dropped
# fan-out frame: it detects the globalSeqNo gap, re-walks the recording chain from segment 0 de-duping
# by globalSeqNo, and keeps delivering rather than wedging in-order delivery. Each node records its own
# node-local tap continuously, so the consumer's co-located member holds ONE continuous recording; the
# re-walk heals the gap straight from that recording. A leader failover is performed first so the gap is
# exercised in a realistic post-failover steady state (and to confirm the consumer's own tap keeps
# flowing across the failover — its member's recording is continuous, never rotated).
#
# Topology keeps member 0 alive throughout: the flood's ClusterIngressSender (and the consumer's)
# bootstrap through member 0's fixed ingress endpoint (localhost:9302) before following REDIRECT to the
# current leader, and the consumer lives on member 0 (co-located, stable ingress). To guarantee the
# killed leader is NOT member 0, members 1 and 2 are started first so one of THEM wins the initial
# election (2 of 3 is a quorum); member 0 then joins as a follower and is never killed.
#
# Sequence:
#   1. Start members 1 and 2 -> one becomes the tenure-1 leader. Then start member 0 (follower) and a
#      RouterNode per member with -Drouter.faultInjection.
#   2. Consumer (OrderExecClient) on member 0 catches up (following live via the Router fan-out).
#   3. Kill the tenure-1 leader -> a survivor becomes the tenure-2 leader. Member 0's own tap recording
#      keeps flowing across the failover (it is continuous, never rotated).
#   4. Arm a one-frame fan-out drop on member 0's Router (SIGUSR1), then flood direct cluster ingress.
#      The first flooded frame is dropped -> the caught-up consumer sees a globalSeqNo gap and re-walks
#      member 0's continuous recording from segment 0 to heal it.
#   5. SIGTERM the consumer to flush its delivery-latency report.
#
# PASS iff the consumer (a) logged "re-walking the recording chain" (the drop took effect and recovery
# engaged) AND (b) kept delivering — its post-catch-up sample count n >= DELIVER_THRESHOLD (healed).
# A wedge freezes n at a handful; a heal tracks the whole flood, so the two are far apart.
set -uo pipefail

BUILD_DIR="cmake-build-release"
JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/gap-recovery"
FLOOD_ORDERS=300
DELIVER_THRESHOLD=120
CN=0            # consumer's member — member 0, never killed, stable local ingress

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

start_seq() {  # start_seq <memberId>
  local m="$1"
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
}

pkill -f SequencerNode 2>/dev/null; pkill -f RouterNode 2>/dev/null; pkill -f OrderExecClient 2>/dev/null
pkill -f FixSessionClient 2>/dev/null; pkill -f fix_test_server 2>/dev/null; pkill -f aeronmd 2>/dev/null; sleep 1
rm -rf "$BASE_DIR" "${TMPDIR}phixeron-seq-aeron-0" "${TMPDIR}phixeron-seq-aeron-1" \
       "${TMPDIR}phixeron-seq-aeron-2" "$AERON_DIR" 2>/dev/null

CONSUMER_PID=""; MD_PID=""
declare -a SEQ_PIDS ROUTER_PIDS
cleanup() {
  kill "$CONSUMER_PID" "${ROUTER_PIDS[@]}" "$MD_PID" "${SEQ_PIDS[@]}" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── 1. Members 1 & 2 first -> leader is one of them; then member 0 (follower) ──
start_seq 1; start_seq 2
for m in 1 2; do
  W=0; until grep -q "Running" "$LOG_DIR/seq-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "seq $m not up"; exit 1; }; done
done
W=0; LEADER=""
until [[ -n "$LEADER" ]]; do
  sleep 0.5; W=$((W+1))
  for m in 1 2; do grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { LEADER="$m"; break; }; done
  ((W>120)) && { echo "no tenure-1 leader among members 1,2"; exit 1; }
done
OTHER=$([[ "$LEADER" == "1" ]] && echo 2 || echo 1)   # the surviving non-0 member
start_seq 0
W=0; until grep -q "Running" "$LOG_DIR/seq-0.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "seq 0 not up"; exit 1; }; done
echo "cluster up; tenure-1 leader = member $LEADER ; consumer co-located with member $CN"

AERON_DIR="$AERON_DIR" "$AERONMD" > "$LOG_DIR/aeronmd.log" 2>&1 &
MD_PID=$!
W=0; until [[ -f "$AERON_DIR/cnc.dat" ]]; do sleep 0.2; W=$((W+1)); ((W>25)) && { echo "md not up"; exit 1; }; done

for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Drouter.memberId="$m" -Drouter.faultInjection=true -cp "$JAR" \
       org.limitless.phixeron.router.RouterNode > "$LOG_DIR/router-$m.log" 2>&1 &
  ROUTER_PIDS[$m]=$!
done
for m in 0 1 2; do
  W=0; until grep -q "IPC tap live" "$LOG_DIR/router-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done
done
echo "routers up (fault injection enabled)"
sleep 2

# ── 2. Consumer catches up BEFORE the failover (following live via the fan-out) ──
CONSUMER_LOG="$LOG_DIR/consumer.log"
PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${CN}" \
  PHIXERON_NODE_MEMBER_ID="$CN" \
  PHIXERON_ROUTER_CLIENT_ID=9 \
  PHIXERON_CLUSTER_EGRESS_ENDPOINT="localhost:9349" \
  PHIXERON_LATENCY_STATS=1 \
  stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "consumer never caught up"; exit 1; }; done
echo "consumer caught up (following live) on tenure 1"

# ── 3. Force the failover -> recording 2 becomes the active recording ─────────
kill "${SEQ_PIDS[$LEADER]}" 2>/dev/null
echo "killed tenure-1 leader (member $LEADER) -> forcing failover (member 0 stays alive)"
W=0; NEWLEADER=""
until [[ -n "$NEWLEADER" ]]; do
  sleep 0.5; W=$((W+1))
  for m in "$CN" "$OTHER"; do grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { NEWLEADER="$m"; break; }; done
  ((W>120)) && { echo "no new leader emerged"; exit 1; }
done
echo "tenure-2 leader = member $NEWLEADER (member 0's tap recording is continuous across the failover)"
sleep 3  # let tenure-2 recording start and settle

# ── 4. Arm a fan-out drop on the consumer's Router, then flood ingress ────────
kill -USR1 "${ROUTER_PIDS[$CN]}" 2>/dev/null
echo "armed a fan-out drop on member $CN's Router (SIGUSR1)"
sleep 0.5
echo "flooding $FLOOD_ORDERS messages to cluster ingress (first fan-out frame will be dropped)"
PHIXERON_FLOOD_ORDERS="$FLOOD_ORDERS" stdbuf -oL -eL "$BUILD_DIR/fix_test_server" 127.0.0.1 9000 \
  > "$LOG_DIR/flood.log" 2>&1 || true
sleep 8  # let the consumer re-walk, heal, and drain the flood tail

# ── 5. Flush the consumer's delivery-latency report ───────────────────────────
# SIGTERM the consumer and wait for it to actually EXIT (flushing its report) before the trap tears
# down the cluster — otherwise the teardown can orphan/kill it mid-shutdown and lose the report.
kill -TERM "$CONSUMER_PID" 2>/dev/null
for _ in $(seq 1 40); do kill -0 "$CONSUMER_PID" 2>/dev/null || break; sleep 0.5; done
CONSUMER_PID=""  # exited (or gave up waiting); don't re-kill in cleanup

# ── Assertions ────────────────────────────────────────────────────────────────
REWALK=$(grep -c "re-walking the recording chain" "$CONSUMER_LOG" 2>/dev/null)
DELIVERED=$(grep -oE "n=[0-9]+" "$CONSUMER_LOG" 2>/dev/null | head -1 | cut -d= -f2)
DELIVERED=${DELIVERED:-0}

echo ""
echo "=== RESULT ==="
echo "  re-walks triggered on consumer        : $REWALK"
echo "  post-catch-up frames delivered (n)    : $DELIVERED  (threshold $DELIVER_THRESHOLD)"
grep -E "fan-out gap|re-walking|delivery latency" "$CONSUMER_LOG" 2>/dev/null | tail -4 | sed 's/^/    /'

if [[ "$REWALK" -ge 1 && "$DELIVERED" -ge "$DELIVER_THRESHOLD" ]]; then
  echo "GAP-RECOVERY TEST: PASS — consumer re-walked its continuous recording and kept delivering"
  exit 0
else
  echo "GAP-RECOVERY TEST: FAIL — consumer did not heal (re-walk=$REWALK, delivered=$DELIVERED)"
  exit 1
fi
