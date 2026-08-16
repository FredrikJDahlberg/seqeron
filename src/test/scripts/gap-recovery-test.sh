#!/usr/bin/env bash
# Steady-state gap-recovery test (ReplayerStreamReceiver globalSeqNo gap -> resume).
#
# Verifies that a consumer which is CAUGHT UP (following live off the co-located SequencerService tap)
# heals a dropped live tap frame: it detects the globalSeqNo gap, resumes the recording at the frame it
# last dispatched, and keeps delivering rather than wedging in-order delivery. Each
# node records its own node-local tap continuously, so the consumer's co-located member holds ONE
# continuous recording; the resume heals the gap straight from that recording (served by the node's
# ReplayerService). A leader failover is performed first so the gap is exercised in a realistic post-failover
# steady state (and to confirm the consumer's own tap keeps flowing across the failover — its member's
# recording is continuous, never rotated).
#
# The gap is synthesized on the CONSUMER side: the ReplayerStreamReceiver drops the next live tap frame when
# armed via SIGUSR1 (gated by PHIXERON_FAULT_INJECTION=1 at consumer launch). With apps reading the tap
# directly there is no fan-out relay to drop a frame in, so the drop lives where the app reads live.
#
# Topology keeps member 0 alive throughout: the flood's ClusterStreamSender (and the consumer's)
# bootstrap through member 0's fixed ingress endpoint (localhost:9302) before following REDIRECT to the
# current leader, and the consumer lives on member 0 (co-located, stable ingress). To guarantee the
# killed leader is NOT member 0, members 1 and 2 are started first so one of THEM wins the initial
# election (2 of 3 is a quorum); member 0 then joins as a follower and is never killed.
#
# Sequence:
#   1. Start members 1 and 2 -> one becomes the tenure-1 leader. Then start member 0 (follower) and a
#      ReplayerServer per member.
#   2. Consumer (OrderExecServer, PHIXERON_FAULT_INJECTION=1) on member 0 catches up (following the
#      live tap).
#   3. Kill the tenure-1 leader -> a survivor becomes the tenure-2 leader. Member 0's own tap recording
#      keeps flowing across the failover (it is continuous, never rotated).
#   4. SIGUSR1 the consumer to arm a one-frame live-tap drop, then flood direct cluster ingress. The
#      first flooded frame is dropped by the consumer -> it sees a globalSeqNo gap and resumes member
#      0's continuous recording (via its ReplayerService) at the hole to heal it.
#   5. SIGTERM the consumer to flush its delivery-latency report.
#
# PASS iff the consumer (a) logged a tap gap (the drop took effect and recovery engaged) with NO fallback
# to a chain re-walk, AND (b) kept delivering — its post-catch-up sample count n >= DELIVER_THRESHOLD.
# A wedge freezes n at a handful; a heal tracks the whole flood, so the two are far apart.
#
# A second scenario ("larger deliberate gaps") is folded into this same script via an env var, rather
# than duplicating the whole cluster bootstrap above:
#   GAP_SIZE=<n>          drop n consecutive live tap frames per arm instead of 1 — sent as
#                         PHIXERON_FAULT_DROP_COUNT, fixed at consumer startup: standard POSIX signals
#                         are not queued, so sending SIGUSR1 n times would not reliably accumulate to n
#                         — see OrderExecServer.cpp.
#
# A third scenario ("gap discovered mid-replay" — re-arm a second drop while the first walk is still
# actively replaying) was attempted and abandoned: see doc/todo.md's 2026-08-02 note. Local Aeron IPC
# replay of a small gap completes too fast (likely sub-millisecond) for a bash-level poll-then-signal
# loop to reliably land inside that window — every attempt measured zero genuine overlaps. The state
# transition itself (a non-contiguous tap frame arriving while `m_replaySessionId >= 0`, not just
# `m_awaitingReplay`) is instead covered deterministically by
# `ReplayerStreamReceiverGapRecovery.TapFrameAheadOfAnInFlightWalkDoesNotSupersedeIt` in
# `ReplayerStreamReceiverTest.cpp` — as of 2026-08-05 such a frame must NOT supersede the in-flight
# walk, since the tap is now dispatched mid-walk and legitimately runs ahead of the replay.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"

BUILD_DIR="cmake-build-release"
JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/gap-recovery"
FLOOD_ORDERS=300
DELIVER_THRESHOLD=120
CN=0            # consumer's member — member 0, never killed, stable local ingress
GAP_SIZE="${GAP_SIZE:-1}"  # frames dropped per arm (keep comfortably below FLOOD_ORDERS)

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMPDIR:-/tmp}phixeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"
AERON_DIR="${TMPDIR}aeron-$(whoami)"

if command -v aeronmd >/dev/null 2>&1; then AERONMD="$(command -v aeronmd)"; else AERONMD="${BUILD_DIR}/_deps/aeron-build/binaries/aeronmd"; fi

start_seq() {  # start_seq <memberId>
  local m="$1"
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
}

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f OrderExecServer 2>/dev/null
pkill -f FixGateway 2>/dev/null; pkill -f fix_test_server 2>/dev/null; pkill -f aeronmd 2>/dev/null; sleep 1
rm -rf "$BASE_DIR" "${TMPDIR}phixeron-seq-aeron-0" "${TMPDIR}phixeron-seq-aeron-1" \
       "${TMPDIR}phixeron-seq-aeron-2" "$AERON_DIR" 2>/dev/null

CONSUMER_PID=""; MD_PID=""
declare -a SEQ_PIDS REPLAYER_PIDS
cleanup() {
  kill "$CONSUMER_PID" "${REPLAYER_PIDS[@]:-}" "$MD_PID" "${SEQ_PIDS[@]:-}" 2>/dev/null
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
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.server.ReplayerServer > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
done
for m in 0 1 2; do
  W=0; until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done
done
echo "replayers serving"
sleep 2

# ── 2. Consumer catches up BEFORE the failover (following the live tap) ────────
# PHIXERON_FAULT_INJECTION=1 installs the consumer's SIGUSR1 handler and enables the ReplayerStreamReceiver's
# live-tap drop; without it a stray SIGUSR1 would kill the process (default action).
CONSUMER_LOG="$LOG_DIR/consumer.log"
PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${CN}" \
  PHIXERON_NODE_MEMBER_ID="$CN" \
  PHIXERON_REPLAYER_CLIENT_ID=9 \
  PHIXERON_CLUSTER_EGRESS_ENDPOINT="localhost:${TEST_CONSUMER_EGRESS_PORT}" \
  PHIXERON_LATENCY_STATS=1 \
  PHIXERON_FAULT_INJECTION=1 \
  PHIXERON_FAULT_DROP_COUNT="$GAP_SIZE" \
  stdbuf -oL -eL "$BUILD_DIR/OrderExecServer" > "$CONSUMER_LOG" 2>&1 &
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

# ── 4. Arm a live-tap drop on the consumer, then flood ingress ────────────────
kill -USR1 "$CONSUMER_PID" 2>/dev/null
echo "armed a $GAP_SIZE-frame live-tap drop on the consumer (SIGUSR1, PHIXERON_FAULT_DROP_COUNT=$GAP_SIZE)"
sleep 0.5
echo "flooding $FLOOD_ORDERS messages to cluster ingress (first $GAP_SIZE live tap frame(s) will be dropped)"
PHIXERON_FLOOD_ORDERS="$FLOOD_ORDERS" stdbuf -oL -eL "$BUILD_DIR/fix_test_server" 127.0.0.1 9000 \
  > "$LOG_DIR/flood.log" 2>&1 || true
sleep 8  # let the consumer resume, heal, and drain the flood tail

# ── 5. Flush the consumer's delivery-latency report ───────────────────────────
# SIGTERM the consumer and wait for it to actually EXIT (flushing its report) before the trap tears
# down the cluster — otherwise the teardown can orphan/kill it mid-shutdown and lose the report.
kill -TERM "$CONSUMER_PID" 2>/dev/null
for _ in $(seq 1 40); do kill -0 "$CONSUMER_PID" 2>/dev/null || break; sleep 0.5; done
CONSUMER_PID=""  # exited (or gave up waiting); don't re-kill in cleanup

# ── Assertions ────────────────────────────────────────────────────────────────
RECOVERIES=$(grep -c "tap gap: expected globalSeqNo" "$CONSUMER_LOG" 2>/dev/null)
# The gap must have been repaired by a RESUME, not by a chain re-walk: a re-walk here replays the whole
# recording to close a one-frame hole, and only appears as a fallback (the recording rotated under the
# position the resume anchored on) — which cannot happen in this scenario, whose member records
# continuously across the failover.
REWALK=$(grep -c "re-walking the recording chain" "$CONSUMER_LOG" 2>/dev/null)
DELIVERED=$(grep -oE "n=[0-9]+" "$CONSUMER_LOG" 2>/dev/null | head -1 | cut -d= -f2)
DELIVERED=${DELIVERED:-0}

echo ""
echo "=== RESULT ==="
echo "  gap size (frames dropped per arm)       : $GAP_SIZE"
echo "  gap recoveries triggered on consumer    : $RECOVERIES"
echo "  of which fell back to a chain re-walk   : $REWALK  (expected 0)"
echo "  post-catch-up frames delivered (n)      : $DELIVERED  (threshold $DELIVER_THRESHOLD)"
grep -E "tap gap|re-walking|delivery latency" "$CONSUMER_LOG" 2>/dev/null | tail -6 | sed 's/^/    /'

if [[ "$RECOVERIES" -ge 1 && "$REWALK" -eq 0 && "$DELIVERED" -ge "$DELIVER_THRESHOLD" ]]; then
  echo "GAP-RECOVERY TEST: PASS — consumer resumed its continuous recording at the hole and kept delivering"
  exit 0
else
  echo "GAP-RECOVERY TEST: FAIL — consumer did not heal (recoveries=$RECOVERIES, re-walk=$REWALK, delivered=$DELIVERED)"
  exit 1
fi
