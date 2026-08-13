#!/usr/bin/env bash
# chaos-runner.sh — SKETCH. Randomized fault-injection loop against a live 3-node cluster.
#
# WHAT THIS IS (and is NOT):
#   This script CONDUCTS and CHECKS INVARIANTS. Each round it perturbs the running system with one
#   randomly-chosen fault, lets it heal, then asserts steady-state invariants (a leader exists, the
#   FIX path still round-trips, the consumer is still delivering in order). It is deliberately NOT a
#   latency/throughput measurement tool — that belongs in a purpose-built driver (PHIXERON_LATENCY_STATS
#   already records post-consensus delivery latency; a load harness generates the arrival process). A
#   shell loop cannot make defensible tail-latency claims, so it does not try to. It kills, pauses,
#   drops, and probes — nothing it does produces a number you would put in a non-functional report.
#
# REPRODUCIBILITY: every run prints its SEED. Re-run with SEED=<n> to replay the exact fault sequence
#   (the whole point of chaos testing is a reproducing case, not noise). ROUNDS and STEADY_STATE_SECS
#   are env-overridable.
#
# TOPOLOGY (borrowed from gap-recovery-test.sh for a deterministic initial leader): members 1 & 2 start
#   first so the initial leader is one of them; member 0 joins as a follower after. Member 0 (CN) hosts the
#   safety-oracle/tap-drop-target OrderExecClient consumer (PHIXERON_FAULT_INJECTION=1) and the primary FIX
#   gateway GW-A (port 9000); member 1 (STANDBY_MEMBER) additionally hosts the hot-standby gateway GW-B
#   (port 9001), the same active/standby pair gateway-failover-test.sh drives — sharing gatewaySourceId 0
#   per BasicDataConstants.hpp, so a real promotion (not a dead end) happens whichever one dies. All three
#   members are legal fault targets; restart_colocated_apps brings each member's co-located apps (gateway
#   and/or consumer included) back up in place. active_gateway_port tracks which of GW-A/GW-B is currently
#   serving, for the probe and the background load to target. LEADER_CHANGES controls the round count for a
#   dedicated ONLY_FAULT=fault_kill_leader run (every round is a genuine failover, since every member is a
#   legal kill target).
#
# PASS/FAIL: the loop runs ROUNDS rounds; a round FAILS if any steady-state invariant is violated after
#   the heal window. On first failure it stops and prints the fault history + SEED to reproduce.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"

# ── Config ────────────────────────────────────────────────────────────────────
BUILD_DIR="cmake-build-release"
JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/chaos"
ROUNDS="${ROUNDS:-20}"
STEADY_STATE_SECS="${STEADY_STATE_SECS:-4}"     # heal window between injecting a fault and checking invariants
SEED="${SEED:-$RANDOM}"                           # export SEED=<n> to replay a run exactly
BACKGROUND_LOAD="${BACKGROUND_LOAD:-1}"          # 1 = keep a low-rate FIX order flow running under the chaos
# Round count used instead of ROUNDS when ONLY_FAULT=fault_kill_leader: every round kills whoever is
# currently leading (see target_leader below), so with all three members killable this is genuinely
# LEADER_CHANGES failovers, not just that many kill attempts.
LEADER_CHANGES="${LEADER_CHANGES:-10}"
[[ "${ONLY_FAULT:-}" == "fault_kill_leader" ]] && ROUNDS="$LEADER_CHANGES"
RANDOM="$SEED"
# How long a co-located client may take to notice its media driver died before we call it a
# fail-fast violation. Must exceed Aeron's DEFAULT_MEDIA_DRIVER_TIMEOUT_MS (10s, Context.h) — a
# client CANNOT detect driver death sooner than that, so anything less produces spurious failures.
DRIVER_LOSS_GRACE_SECS="${DRIVER_LOSS_GRACE_SECS:-15}"
# 1 = a fail-fast violation fails the round; 0 = report it and carry on (for triaging a run whose
# clients are known not to exit yet, without going red every kill round).
DRIVER_LOSS_STRICT="${DRIVER_LOSS_STRICT:-1}"
# How long fault_sigkill_node waits before restarting the member it SIGKILLed, so Aeron ages out the
# mark files the kill left "active". Must exceed the mark-file liveness timeout (driverTimeoutMs, 10s).
SIGKILL_MARKFILE_SETTLE_SECS="${SIGKILL_MARKFILE_SETTLE_SECS:-12}"

LEADER_TIMEOUT_SECS=30
ELECTION_TIMEOUT_SECS=15
NODE_START_TIMEOUT_SECS=15
APP_CATCHUP_TIMEOUT_SECS=30
GATEWAY_PORT_TIMEOUT_SECS=15   # gateway TCP listener after (re)start: measured 2.0s
PROC_EXIT_TIMEOUT_SECS=10      # a signalled member actually dying: measured 0.2s
TAP_STALL_DEADLINE_SECS=10
PAUSE_SECS=0.5

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMPDIR:-/tmp}phixeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"
FIX_TCP_PORT="$(fix_tcp_port)"
AERON_DIR="${TMPDIR}aeron-$(whoami)"
CN=0            # primary gateway (GW-A) / observation consumer host — a fault target like any other member
STANDBY_MEMBER=1  # hot-standby gateway (GW-B) host — see gateway-failover-test.sh's active/standby model
STANDBY_PORT="$(fix_tcp_port 1)"
if command -v aeronmd >/dev/null 2>&1; then AERONMD="$(command -v aeronmd)"; else AERONMD="${BUILD_DIR}/_deps/aeron-build/binaries/aeronmd"; fi

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
declare -a SEQ_PIDS REPLAYER_PIDS BASICDATA_PIDS EXTRA_CONSUMER_PIDS
CONSUMER_PID=""; FIX_PID=""; STANDBY_FIX_PID=""; MD_PID=""; LOAD_PID=""
declare -a FAULT_HISTORY=()
DRIVER_LOSS_FAIL=0   # set by check_driver_loss_failfast, folded into the round result by check_invariants
RESTART_FAIL=0       # set by assert_restarted, folded into the round result by check_invariants
TAP_STALL_FAIL=0     # set by fault_tap_stall, folded into the round result by check_invariants
FAILOVER_FAIL=0      # set by fault_kill_leader, folded into the round result by check_invariants
declare -a SEQ_LOG_OFFSET   # lines already in seq-<m>.log when its CURRENT boot started — see wait_running
# Set by fault_tap_drop, consumed by check_invariants: which replica was armed and its pre-fault gap
# counts, so the round can assert the drop ACTUALLY LANDED rather than passing on liveness alone.
TAP_DROP_TARGET=""; TAP_DROP_LOG=""; TAP_DROP_RESUMES=0; TAP_DROP_REWALKS=0
# Per-replica RecoveryStalled alarms already accounted for, so check_invariants (c) fails a round on a
# NEW one rather than re-reporting an episode some earlier round already owned.
declare -a RECOVERY_STALL_BASELINE=(0 0 0)

# ── Helpers ─────────────────────────────────────────────────────────────────────
start_seq() {  # start_seq <memberId> — append so leadership history survives restarts (last isLeader= wins)
  local m="$1"
  # Remember where this boot's output begins, BEFORE launching, so wait_running can tell a fresh
  # "Running" from the one the previous boot left in the same (appended-to) file.
  SEQ_LOG_OFFSET[$m]=0
  [[ -f "$LOG_DIR/seq-$m.log" ]] && SEQ_LOG_OFFSET[$m]=$(wc -l < "$LOG_DIR/seq-$m.log")
  # PHIXERON_FAULT_INJECTION arms the tap-recording fault fault_tap_stall triggers by file (the archive is
  # in-process, so it cannot be stalled from outside); inert until that file appears.
  PHIXERON_FAULT_INJECTION=1 \
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" >> "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
}
# NB: a bash loop's exit status is that of the last command in its body, so we MUST end with an explicit
# `return 0` — otherwise the trailing budget test (false, status 1) would make a SUCCESSFUL wait report failure.
#
# Scoped to the CURRENT boot's output. start_seq appends (so leadership history survives a restart), so a
# whole-file `grep -q "Running"` matched the PREVIOUS boot's line and returned instantly on every restart —
# the harness never actually waited for a restarted node, and rounds went on to check invariants against a
# member that was still replaying the log. Read from this boot's starting offset instead.
wait_running() {
  # NB: two statements, not one — `local m="$1" off="${SEQ_LOG_OFFSET[$m]}"` expands every word BEFORE
  # `local` performs any assignment, so the subscript would read the (unset, `set -u`) GLOBAL m.
  local m="$1" W=0
  local off="${SEQ_LOG_OFFSET[$m]:-0}"
  until tail -n "+$((off + 1))" "$LOG_DIR/seq-$m.log" 2>/dev/null | grep -q "Running"; do
    sleep 0.5; W=$((W+1)); ((W > NODE_START_TIMEOUT_SECS * 2)) && return 1
  done
  return 0
}

# A member that does not come back is a FAILED ROUND, not a warning. Nothing else here notices one:
# every later round still finds a leader and passes its probe on the two survivors, so a run whose
# member 2 died on an archive its SIGKILL had torn reported 24/24 liveness while permanently down a
# node, and only the end-of-run convergence check caught it. Reported through the same flag mechanism
# as the driver-loss assertions rather than by returning: the fault function must still run
# restart_colocated_apps, and the round must still check its other invariants.
assert_restarted() {  # <memberId> — start_seq must already have been called
  local m="$1"
  wait_running "$m" && return 0
  log "  RESTART FAIL: member $m never printed \"Running\" within ${NODE_START_TIMEOUT_SECS}s (see $LOG_DIR/seq-$m.log)"
  RESTART_FAIL=1
  return 1
}

alive() { kill -0 "${SEQ_PIDS[$1]:-0}" 2>/dev/null; }   # is member $1's SequencerNode process up (not SIGSTOPped-aware)

current_leader() {  # echo the memberId whose most-recent leadership line is isLeader=true, or "" if none/split
  local m line
  for m in 0 1 2; do
    alive "$m" || continue
    line="$(grep 'isLeader=' "$LOG_DIR/seq-$m.log" 2>/dev/null | tail -1)"
    [[ "$line" == *"isLeader=true"* ]] && { echo "$m"; return; }
  done
  echo ""
}
wait_for_leader() { local W=0 L; while :; do L="$(current_leader)"; [[ -n "$L" ]] && { echo "$L"; return 0; }; sleep 0.5; W=$((W+1)); ((W > LEADER_TIMEOUT_SECS * 2)) && { echo ""; return 1; }; done; }

log() { printf '[chaos %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

cleanup() {
  log "tearing down"
  for m in 0 1 2; do kill -CONT "${SEQ_PIDS[$m]:-0}" 2>/dev/null; done   # un-pause before killing
  kill "${LOAD_PID:-}" "${CONSUMER_PID:-}" "${FIX_PID:-}" "${STANDBY_FIX_PID:-}" "${MD_PID:-}" 2>/dev/null
  pkill -f fix_test_server 2>/dev/null   # the background-load loop's in-flight child outlives its subshell
  # ${arr[@]+"${arr[@]}"} — the bash 3.2 / set -u safe way to expand a possibly-empty array to nothing.
  for p in "${SEQ_PIDS[@]+"${SEQ_PIDS[@]}"}" "${REPLAYER_PIDS[@]+"${REPLAYER_PIDS[@]}"}" \
           "${BASICDATA_PIDS[@]+"${BASICDATA_PIDS[@]}"}" "${EXTRA_CONSUMER_PIDS[@]+"${EXTRA_CONSUMER_PIDS[@]}"}"; do
    kill "$p" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── Bring-up ─────────────────────────────────────────────────────────────────────
[[ -f "$JAR" ]] || { echo "missing $JAR — run ./gradlew uberJar"; exit 1; }
[[ -x "$BUILD_DIR/OrderExecClient" && -x "$BUILD_DIR/FixGateway" && -x "$BUILD_DIR/fix_test_server" \
   && -x "$BUILD_DIR/BasicDataClient" ]] \
  || { echo "missing C++ targets — run cmake --build $BUILD_DIR"; exit 1; }

# Idempotent pre-clean so back-to-back runs don't collide: SIGKILL any survivors, then WAIT for the
# member archive-control ports (Aeron binds these as UDP) to actually release.
# NB: SequencerNode launches via `java -jar "$JAR"`, so its command line contains NO "SequencerNode"
# substring — pkilling by class name misses it. Match the jar path (in both SequencerNode's `-jar` and
# ReplayerNode's `-cp` lines) and the -Dsequencer marker instead.
pkill -9 -f "$JAR" 2>/dev/null; pkill -9 -f "sequencer.memberId" 2>/dev/null
for p in OrderExecClient FixGateway fix_test_server BasicDataClient aeronmd; do pkill -9 -f "$p" 2>/dev/null; done
rm -rf "$BASE_DIR" "${TMPDIR}phixeron-seq-aeron-0" "${TMPDIR}phixeron-seq-aeron-1" \
       "${TMPDIR}phixeron-seq-aeron-2" "$AERON_DIR" 2>/dev/null
W=0; while lsof -nP -iUDP:"$(archive_port 0)" -iUDP:"$(archive_port 1)" -iUDP:"$(archive_port 2)" 2>/dev/null \
  | grep -q java; do sleep 0.5; W=$((W+1)); ((W>20)) && { echo "UDP archive ports still held after 10s — stale cluster?"; exit 1; }; done

log "SEED=$SEED  ROUNDS=$ROUNDS  STEADY_STATE_SECS=$STEADY_STATE_SECS   (replay with SEED=$SEED)"
start_seq 1; start_seq 2
wait_running 1 && wait_running 2 || { echo "members 1/2 not up"; exit 1; }
wait_for_leader >/dev/null || { echo "no initial leader among 1/2"; exit 1; }
start_seq 0; wait_running 0 || { echo "member 0 not up"; exit 1; }

AERON_DIR="$AERON_DIR" "$AERONMD" > "$LOG_DIR/aeronmd.log" 2>&1 & MD_PID=$!
W=0; until [[ -f "$AERON_DIR/cnc.dat" ]]; do sleep 0.2; W=$((W+1)); ((W>25)) && { echo "media driver not up"; exit 1; }; done

for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.ReplayerNode > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
done
for m in 0 1 2; do W=0; until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && break; done; done

# BasicDataClient replica on every node (see start-three-node-cluster.sh): dual-role, producer on
# whichever member is leader, consumer elsewhere. Without one running on every node, no member ever
# publishes the Gateway/Session/TradingDay rows the gateway needs — EndBasicData never arrives, the
# gateway's accept gate never opens (m_basicDataLoaded stays false), and every Logon just queues in the
# TCP backlog until the client times out. One per node so a leader failover always has a local producer.
for m in 0 1 2; do
  PHIXERON_BASICDATA_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
    PHIXERON_REPLAYER_CLIENT_ID=3 PHIXERON_BASICDATA_EGRESS_ENDPOINT="localhost:$(basicdata_egress_port "$m")" \
    stdbuf -oL -eL "$BUILD_DIR/BasicDataClient" > "$LOG_DIR/basicdata-$m.log" 2>&1 &
  BASICDATA_PIDS[$m]=$!
done

# Consumer on member 0: fault-injection ON (SIGUSR1 tap-drop) + latency stats (flushed on exit, not read here).
# Extracted into start_consumer/start_gateway (below) so restart_colocated_apps can relaunch the same pair
# in place when member 0 itself is a fault target — same driver-death problem members 1/2's replicas have
# on restart, just for the gateway/consumer instead of a plain OrderExecClient replica.
CONSUMER_LOG="$LOG_DIR/consumer.log"
start_consumer() {
  PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${CN}" PHIXERON_NODE_MEMBER_ID="$CN" \
    PHIXERON_REPLAYER_CLIENT_ID=9 PHIXERON_CLUSTER_EGRESS_ENDPOINT="localhost:${TEST_CONSUMER_EGRESS_PORT}" \
    PHIXERON_LATENCY_STATS=1 PHIXERON_FAULT_INJECTION=1 \
    stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$CONSUMER_LOG" 2>&1 &
  CONSUMER_PID=$!
}
start_consumer
W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "consumer never caught up"; exit 1; }; done

# OrderExecClient replica on members 1 and 2 too (see start-three-node-cluster.sh): sendNewExecutionReport
# only fires on the replica CO-LOCATED WITH THE CURRENT LEADER (OrderExecClient.cpp, m_replayer.isCaughtUp()
# && currentLeaderMemberId()==m_nodeMemberId — one fill per order, not one per replica). Bring-up always
# elects the initial leader from members 1/2 before member 0 even joins (see below), so without a replica on
# every node NO replica is ever positioned to answer a NewOrderSingle with an ExecutionReport, and every FIX
# round-trip probe hangs waiting for one. Latency stats stay unique to the member-0 observation consumer,
# but fault injection is armed on ALL THREE so fault_tap_drop can aim at whichever replica is currently
# leader-co-located — the only one doing external-facing work. Defined as a function for the same reason
# start_consumer is: restart_colocated_apps must relaunch a replica with the SAME env, or a restarted one
# silently loses its arm and every later tap-drop aimed at it is a no-op.
start_replica() {  # start_replica <memberId>  (members 1/2; member 0's replica is start_consumer)
  local m="$1"
  PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
    PHIXERON_FAULT_INJECTION=1 \
    stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$LOG_DIR/orderexec-$m.log" 2>&1 &
  EXTRA_CONSUMER_PIDS[$m]=$!
}
for m in 1 2; do start_replica "$m"; done
for m in 1 2; do W=0; until grep -q "following live" "$LOG_DIR/orderexec-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN orderexec-$m never caught up"; break; }; done; done

# FIX gateway on member 0 (port $FIX_TCP_PORT).
FIX_LOG="$LOG_DIR/fix.log"
start_gateway() {
  PHIXERON_FIX_GATEWAY_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${CN}" PHIXERON_NODE_MEMBER_ID="$CN" \
    PHIXERON_REPLAYER_CLIENT_ID=2 PHIXERON_FIX_TCP_PORT="$FIX_TCP_PORT" \
    PHIXERON_FIX_GATEWAY_NAME=GW-A \
    stdbuf -oL -eL "$BUILD_DIR/FixGateway" > "$FIX_LOG" 2>&1 &
  FIX_PID=$!
}
start_gateway
W=0; until nc -z 127.0.0.1 "$FIX_TCP_PORT" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > GATEWAY_PORT_TIMEOUT_SECS * 2)) && { echo "gateway $FIX_TCP_PORT not up"; exit 1; }; done
# The TCP port alone isn't "ready to Logon": the gateway gates on EndBasicData (a client that connects
# earlier just queues in the listen backlog and eventually times out) — see start-three-node-cluster.sh.
W=0; until grep -q "Basic data loaded" "$FIX_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "gateway never saw EndBasicData — its logon gate is still shut"; exit 1; }; done

# Hot-standby gateway GW-B on member $STANDBY_MEMBER (port $STANDBY_PORT): shadows the tap, gate shut,
# promoted in place of GW-A on a GatewayActive naming it (see gateway-failover-test.sh). Without this,
# killing GW-A's host would promote to a sibling gatewayId that has no live process behind it — a dead end.
STANDBY_FIX_LOG="$LOG_DIR/fix-standby.log"
start_standby_gateway() {
  PHIXERON_FIX_GATEWAY_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${STANDBY_MEMBER}" PHIXERON_NODE_MEMBER_ID="$STANDBY_MEMBER" \
    PHIXERON_REPLAYER_CLIENT_ID=2 PHIXERON_FIX_TCP_PORT="$STANDBY_PORT" \
    PHIXERON_FIX_GATEWAY_NAME=GW-B \
    stdbuf -oL -eL "$BUILD_DIR/FixGateway" > "$STANDBY_FIX_LOG" 2>&1 &
  STANDBY_FIX_PID=$!
}
start_standby_gateway
W=0; until grep -q "Caught up to live stream" "$STANDBY_FIX_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "standby gateway never caught up"; exit 1; }; done
log "cluster READY — gateway up (GW-A active, GW-B standby), consumer following live"

# Which of GW-A/GW-B is currently serving — each gateway logs "is now active"/"is now standby" (only) when
# ITS OWN activation state changes (FixGateway.cpp's GatewayActive handler), so the last such line in EACH
# log independently reflects that instance's current state; whichever says "active" wins. No cross-file
# time correlation needed. Falls back to GW-A (the bootstrap-designated primary) if neither has resolved yet.
gateway_status() { grep -o "is now active\|is now standby" "$1" 2>/dev/null | tail -1 | grep -o "active\|standby"; }
active_gateway_port() {
  [[ "$(gateway_status "$FIX_LOG")" == "active" ]] && { echo "$FIX_TCP_PORT"; return; }
  [[ "$(gateway_status "$STANDBY_FIX_LOG")" == "active" ]] && { echo "$STANDBY_PORT"; return; }
  echo "$FIX_TCP_PORT"
}

# Optional steady background order flow so faults land on a system that is actually doing work.
# Streams NewOrderSingles THROUGH whichever gateway currently holds the accept gate, as SenderCompID
# "LOADGEN" — distinct from the "PROBE" the liveness probe uses, so the load fix and a concurrent probe fix
# coexist (proven by fix_test_server's runTwoDifferentSendersTest). Loops so order flow is continuous for
# the whole run, following the gateway across a GW-A/GW-B promotion.
start_background_load() {
  [[ "$BACKGROUND_LOAD" == "1" ]] || return 0
  # A normal iteration is naturally rate-limited by its own session duration (Logon..50 orders..Logout).
  # On failure (e.g. mid leader-failover) that natural throttle disappears — back off instead of
  # respawning in a hot spin, or a run of instant failures floods the gateway's small TCP backlog with
  # connect attempts, which starves other connections (the liveness probe included) with connection
  # refusals of its own making.
  ( while true; do PHIXERON_FIX_LOADGEN=50 "$BUILD_DIR/fix_test_server" 127.0.0.1 "$(active_gateway_port)" >/dev/null 2>&1 || sleep 0.5; done ) &
  LOAD_PID=$!
}
start_background_load

# ── Fault menu ───────────────────────────────────────────────────────────────────
# Each injector picks its own target relative to the CURRENT leader and heals the cluster back to full
# strength (so the next round starts from 3/3, never draining quorum across rounds). Every fault here is
# an in-process / signal mechanism already exercised by the existing single-shot tests.
# All three members, including CN, are legal fault targets — killing/pausing member 0 takes its co-located
# gateway + consumer down too, and restart_colocated_apps brings that specific pair back (see below).
target_leader() { echo "$1"; }                                    # the current leader — always a legal kill target
a_follower()    { local l="$1" p; while :; do p=$(( RANDOM % 3 )); [[ "$p" != "$l" ]] && { echo "$p"; return; }; done; }  # a random non-leader among {0,1,2}
# Member 0's OrderExecClient is the observation consumer (CONSUMER_PID/consumer.log); 1 and 2 are plain
# replicas. Both are legal tap-drop targets, so resolve either by memberId.
replica_pid() { local m="$1"; [[ "$m" == "$CN" ]] && echo "${CONSUMER_PID:-}" || echo "${EXTRA_CONSUMER_PIDS[$m]:-}"; }
replica_log() { local m="$1"; [[ "$m" == "$CN" ]] && echo "$CONSUMER_LOG" || echo "$LOG_DIR/orderexec-$m.log"; }
# A gap the consumer repaired by RESUMING its recording at the hole — the healthy recovery, and the proof
# that an armed tap-drop actually landed. Distinct from the re-walk fallback counted below.
count_tap_resumes() { grep -c 'tap gap: expected globalSeqNo' "$1" 2>/dev/null || true; }
count_tap_rewalks() { grep -c 're-walking the recording chain' "$1" 2>/dev/null || true; }
# The client's own "I am not serving" alarm (RecoveryStalled, ReplayerStreamReceiver::checkRecoveryProgress):
# recovery dispatched nothing for RECOVERY_PROGRESS_TIMEOUT_MS while not caught up. Unlike the one-shot
# "following live" marker this fires per episode, so it reads CURRENT state rather than latching at boot.
count_recovery_stalls() { grep -c 'recovery has dispatched nothing' "$1" 2>/dev/null || true; }

# ── Media-driver fail-fast assertions ────────────────────────────────────────────
# SequencerNode embeds its media driver (ClusteredMediaDriver, SequencerNode.java:191), so killing a
# member IS a media-driver kill for every client sharing that member's aeron dir — the standalone
# aeronmd this script starts serves only fix_test_server. Every kill fault therefore already injects
# "driver dies, restarts at the same path"; what was missing is asserting on it, because
# restart_colocated_apps below relaunches these clients unconditionally and so guarantees the
# recovery the test should have been proving.
#
# The property under test is FAIL-FAST: a client that loses its driver must EXIT, non-zero, so a real
# supervisor restarts it. Staying alive is the bug — for FixGateway it means a live TCP listener in
# front of a process that can no longer reach the cluster, so a FIX client connects, Logons, and
# hangs. Checked BEFORE restart_colocated_apps kills them, or the kill masks the result.
assert_died_on_driver_loss() {  # <label> <pid> — waits out the driver timeout, then asserts exit
  local label="$1" pid="${2:-}" W=0 rc
  [[ -z "$pid" || "$pid" == "0" ]] && return 0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); (( W > DRIVER_LOSS_GRACE_SECS * 2 )) && break
  done
  if kill -0 "$pid" 2>/dev/null; then
    log "  DRIVER-LOSS FAIL: $label (pid $pid) still running ${DRIVER_LOSS_GRACE_SECS}s after its media driver died"
    DRIVER_LOSS_FAIL=1
    return 1
  fi
  # `wait` on a still-unreaped child yields its exit status; 127 means already reaped, so treat an
  # unavailable status as inconclusive rather than inventing a verdict.
  wait "$pid" 2>/dev/null; rc=$?
  if (( rc == 127 )); then
    log "  ok: $label exited on driver loss (status unavailable — already reaped)"
  elif (( rc == 0 )); then
    log "  DRIVER-LOSS FAIL: $label exited 0 on driver loss — a restart-on-failure supervisor would not restart it"
    DRIVER_LOSS_FAIL=1
    return 1
  else
    log "  ok: $label exited $rc on driver loss"
  fi
  return 0
}

# Only meaningful on the failure path above: once the process is gone the OS has closed its listening
# socket, so a port probe on a dead gateway asserts nothing. Alive AND still accepting is what turns a
# leaked process into a client-visible hang, so that is what gets reported.
report_gateway_socket() {  # <port> <label> <pid>
  kill -0 "${3:-0}" 2>/dev/null || return 0
  nc -z 127.0.0.1 "$1" 2>/dev/null \
    && log "  DRIVER-LOSS FAIL: $2 is STILL ACCEPTING TCP on port $1 with no media driver — a FIX client would connect and hang"
  return 0
}

check_driver_loss_failfast() {  # <memberId whose driver just died>
  local m="$1"
  assert_died_on_driver_loss "replayer-$m"  "${REPLAYER_PIDS[$m]:-}"
  assert_died_on_driver_loss "basicdata-$m" "${BASICDATA_PIDS[$m]:-}"
  if [[ "$m" == "$CN" ]]; then
    assert_died_on_driver_loss "consumer"     "${CONSUMER_PID:-}"
    assert_died_on_driver_loss "gateway GW-A" "${FIX_PID:-}"
    report_gateway_socket "$FIX_TCP_PORT" "gateway GW-A" "${FIX_PID:-}"
  else
    assert_died_on_driver_loss "orderexec-$m" "${EXTRA_CONSUMER_PIDS[$m]:-}"
  fi
  if [[ "$m" == "$STANDBY_MEMBER" ]]; then
    assert_died_on_driver_loss "gateway GW-B" "${STANDBY_FIX_PID:-}"
    report_gateway_socket "$STANDBY_PORT" "gateway GW-B" "${STANDBY_FIX_PID:-}"
  fi
  return 0
}

# A killed member's co-located ReplayerNode/BasicDataClient/OrderExecClient (or, for CN, the gateway +
# observation consumer) share its embedded media driver (same aeron dir) and don't survive the member's
# restart: the driver dies with the SequencerNode process, and none of these clients reconnect to the
# fresh driver the restart creates at the same path — they just fault (DriverTimeoutException /
# "MediaDriver has been shutdown") and sit dead for the rest of the run. That leaves a permanent hole:
# e.g. if member $m later becomes leader, its dead OrderExecClient replica can't be the one that answers a
# NewOrderSingle with an ExecutionReport (leader-only emission), so a later round's FIX round-trip probe
# hangs waiting for one that will never come.
restart_colocated_apps() {
  local m="$1" W=0
  check_driver_loss_failfast "$m"
  kill "${REPLAYER_PIDS[$m]:-0}" "${BASICDATA_PIDS[$m]:-0}" 2>/dev/null
  if [[ "$m" == "$CN" ]]; then
    kill "${CONSUMER_PID:-0}" "${FIX_PID:-0}" 2>/dev/null
  else
    kill "${EXTRA_CONSUMER_PIDS[$m]:-0}" 2>/dev/null
  fi
  [[ "$m" == "$STANDBY_MEMBER" ]] && kill "${STANDBY_FIX_PID:-0}" 2>/dev/null
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.ReplayerNode > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
  until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN replayer-$m not serving after restart"; break; }
  done
  PHIXERON_BASICDATA_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
    PHIXERON_REPLAYER_CLIENT_ID=3 PHIXERON_BASICDATA_EGRESS_ENDPOINT="localhost:$(basicdata_egress_port "$m")" \
    stdbuf -oL -eL "$BUILD_DIR/BasicDataClient" > "$LOG_DIR/basicdata-$m.log" 2>&1 &
  BASICDATA_PIDS[$m]=$!
  if [[ "$m" == "$CN" ]]; then
    start_consumer
    W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN consumer not caught up after restart"; break; }
    done
    start_gateway
    W=0; until nc -z 127.0.0.1 "$FIX_TCP_PORT" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > GATEWAY_PORT_TIMEOUT_SECS * 2)) && { log "  WARN gateway port $FIX_TCP_PORT not up after restart"; break; }
    done
    W=0; until grep -q "Basic data loaded" "$FIX_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN gateway never saw EndBasicData after restart"; break; }
    done
  else
    start_replica "$m"
    W=0; until grep -q "following live" "$LOG_DIR/orderexec-$m.log" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN orderexec-$m not caught up after restart"; break; }
    done
  fi
  if [[ "$m" == "$STANDBY_MEMBER" ]]; then
    start_standby_gateway
    W=0; until grep -q "Caught up to live stream" "$STANDBY_FIX_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN standby gateway not caught up after restart"; break; }
    done
  fi
}

fault_kill_leader() {  # crash the leader -> real Raft failover -> restore it as a follower
  local L="$1" T; T="$(target_leader "$L")"   # every member is a legal kill target, so T is always the actual leader
  log "FAULT kill-leader: member $T"; kill "${SEQ_PIDS[$T]}" 2>/dev/null
  local W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W > PROC_EXIT_TIMEOUT_SECS * 5)) && break; done  # let it actually die
  # Wait for a DIFFERENT member to win the election (a genuine failover). Restarting T too fast — before
  # the survivors elect — lets T just bounce and reclaim leadership, which half-wedges the cluster instead
  # of failing over. (If we killed a follower, current_leader is unchanged and this returns immediately.)
  local NL="" W2=0
  until NL="$(current_leader)"; [[ -n "$NL" && "$NL" != "$T" ]]; do sleep 0.5; W2=$((W2+1)); ((W2 > ELECTION_TIMEOUT_SECS * 2)) && break; done
  log "  new leader: member ${NL:-<none>}"
  # The loop above also exits on TIMEOUT, which until now was indistinguishable from a real failover: it
  # just logged "<none>" and carried on. Invariant (a) does not catch it — (a) is satisfied by ANY leader,
  # including T bouncing back and reclaiming leadership, which is the half-wedged state the wait exists to
  # avoid. T is always the leader here (target_leader), so leadership MUST move off it.
  if [[ -z "$NL" || "$NL" == "$T" ]]; then
    log "  INVARIANT FAIL: no genuine failover within ${ELECTION_TIMEOUT_SECS}s — leadership did not move off member $T"
    FAILOVER_FAIL=1
  fi
  sleep 1                                                       # let the new leader settle (election measures 0.5s)
  start_seq "$T"; assert_restarted "$T"
  sleep 2                                                       # let the restored member rejoin (full-log replay)
  restart_colocated_apps "$T"
}
fault_kill_follower() {  # crash a follower -> should be transparent (quorum holds) -> restart it
  local L="$1" F; F="$(a_follower "$L")"
  log "FAULT kill-follower: member $F"; kill "${SEQ_PIDS[$F]}" 2>/dev/null; sleep 1
  start_seq "$F"; assert_restarted "$F"
  restart_colocated_apps "$F"
}
# The corrective action Aeron's own error message names, run on a SIGKILLed member's archive before
# restarting it. A kill mid-write can leave the last data fragment of a recording straddling a page
# boundary — unverifiable, so Archive.launch REFUSES to open the catalog ("Found potentially incomplete
# last fragment straddling page boundary in file: …/1-0.rec. Run `ArchiveTool verify` for corrective
# action!") and the member never comes back for the rest of the run. Observed on the cluster LOG
# recording (streamId 100), not just the tap. verify truncates the torn fragment and writes the
# recovered stopPosition into the catalog descriptor, which is what stops the next boot from tripping
# over it: Catalog.refreshAndFixDescriptor only recomputes a stopPosition that is still NULL_POSITION.
# Nothing committed is lost — the fragment was never fully written, so it is behind this member's acked
# append position, and it re-replicates from the leader on rejoin.
# Only the SIGKILL path needs this: every other fault here leaves through the shutdown barrier
# (fault_tap_stall's self-termination included, SequencerNode.java), so the Archive gets a clean close.
repair_archive() {  # repair_archive <memberId>
  # One `local` per variable, deliberately: bash 3.2 (the macOS default) expands $m in the SAME
  # `local` statement from the OUTER scope rather than from the local just assigned, so a combined
  # declaration silently aimed every repair at whatever the last `for m in 0 1 2` loop left behind.
  local m="$1"
  local archive="$BASE_DIR/archive-$m"
  local out="$LOG_DIR/archive-verify-$m.log"
  [[ -f "$archive/archive.catalog" ]] || return 0
  # Never touch a LIVE archive. verify opens the catalog READ-WRITE and writes a stopPosition into
  # every in-progress recording — bookkeeping the running archive owns — which leaves that node's
  # recording looking stopped and fails the convergence check at the end of the run. The call site
  # only ever passes a member it has just watched die, so this is a backstop, not a race.
  kill -0 "${SEQ_PIDS[$m]:-0}" 2>/dev/null \
    && { log "  WARN member $m still running — skipping archive repair"; return 0; }
  # verify PROMPTS on stdin before truncating ("(y) to truncate the file or (n) to do nothing"), so feed
  # it an endless `yes` — one prompt per straddling recording, and a bare `printf y` would leave a second
  # prompt reading EOF. Redirected from a process substitution rather than piped: under `pipefail` the
  # SIGPIPE that kills `yes` when the JVM exits would otherwise mask ArchiveTool's own exit status.
  if java "${JAVA_OPTS[@]}" -cp "$JAR" io.aeron.archive.ArchiveTool "$archive" verify \
       < <(yes) >> "$out" 2>&1; then
    grep -q "straddles a page boundary" "$out" \
      && log "  repaired member $m's archive: truncated a torn last fragment"
  else
    log "  WARN ArchiveTool verify reported errors for member $m (see $out)"
  fi
  return 0   # a clean archive prints nothing to grep for; that is not a failure
}
fault_sigkill_node() {  # SIGKILL a follower — driver dies with NO clean shutdown, so clients must time out
  # The harsher twin of fault_kill_follower, and the only fault that exercises the media-driver TIMEOUT
  # path. A plain `kill` (SIGTERM) lets the JVM's shutdown hook close the ClusteredMediaDriver cleanly, so
  # co-located clients see "MediaDriver has been shutdown" and exit within ~1s. SIGKILL leaves the CnC file
  # frozen with no such marker, so the only detector is DEFAULT_MEDIA_DRIVER_TIMEOUT_MS (10s) — a genuinely
  # different code path in the client, and the one a real machine/OOM kill takes.
  # Aimed at a follower: failover is already covered by fault_kill_leader, and this asserts driver-loss
  # detection, not the election.
  local L="$1" F; F="$(a_follower "$L")"
  log "FAULT sigkill-node: SIGKILL member $F (expect co-located clients to exit via the 10s driver timeout)"
  kill -9 "${SEQ_PIDS[$F]}" 2>/dev/null
  local W=0; while kill -0 "${SEQ_PIDS[$F]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W > PROC_EXIT_TIMEOUT_SECS * 5)) && break; done
  # A SIGKILLed node never unwinds, so its Archive/consensus mark files are left "active" — restarting
  # before Aeron ages them out fails with "active mark file detected". Wait out the mark-file liveness
  # timeout (driverTimeoutMs, 10s) before restarting, or the round after this one fails for that reason
  # rather than for anything this fault is testing. This also gives the co-located clients the full
  # timeout window they need, which check_driver_loss_failfast then asserts they used.
  sleep "$SIGKILL_MARKFILE_SETTLE_SECS"
  repair_archive "$F"
  start_seq "$F"; assert_restarted "$F"
  sleep 2                                                       # let the restored member rejoin (full-log replay)
  restart_colocated_apps "$F"
}
fault_pause_node() {  # SIGSTOP a killable node (GC-pause / stall simulation: socket stays half-open) then SIGCONT
  local L="$1" P; P=$(( RANDOM % 2 == 0 ? $(target_leader "$L") : $(a_follower "$L") ))
  log "FAULT pause-node: SIGSTOP member $P for ${PAUSE_SECS}s"; kill -STOP "${SEQ_PIDS[$P]}" 2>/dev/null
  sleep "$PAUSE_SECS"; kill -CONT "${SEQ_PIDS[$P]}" 2>/dev/null; log "  SIGCONT member $P"
}
fault_tap_drop() {  # drop one live tap frame -> gap -> resume-at-the-hole recovery (SIGUSR1 path)
  # Aimed at the replica CO-LOCATED WITH THE LEADER, not always member 0's. That replica is the one
  # answering NewOrderSingle with an ExecutionReport and emitting query replies (the isCaughtUp() &&
  # currentLeaderMemberId()==m_nodeMemberId gate), so a gap episode there tests recovery WHILE emitting.
  # Member 0 is usually a follower — bring-up elects the initial leader from members 1/2 — so the old
  # fixed target exercised only the passive following path.
  local L="$1" T; T="$(target_leader "$L")"
  local pid; pid="$(replica_pid "$T")"
  TAP_DROP_TARGET="$T"
  TAP_DROP_LOG="$(replica_log "$T")"
  TAP_DROP_RESUMES="$(count_tap_resumes "$TAP_DROP_LOG")"
  TAP_DROP_REWALKS="$(count_tap_rewalks "$TAP_DROP_LOG")"
  log "FAULT tap-drop: SIGUSR1 member $T's replica (leader-co-located; arm one-frame live-tap drop)"
  kill -USR1 "$pid" 2>/dev/null || log "  WARN no live replica pid for member $T"
}
fault_tap_stall() {  # kill a node's local tap recording -> it must notice within a tick and TERMINATE ITSELF
  # The one fault the node is expected to answer by dying: a node that cannot record its own tap can no
  # longer serve the history it is responsible for, so it exits non-zero (SequencerService.fatalTapFailure)
  # and is restarted, rebuilding its recording over the full-log replay. Aimed at a follower — failover is
  # already covered by fault_kill_leader, and this asserts the self-termination, not the election.
  local L="$1" T; T="$(a_follower "$L")"
  log "FAULT tap-stall: stop member $T's tap recording (expect FATAL + self-termination)"
  touch "$BASE_DIR/cluster-$T/tap-stall-fault"
  local W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W > TAP_STALL_DEADLINE_SECS * 5)) && break; done
  if kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; then
    log "  INVARIANT FAIL: member $T kept running for ${TAP_STALL_DEADLINE_SECS}s with no tap recording"
    TAP_STALL_FAIL=1
    # Still wait for it to actually die before restarting below, or the restart hits its own archive
    # mark file ("active mark file detected") and the round after this one fails for the wrong reason.
    kill "${SEQ_PIDS[$T]}" 2>/dev/null
    W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W > PROC_EXIT_TIMEOUT_SECS * 5)) && break; done
  else
    # Scoped to the CURRENT boot, like wait_running: start_seq APPENDS, so a whole-file grep matches the
    # FATAL line an EARLIER tap-stall round left behind and passes a member that has since died of
    # something else entirely. The same member does get stalled twice in a run, so this is reachable.
    if tail -n "+$(( ${SEQ_LOG_OFFSET[$T]:-0} + 1 ))" "$LOG_DIR/seq-$T.log" 2>/dev/null \
       | grep -q "FATAL:.*terminating this node"; then
      log "  ok: member $T detected the dead recording and terminated"
    else
      log "  INVARIANT FAIL: member $T died without the FATAL tap-recording line"; TAP_STALL_FAIL=1
    fi
  fi
  rm -f "$BASE_DIR/cluster-$T/tap-stall-fault"   # else the restarted node re-arms it immediately
  sleep 2
  start_seq "$T"; assert_restarted "$T"
  sleep 2                                                       # let the restored member rejoin (full-log replay)
  restart_colocated_apps "$T"
}
# Targeted mode: set ONLY_FAULT=<fn> (e.g. fault_kill_leader) to run just that fault every round,
# for deterministically exercising one failure path in-loop rather than seed-hunting for it.
FAULTS=(fault_kill_leader fault_kill_follower fault_sigkill_node fault_pause_node fault_tap_drop fault_tap_stall)
[[ -n "${ONLY_FAULT:-}" ]] && FAULTS=("$ONLY_FAULT")

# NOTE — on killing the media driver: there is deliberately no fault_kill_aeronmd, and adding one would
#   test the harness rather than the system. The standalone aeronmd started above serves ONLY
#   fix_test_server (default aeron::Context, FixTestServer.cpp:302,1118) — i.e. the probe and the
#   background load. Every production process is on a per-member driver embedded in its SequencerNode
#   (ClusteredMediaDriver at ${TMPDIR}phixeron-seq-aeron-<m>): ReplayerNode (ReplayerNode.java:67),
#   FixGateway, OrderExecClient and BasicDataClient all attach there. So killing aeronmd would break the
#   probe while leaving the system untouched, and the real "driver dies and restarts at the same path"
#   fault is ALREADY injected by every kill fault above — asserted by check_driver_loss_failfast.
#
# NOTE — heavier / platform-specific faults intentionally left as seams, not enabled by default:
#   * network delay/loss/partition — Linux uses `tc qdisc ... netem`; macOS uses dnctl/pfctl (dummynet) and
#     needs root. This host is darwin, so netem is NOT wired up here; add it under an OS+root guard for CI-on-Linux.
#   * a standalone-driver kill becomes a genuinely distinct fault only if the C++ clients are ever moved
#     off the embedded drivers onto a shared aeronmd — driver dies, cluster node survives, which the
#     embedded topology cannot produce. Wire it then.

# ── Steady-state oracle ──────────────────────────────────────────────────────────
# Liveness + a safety PROXY via log grep. The RIGOROUS safety oracle (gap-free, monotone globalSeqNo across
# the ordered stream, and replica convergence) should be a decode of the cluster log, not grep — pipe the
# recording through src/main/scripts/sbe-log-printer.sh and assert no globalSeqNo gap. That is the TODO seam
# marked below; grep gives liveness + smoke, the decoder gives the actual proof.
check_invariants() {
  local fail=0
  # (a) exactly one leader
  local L; L="$(current_leader)"
  [[ -z "$L" ]] && { log "  INVARIANT FAIL: no leader"; fail=1; } || log "  ok: leader = member $L"
  # (b) liveness: a single strict FIX round-trip as "PROBE" (Logon -> NewOrderSingle -> ExecutionReport
  # -> Logout), against whichever of GW-A/GW-B currently holds the accept gate. Distinct CompID from the
  # "LOADGEN" background load, so the two never collide; exit 0 iff Logon is accepted AND the order
  # round-trips through the cluster and back.
  if PHIXERON_FIX_PROBE=1 "$BUILD_DIR/fix_test_server" 127.0.0.1 "$(active_gateway_port)" > "$LOG_DIR/probe.log" 2>&1; then
    log "  ok: FIX round-trip probe passed"
  else
    log "  INVARIANT FAIL: FIX round-trip probe failed (see $LOG_DIR/probe.log)"; fail=1
  fi
  # (c) safety proxy: every co-located replica is still up and still SERVING.
  #     This used to grep the consumer log for "following live" — which OrderExecClient prints ONCE on the
  #     replay->live transition and never again (m_announcedLive latches, OrderExecClient.cpp:440). It
  #     therefore matched the startup line forever and passed vacuously: a replica that later fell out of
  #     isCaughtUp() and stopped serving looked identical to a healthy one. That is not hypothetical — a
  #     wedged consumer is exactly what a per-poll request resend produced, and only the FIX probe caught
  #     it, and then only because the wedged replica happened to be the leader-co-located one. No log grep
  #     can read the current state, so use the alarm the client raises for precisely this condition.
  #     Detection is not instant: the alarm needs RECOVERY_PROGRESS_TIMEOUT_MS (30s) of recovery
  #     dispatching nothing, so a fresh wedge surfaces a round or two later, not in the round that caused
  #     it. That latency belongs to the policy, not here. All three replicas, not just member 0's: any of
  #     them can wedge, and only the leader-co-located one is implicitly covered by the probe in (b).
  local cfail=0 m pid rlog stalls
  for m in 0 1 2; do
    pid="$(replica_pid "$m")"
    rlog="$(replica_log "$m")"
    if ! kill -0 "${pid:-0}" 2>/dev/null; then
      log "  INVARIANT FAIL: member $m's OrderExecClient replica is not running"; cfail=1; continue
    fi
    stalls="$(count_recovery_stalls "$rlog")"
    # A restart truncates the log (start_consumer/start_replica use >), so the count drops to 0 and the
    # baseline follows it down — a restarted replica cannot inherit a predecessor's alarm.
    if (( stalls > ${RECOVERY_STALL_BASELINE[$m]:-0} )); then
      log "  INVARIANT FAIL: member $m's replica reported recovery stalled — up but not serving (see $rlog)"
      cfail=1
    fi
    RECOVERY_STALL_BASELINE[$m]="$stalls"
  done
  if [[ "$cfail" == "1" ]]; then
    fail=1
  else
    log "  ok: all 3 replicas up and serving (consumer re-walks so far: $(count_tap_rewalks "$CONSUMER_LOG"))"
  fi
  # (c2) EFFICACY, for tap-drop rounds only: the armed drop must have produced a real gap episode on the
  #      target, repaired by a RESUME at the hole. Without this the fault is unverified — a SIGUSR1 to a
  #      dead/unarmed replica silently does nothing and the round still passes (a), (b) and (c), which is
  #      exactly what it did before. A re-walk is the DEGRADED repair (replaying the whole recording to
  #      close a one-frame hole; gap-recovery-test.sh asserts 0 of them), so it fails the round too.
  if [[ -n "$TAP_DROP_TARGET" ]]; then
    local resumes; resumes="$(count_tap_resumes "$TAP_DROP_LOG")"
    local rewalks; rewalks="$(count_tap_rewalks "$TAP_DROP_LOG")"
    if (( resumes > TAP_DROP_RESUMES )); then
      log "  ok: tap-drop landed on member $TAP_DROP_TARGET — gap resumed at the hole ($TAP_DROP_RESUMES -> $resumes)"
    else
      log "  INVARIANT FAIL: tap-drop on member $TAP_DROP_TARGET produced NO gap episode (resumes stuck at $resumes) — fault did not land"; fail=1
    fi
    if (( rewalks > TAP_DROP_REWALKS )); then
      log "  INVARIANT FAIL: member $TAP_DROP_TARGET fell back to a chain re-walk ($TAP_DROP_REWALKS -> $rewalks) — expected a resume"; fail=1
    fi
    TAP_DROP_TARGET=""
  fi
  # (d) fail-fast on media-driver loss, as observed by this round's check_driver_loss_failfast (kill
  #     faults only; the flag stays 0 for rounds that never killed a member). Reset either way so a
  #     violation is attributed to the round that caused it.
  if [[ "$DRIVER_LOSS_FAIL" == "1" ]]; then
    if [[ "$DRIVER_LOSS_STRICT" == "1" ]]; then
      fail=1
    else
      log "  (driver-loss violation above not failing the round — DRIVER_LOSS_STRICT=0)"
    fi
  fi
  DRIVER_LOSS_FAIL=0
  # (e) every member this round's fault restarted came back, as observed by assert_restarted. Reset
  #     either way, same as (d), so a violation is attributed to the round that caused it.
  [[ "$RESTART_FAIL" == "1" ]] && fail=1
  RESTART_FAIL=0
  # (f) the tap-stall target self-terminated, and did it for the RIGHT reason, as observed by
  #     fault_tap_stall. Its two INVARIANT FAIL branches used to only log — they set no flag, so a node
  #     that kept sequencing history it could not record printed a failure and the round still PASSED.
  [[ "$TAP_STALL_FAIL" == "1" ]] && fail=1
  TAP_STALL_FAIL=0
  # (g) a kill-leader round produced a GENUINE failover, as observed by fault_kill_leader.
  [[ "$FAILOVER_FAIL" == "1" ]] && fail=1
  FAILOVER_FAIL=0
  # (h) The rigorous safety property (gap-free, monotone globalSeqNo) is asserted ONCE at end of run by
  #     verify_sequence below — decoding it every round would re-dump the whole recording each time.
  return $fail
}

# ── End-of-run safety oracle: verify EVERY node's tap ────────────────────────────
# The real safety property of the sequencer: the SEQUENCED stream (out of the cluster — sbe-sequenced,
# schema 202, carrying globalSeqNo; ingress into the cluster is unsequenced and has none) must be a
# gap-free, strictly-monotone run 1,2,3,…,N — and EVERY node must have recorded the identical run
# (Raft replicates the same state to all). So decode all three members' archives and assert both.
#
# A killed/restarted node ROTATES its tap: a pre-kill partial recording (globalSeqNo 1..K) plus a
# post-restart recording that re-records 1..N from scratch (recovery is always full-log replay from
# gseq 1 — there are no snapshots). So each RECORDING is checked independently and reset at its
# "[Catalog] Recording ID" boundary — NOT concatenated (that would read the K→1 restart as a regression).
# The co-resident raft consensus-log recording is schema 111, which the 202 spec can't decode, so it
# yields zero globalSeqNo and is skipped. The node's high-water mark is the max globalSeqNo it recorded;
# all nodes' high-water marks must match (convergence).
verify_sequence() {
  local spec="build/generated/sources/sbe/main/java/sbe-sequenced.sbeir" m rc=0
  [[ -f "$spec" ]] || { log "SAFETY: skipped (no $spec — run ./gradlew generateSequencedSbe)"; return 0; }
  # Quiesce first: stop the background load and let the last sequenced messages replicate to every node.
  # That alone isn't enough for a stationary stream, though: the 1 Hz Tick (the cluster clock) keeps
  # advancing globalSeqNo forever by design, background load or not, and the three archives are dumped
  # sequentially (each a multi-second decode of thousands of frames) — so without freezing the cluster,
  # a tick or two legitimately lands between reading member 0's archive and member 2's, and convergence
  # fails on a phantom 1-frame "divergence" that's really just clock drift across a non-atomic snapshot.
  # SIGSTOP every member right before dumping: the archive is in-process, so pausing the JVM pauses the
  # tick generator with it, giving a true stationary snapshot. Already-flushed archive files on disk are
  # unaffected by a stopped process. cleanup() SIGCONTs everything before killing it, same as the
  # fault_pause_node path, so nothing is left stopped on exit.
  kill "${LOAD_PID:-}" 2>/dev/null; pkill -f fix_test_server 2>/dev/null; LOAD_PID=""
  sleep 3
  log "  pausing the cluster for an atomic snapshot…"
  for m in 0 1 2; do kill -STOP "${SEQ_PIDS[$m]:-0}" 2>/dev/null; done
  sleep 0.5   # let any in-flight archive write land before reading
  local -a highwater=("" "" "")
  for m in 0 1 2; do
    local archive="${BASE_DIR}/archive-${m}"
    [[ -f "$archive/archive.catalog" ]] && ./src/main/scripts/sbe-log-printer.sh "$spec" "$archive" \
      > "$LOG_DIR/sequenced-dump-$m.txt" 2>&1 || { log "  member $m: no recording at $archive"; rc=1; continue; }
    local hw
    hw=$(awk -v member="$m" '
      /\[Catalog\] Recording ID/ { expected=0; next }           # new recording -> reset (rotated tap)
      /"globalSeqNo"/ {
        s=$0; gsub(/[^0-9]/, "", s); n=s+0; total++
        if (expected==0) {                                       # first sequenced msg in THIS recording
          if (n!=1) { printf "  member %s SAFETY FAIL: recording starts at globalSeqNo %d, expected 1\n", member, n > "/dev/stderr"; exit 1 }
        } else if (n != expected+1) {
          printf "  member %s SAFETY FAIL: gap/regression — expected %d, got %d\n", member, expected+1, n > "/dev/stderr"; exit 1
        }
        expected=n; if (n>max) max=n
      }
      END { if (total==0) { printf "  member %s SAFETY FAIL: no sequenced messages decoded\n", member > "/dev/stderr"; exit 1 } print max }
    ' "$LOG_DIR/sequenced-dump-$m.txt") || { rc=1; continue; }
    highwater[$m]="$hw"
    log "  member $m: tap gap-free & monotone, high-water globalSeqNo=$hw"
  done
  for m in 0 1 2; do kill -CONT "${SEQ_PIDS[$m]:-0}" 2>/dev/null; done
  # Convergence: every node must have recorded the same final globalSeqNo.
  if [[ "$rc" == 0 && "${highwater[0]}" == "${highwater[1]}" && "${highwater[1]}" == "${highwater[2]}" ]]; then
    log "  convergence OK: all 3 nodes recorded globalSeqNo 1..${highwater[0]}"
  else
    log "  SAFETY FAIL: nodes diverged (high-water 0=${highwater[0]} 1=${highwater[1]} 2=${highwater[2]})"; rc=1
  fi
  return $rc
}

# ── Chaos loop ───────────────────────────────────────────────────────────────────
PASSED=0
for (( round=1; round<=ROUNDS; round++ )); do
  L="$(wait_for_leader)"; [[ -z "$L" ]] && { log "round $round: cluster lost leadership before fault — ABORT"; break; }
  fault="${FAULTS[$(( RANDOM % ${#FAULTS[@]} ))]}"
  FAULT_HISTORY+=("round $round: $fault (leader was $L)")
  log "── round $round/$ROUNDS ──"
  "$fault" "$L"
  sleep "$STEADY_STATE_SECS"
  if check_invariants; then
    log "round $round: PASS"; PASSED=$((PASSED+1))
  else
    log "round $round: FAIL"
    echo ""; echo "=== CHAOS FAILED at round $round ==="
    printf '  %s\n' "${FAULT_HISTORY[@]}"
    echo "  reproduce with: SEED=$SEED ROUNDS=$ROUNDS $0"
    exit 1
  fi
done

echo ""
log "verifying sequenced-stream integrity across all node tap recordings…"
if verify_sequence; then
  echo "=== CHAOS SURVIVED: $PASSED/$ROUNDS liveness rounds + sequence intact (SEED=$SEED) ==="
else
  echo "=== CHAOS: $PASSED/$ROUNDS liveness rounds passed, but SEQUENCE INTEGRITY FAILED (SEED=$SEED) ==="
  echo "  reproduce with: SEED=$SEED ROUNDS=$ROUNDS $0"
  exit 1
fi
