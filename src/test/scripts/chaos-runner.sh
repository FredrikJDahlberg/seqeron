#!/usr/bin/env bash
# chaos-runner.sh — SKETCH. Randomized fault-injection loop against a live 3-node cluster.
#
# WHAT THIS IS (and is NOT):
#   This script CONDUCTS and CHECKS INVARIANTS. Each round it perturbs the running system with one
#   randomly-chosen fault, lets it heal, then asserts steady-state invariants (a leader exists, the
#   FIX path still round-trips, the consumer is still delivering in order). It is deliberately NOT a
#   latency/throughput measurement tool — that belongs in a purpose-built driver (SEQERON_LATENCY_STATS
#   already records post-consensus delivery latency; a load harness generates the arrival process). A
#   shell loop cannot make defensible tail-latency claims, so it does not try to. It kills, pauses,
#   drops, and probes — nothing it does produces a number you would put in a non-functional report.
#
# REPRODUCIBILITY: every run prints its SEED. Re-run with SEED=<n> to replay the exact fault sequence
#   (the whole point of chaos testing is a reproducing case, not noise). ROUNDS and STEADY_STATE_SECS
#   are env-overridable.
#
# JAVA ONLY — no C++ binary is built or launched. Consumer replicas are
#   ClusterProbe follow; the gateway pair under the faults is the core-owned TestGateway (GW-T-A/GW-T-B,
#   gatewaySourceId 9), which speaks no FIX and holds no session state — an elected active/standby producer
#   with a real listening socket and nothing else. It restores what retargeting this harness off the C++
#   edge removed, and it is the ONLY harness that runs a gateway pair under fault injection
#   (gateway-failover-test.sh drives a pair, but injects nothing).
#
# TOPOLOGY (borrowed from gap-recovery-test.sh for a deterministic initial leader): members 1 & 2 start
#   first so the initial leader is one of them; member 0 joins as a follower after. Member 0 (CN) hosts the
#   safety-oracle/tap-drop-target consumer (fault injection armed for the SIGUSR1 tap-drop); members 1 and 2
#   host a plain consumer replica each. All three members are legal fault targets; restart_colocated_apps
#   brings each member's co-located apps back up in place. LEADER_CHANGES controls the round count for a
#   dedicated ONLY_FAULT=fault_kill_leader run (every round is a genuine failover, since every member is a
#   legal kill target).
#
# The gateway list loaded is src/test/resources/topology-test-gateway.xml, and it names exactly the two
#   instances this script starts: a listed pair no process starts is designated, times out after
#   GATEWAY_ACTIVATION_TIMEOUT_MS, hands over to its standby and times out again, forever. Both gateways go
#   up immediately behind load-topology for that reason.
#
# PASS/FAIL: the loop runs ROUNDS rounds; a round FAILS if any steady-state invariant is violated after
#   the heal window. On first failure it stops and prints the fault history + SEED to reproduce.
set -uo pipefail

# A full run takes many minutes, and a host that sleeps mid-round takes the whole cluster down with it:
# Aeron's service-interval watchdog measures WALL clock, so on wake every client aborts at once and the
# round reads as a product failure (observed 2026-08-13: 'Clamshell Sleep' at 22:19, ~15 min gap, all
# three replicas dead). Re-exec under caffeinate so the run holds sleep off itself. A lid close can still
# force sleep — `sudo pmset -c disablesleep 1` is the harder lock. NO_CAFFEINATE=1 to skip.
if [[ -z "${NO_CAFFEINATE:-}" && "${CHAOS_CAFFEINATED:-}" != 1 ]] && command -v caffeinate >/dev/null 2>&1; then
  export CHAOS_CAFFEINATED=1
  exec caffeinate -dims "$BASH" "$0" "$@"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"

# ── Config ────────────────────────────────────────────────────────────────────
JAR="build/libs/seqeron-0.1.0-uber.jar"
# TestGateway is harness code and lives in :cluster's TEST source set, so it is in no jar — launched off
# the compiled test classes beside it.
TEST_CLASSES="build/classes/java/test"
GW_CP="$JAR:$TEST_CLASSES"
LOG_DIR="logs/chaos"
ROUNDS="${ROUNDS:-20}"
STEADY_STATE_SECS="${STEADY_STATE_SECS:-4}"     # heal window between injecting a fault and checking invariants
SEED="${SEED:-$RANDOM}"                           # export SEED=<n> to replay a run exactly
BACKGROUND_LOAD="${BACKGROUND_LOAD:-1}"          # 1 = keep a low-rate probe frame flow running under the chaos
LOAD_BATCH_FRAMES="${LOAD_BATCH_FRAMES:-25}"     # request lines per background-load batch
LOAD_BATCH_PAUSE_SECS="${LOAD_BATCH_PAUSE_SECS:-1}"   # pause between batches — a low steady rate
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

# Env-overridable, defaults unchanged: a shared CI runner puts three members, their embedded
# drivers and archives, a gateway pair and the consumer replicas on 4 vCPUs, and every one of these
# measures WALL clock — so the runner needs to raise them without editing the script.
LEADER_TIMEOUT_SECS="${LEADER_TIMEOUT_SECS:-30}"
ELECTION_TIMEOUT_SECS="${ELECTION_TIMEOUT_SECS:-15}"
NODE_START_TIMEOUT_SECS="${NODE_START_TIMEOUT_SECS:-15}"
APP_CATCHUP_TIMEOUT_SECS="${APP_CATCHUP_TIMEOUT_SECS:-30}"
PROC_EXIT_TIMEOUT_SECS="${PROC_EXIT_TIMEOUT_SECS:-10}"   # a signalled member actually dying: measured 0.2s
TAP_STALL_DEADLINE_SECS="${TAP_STALL_DEADLINE_SECS:-10}"
# Not a harness deadline — the CLUSTER's own session timeout, passed to every member below. It is the
# one Aeron timeout a slow host does lengthen: a gateway's keep-alive shares its duty cycle with a
# blocking ingress offer, so a failover that leaves ingress unconnected can starve keep-alives past
# the 1s default, and the pair fences itself on a cluster that is otherwise healthy.
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-1000}"
PAUSE_SECS=0.5

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMP_DIR}/seqeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"
CN=0            # observation / tap-drop-target consumer host — a fault target like any other member
# The gateway pair, one instance per member so a kill of either host is a genuine promotion. Rank 0 is
# GW-T-A, so a quiet run has A serving on member CN and B standing by on GW_B_MEMBER.
GW_A_MEMBER=$CN
GW_B_MEMBER=1
# The gatewayIds $TOPOLOGY names for the pair above; clusterctl addresses an instance by id, not by name.
GW_A_ID=10
GW_B_ID=11
GW_A_PORT="$(test_gateway_port 0)"
GW_B_PORT="$(test_gateway_port 1)"
TOPOLOGY="src/test/resources/topology-test-gateway.xml"

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
declare -a SEQ_PIDS REPLAYER_PIDS EXTRA_CONSUMER_PIDS GW_PIDS
CONSUMER_PID=""; LOAD_PID=""
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
  # SEQERON_FAULT_INJECTION arms the tap-recording fault fault_tap_stall triggers by file (the archive is
  # in-process, so it cannot be stalled from outside); inert until that file appears.
  SEQERON_FAULT_INJECTION=1 \
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -Dsequencer.sessionTimeoutMs="$SESSION_TIMEOUT_MS" \
       -jar "$JAR" >> "$LOG_DIR/seq-$m.log" 2>&1 &
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

alive() { kill -0 "${SEQ_PIDS[$1]:-0}" 2>/dev/null; }   # is member $1's SequencerServer process up (not SIGSTOPped-aware)

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
  kill "${LOAD_PID:-}" "${CONSUMER_PID:-}" 2>/dev/null
  pkill -f ClusterProbe 2>/dev/null
  pkill -f TestGateway 2>/dev/null   # the background-load loop's in-flight client outlives its subshell
  # ${arr[@]+"${arr[@]}"} — the bash 3.2 / set -u safe way to expand a possibly-empty array to nothing.
  for p in "${SEQ_PIDS[@]+"${SEQ_PIDS[@]}"}" "${REPLAYER_PIDS[@]+"${REPLAYER_PIDS[@]}"}" \
           "${EXTRA_CONSUMER_PIDS[@]+"${EXTRA_CONSUMER_PIDS[@]}"}" "${GW_PIDS[@]+"${GW_PIDS[@]}"}"; do
    kill "$p" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── Bring-up ─────────────────────────────────────────────────────────────────────
[[ -f "$JAR" ]] || { echo "missing $JAR — run ./gradlew uberJar"; exit 1; }
[[ -f "$TEST_CLASSES/org/limitless/seqeron/tools/TestGateway.class" ]] \
  || { echo "missing TestGateway in $TEST_CLASSES — run ./gradlew compileTestJava"; exit 1; }

# Idempotent pre-clean so back-to-back runs don't collide: SIGKILL any survivors, then WAIT for the
# member archive-control ports (Aeron binds these as UDP) to actually release.
# NB: SequencerServer launches via `java -jar "$JAR"`, so its command line contains NO "SequencerServer"
# substring — pkilling by class name misses it. Match the jar path (in both SequencerServer's `-jar` and
# ReplayerServer's `-cp` lines) and the -Dsequencer marker instead.
pkill -9 -f "$JAR" 2>/dev/null; pkill -9 -f "sequencer.memberId" 2>/dev/null
pkill -9 -f "probe.gatewayName" 2>/dev/null
rm -rf "$BASE_DIR" "${TMP_DIR}/seqeron-seq-aeron-0" "${TMP_DIR}/seqeron-seq-aeron-1" \
       "${TMP_DIR}/seqeron-seq-aeron-2" 2>/dev/null
W=0; while lsof -nP -iUDP:"$(archive_port 0)" -iUDP:"$(archive_port 1)" -iUDP:"$(archive_port 2)" 2>/dev/null \
  | grep -q java; do sleep 0.5; W=$((W+1)); ((W>20)) && { echo "UDP archive ports still held after 10s — stale cluster?"; exit 1; }; done

log "SEED=$SEED  ROUNDS=$ROUNDS  STEADY_STATE_SECS=$STEADY_STATE_SECS   (replay with SEED=$SEED)"
start_seq 1; start_seq 2
wait_running 1 && wait_running 2 || { echo "members 1/2 not up"; exit 1; }
wait_for_leader >/dev/null || { echo "no initial leader among 1/2"; exit 1; }
start_seq 0; wait_running 0 || { echo "member 0 not up"; exit 1; }

for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.seqeron.replayer.server.ReplayerServer > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
done
for m in 0 1 2; do W=0; until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && break; done; done

# Consumer on member 0: fault-injection ON (SIGUSR1 tap-drop) + latency stats (flushed on exit, not read here).
# Extracted into start_consumer (below) so restart_colocated_apps can relaunch it in place when member 0
# itself is a fault target — the same driver-death problem members 1/2's replicas have on restart.
CONSUMER_LOG="$LOG_DIR/consumer.log"
start_consumer() {
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -Dprobe.clientId=9 \
       -Dprobe.latencyStats=true -Dprobe.faultInjection=true \
       -cp "$JAR" org.limitless.seqeron.tools.ClusterProbe follow > "$CONSUMER_LOG" 2>&1 &
  CONSUMER_PID=$!
}
start_consumer
W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "consumer never caught up"; exit 1; }; done

# A consumer replica on members 1 and 2 too: every node's tap must have a reader, so a fault on any member
# has something co-located to take down with it and something to bring back. Latency stats stay unique to
# the member-0 observation consumer, but fault injection is armed on ALL THREE so fault_tap_drop can aim at
# whichever replica is currently leader-co-located. Defined as a function for the same reason start_consumer
# is: restart_colocated_apps must relaunch a replica with the SAME arguments, or a restarted one silently
# loses its arm and every later tap-drop aimed at it is a no-op.
start_replica() {  # start_replica <memberId>  (members 1/2; member 0's replica is start_consumer)
  local m="$1"
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$m" -Dprobe.clientId=9 -Dprobe.faultInjection=true \
       -cp "$JAR" org.limitless.seqeron.tools.ClusterProbe follow > "$LOG_DIR/consumer-$m.log" 2>&1 &
  EXTRA_CONSUMER_PIDS[$m]=$!
}
for m in 1 2; do start_replica "$m"; done
for m in 1 2; do W=0; until grep -q "following live" "$LOG_DIR/consumer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN consumer-$m never caught up"; break; }; done; done

# The gateway list is not reference data: `clusterctl load-topology` puts it in the ordered log, and the
# sequencer synthesizes the bootstrap GatewayActive behind its last row. Both instances go up right after,
# or the pair ping-pongs on GATEWAY_ACTIVATION_TIMEOUT_MS until one of them registers.
if ! src/main/scripts/clusterctl.sh load-topology "$TOPOLOGY" > "$LOG_DIR/load-topology.log" 2>&1; then
  echo "clusterctl load-topology failed — see $LOG_DIR/load-topology.log"; exit 1
fi

gateway_name() { [[ "$1" == "$GW_A_MEMBER" ]] && echo "GW-T-A" || echo "GW-T-B"; }
gateway_port() { [[ "$1" == "$GW_A_MEMBER" ]] && echo "$GW_A_PORT" || echo "$GW_B_PORT"; }
gateway_log()  { echo "$LOG_DIR/gateway-$(gateway_name "$1").log"; }
hosts_gateway() { [[ "$1" == "$GW_A_MEMBER" || "$1" == "$GW_B_MEMBER" ]]; }
# Truncates, like start_consumer/start_replica: gateway_status reads the LAST activation line, so a
# restarted instance must not inherit its predecessor's.
start_gateway() {  # start_gateway <memberId>
  local m="$1"
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$m" -Dprobe.clientId=10 \
       -Dprobe.gatewayName="$(gateway_name "$m")" -Dprobe.listenPort="$(gateway_port "$m")" \
       -cp "$GW_CP" org.limitless.seqeron.tools.TestGateway serve > "$(gateway_log "$m")" 2>&1 &
  GW_PIDS[$m]=$!
}
for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do start_gateway "$m"; done
for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do
  W=0; until grep -q "Caught up" "$(gateway_log "$m")" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "gateway on member $m never caught up"; exit 1; }
  done
done

# Which instance the log says is serving, and where. The activation lines are a state assignment, so the
# LAST one wins: an instance superseded and later promoted again reads as active, which is the point.
gateway_status() { grep -o "is now active\|is now standby" "$1" 2>/dev/null | tail -1 | grep -o "active\|standby"; }
active_gateway_port() {
  local m
  for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do
    [[ "$(gateway_status "$(gateway_log "$m")")" == "active" ]] && { echo "$(gateway_port "$m")"; return; }
  done
  echo ""
}
# One line through the gate, answered only if it round-tripped consensus: the accept gate observed from
# outside the process, which is the whole reason this pair has a socket.
gateway_roundtrip() {  # gateway_roundtrip <port> <lines> <log label>
  java "${JAVA_OPTS[@]}" -Dprobe.listenPort="$1" -Dprobe.count="$2" \
       -cp "$GW_CP" org.limitless.seqeron.tools.TestGateway client > "$LOG_DIR/gateway-$3.log" 2>&1
}

W=0; until [[ -n "$(active_gateway_port)" ]]; do
  sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "no gateway instance was designated active"; exit 1; }
done
log "cluster READY — consumer following live, gateway active on port $(active_gateway_port)"

# One operator handover before the faults start. `clusterctl activate` is the only path that submits a
# GatewayActivationRequested at ingress and waits for the GatewayActive the sequencer synthesizes behind
# it, and nothing else in these harnesses walks it. What it proves is an ordinary ingress submit making
# the full round trip — the thing that breaks when a leadership change is read as a dead session — so it
# runs here, where a pair is up and a promotion is observable from outside both processes.
gateway_id() { [[ "$1" == "$GW_A_MEMBER" ]] && echo "$GW_A_ID" || echo "$GW_B_ID"; }
# The standby is whichever instance the designation did not name: an instance that has never been
# superseded logs no state assignment of its own, so gateway_status reads empty for it rather than
# "standby".
active_gateway_member() {
  local m
  for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do
    [[ "$(gateway_status "$(gateway_log "$m")")" == "active" ]] && { echo "$m"; return; }
  done
  echo ""
}
ACTIVE_MEMBER="$(active_gateway_member)"
[[ -n "$ACTIVE_MEMBER" ]] || { echo "no active gateway instance to hand over from"; exit 1; }
if [[ "$ACTIVE_MEMBER" == "$GW_A_MEMBER" ]]; then STANDBY_MEMBER="$GW_B_MEMBER"; else STANDBY_MEMBER="$GW_A_MEMBER"; fi
if ! src/main/scripts/clusterctl.sh activate "$(gateway_id "$STANDBY_MEMBER")" > "$LOG_DIR/activate.log" 2>&1; then
  echo "clusterctl activate failed — see $LOG_DIR/activate.log"; exit 1
fi
W=0; until [[ "$(gateway_status "$(gateway_log "$STANDBY_MEMBER")")" == "active" ]]; do
  sleep 0.5; W=$((W+1))
  ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { echo "activate: $(gateway_name "$STANDBY_MEMBER") never took the role"; exit 1; }
done
log "operator handover OK — $(gateway_name "$STANDBY_MEMBER") active on port $(active_gateway_port)"

# Optional steady background frame flow so faults land on a system that is actually doing work.
# Batched rather than one endless submit, so a batch interrupted mid leader-failover simply ends and the
# next one reconnects. Backs off on failure instead of respawning in a hot spin.
# Through the ACTIVE GATE rather than straight at cluster ingress: the load then has to follow the gate
# across a promotion, which is the thing a gateway pair adds to this harness. The port is resolved per
# batch for the same reason. Backs off on failure instead of respawning in a hot spin.
start_background_load() {
  [[ "$BACKGROUND_LOAD" == "1" ]] || return 0
  ( while true; do
      port="$(active_gateway_port)"
      if [[ -z "$port" ]] || ! gateway_roundtrip "$port" "$LOAD_BATCH_FRAMES" load; then
        sleep 0.5
      else
        sleep "$LOAD_BATCH_PAUSE_SECS"
      fi
    done ) &
  LOAD_PID=$!
}
start_background_load

# ── Fault menu ───────────────────────────────────────────────────────────────────
# Each injector picks its own target relative to the CURRENT leader and heals the cluster back to full
# strength (so the next round starts from 3/3, never draining quorum across rounds). Every fault here is
# an in-process / signal mechanism already exercised by the existing single-shot tests.
# All three members, including CN, are legal fault targets — killing/pausing member 0 takes its co-located
# Replayer and observation consumer down too, and restart_colocated_apps brings them back (see below).
target_leader() { echo "$1"; }                                    # the current leader — always a legal kill target
a_follower()    { local l="$1" p; while :; do p=$(( RANDOM % 3 )); [[ "$p" != "$l" ]] && { echo "$p"; return; }; done; }  # a random non-leader among {0,1,2}
# Member 0's consumer is the observation one (CONSUMER_PID/consumer.log); 1 and 2 are plain
# replicas. Both are legal tap-drop targets, so resolve either by memberId.
replica_pid() { local m="$1"; [[ "$m" == "$CN" ]] && echo "${CONSUMER_PID:-}" || echo "${EXTRA_CONSUMER_PIDS[$m]:-}"; }
replica_log() { local m="$1"; [[ "$m" == "$CN" ]] && echo "$CONSUMER_LOG" || echo "$LOG_DIR/consumer-$m.log"; }
# A gap the consumer repaired by RESUMING its recording at the hole — the healthy recovery, and the proof
# that an armed tap-drop actually landed. Distinct from the re-walk fallback counted below.
count_tap_resumes() { grep -c 'tap gap: expected globalSeqNo' "$1" 2>/dev/null || true; }
count_tap_rewalks() { grep -c 're-walking the recording chain' "$1" 2>/dev/null || true; }
# The client's own "I am not serving" alarm (RecoveryStalled, ReplayerStreamReceiver::checkRecoveryProgress):
# recovery dispatched nothing for RECOVERY_PROGRESS_TIMEOUT_MS while not caught up. Unlike the one-shot
# "following live" marker this fires per episode, so it reads CURRENT state rather than latching at boot.
count_recovery_stalls() { grep -c 'recovery has dispatched nothing' "$1" 2>/dev/null || true; }

# ── Media-driver fail-fast assertions ────────────────────────────────────────────
# SequencerServer embeds its media driver (ClusteredMediaDriver, SequencerServer.java:191), so killing a
# member IS a media-driver kill for every client sharing that member's aeron dir. Every kill fault injects
# "driver dies, restarts at the same path"; what was missing is asserting on it, because
# restart_colocated_apps below relaunches these clients unconditionally and so guarantees the
# recovery the test should have been proving.
#
# The property under test is FAIL-FAST: a client that loses its driver must EXIT, non-zero, so a real
# supervisor restarts it. Staying alive is the bug — for an edge process it means a live listener in
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

# The gateway is the only client here with a LISTENING SOCKET in front of it, so it is the only one where
# failing to fail-fast is visible from outside the process: a client connects, is accepted, sends a line
# and hangs forever, because nothing behind the gate can reach the cluster any more. Checked before the
# exit assertion below, while the process is (wrongly) still up.
report_gateway_socket() {  # <port> <label> <pid>
  kill -0 "${3:-0}" 2>/dev/null || return 0
  nc -z 127.0.0.1 "$1" 2>/dev/null \
    && log "  DRIVER-LOSS FAIL: $2 is STILL ACCEPTING TCP on port $1 with no media driver — a client would connect and hang"
  return 0
}

check_driver_loss_failfast() {  # <memberId whose driver just died>
  local m="$1"
  assert_died_on_driver_loss "replayer-$m" "${REPLAYER_PIDS[$m]:-}"
  if [[ "$m" == "$CN" ]]; then
    assert_died_on_driver_loss "consumer" "${CONSUMER_PID:-}"
  else
    assert_died_on_driver_loss "consumer-$m" "${EXTRA_CONSUMER_PIDS[$m]:-}"
  fi
  if hosts_gateway "$m"; then
    report_gateway_socket "$(gateway_port "$m")" "$(gateway_name "$m")" "${GW_PIDS[$m]:-}"
    assert_died_on_driver_loss "$(gateway_name "$m")" "${GW_PIDS[$m]:-}"
  fi
  return 0
}

# A killed member's co-located ReplayerServer and consumer replica (member CN's being the observation
# consumer) share its embedded media driver (same aeron dir) and don't survive the member's
# restart: the driver dies with the SequencerServer process, and none of these clients reconnect to the
# fresh driver the restart creates at the same path — they just fault (DriverTimeoutException /
# "MediaDriver has been shutdown") and sit dead for the rest of the run. That leaves a permanent hole:
# e.g. if member $m later becomes leader, its dead consumer replica can't be the one that reads its tap —
# NewOrderSingle with an ExecutionReport (leader-only emission), so a later round's FIX round-trip probe
# hangs waiting for one that will never come.
restart_colocated_apps() {
  local m="$1" W=0
  check_driver_loss_failfast "$m"
  kill "${REPLAYER_PIDS[$m]:-0}" 2>/dev/null
  if [[ "$m" == "$CN" ]]; then
    kill "${CONSUMER_PID:-0}" 2>/dev/null
  else
    kill "${EXTRA_CONSUMER_PIDS[$m]:-0}" 2>/dev/null
  fi
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.seqeron.replayer.server.ReplayerServer > "$LOG_DIR/replayer-$m.log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
  until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN replayer-$m not serving after restart"; break; }
  done
  if [[ "$m" == "$CN" ]]; then
    start_consumer
    W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN consumer not caught up after restart"; break; }
    done
  else
    start_replica "$m"
    W=0; until grep -q "following live" "$LOG_DIR/consumer-$m.log" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN consumer-$m not caught up after restart"; break; }
    done
  fi
  # The gateway comes back as whatever the cluster now says it is: its peer was promoted the moment this
  # instance's cluster session closed, so a restarted one normally rejoins as the STANDBY. That asymmetry
  # accumulating across rounds is the point — it is what makes a later round's promotion go the other way.
  if hosts_gateway "$m"; then
    kill "${GW_PIDS[$m]:-0}" 2>/dev/null
    start_gateway "$m"
    W=0; until grep -q "Caught up" "$(gateway_log "$m")" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W > APP_CATCHUP_TIMEOUT_SECS * 2)) && { log "  WARN $(gateway_name "$m") not caught up after restart"; break; }
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
# (fault_tap_stall's self-termination included, SequencerServer.java), so the Archive gets a clean close.
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
  # Aimed at the replica CO-LOCATED WITH THE LEADER, not always member 0's: that node's tap is the busiest
  # (its own sequencer is the one publishing), so a gap episode there is the one under most pressure.
  # Member 0 is usually a follower — bring-up elects the initial leader from members 1/2 — so a fixed
  # target would exercise only the quiet case.
  local L="$1" T; T="$(target_leader "$L")"
  local pid; pid="$(replica_pid "$T")"
  TAP_DROP_TARGET="$T"
  TAP_DROP_LOG="$(replica_log "$T")"
  TAP_DROP_RESUMES="$(count_tap_resumes "$TAP_DROP_LOG")"
  TAP_DROP_REWALKS="$(count_tap_rewalks "$TAP_DROP_LOG")"
  log "FAULT tap-drop: SIGUSR1 member $T's replica (leader-co-located; arm one-frame live-tap drop)"
  kill -USR1 "$pid" 2>/dev/null || log "  WARN no live replica pid for member $T"
}
fault_tap_stall() {  # kill a node's local tap recording -> it must notice within a heartbeat and TERMINATE ITSELF
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

# NOTE — on killing the media driver: there is deliberately no fault_kill_aeronmd, and there is no
#   standalone aeronmd left to kill. Every process here is on a per-member driver embedded in its
#   SequencerServer (ClusteredMediaDriver at ${TMP_DIR}/seqeron-seq-aeron-<m>): ReplayerServer
#   (ReplayerServer.java:67) and every ClusterProbe attach there, and the probe's submit/ping runs attach
#   to member $CN's. So the real "driver dies and restarts at the same path" fault is ALREADY injected by
#   every kill fault above — asserted by check_driver_loss_failfast.
#
# NOTE — heavier / platform-specific faults intentionally left as seams, not enabled by default:
#   * network delay/loss/partition — Linux uses `tc qdisc ... netem`; macOS uses dnctl/pfctl (dummynet) and
#     needs root. This host is darwin, so netem is NOT wired up here; add it under an OS+root guard for CI-on-Linux.
#   * a standalone-driver kill becomes a genuinely distinct fault only if clients are ever moved off the
#     embedded drivers onto a shared aeronmd — driver dies, cluster node survives, which the embedded
#     topology cannot produce. Wire it then.

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
  # (b) liveness: one round trip through the whole path — submit a ProbeMarker to cluster ingress and
  # wait for its own sequenced echo off the co-located tap. Exit 0 iff ingress accepted it, consensus
  # committed it, and it came back. Its seqNo is a timestamp, so the background load's 1..N cannot be
  # mistaken for it.
  if java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -cp "$JAR" \
       org.limitless.seqeron.tools.ClusterProbe ping > "$LOG_DIR/probe.log" 2>&1; then
    log "  ok: ingress->consensus->tap round-trip probe passed"
  else
    log "  INVARIANT FAIL: round-trip probe failed (see $LOG_DIR/probe.log)"; fail=1
  fi
  # (b2) the GATE, which (b) says nothing about: exactly one instance of the pair must be serving, and a
  #      line put through its socket must come back — which it can only do by having been sequenced. The
  #      two are deliberately separate assertions: (b) passing while this fails is a healthy cluster with
  #      no usable edge, which is precisely the state the four gateway fences exist to avoid and the one
  #      that a demoted primary still latched open used to hide.
  local serving=0 m
  for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do
    [[ "$(gateway_status "$(gateway_log "$m")")" == "active" ]] && serving=$((serving+1))
  done
  if (( serving != 1 )); then
    log "  INVARIANT FAIL: $serving gateway instance(s) report active — the pair must have exactly one"; fail=1
  elif gateway_roundtrip "$(active_gateway_port)" 1 probe; then
    log "  ok: gateway round-trip through the active gate on port $(active_gateway_port)"
  else
    log "  INVARIANT FAIL: gateway round-trip failed on port $(active_gateway_port) (see $LOG_DIR/gateway-probe.log)"; fail=1
  fi
  # Both instances must be RUNNING, not merely one of them serving: a standby that died is a pair with no
  # promotion left in it, and every later round would still pass (b2) on the survivor alone.
  for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do
    kill -0 "${GW_PIDS[$m]:-0}" 2>/dev/null \
      || { log "  INVARIANT FAIL: $(gateway_name "$m") is not running (see $(gateway_log "$m"))"; fail=1; }
  done
  # (c) safety proxy: every co-located replica is still up and still SERVING.
  #     This used to grep the consumer log for "following live" — which is printed ONCE on the first
  #     replay->live transition and never again. It therefore matched the startup line forever and passed
  #     vacuously: a replica that later fell out of isCaughtUp() and stopped serving looked identical to a
  #     healthy one. That is not hypothetical — a wedged consumer is exactly what a per-poll request resend
  #     produced, and only the round-trip probe caught it, and then only because the wedged replica
  #     happened to be the leader-co-located one. No log grep
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
      log "  INVARIANT FAIL: member $m's consumer replica is not running"; cfail=1; continue
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
# The co-resident raft consensus-log recording decodes too (schema 111), but none of its messages carry
# a globalSeqNo, so it contributes nothing to the scan below. The node's high-water mark is the max globalSeqNo it recorded;
# all nodes' high-water marks must match (convergence).
verify_sequence() {
  local jar="build/libs/seqeron-0.1.0-uber.jar" m rc=0
  [[ -f "$jar" ]] || { log "SAFETY: skipped (no $jar — run ./gradlew uberJar)"; return 0; }
  # Quiesce first: stop the background load and let the last sequenced messages replicate to every node.
  # That alone isn't enough for a stationary stream, though: the 1 Hz ClusterHeartbeat (the cluster clock) keeps
  # advancing globalSeqNo forever by design, background load or not, and the three archives are dumped
  # sequentially (each a multi-second decode of thousands of frames) — so without freezing the cluster,
  # a heartbeat or two legitimately lands between reading member 0's archive and member 2's, and convergence
  # fails on a phantom 1-frame "divergence" that's really just clock drift across a non-atomic snapshot.
  # SIGSTOP every member right before dumping: the archive is in-process, so pausing the JVM pauses the
  # heartbeat generator with it, giving a true stationary snapshot. Already-flushed archive files on disk are
  # unaffected by a stopped process. cleanup() SIGCONTs everything before killing it, same as the
  # fault_pause_node path, so nothing is left stopped on exit.
  kill "${LOAD_PID:-}" 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
  pkill -f "TestGateway client" 2>/dev/null; LOAD_PID=""
  sleep 3
  # The gateways go first: SIGSTOPping the members stops the consensus modules too, and a 1s
  # sessionTimeoutNs means every gateway would fence on TIMEOUT mid-snapshot and fill its log with a
  # failure that is this script's doing, not the run's.
  for m in "$GW_A_MEMBER" "$GW_B_MEMBER"; do kill "${GW_PIDS[$m]:-0}" 2>/dev/null; done
  log "  pausing the cluster for an atomic snapshot…"
  for m in 0 1 2; do kill -STOP "${SEQ_PIDS[$m]:-0}" 2>/dev/null; done
  sleep 0.5   # let any in-flight archive write land before reading
  local -a highwater=("" "" "")
  for m in 0 1 2; do
    local archive="${BASE_DIR}/archive-${m}"
    [[ -f "$archive/archive.catalog" ]] && ./src/main/scripts/sbe-log-printer.sh "$archive" \
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
