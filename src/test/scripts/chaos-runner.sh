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

# ── Helpers ─────────────────────────────────────────────────────────────────────
start_seq() {  # start_seq <memberId> — append so leadership history survives restarts (last isLeader= wins)
  local m="$1"
  # PHIXERON_FAULT_INJECTION arms the tap-recording fault fault_tap_stall triggers by file (the archive is
  # in-process, so it cannot be stalled from outside); inert until that file appears.
  PHIXERON_FAULT_INJECTION=1 \
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" >> "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
}
# NB: a bash loop's exit status is that of the last command in its body, so we MUST end with an explicit
# `return 0` — otherwise the trailing `((W>60))` (false, status 1) would make a SUCCESSFUL wait report failure.
wait_running() { local m="$1" W=0; until grep -q "Running" "$LOG_DIR/seq-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && return 1; done; return 0; }

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
wait_for_leader() { local W=0 L; while :; do L="$(current_leader)"; [[ -n "$L" ]] && { echo "$L"; return 0; }; sleep 0.5; W=$((W+1)); ((W>120)) && { echo ""; return 1; }; done; }

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
for m in 0 1 2; do W=0; until grep -q "serving replay" "$LOG_DIR/replayer-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && break; done; done

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
W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "consumer never caught up"; exit 1; }; done

# OrderExecClient replica on members 1 and 2 too (see start-three-node-cluster.sh): sendNewExecutionReport
# only fires on the replica CO-LOCATED WITH THE CURRENT LEADER (OrderExecClient.cpp, m_replayer.isCaughtUp()
# && currentLeaderMemberId()==m_nodeMemberId — one fill per order, not one per replica). Bring-up always
# elects the initial leader from members 1/2 before member 0 even joins (see below), so without a replica on
# every node NO replica is ever positioned to answer a NewOrderSingle with an ExecutionReport, and every FIX
# round-trip probe hangs waiting for one. Plain replicas: no latency stats, no fault injection (those stay
# unique to the member-0 observation consumer above).
for m in 1 2; do
  PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
    stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$LOG_DIR/orderexec-$m.log" 2>&1 &
  EXTRA_CONSUMER_PIDS[$m]=$!
done
for m in 1 2; do W=0; until grep -q "following live" "$LOG_DIR/orderexec-$m.log" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN orderexec-$m never caught up"; break; }; done; done

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
W=0; until nc -z 127.0.0.1 "$FIX_TCP_PORT" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>40)) && { echo "gateway $FIX_TCP_PORT not up"; exit 1; }; done
# The TCP port alone isn't "ready to Logon": the gateway gates on EndBasicData (a client that connects
# earlier just queues in the listen backlog and eventually times out) — see start-three-node-cluster.sh.
W=0; until grep -q "Basic data loaded" "$FIX_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "gateway never saw EndBasicData — its logon gate is still shut"; exit 1; }; done

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
W=0; until grep -q "Caught up to live stream" "$STANDBY_FIX_LOG" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>60)) && { echo "standby gateway never caught up"; exit 1; }; done
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
    sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN replayer-$m not serving after restart"; break; }
  done
  PHIXERON_BASICDATA_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
    PHIXERON_REPLAYER_CLIENT_ID=3 PHIXERON_BASICDATA_EGRESS_ENDPOINT="localhost:$(basicdata_egress_port "$m")" \
    stdbuf -oL -eL "$BUILD_DIR/BasicDataClient" > "$LOG_DIR/basicdata-$m.log" 2>&1 &
  BASICDATA_PIDS[$m]=$!
  if [[ "$m" == "$CN" ]]; then
    start_consumer
    W=0; until grep -q "following live" "$CONSUMER_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN consumer not caught up after restart"; break; }
    done
    start_gateway
    W=0; until nc -z 127.0.0.1 "$FIX_TCP_PORT" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W>40)) && { log "  WARN gateway port $FIX_TCP_PORT not up after restart"; break; }
    done
    W=0; until grep -q "Basic data loaded" "$FIX_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN gateway never saw EndBasicData after restart"; break; }
    done
  else
    PHIXERON_ORDER_EXEC_AERON_DIR="${TMPDIR}phixeron-seq-aeron-${m}" PHIXERON_NODE_MEMBER_ID="$m" \
      stdbuf -oL -eL "$BUILD_DIR/OrderExecClient" > "$LOG_DIR/orderexec-$m.log" 2>&1 &
    EXTRA_CONSUMER_PIDS[$m]=$!
    W=0; until grep -q "following live" "$LOG_DIR/orderexec-$m.log" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN orderexec-$m not caught up after restart"; break; }
    done
  fi
  if [[ "$m" == "$STANDBY_MEMBER" ]]; then
    start_standby_gateway
    W=0; until grep -q "Caught up to live stream" "$STANDBY_FIX_LOG" 2>/dev/null; do
      sleep 0.5; W=$((W+1)); ((W>60)) && { log "  WARN standby gateway not caught up after restart"; break; }
    done
  fi
}

fault_kill_leader() {  # crash the leader -> real Raft failover -> restore it as a follower
  local L="$1" T; T="$(target_leader "$L")"   # every member is a legal kill target, so T is always the actual leader
  log "FAULT kill-leader: member $T"; kill "${SEQ_PIDS[$T]}" 2>/dev/null
  local W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W>50)) && break; done  # let it actually die
  # Wait for a DIFFERENT member to win the election (a genuine failover). Restarting T too fast — before
  # the survivors elect — lets T just bounce and reclaim leadership, which half-wedges the cluster instead
  # of failing over. (If we killed a follower, current_leader is unchanged and this returns immediately.)
  local NL="" W2=0
  until NL="$(current_leader)"; [[ -n "$NL" && "$NL" != "$T" ]]; do sleep 0.5; W2=$((W2+1)); ((W2>120)) && break; done
  log "  new leader: member ${NL:-<none>}"
  sleep 3                                                       # let the new leader settle
  start_seq "$T"; wait_running "$T" || log "  WARN member $T did not restart"
  sleep 2                                                       # let the restored member rejoin (full-log replay)
  restart_colocated_apps "$T"
}
fault_kill_follower() {  # crash a follower -> should be transparent (quorum holds) -> restart it
  local L="$1" F; F="$(a_follower "$L")"
  log "FAULT kill-follower: member $F"; kill "${SEQ_PIDS[$F]}" 2>/dev/null; sleep 1
  start_seq "$F"; wait_running "$F" || log "  WARN member $F did not restart"
  restart_colocated_apps "$F"
}
fault_pause_node() {  # SIGSTOP a killable node (GC-pause / stall simulation: socket stays half-open) then SIGCONT
  local L="$1" P; P=$(( RANDOM % 2 == 0 ? $(target_leader "$L") : $(a_follower "$L") ))
  log "FAULT pause-node: SIGSTOP member $P for 3s"; kill -STOP "${SEQ_PIDS[$P]}" 2>/dev/null
  sleep 3; kill -CONT "${SEQ_PIDS[$P]}" 2>/dev/null; log "  SIGCONT member $P"
}
fault_tap_drop() {  # drop one live tap frame at the consumer -> gap -> re-walk recovery (SIGUSR1 path)
  log "FAULT tap-drop: SIGUSR1 consumer (arm one-frame live-tap drop)"; kill -USR1 "$CONSUMER_PID" 2>/dev/null
}
fault_tap_stall() {  # kill a node's local tap recording -> it must notice within a tick and TERMINATE ITSELF
  # The one fault the node is expected to answer by dying: a node that cannot record its own tap can no
  # longer serve the history it is responsible for, so it exits non-zero (SequencerService.fatalTapFailure)
  # and is restarted, rebuilding its recording over the full-log replay. Aimed at a follower — failover is
  # already covered by fault_kill_leader, and this asserts the self-termination, not the election.
  local L="$1" T; T="$(a_follower "$L")"
  log "FAULT tap-stall: stop member $T's tap recording (expect FATAL + self-termination)"
  touch "$BASE_DIR/cluster-$T/tap-stall-fault"
  local W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W>50)) && break; done
  if kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; then
    log "  INVARIANT FAIL: member $T kept running for 10s with no tap recording"
    # Still wait for it to actually die before restarting below, or the restart hits its own archive
    # mark file ("active mark file detected") and the round after this one fails for the wrong reason.
    kill "${SEQ_PIDS[$T]}" 2>/dev/null
    W=0; while kill -0 "${SEQ_PIDS[$T]}" 2>/dev/null; do sleep 0.2; W=$((W+1)); ((W>50)) && break; done
  else
    grep -q "FATAL:.*terminating this node" "$LOG_DIR/seq-$T.log" \
      && log "  ok: member $T detected the dead recording and terminated" \
      || log "  INVARIANT FAIL: member $T died without the FATAL tap-recording line"
  fi
  rm -f "$BASE_DIR/cluster-$T/tap-stall-fault"   # else the restarted node re-arms it immediately
  sleep 2
  start_seq "$T"; wait_running "$T" || log "  WARN member $T did not restart"
  sleep 2                                                       # let the restored member rejoin (full-log replay)
  restart_colocated_apps "$T"
}
# Targeted mode: set ONLY_FAULT=<fn> (e.g. fault_kill_leader) to run just that fault every round,
# for deterministically exercising one failure path in-loop rather than seed-hunting for it.
FAULTS=(fault_kill_leader fault_kill_follower fault_pause_node fault_tap_drop fault_tap_stall)
[[ -n "${ONLY_FAULT:-}" ]] && FAULTS=("$ONLY_FAULT")

# NOTE — heavier / platform-specific faults intentionally left as seams, not enabled by default:
#   * kill aeronmd (destructive: the co-located C++ clients lose their media driver; needs a client restart)
#   * network delay/loss/partition — Linux uses `tc qdisc ... netem`; macOS uses dnctl/pfctl (dummynet) and
#     needs root. This host is darwin, so netem is NOT wired up here; add it under an OS+root guard for CI-on-Linux.

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
  # (c) safety proxy: the consumer process is still up and following live (i.e. delivering in order, not wedged)
  if kill -0 "$CONSUMER_PID" 2>/dev/null && grep -q "following live" "$CONSUMER_LOG"; then
    log "  ok: consumer alive & following live (re-walks so far: $(grep -c 're-walking the recording chain' "$CONSUMER_LOG"))"
  else
    log "  INVARIANT FAIL: consumer down or not following live"; fail=1
  fi
  # (d) The rigorous safety property (gap-free, monotone globalSeqNo) is asserted ONCE at end of run by
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
