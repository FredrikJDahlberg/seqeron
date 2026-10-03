#!/usr/bin/env bash
# snapshot-test.sh — application snapshots end to end (doc/snapshot.md), on a three-node cluster with the
# TestGateway pair taking part in rounds every 2 s: GW-T-A on member 0, GW-T-B on member 1, each restoring
# from its own node's Replayer.
#
# TestGateway's state is the cluster's view of its connections and a count of the markers sequenced under
# its sourceId, and its caught-up line reports both. Every line the client sends is one marker and every
# client connection closes before the next step, so the expected state is known at each step: an instance
# that restored must report exactly it. Every instance also compares its own serialization with each
# round's sequenced SnapshotEnd and fences on a difference (spec §16 A-7), so a run with no FENCED line has
# compared the restored state plus the tail against the publisher's, round after round.
#
#   1. Both instances up with snapshots; load through the active one; rounds complete.
#   2. The standby restarts: it restores, reports the expected state, and compares later rounds.
#   3. The active one is killed: the restored instance takes over and serves.
#   4. The killed one returns passive: it holds nothing until the active one is killed, then restores,
#      catches up and serves.
#   5. The other returns as a hot standby, restoring from rounds the passive-turned-active one published.
#
# PASS iff every step's restore and state check holds, every round trip is answered, the round counter
# advances on both nodes, and no instance is fenced. Needs ./gradlew uberJar :seqeron-service:compileTestJava.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
TEST_CLASSES="seqeron-service/build/classes/java/test"
GW_CP="$JAR:$TEST_CLASSES"
TOPOLOGY="seqeron-service/src/test/resources/topology-test-snapshot.xml"
LOG_DIR="logs/snapshot"
SOURCE_ID=9
NODE_START_TIMEOUT_SECS="${NODE_START_TIMEOUT_SECS:-30}"
APP_CATCHUP_TIMEOUT_SECS="${APP_CATCHUP_TIMEOUT_SECS:-30}"
ROUND_TIMEOUT_SECS="${ROUND_TIMEOUT_SECS:-30}"
TAKEOVER_TIMEOUT_SECS="${TAKEOVER_TIMEOUT_SECS:-30}"

[[ -f "$TEST_CLASSES/org/limitless/seqeron/tools/TestGateway.class" ]] \
  || { echo "missing TestGateway in $TEST_CLASSES — run ./gradlew :seqeron-service:compileTestJava"; exit 1; }

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

declare -a SEQ_PIDS
GW_PIDS=("" "")          # by member: GW-T-A on 0, GW-T-B on 1
GW_RUNS=(0 0)            # each member's gateway log is numbered per launch
FAILURES=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
check() {  # check <description> <command...>
  local what="$1"; shift
  if "$@"; then log "  ok   $what"; else log "  FAIL $what"; FAILURES=$((FAILURES+1)); fi
}

start_seq() {  # start_seq <memberId>
  local m="$1"
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
}

gateway_name() { [[ "$1" == 0 ]] && echo "GW-T-A" || echo "GW-T-B"; }
gateway_log() { echo "$LOG_DIR/gateway-$(gateway_name "$1")-${GW_RUNS[$1]}.log"; }

start_gateway() {  # start_gateway <memberId> <passive: true|false>
  local m="$1"
  GW_RUNS[$m]=$(( ${GW_RUNS[$m]} + 1 ))
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$m" -Dprobe.clientId=10 -Dprobe.gatewayName="$(gateway_name "$m")" \
       -Dprobe.listenPort="$(test_gateway_port "$m")" -Dprobe.snapshot=true -Dprobe.passive="$2" \
       -Dprobe.sourceId="$SOURCE_ID" -cp "$GW_CP" org.limitless.seqeron.tools.TestGateway serve \
       > "$(gateway_log "$m")" 2>&1 &
  GW_PIDS[$m]=$!
  wait_for_log "$(gateway_log "$m")" "Caught up" "$APP_CATCHUP_TIMEOUT_SECS" \
    || { log "$(gateway_name "$m") never caught up (see $(gateway_log "$m"))"; exit 1; }
}

stop_gateway() {  # stop_gateway <memberId>
  local m="$1" W=0
  kill "${GW_PIDS[$m]}" 2>/dev/null
  while kill -0 "${GW_PIDS[$m]}" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>20)) && break; done
  GW_PIDS[$m]=""
}

# The activation lines are a state assignment, so the last one wins.
is_active() { [[ "$(grep -o "is now active\|is now standby" "$(gateway_log "$1")" 2>/dev/null | tail -1)" == "is now active" ]]; }

wait_active() {  # wait_active <memberId>
  local W=0
  until is_active "$1" && grep -q "gate OPEN" "$(gateway_log "$1")"; do
    sleep 0.5; W=$((W+1)); ((W > TAKEOVER_TIMEOUT_SECS * 2)) && return 1
  done
  return 0
}

active_member() { for m in 0 1; do [[ -n "${GW_PIDS[$m]}" ]] && is_active "$m" && { echo "$m"; return; }; done; echo ""; }

roundtrip() {  # roundtrip <memberId> <lines>
  java "${JAVA_OPTS[@]}" -Dprobe.listenPort="$(test_gateway_port "$1")" -Dprobe.count="$2" \
       -cp "$GW_CP" org.limitless.seqeron.tools.TestGateway client >> "$LOG_DIR/client.log" 2>&1
}

# The round of source 9's latest snapshot that member <m>'s Replayer has indexed, 0 for none.
round_of() {
  local value
  value=$(CLUSTERCTL_MEMBER_ID="$1" seqeron-service/src/main/scripts/clusterctl.sh counters 2>/dev/null \
            | grep "snapshotRound source=$SOURCE_ID " | grep -oE "[0-9]+ *$" | tr -d ' ')
  echo "${value:-0}"
}

# Waits until member <m>'s index is <n> rounds past where it is now: a round cut after the last load.
wait_rounds() {  # wait_rounds <memberId> <n>
  local target=$(( $(round_of "$1") + $2 )) W=0
  until (( $(round_of "$1") >= target )); do
    sleep 0.5; W=$((W+1)); ((W > ROUND_TIMEOUT_SECS * 2)) && return 1
  done
  return 0
}

caught_up_with() {  # caught_up_with <memberId> <markers>: the last caught-up line reports this state
  grep "Caught up" "$(gateway_log "$1")" | tail -1 | grep -q "0 connection(s) open, $2 marker(s) sequenced"
}
restores() { grep -c "restoring source $SOURCE_ID from round" "$(gateway_log "$1")"; }
no_fence() { ! grep -q "FENCED" "$LOG_DIR"/gateway-*.log; }

cleanup() {
  pkill -f "TestGateway client" 2>/dev/null
  for p in "${GW_PIDS[@]}" "${SEQ_PIDS[@]+"${SEQ_PIDS[@]}"}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

pkill -9 -f "sequencer.memberId" 2>/dev/null; pkill -9 -f "probe.gatewayName" 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}/seqeron-seq-aeron-0" "${TMP_DIR}/seqeron-seq-aeron-1" \
       "${TMP_DIR}/seqeron-seq-aeron-2" 2>/dev/null

# ── 0. The cluster and the topology ───────────────────────────────────────────────
for m in 0 1 2; do start_seq "$m"; done
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "serving replay" "$NODE_START_TIMEOUT_SECS" || { log "member $m not up"; exit 1; }
done
seqeron-service/src/main/scripts/clusterctl.sh load-topology "$TOPOLOGY" > "$LOG_DIR/load-topology.log" 2>&1 \
  || { log "load-topology failed (see $LOG_DIR/load-topology.log)"; exit 1; }
log "cluster up, topology loaded: GW-T-A on member 0, GW-T-B on member 1, rounds every 2 s"

# ── 1. Both instances with snapshots; load; rounds ────────────────────────────────
start_gateway 0 false; start_gateway 1 false
W=0; until [[ -n "$(active_member)" ]]; do
  sleep 0.5; W=$((W+1)); ((W > TAKEOVER_TIMEOUT_SECS * 2)) && { log "no instance was designated active"; exit 1; }
done
ACTIVE=$(active_member); STANDBY=$((1 - ACTIVE)); MARKERS=0
log "1. $(gateway_name "$ACTIVE") active, $(gateway_name "$STANDBY") standby"
check "40 lines round-trip through the active instance" roundtrip "$ACTIVE" 40; MARKERS=$((MARKERS+40))
check "40 more" roundtrip "$ACTIVE" 40; MARKERS=$((MARKERS+40))
check "rounds complete after the load (member $STANDBY's index)" wait_rounds "$STANDBY" 2
check "no instance fenced" no_fence

# ── 2. The standby restarts and restores ──────────────────────────────────────────
log "2. restarting $(gateway_name "$STANDBY")"
stop_gateway "$STANDBY"; start_gateway "$STANDBY" false
check "it restored a snapshot" test "$(restores "$STANDBY")" -eq 1
check "it caught up holding $MARKERS markers and no connection" caught_up_with "$STANDBY" "$MARKERS"
check "20 lines round-trip through the active instance" roundtrip "$ACTIVE" 20; MARKERS=$((MARKERS+20))
check "rounds complete, the restored instance comparing them" wait_rounds "$STANDBY" 2
check "no instance fenced" no_fence

# ── 3. Failover onto the restored instance ────────────────────────────────────────
log "3. killing the active $(gateway_name "$ACTIVE")"
stop_gateway "$ACTIVE"
check "$(gateway_name "$STANDBY") takes over" wait_active "$STANDBY"
check "10 lines round-trip through it" roundtrip "$STANDBY" 10; MARKERS=$((MARKERS+10))
check "it publishes rounds (member $STANDBY's index)" wait_rounds "$STANDBY" 2
PASSIVE=$ACTIVE; ACTIVE=$STANDBY

# ── 4. Passive: nothing held until activated ──────────────────────────────────────
log "4. $(gateway_name "$PASSIVE") returns passive"
start_gateway "$PASSIVE" true
check "it holds no state while passive" caught_up_with "$PASSIVE" 0
check "rounds complete while it is passive" wait_rounds "$PASSIVE" 2
log "   killing the active $(gateway_name "$ACTIVE")"
stop_gateway "$ACTIVE"
check "$(gateway_name "$PASSIVE") takes over" wait_active "$PASSIVE"
check "it restored again on activation" test "$(restores "$PASSIVE")" -eq 2
check "it caught up holding $MARKERS markers and no connection" caught_up_with "$PASSIVE" "$MARKERS"
check "10 lines round-trip through it" roundtrip "$PASSIVE" 10; MARKERS=$((MARKERS+10))
ACTIVE=$PASSIVE; STANDBY=$((1 - ACTIVE))

# ── 5. A hot standby restores from the passive-turned-active instance's rounds ─────
check "rounds complete after the load (member $ACTIVE's index)" wait_rounds "$ACTIVE" 2
log "5. $(gateway_name "$STANDBY") returns as a hot standby"
start_gateway "$STANDBY" false
check "it restored a snapshot" test "$(restores "$STANDBY")" -eq 1
check "it caught up holding $MARKERS markers and no connection" caught_up_with "$STANDBY" "$MARKERS"
check "rounds complete, the restored instance comparing them" wait_rounds "$STANDBY" 2
check "no instance fenced" no_fence

echo ""
echo "=== RESULT ==="
echo "  markers sequenced          : $MARKERS"
echo "  latest round, member 0 / 1 : $(round_of 0) / $(round_of 1)"
grep -h "restoring source" "$LOG_DIR"/gateway-*.log | sed 's/^/    /'
if (( FAILURES == 0 )); then
  echo "SNAPSHOT TEST: PASS — every restore reached the expected state and no instance diverged"
  exit 0
fi
echo "SNAPSHOT TEST: FAIL — $FAILURES check(s) failed (logs in $LOG_DIR)"
exit 1
