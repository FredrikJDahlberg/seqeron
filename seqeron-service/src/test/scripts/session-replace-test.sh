#!/usr/bin/env bash
# Session-replacement test: a client whose cluster session is lost opens a new one, rather than fencing, both
# when it is the client that went quiet and when it is the cluster that went away, and its duty cycle keeps
# running while an attempt is pending; and a gateway standby that loses its session in the same outage as the
# active instance takes the role over rather than fencing too.
#
# The client is TestApplication, a co-located application on a follower: an Application may always replace its
# session (a designated gateway instance fences instead, doc/fault-tolerance.md §2.1). Each attempt tries its
# member's IPC ingress first, which a follower never answers, and its IPC timeout is set to IPC_CONNECT_TIMEOUT_MS,
# so every attempt spends that long pending while the cluster is healthy and the tap carries a heartbeat a second.
# TestApplication logs any heartbeat silence of 3 s or more, timed on dispatch: a replacement that blocked the
# duty cycle would log one at least as long as the IPC timeout, where the pause alone logs one about a second
# longer than itself.
#
# Sequence:
#   1. A three-node cluster; TestApplication on a member that does not lead catches up.
#   2. SIGSTOP the application for CLIENT_PAUSE_SECS, past the cluster's 1 s session timeout, and SIGCONT it.
#   3. SIGSTOP the other two members for QUORUM_PAUSE_SECS, so the application's member has no quorum: long
#      enough for the client's 5 s new-leader timeout to close its session, short enough for the members' own
#      10 s service interval to survive the pause. SIGCONT them.
#   4. Load the TestGateway pair: GW-T-A, which the bootstrap designates, on a paused member's driver, and GW-T-B,
#      the standby, beside the application. Pause the same two members again. The cluster times out both
#      sessions; GW-T-A had announced its activation on its own and fences, which promotes GW-T-B, whose own was
#      lost before it was designated, so it replaces it and announces on the new one (doc/fault-tolerance.md §2.1).
#
# PASS iff phases 1 and 2 logged a replacement, phase 1 logged no heartbeat silence of SILENCE_LIMIT_MS or more,
# the application is still running and unfenced, GW-T-B opened its gate after the pause and is unfenced, and
# every member is still up.
#
# Java only, on the members' embedded drivers. Needs ./gradlew uberJar :seqeron-service:compileTestJava.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
# TestApplication is harness code in the test source set, so it is in no jar.
TEST_CLASSES="seqeron-service/build/classes/java/test"
APP_CP="$JAR:$TEST_CLASSES"
LOG_DIR="logs/session-replace"
TOPOLOGY="seqeron-service/src/test/resources/topology-test-gateway.xml"
CLIENT_PAUSE_SECS="${CLIENT_PAUSE_SECS:-3}"
QUORUM_PAUSE_SECS="${QUORUM_PAUSE_SECS:-7}"
IPC_CONNECT_TIMEOUT_MS=8000   # well past CLIENT_PAUSE_SECS, well inside the 20 s the façade allows a replacement
SILENCE_LIMIT_MS=6000         # between the pause's silence and the IPC timeout's
REPLACE_TIMEOUT_SECS=40

[[ -f "$TEST_CLASSES/org/limitless/seqeron/tools/TestApplication.class" ]] \
  || { echo "missing TestApplication in $TEST_CLASSES — run ./gradlew :seqeron-service:compileTestJava"; exit 1; }

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqsr"
SNAPSHOT_DIR="${TMP_DIR}/seqeron-seqsr-snapshots"
CLUSTER_HOSTS="$(cluster_hosts_string 3)"

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f TestApplication 2>/dev/null
pkill -f TestGateway 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "$SNAPSHOT_DIR" "${TMP_DIR}"/seqeron-seq-aeron-{0,1,2} 2>/dev/null

APP_PID=""; GW_A_PID=""; GW_B_PID=""
declare -a SEQ_PIDS
cleanup() {
  kill -CONT "${SEQ_PIDS[@]:-}" 2>/dev/null
  kill "$APP_PID" "$GW_A_PID" "$GW_B_PID" "${SEQ_PIDS[@]:-}" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.hosts="$CLUSTER_HOSTS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "Running" 30 || { echo "seq $m not up"; exit 1; }
  wait_for_log "$LOG_DIR/seq-$m.log" "new leader is" 30 || { echo "seq $m saw no leader"; exit 1; }
done
LEADER=$(grep -ho "new leader is memberId=[0-9]" "$LOG_DIR/seq-0.log" | tail -1 | grep -o "[0-9]$")
[[ -n "$LEADER" ]] || { echo "no leader in $LOG_DIR/seq-0.log"; exit 1; }
AN=$(( (LEADER + 1) % 3 ))   # the application's member, a follower
OTHERS=()
for m in 0 1 2; do [[ "$m" != "$AN" ]] && OTHERS+=("${SEQ_PIDS[$m]}"); done
echo "cluster up; member $LEADER leads, the application runs on member $AN"

APP_LOG="$LOG_DIR/app.log"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$AN" -Dprobe.snapshotDir="$SNAPSHOT_DIR" \
     -Dprobe.ipcConnectTimeoutMs="$IPC_CONNECT_TIMEOUT_MS" -cp "$APP_CP" \
     org.limitless.seqeron.tools.TestApplication > "$APP_LOG" 2>&1 &
APP_PID=$!
wait_for_log "$APP_LOG" "Caught up" 30 || { echo "application never caught up"; exit 1; }
sleep 2

replaced() { grep -c "replaces the lost one" "$APP_LOG" 2>/dev/null; }
await_replaced() {  # await_replaced <count before>
  local W=0
  until (( $(replaced) > $1 )); do
    sleep 0.5; W=$((W+1)); ((W > REPLACE_TIMEOUT_SECS * 2)) && return 1
  done
  return 0
}

echo "phase 1: application paused for ${CLIENT_PAUSE_SECS}s"
BEFORE=$(replaced); PHASE1_LINE=$(wc -l < "$APP_LOG")
kill -STOP "$APP_PID"; sleep "$CLIENT_PAUSE_SECS"; kill -CONT "$APP_PID"
await_replaced "$BEFORE"; PHASE1=$(( $(replaced) - BEFORE ))
sleep 2
# The longest heartbeat silence phase 1 logged, the pause itself included.
SILENCE_MS=$(tail -n "+$((PHASE1_LINE + 1))" "$APP_LOG" | grep -o "resumed after [0-9]*ms" | grep -o "[0-9]*" \
  | sort -n | tail -1)
SILENCE_MS=${SILENCE_MS:-0}

echo "phase 2: members other than $AN paused for ${QUORUM_PAUSE_SECS}s"
BEFORE=$(replaced)
kill -STOP "${OTHERS[@]}"; sleep "$QUORUM_PAUSE_SECS"; kill -CONT "${OTHERS[@]}"
await_replaced "$BEFORE"; PHASE2=$(( $(replaced) - BEFORE ))
sleep 2

echo "phase 3: gateway pair; members other than $AN paused for ${QUORUM_PAUSE_SECS}s, GW-T-A's among them"
seqeron-service/src/main/scripts/clusterctl.sh load-topology "$TOPOLOGY" > "$LOG_DIR/load-topology.log" 2>&1 \
  || { echo "clusterctl load-topology failed — see $LOG_DIR/load-topology.log"; exit 1; }
start_gateway() {  # start_gateway <memberId> <name> <port index> — echoes the pid
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$1" -Dprobe.clientId=10 -Dprobe.gatewayName="$2" \
       -Dprobe.listenPort="$(test_gateway_port "$3")" -Dprobe.snapshot=true -Dprobe.snapshotDir="$SNAPSHOT_DIR/$2" \
       -cp "$APP_CP" org.limitless.seqeron.tools.TestGateway serve > "$LOG_DIR/gateway-$2.log" 2>&1 &
  echo $!
}
GW_A_PID=$(start_gateway "$LEADER" GW-T-A 0); GW_B_PID=$(start_gateway "$AN" GW-T-B 1)
wait_for_log "$LOG_DIR/gateway-GW-T-A.log" "gate OPEN" 30 || { echo "GW-T-A never opened its gate"; exit 1; }
wait_for_log "$LOG_DIR/gateway-GW-T-B.log" "Caught up" 30 || { echo "GW-T-B never caught up"; exit 1; }
sleep 2
GW_B_LINE=$(wc -l < "$LOG_DIR/gateway-GW-T-B.log")
kill -STOP "${OTHERS[@]}"; sleep "$QUORUM_PAUSE_SECS"; kill -CONT "${OTHERS[@]}"
W=0
until tail -n "+$((GW_B_LINE + 1))" "$LOG_DIR/gateway-GW-T-B.log" | grep -q "gate OPEN\|FENCED"; do
  sleep 0.5; W=$((W+1)); ((W > REPLACE_TIMEOUT_SECS * 2)) && break
done
sleep 2
GW_B_OPEN=$(tail -n "+$((GW_B_LINE + 1))" "$LOG_DIR/gateway-GW-T-B.log" | grep -c "gate OPEN")
GW_B_FENCED=$(grep -c "FENCED" "$LOG_DIR/gateway-GW-T-B.log")

# ── Assertions ────────────────────────────────────────────────────────────────
DOWN=""
for m in 0 1 2; do kill -0 "${SEQ_PIDS[$m]}" 2>/dev/null || DOWN+=" $m"; done
APP_UP=0; kill -0 "$APP_PID" 2>/dev/null && APP_UP=1
FENCED=$(grep -c "FENCED" "$APP_LOG" 2>/dev/null)

echo ""
echo "=== RESULT ==="
echo "  members down                         : ${DOWN:- none}"
echo "  replacements, application paused     : $PHASE1  (expected at least 1)"
echo "  longest heartbeat silence, phase 1   : ${SILENCE_MS}ms  (expected under ${SILENCE_LIMIT_MS}ms)"
echo "  replacements, quorum lost            : $PHASE2  (expected at least 1)"
echo "  application running / fences        : $APP_UP / $FENCED  (expected 1 / 0)"
echo "  GW-T-B gate opened / fences          : $GW_B_OPEN / $GW_B_FENCED  (expected 1 / 0)"
grep -E "replaces the lost one|could not replace|heartbeat resumed|FENCED" "$APP_LOG" 2>/dev/null | cut -c1-160 \
  | tail -8 | sed 's/^/    /'
for g in GW-T-A GW-T-B; do
  tail -n +2 "$LOG_DIR/gateway-$g.log" | grep -E "is now|gate OPEN|replaces the lost one|FENCED" | cut -c1-160 \
    | sed 's/^/    /'
done

if [[ -z "$DOWN" && "$PHASE1" -ge 1 && "$SILENCE_MS" -lt "$SILENCE_LIMIT_MS" && "$PHASE2" -ge 1 &&
      "$APP_UP" -eq 1 && "$FENCED" -eq 0 && "$GW_B_OPEN" -ge 1 && "$GW_B_FENCED" -eq 0 ]]; then
  echo "SESSION-REPLACE TEST: PASS — the application replaced its session both times without stopping its duty"
  echo "  cycle, the gateway standby took the role over, and neither was fenced"
  exit 0
else
  echo "SESSION-REPLACE TEST: FAIL — down:${DOWN:- none}, replacements=$PHASE1/$PHASE2, silence=${SILENCE_MS}ms," \
       "running=$APP_UP, fences=$FENCED, GW-T-B open=$GW_B_OPEN fenced=$GW_B_FENCED"
  exit 1
fi
