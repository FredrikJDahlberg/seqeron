#!/usr/bin/env bash
# The C# client tier on Windows, against a cluster on Linux: the deployment shape where the members are
# Linux hosts and the C# clients run on a Windows host beside a gateway host's media driver.
#
# One Windows machine stands in for both. The member runs in WSL1, which shares Windows' network stack, so
# everything meets on localhost UDP. Windows runs the gateway host (node 3: ReplayerServer relaying the
# member's tap onto its own) and, attached to its driver through the default Aeron directory, the C# probe and
# the C# examples. Three phases, all of them client functionality:
#
#   A  UDP ingress from the gateway host, through PendingSends            confirm EXACT
#   B  a cold start on the gateway host, replayed from globalSeqNo 1        follow CONTIGUOUS
#   C  the C# gateway pair handed over when its active instance is killed  standby resumes at connection 1
#
# and the member logs no rejected ingress frame. One member, not three: under WSL1's system-call translation
# Raft's heartbeats do not hold, and three members elect without end. A leader kill is csharp-client-test.sh's,
# on Linux, through the same C# code. Runs in Git Bash on Windows, with a WSL1 distribution that
# has a JDK 21 (WSL_DISTRO, default Ubuntu-24.04), the uber jar and the .NET SDK. CI's windows.yml runs it.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
DOTNET="${DOTNET:-dotnet}"
WSL_DISTRO="${WSL_DISTRO:-Ubuntu-24.04}"
NODE=3
# One member, at localhost, for every process here: the C# clients' ingress, clusterctl, the gateway host.
export SEQERON_HOSTS=localhost
JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

# Git Bash rewrites an argument that starts with / into a Windows path; inside WSL it must stay a Linux one.
wsl_run() { MSYS_NO_PATHCONV=1 wsl.exe -d "$WSL_DISTRO" -e "$@"; }
# A native Windows process started from Git Bash is stopped through its Windows pid.
win_kill() {
  local winpid
  winpid=$(cat "/proc/$1/winpid" 2>/dev/null) && taskkill //F //PID "$winpid" > /dev/null 2>&1
  kill -9 "$1" 2>/dev/null
}

LOG_DIR="logs/csharp-windows"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

# The probe, and the examples from the packed package, as csharp-client-test.sh builds them.
PROBE_OUT="${TMP_DIR}/seqeron-csharp-probe"
EXAMPLES_OUT="${TMP_DIR}/seqeron-csharp-examples"
"$DOTNET" build -c Release -o "$(cygpath -w "$PROBE_OUT")" \
  "$(cygpath -w "${REPO_ROOT}/seqeron-client/src/test/csharp-probe/Seqeron.ClusterProbe.csproj")" \
  > "$LOG_DIR/build.log" 2>&1 || { echo "probe build failed: $LOG_DIR/build.log"; exit 1; }
"$DOTNET" pack -c Release -o "$(cygpath -w "${REPO_ROOT}/build/nuget")" \
  "$(cygpath -w "${REPO_ROOT}/seqeron-client/src/main/csharp/Seqeron.Client.csproj")" \
  >> "$LOG_DIR/build.log" 2>&1 || { echo "pack failed: $LOG_DIR/build.log"; exit 1; }
for ex in FollowStream GatewayApp; do
  "$DOTNET" build -c Release -o "$(cygpath -w "$EXAMPLES_OUT/$ex")" \
    "$(cygpath -w "${REPO_ROOT}/seqeron-examples/src/csharp/$ex/$ex.csproj")" \
    >> "$LOG_DIR/build.log" 2>&1 || { echo "$ex build failed: $LOG_DIR/build.log"; exit 1; }
done

# The member, in WSL. Its state stays on WSL's own filesystem; its output comes back through wsl.exe.
WSL_JAR=$(wsl_run wslpath -a "$(cygpath -wa "$JAR")" | tr -d '\r')
wsl_run pkill -9 -f "sequencer.memberId=" 2>/dev/null
wsl_run rm -rf /tmp/seqeron-seq /tmp/seqeron-seq-aeron-0
wsl_run java "${JAVA_OPTS[@]}" -Dsequencer.memberId=0 -Dsequencer.baseDir=/tmp/seqeron-seq \
  -jar "$WSL_JAR" > "$LOG_DIR/seq-0.log" 2>&1 &
GATEWAY_HOST_PID=""
declare -a CLIENT_PIDS=()
cleanup() {
  for pid in "${CLIENT_PIDS[@]}" $GATEWAY_HOST_PID; do win_kill "$pid"; done
  wsl_run pkill -9 -f "sequencer.memberId=" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT
wait_for_log "$LOG_DIR/seq-0.log" "serving replay" 90 || { echo "member not up"; cat "$LOG_DIR/seq-0.log"; exit 1; }
echo "member 0 up in WSL"

# The gateway host, on Windows.
ENDPOINTS="localhost:$(archive_port 0)"
GATEWAY_BASE="$(cygpath -w "${TMP_DIR}/seqeron-seq-windows")"
rm -rf "${TMP_DIR}/seqeron-seq-windows"
# UTF-8 output, as the log patterns expect: redirected, a Windows JVM writes the system code page.
java "${JAVA_OPTS[@]}" -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 \
  -Dreplayer.memberId="$NODE" -Dreplayer.baseDir="$GATEWAY_BASE" \
  -Dreplayer.archiveEndpoints="$ENDPOINTS" -cp "$(cygpath -wa "$JAR")" \
  org.limitless.seqeron.replayer.server.ReplayerServer > "$LOG_DIR/gateway-host.log" 2>&1 &
GATEWAY_HOST_PID=$!
wait_for_log "$LOG_DIR/gateway-host.log" "ready —" 90 ||
  { echo "gateway host not serving"; cat "$LOG_DIR/gateway-host.log"; exit 1; }
echo "gateway host (node $NODE) serving on Windows, member archives $ENDPOINTS"

# probe <log> <mode> <clientId> [options...]: the C# probe on the gateway host, in the foreground.
probe() {
  local log=$1 mode=$2 client=$3
  shift 3
  "$PROBE_OUT/Seqeron.ClusterProbe.exe" "$mode" --member "$NODE" --client-id "$client" "$@" > "$LOG_DIR/$log.log" 2>&1
}
# example <log> <name> [VAR=value...]: a C# example on the gateway host, in the background.
example() {
  local log=$1 name=$2
  shift 2
  env SEQERON_NODE_MEMBER_ID="$NODE" "$@" "$EXAMPLES_OUT/$name/$name.exe" > "$LOG_DIR/$log.log" 2>&1 &
}
verdict() { grep -hE 'confirm: (EXACT|NOT EXACT)|follow: |failed' "$LOG_DIR/$1.log" | tail -1; }
wait_for_count() {
  local i
  for ((i = 0; i < $4 * 2; i++)); do
    (( $(grep -c -- "$2" "$LOG_DIR/$1.log" 2>/dev/null) >= $3 )) && return 0
    sleep 0.5
  done
  return 1
}
COUNT="${CONFIRM_COUNT:-5000}"

echo "=== A: UDP ingress from the gateway host"
probe confirm-udp confirm 41 --count "$COUNT"; A_RC=$?
echo "    $(verdict confirm-udp)"

echo "=== B: cold start on the gateway host"
probe follow follow 42 --live-seconds 5; B_RC=$?
echo "    $(verdict follow)"

echo "=== C: the C# gateway pair, handed over"
example follow-ex FollowStream; FOLLOW_PID=$!; CLIENT_PIDS+=("$FOLLOW_PID")
example gw-a GatewayApp SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-A; GW_A_PID=$!; CLIENT_PIDS+=("$GW_A_PID")
example gw-b GatewayApp SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-B; GW_B_PID=$!; CLIENT_PIDS+=("$GW_B_PID")
C_OK=1
for f in gw-a gw-b; do
  wait_for_log "$LOG_DIR/$f.log" "# caught up" 90 || { echo "    $f never caught up"; C_OK=0; }
done
# clusterctl on the gateway host: its Aeron directory is node 3's, and it reaches the leader over UDP.
CLUSTERCTL_MEMBER_ID="$NODE" "${REPO_ROOT}/seqeron-service/src/main/scripts/clusterctl.sh" load-topology \
  "$(cygpath -w "${REPO_ROOT}/seqeron-examples/topology-csharp.xml")" > "$LOG_DIR/load-topology.log" 2>&1 ||
  { echo "    load-topology failed: $LOG_DIR/load-topology.log"; C_OK=0; }
wait_for_count gw-a "ping echoed on connection 0" 3 60 || { echo "    GW-EX-CS-A never served connection 0"; C_OK=0; }
wait_for_log "$LOG_DIR/gw-b.log" "# connection 0 opened (example-client)" 20 ||
  { echo "    the standby never saw connection 0 open"; C_OK=0; }
win_kill "$GW_A_PID"
echo "    killed the active instance, GW-EX-CS-A"
wait_for_count gw-b "ping echoed on connection 1" 3 60 || { echo "    GW-EX-CS-B never served connection 1"; C_OK=0; }
# Matched without the dash: redirected, .NET on Windows writes the console's code page.
grep -q "connection ids from 1" "$LOG_DIR/gw-b.log" ||
  { echo "    GW-EX-CS-B did not resume at connection id 1"; C_OK=0; }
wait_for_count follow-ex "ping echoed" 3 20 || { echo "    FollowStream echoed no pings"; C_OK=0; }
grep -q "^# gap" "$LOG_DIR/follow-ex.log" && { echo "    FollowStream saw a gap"; C_OK=0; }
win_kill "$GW_B_PID"; win_kill "$FOLLOW_PID"
echo "    GW-EX-CS-B: $(grep -m1 "designated" "$LOG_DIR/gw-b.log"), $(grep -c "ping echoed" "$LOG_DIR/gw-b.log") pings echoed"

REJECTED=$(cat "$LOG_DIR/seq-0.log" | grep -cE "skipping (malformed )?ingress message")
echo "=== ingress frames rejected by the sequencer: $REJECTED"

if ((A_RC == 0 && B_RC == 0 && C_OK == 1 && REJECTED == 0)); then
  echo "C# WINDOWS TEST: PASS"; exit 0
else
  echo "C# WINDOWS TEST: FAIL (A=$A_RC B=$B_RC C=$C_OK rejected=$REJECTED)"
  exit 1
fi
