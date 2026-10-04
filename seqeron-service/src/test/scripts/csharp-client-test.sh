#!/usr/bin/env bash
# The C# client tier against a live three-node cluster.
#
# Seqeron.ClusterProbe (seqeron-client/src/test/csharp-probe) is the C# twin of ClusterProbe's confirm and follow
# modes, on Aeron.NET against the members' own embedded Java drivers. Six phases:
#
#   A  UDP ingress from a follower's host, through PendingSends            confirm EXACT
#   B  IPC ingress co-located with the leader (ConnectColocated)            confirm EXACT, no UDP fallback
#   C  ConnectColocated on a follower, which never answers IPC ingress      confirm EXACT, after the UDP fallback
#   D  the leader killed mid-stream, PendingSends beside an untracked control   tracked confirm EXACT
#   E  a cold start on the new leader's node, replayed from globalSeqNo 1   follow CONTIGUOUS, >= 1 replay served
#   F  the C# examples, built against the packed NuGet package: FollowStream and ColocatedApp echo their pings,
#      and the active GatewayApp of the pair in seqeron-examples/topology-csharp.xml is killed — the standby
#      is designated, resumes past its predecessor's connection id, and serves
#
# and no member logs a rejected ingress frame. The control in D reports what the kill had in flight; like
# failover-test.sh's, it is not asserted, since a kill can land between two frames.
#
# Needs the uber jar and the .NET SDK (`dotnet`, or DOTNET naming one). Run it on every Aeron upgrade: the C#
# client is on Aeron.NET, which trails Java (doc/seqeron-protocol-spec.md, V-1).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
DOTNET="${DOTNET:-dotnet}"
command -v "$DOTNET" > /dev/null || { echo "no .NET SDK: install one, or set DOTNET"; exit 1; }

LOG_DIR="logs/csharp-client"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
PROBE_PROJECT="${SCRIPT_DIR}/../../../../seqeron-client/src/test/csharp-probe/Seqeron.ClusterProbe.csproj"
PROBE_OUT="${TMP_DIR}/seqeron-csharp-probe"
"$DOTNET" build -c Release -o "$PROBE_OUT" "$PROBE_PROJECT" > "$LOG_DIR/build.log" 2>&1 ||
  { echo "probe build failed: $LOG_DIR/build.log"; exit 1; }
PROBE="${PROBE_OUT}/Seqeron.ClusterProbe"

# The examples build against the package, not the source tree, from the local feed their NuGet.config names.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
EXAMPLES_OUT="${TMP_DIR}/seqeron-csharp-examples"
"$DOTNET" pack -c Release -o "${REPO_ROOT}/build/nuget" "${REPO_ROOT}/seqeron-client/src/main/csharp/Seqeron.Client.csproj" \
  >> "$LOG_DIR/build.log" 2>&1 || { echo "pack failed: $LOG_DIR/build.log"; exit 1; }
# NuGet serves a version it has cached rather than the one just packed under the same number.
rm -rf "${NUGET_PACKAGES:-$HOME/.nuget/packages}/org.limitless.seqeron/$(cat "${REPO_ROOT}/VERSION")"
for ex in FollowStream ColocatedApp GatewayApp; do
  "$DOTNET" build -c Release -o "$EXAMPLES_OUT/$ex" "${REPO_ROOT}/seqeron-examples/src/csharp/$ex/$ex.csproj" \
    >> "$LOG_DIR/build.log" 2>&1 || { echo "$ex build failed: $LOG_DIR/build.log"; exit 1; }
done

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqcs"
CLUSTER_HOSTS="$(cluster_hosts_string 3)"
aeron_dir() { echo "${TMP_DIR}/seqeron-seq-aeron-$1"; }

# probe <log> <mode> <member> <clientId> [options...]: one C# probe co-located with <member>.
probe() {
  local log=$1 mode=$2 member=$3 client=$4
  shift 4
  "$PROBE" "$mode" --member "$member" --client-id "$client" --aeron-dir "$(aeron_dir "$member")" "$@" \
    > "$LOG_DIR/$log.log" 2>&1
}
# example <log> <name> <member> [VAR=value...]: one C# example co-located with <member>, in the background.
example() {
  local log=$1 name=$2 member=$3
  shift 3
  env SEQERON_NODE_MEMBER_ID="$member" SEQERON_AERON_DIR="$(aeron_dir "$member")" "$@" "$EXAMPLES_OUT/$name/$name" \
    > "$LOG_DIR/$log.log" 2>&1 &
}
# wait_for_count <log> <pattern> <count> <seconds>
wait_for_count() {
  local i
  for ((i = 0; i < $4 * 2; i++)); do
    (( $(grep -c -- "$2" "$LOG_DIR/$1.log" 2>/dev/null) >= $3 )) && return 0
    sleep 0.5
  done
  return 1
}
verdict() { grep -hE 'confirm: (EXACT|NOT EXACT)|follow: |failed' "$LOG_DIR/$1.log" | tail -1; }

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f Seqeron.ClusterProbe 2>/dev/null
pkill -f "$EXAMPLES_OUT" 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "$(aeron_dir 0)" "$(aeron_dir 1)" "$(aeron_dir 2)" 2>/dev/null

declare -a SEQ_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.hosts="$CLUSTER_HOSTS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
cleanup() {
  kill "${SEQ_PIDS[@]}" 2>/dev/null; pkill -f Seqeron.ClusterProbe 2>/dev/null; pkill -f "$EXAMPLES_OUT" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "Running" 30 || { echo "seq $m not up"; exit 1; }
done
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "serving replay" 30 || true
done
sleep 2  # let the first LeadershipChanged replicate to followers
L1=$(grep -h "isLeader=true" "$LOG_DIR"/seq-*.log | grep -oE 'SequencerService/[0-9]+' | head -1 | cut -d/ -f2)
P=$(( (L1 + 1) % 3 ))
echo "cluster up: leader member $L1, follower member $P"

COUNT="${CONFIRM_COUNT:-20000}"
PACING="${CONFIRM_PACING_MICROS:-100}"

echo "=== A: UDP ingress from follower $P"
probe udp confirm "$P" 31 --count "$COUNT"; A_RC=$?
echo "    $(verdict udp)"

echo "=== B: IPC ingress co-located with leader $L1"
probe ipc confirm "$L1" 32 --count "$COUNT" --colocated; B_RC=$?
echo "    $(verdict ipc)"
B_FELL_BACK=$(grep -c "falling back to UDP" "$LOG_DIR/ipc.log")

echo "=== C: ConnectColocated on follower $P"
probe fallback confirm "$P" 33 --count 5000 --colocated; C_RC=$?
echo "    $(verdict fallback)"
C_FELL_BACK=$(grep -c "falling back to UDP" "$LOG_DIR/fallback.log")

echo "=== D: leader $L1 killed mid-stream"
probe confirm confirm "$P" 34 --count "$COUNT" --pacing-micros "$PACING" &
CONFIRM_PID=$!
probe control confirm "$P" 35 --count "$COUNT" --pacing-micros "$PACING" --untracked &
CONTROL_PID=$!
for f in confirm control; do
  wait_for_log "$LOG_DIR/$f.log" "confirm: sending" 30 || echo "$f producer never started"
done
sleep 1  # both mid-stream when the leader dies
kill "${SEQ_PIDS[$L1]}" 2>/dev/null
echo "    killed leader member $L1"
L2=""; W=0
until [[ -n "$L2" ]]; do
  sleep 0.5; W=$((W+1))
  for m in 0 1 2; do
    [[ "$m" == "$L1" ]] && continue
    grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { L2="$m"; break; }
  done
  ((W>120)) && { echo "no new leader emerged"; break; }
done
echo "    new leader member $L2"
wait "$CONFIRM_PID"; D_RC=$?
wait "$CONTROL_PID"
echo "    PendingSends : $(verdict confirm)"
echo "    control      : $(verdict control)"

echo "=== E: cold start on new leader $L2"
sleep 3  # let the new leader settle
probe follow follow "${L2:-$P}" 36 --live-seconds 5; E_RC=$?
echo "    $(verdict follow)"
REPLAYS=$(grep -c "replay for client 36" "$LOG_DIR/seq-${L2:-$P}.log")
echo "    replays served to client 36: $REPLAYS"

echo "=== F: the C# examples, and a gateway handover"
L2="${L2:-$P}"
Q=$(( 3 - L1 - L2 ))
example follow-ex FollowStream "$Q"; FOLLOW_PID=$!
example colocated-ex ColocatedApp "$L2"; COLOCATED_PID=$!
example gw-a GatewayApp "$Q" SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-A; GW_A_PID=$!
example gw-b GatewayApp "$L2" SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-CS-B; GW_B_PID=$!
F_OK=1
for f in gw-a gw-b; do
  wait_for_log "$LOG_DIR/$f.log" "# caught up" 60 || { echo "    $f never caught up"; F_OK=0; }
done
# Both instances are up before the list lands, so the bootstrap designation is answered inside its 5 s.
CLUSTERCTL_MEMBER_ID="$Q" "${REPO_ROOT}/seqeron-service/src/main/scripts/clusterctl.sh" load-topology \
  "${REPO_ROOT}/seqeron-examples/topology-csharp.xml" > "$LOG_DIR/load-topology.log" 2>&1 ||
  { echo "    load-topology failed: $LOG_DIR/load-topology.log"; F_OK=0; }
wait_for_count gw-a "ping echoed on connection 0" 3 30 || { echo "    GW-EX-CS-A never served connection 0"; F_OK=0; }
wait_for_log "$LOG_DIR/gw-b.log" "# connection 0 opened (example-client)" 10 ||
  { echo "    the standby never saw connection 0 open"; F_OK=0; }
kill -9 "$GW_A_PID" 2>/dev/null
wait "$GW_A_PID" 2>/dev/null
echo "    killed the active instance, GW-EX-CS-A"
# Resuming past the predecessor's id is what keeps the pair's connection ids unique across the handover.
wait_for_count gw-b "ping echoed on connection 1" 3 30 || { echo "    GW-EX-CS-B never served connection 1"; F_OK=0; }
grep -q "designated — serving, connection ids from 1" "$LOG_DIR/gw-b.log" ||
  { echo "    GW-EX-CS-B did not resume at connection id 1"; F_OK=0; }
wait_for_count follow-ex "ping echoed" 3 10 || { echo "    FollowStream echoed no pings"; F_OK=0; }
wait_for_count colocated-ex "ping echoed" 3 10 || { echo "    ColocatedApp echoed no pings"; F_OK=0; }
kill "$FOLLOW_PID" "$COLOCATED_PID" "$GW_B_PID" 2>/dev/null
wait "$FOLLOW_PID"; FOLLOW_RC=$?   # 1 on a gap in globalSeqNo
wait "$COLOCATED_PID"; COLOCATED_RC=$?
wait "$GW_B_PID"; GW_B_RC=$?
((FOLLOW_RC == 0 && COLOCATED_RC == 0 && GW_B_RC == 0)) ||
  { echo "    exits: FollowStream=$FOLLOW_RC ColocatedApp=$COLOCATED_RC GW-EX-CS-B=$GW_B_RC"; F_OK=0; }
echo "    GW-EX-CS-A: $(grep -c "ping echoed" "$LOG_DIR/gw-a.log") pings echoed;" \
     "GW-EX-CS-B: $(grep -m1 "designated" "$LOG_DIR/gw-b.log"), $(grep -c "ping echoed" "$LOG_DIR/gw-b.log") pings echoed"

REJECTED=$(cat "$LOG_DIR"/seq-*.log | grep -cE "skipping (malformed )?ingress message")
echo "=== ingress frames rejected by the sequencer: $REJECTED"

if ((A_RC == 0 && B_RC == 0 && B_FELL_BACK == 0 && C_RC == 0 && C_FELL_BACK == 1 && D_RC == 0 && E_RC == 0 &&
      REPLAYS >= 1 && F_OK == 1 && REJECTED == 0)); then
  echo "C# CLIENT TEST: PASS"; exit 0
else
  echo "C# CLIENT TEST: FAIL (A=$A_RC B=$B_RC/fallback $B_FELL_BACK C=$C_RC/fallback $C_FELL_BACK D=$D_RC E=$E_RC" \
       "replays=$REPLAYS F=$F_OK rejected=$REJECTED)"
  exit 1
fi
