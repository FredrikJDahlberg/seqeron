#!/usr/bin/env bash
# The C++ client tier against a live three-node cluster.
#
# cluster_probe (seqeron-client/src/test/cpp-probe) is the C++ twin of ClusterProbe's confirm and follow modes,
# against the members' own embedded Java drivers. Eight phases:
#
#   A  UDP ingress from a follower's host, through PendingSends            confirm EXACT
#   B  IPC ingress co-located with the leader (connectColocated)            confirm EXACT, no UDP fallback
#   C  connectColocated on a follower, which never answers IPC ingress      confirm EXACT, after the UDP fallback
#   D  the leader killed mid-stream, PendingSends beside an untracked control   tracked confirm EXACT
#   E  a cold start on the new leader's node, replayed from groupSeqNo 1    follow CONTIGUOUS, >= 1 replay served
#   F  the C++ examples, built as an outside consumer builds them: follow_stream and colocated_app echo their pings,
#      and the active gateway_app of the pair in seqeron-examples/topology.xml is killed — the standby is
#      designated, resumes past its predecessor's connection id, and serves
#   G  snapshot_app confirms a snapshot round, and its restart restores from it with the ledger's total intact
#   H  a member restarted at a cut: ClusterStreamClient replays its recording from the cut   scan CAUGHT UP;
#      a ReplayerStreamReceiver with no snapshot file walks it from its start                 follow FAILED
#
# and no member logs a rejected ingress frame. The control in D reports what the kill had in flight; like
# failover-test.sh's, it is not asserted, since a kill can land between two frames.
#
# Needs the uber jar and CMake. The probe and the examples build in one tree, SEQERON_EXAMPLES_BUILD (default
# seqeron-examples/cmake-build-e2e), so Aeron is built once: SEQERON_BUILD_TESTS=ON has the seqeron it fetches define
# cluster_probe too. Its own tree, since that setting sticks. One that does not exist yet is configured as Release;
# an existing one keeps its type.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"
source "${SCRIPT_DIR}/../../main/scripts/seqeron-home.sh"

seqeron_require_jar
JAR="${SEQERON_JAR}"
command -v cmake > /dev/null || { echo "no cmake on PATH"; exit 1; }

LOG_DIR="logs/cpp-client"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
EXAMPLES_BUILD="${SEQERON_EXAMPLES_BUILD:-${REPO_ROOT}/seqeron-examples/cmake-build-e2e}"
CONFIGURE=(-DSEQERON_BUILD_TESTS=ON)
[[ -f "$EXAMPLES_BUILD/CMakeCache.txt" ]] || CONFIGURE+=(-DCMAKE_BUILD_TYPE=Release)
{ cmake -S "${REPO_ROOT}/seqeron-examples" -B "$EXAMPLES_BUILD" "${CONFIGURE[@]}" &&
  cmake --build "$EXAMPLES_BUILD" -j "$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)" \
    --target cluster_probe follow_stream colocated_app gateway_app snapshot_app; } > "$LOG_DIR/build.log" 2>&1 ||
  { echo "build failed: $LOG_DIR/build.log"; exit 1; }
PROBE="${EXAMPLES_BUILD}/_deps/seqeron-build/cluster_probe"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")
BASE_DIR="${TMP_DIR}/seqeron-seqcpp"
SNAPSHOT_DIR="${BASE_DIR}/snapshot-app"
CLUSTER_HOSTS="$(cluster_hosts_string 3)"
# One egress port per probe process: the C++ sender sends the cluster the port it is given (doc/ops.md, "Ports").
PROBE_EGRESS_PORT_BASE=9220
aeron_dir() { seqeron_aeron_dir "$1"; }

# probe <log> <mode> <member> <clientId> [options...]: one C++ probe co-located with <member>.
probe() {
  local log=$1 mode=$2 member=$3 client=$4
  shift 4
  "$PROBE" "$mode" --member "$member" --client-id "$client" --aeron-dir "$(aeron_dir "$member")" "$@" \
    > "$LOG_DIR/$log.log" 2>&1
}
# example <log> <binary> <member> [VAR=value...]: one C++ example co-located with <member>, in the background.
example() {
  local log=$1 name=$2 member=$3
  shift 3
  env SEQERON_NODE_MEMBER_ID="$member" SEQERON_AERON_DIR="$(aeron_dir "$member")" "$@" "$EXAMPLES_BUILD/$name" \
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

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f "$PROBE" 2>/dev/null
pkill -f "$EXAMPLES_BUILD" 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "$(aeron_dir 0)" "$(aeron_dir 1)" "$(aeron_dir 2)" 2>/dev/null

declare -a SEQ_PIDS
for m in 0 1 2; do
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.hosts="$CLUSTER_HOSTS" -jar "$JAR" > "$LOG_DIR/seq-$m.log" 2>&1 &
  SEQ_PIDS[$m]=$!
done
cleanup() {
  kill "${SEQ_PIDS[@]}" 2>/dev/null; pkill -f "$PROBE" 2>/dev/null; pkill -f "$EXAMPLES_BUILD" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT
for m in 0 1 2; do
  wait_for_log "$LOG_DIR/seq-$m.log" "Running" 30 || { echo "seq $m not up"; exit 1; }
done
# topology.xml less its gateway list first, since the cluster refuses everything before its groups; the list
# lands in F, once both instances are up.
sed -e '/<gateways>/,/<\/gateways>/d' "${REPO_ROOT}/seqeron-examples/topology.xml" > "$LOG_DIR/topology-groups.xml"
"${REPO_ROOT}/seqeron-service/src/main/scripts/clusterctl.sh" load-topology "$LOG_DIR/topology-groups.xml" \
  > "$LOG_DIR/load-groups.log" 2>&1 || { echo "load-topology failed: $LOG_DIR/load-groups.log"; exit 1; }
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
probe udp confirm "$P" 31 --count "$COUNT" --egress-port $((PROBE_EGRESS_PORT_BASE + 0)); A_RC=$?
echo "    $(verdict udp)"

echo "=== B: IPC ingress co-located with leader $L1"
probe ipc confirm "$L1" 32 --count "$COUNT" --colocated --egress-port $((PROBE_EGRESS_PORT_BASE + 1)); B_RC=$?
echo "    $(verdict ipc)"
B_FELL_BACK=$(grep -c "falling back to UDP" "$LOG_DIR/ipc.log")

echo "=== C: connectColocated on follower $P"
probe fallback confirm "$P" 33 --count 5000 --colocated --egress-port $((PROBE_EGRESS_PORT_BASE + 2)); C_RC=$?
echo "    $(verdict fallback)"
C_FELL_BACK=$(grep -c "falling back to UDP" "$LOG_DIR/fallback.log")

echo "=== D: leader $L1 killed mid-stream"
probe confirm confirm "$P" 34 --count "$COUNT" --pacing-micros "$PACING" \
  --egress-port $((PROBE_EGRESS_PORT_BASE + 3)) &
CONFIRM_PID=$!
probe control confirm "$P" 35 --count "$COUNT" --pacing-micros "$PACING" --untracked \
  --egress-port $((PROBE_EGRESS_PORT_BASE + 4)) &
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
# The Replayer names a client by the id it requests replays as, which the client logs at start.
REPLY_ID=$(grep -oE "requests replays as client [0-9]+" "$LOG_DIR/follow.log" | grep -oE "[0-9]+$" | tail -1)
REPLAYS=$(grep -c "replay for client ${REPLY_ID:-none}:" "$LOG_DIR/seq-${L2:-$P}.log")
echo "    replays served to client 36: $REPLAYS"

echo "=== F: the C++ examples, and a gateway handover"
L2="${L2:-$P}"
Q=$(( 3 - L1 - L2 ))
example follow-ex follow_stream "$Q"; FOLLOW_PID=$!
example colocated-ex colocated_app "$L2"; COLOCATED_PID=$!
example gw-a gateway_app "$Q" SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-A; GW_A_PID=$!
example gw-b gateway_app "$L2" SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-B; GW_B_PID=$!
F_OK=1
# Both instances are up before the list lands, and read their queue as soon as their rows do, so the bootstrap
# designation is answered inside its 5 s.
CLUSTERCTL_MEMBER_ID="$Q" "${REPO_ROOT}/seqeron-service/src/main/scripts/clusterctl.sh" load-topology \
  "${REPO_ROOT}/seqeron-examples/topology.xml" > "$LOG_DIR/load-topology.log" 2>&1 ||
  { echo "    load-topology failed: $LOG_DIR/load-topology.log"; F_OK=0; }
for f in gw-a gw-b; do
  wait_for_log "$LOG_DIR/$f.log" "# caught up" 60 || { echo "    $f never caught up"; F_OK=0; }
done
wait_for_count gw-a "ping echoed on connection 0" 3 30 || { echo "    GW-EX-A never served connection 0"; F_OK=0; }
wait_for_log "$LOG_DIR/gw-b.log" "# connection 0 opened (example-client)" 10 ||
  { echo "    the standby never saw connection 0 open"; F_OK=0; }
kill -9 "$GW_A_PID" 2>/dev/null
wait "$GW_A_PID" 2>/dev/null
echo "    killed the active instance, GW-EX-A"
# Resuming past the predecessor's id is what keeps the pair's connection ids unique across the handover.
wait_for_count gw-b "ping echoed on connection 1" 3 30 || { echo "    GW-EX-B never served connection 1"; F_OK=0; }
grep -q "designated — serving, connection ids from 1" "$LOG_DIR/gw-b.log" ||
  { echo "    GW-EX-B did not resume at connection id 1"; F_OK=0; }
wait_for_count follow-ex "ping echoed" 3 10 || { echo "    follow_stream echoed no pings"; F_OK=0; }
wait_for_count colocated-ex "ping echoed" 3 10 || { echo "    colocated_app echoed no pings"; F_OK=0; }
kill "$FOLLOW_PID" "$COLOCATED_PID" "$GW_B_PID" 2>/dev/null
wait "$FOLLOW_PID"; FOLLOW_RC=$?   # 1 on a gap in groupSeqNo
wait "$COLOCATED_PID"; COLOCATED_RC=$?
wait "$GW_B_PID"; GW_B_RC=$?
((FOLLOW_RC == 0 && COLOCATED_RC == 0 && GW_B_RC == 0)) ||
  { echo "    exits: follow_stream=$FOLLOW_RC colocated_app=$COLOCATED_RC GW-EX-B=$GW_B_RC"; F_OK=0; }
echo "    GW-EX-A: $(grep -c "ping echoed" "$LOG_DIR/gw-a.log") pings echoed;" \
     "GW-EX-B: $(grep -m1 "designated" "$LOG_DIR/gw-b.log"), $(grep -c "ping echoed" "$LOG_DIR/gw-b.log") pings echoed"

# topology.xml's <snapshots> policy has been running rounds since F loaded it. On the leader, so the replica submits
# transfers and its rounds' SnapshotEnd.
echo "=== G: snapshot_app restores from its own confirmed snapshot"
G_OK=1
example snap-1 snapshot_app "$L2" SEQERON_EXAMPLE_SNAPSHOT_DIR="$SNAPSHOT_DIR"; SNAP_PID=$!
CONFIRMED=""
for ((i = 0; i < 80; i++)); do
  CONFIRMED=$(find "$SNAPSHOT_DIR" -name '*.snapshot' 2>/dev/null | head -1)
  [[ -n "$CONFIRMED" ]] && break
  sleep 0.5
done
[[ -n "$CONFIRMED" ]] || { echo "    no round confirmed a snapshot in 40 s"; G_OK=0; }
kill "$SNAP_PID" 2>/dev/null
wait "$SNAP_PID"; SNAP_1_RC=$?
echo "    confirmed $(basename "${CONFIRMED:-none}") after $(grep -c " transfer " "$LOG_DIR/snap-1.log") transfers"
example snap-2 snapshot_app "$L2" SEQERON_EXAMPLE_SNAPSHOT_DIR="$SNAPSHOT_DIR"; SNAP_PID=$!
wait_for_log "$LOG_DIR/snap-2.log" "# caught up" 30 || { echo "    the restart never caught up"; G_OK=0; }
grep -q "# restored after" "$LOG_DIR/snap-2.log" || { echo "    the restart restored no snapshot"; G_OK=0; }
# Transfers move money and never make it: eight accounts of 1000.
grep -q "# caught up at .* total 8000$" "$LOG_DIR/snap-2.log" || { echo "    the ledger's total changed"; G_OK=0; }
kill "$SNAP_PID" 2>/dev/null
wait "$SNAP_PID"; SNAP_2_RC=$?
((SNAP_1_RC == 0 && SNAP_2_RC == 0)) || { echo "    exits: snapshot_app=$SNAP_1_RC, restarted=$SNAP_2_RC"; G_OK=0; }
echo "    $(grep -m1 "# restored after" "$LOG_DIR/snap-2.log")"

# Q took part in G's rounds, so its restart restores the newest restart-safe one and records each queue from the cut,
# above position 0. With L1 still down, the cluster has no quorum until Q is back.
echo "=== H: ClusterStreamClient replays member $Q's recording from a cut"
H_OK=1
for ((i = 0; i < 60; i++)); do
  grep -q "^safe " "$BASE_DIR/cluster-$Q/restart-safe.log" 2>/dev/null && break
  sleep 0.5
done
grep -q "^safe " "$BASE_DIR/cluster-$Q/restart-safe.log" 2>/dev/null || { echo "    member $Q has no restart-safe cut"; H_OK=0; }
kill "${SEQ_PIDS[$Q]}" 2>/dev/null; wait "${SEQ_PIDS[$Q]}" 2>/dev/null
java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$Q" -Dsequencer.baseDir="$BASE_DIR" \
     -Dsequencer.hosts="$CLUSTER_HOSTS" -jar "$JAR" > "$LOG_DIR/seq-$Q-restart.log" 2>&1 &
SEQ_PIDS[$Q]=$!
wait_for_log "$LOG_DIR/seq-$Q-restart.log" "serving replay" 60 || { echo "    member $Q not back"; H_OK=0; }
probe scan scan "$Q" 37; H_RC=$?
SCAN=$(grep -hE "scan: |failed" "$LOG_DIR/scan.log" | tail -1)
echo "    $SCAN"
FIRST=$(sed -nE 's/.*first frame groupSeqNo ([0-9]+) at position ([0-9]+), a cut$/\1 \2/p' <<< "$SCAN")
read -r FIRST_GROUP_SEQ_NO FIRST_POSITION <<< "${FIRST:-0 0}"
((H_RC == 0 && FIRST_GROUP_SEQ_NO > 1 && FIRST_POSITION > 0)) ||
  { echo "    expected a caught-up scan whose first frame is a cut above groupSeqNo 1 and position 0"; H_OK=0; }
# With no snapshot file, a ReplayerStreamReceiver walks that recording from its start, and its poll throws.
probe walk follow "$Q" 38 --live-seconds 1; WALK_RC=$?
echo "    $(verdict walk)"
((WALK_RC != 0)) && grep -q "follow failed: .*expected 1" "$LOG_DIR/walk.log" ||
  { echo "    expected the walk from the recording's start to fail on the cut"; H_OK=0; }

REJECTED=$(cat "$LOG_DIR"/seq-*.log | grep -cE "skipping (malformed )?ingress message")
echo "=== ingress frames rejected by the sequencer: $REJECTED"

if ((A_RC == 0 && B_RC == 0 && B_FELL_BACK == 0 && C_RC == 0 && C_FELL_BACK == 1 && D_RC == 0 && E_RC == 0 &&
      REPLAYS >= 1 && F_OK == 1 && G_OK == 1 && H_OK == 1 && REJECTED == 0)); then
  echo "C++ CLIENT TEST: PASS"; exit 0
else
  echo "C++ CLIENT TEST: FAIL (A=$A_RC B=$B_RC/fallback $B_FELL_BACK C=$C_RC/fallback $C_FELL_BACK D=$D_RC E=$E_RC" \
       "replays=$REPLAYS F=$F_OK G=$G_OK H=$H_OK rejected=$REJECTED)"
  exit 1
fi
