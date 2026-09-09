#!/usr/bin/env bash
# replay-bench.sh — how fast does a COLD replica replay recorded history to caught-up?
#
# This is the measurement behind the chaos-runner's kill-leader failures: a restarted replica has to
# reach "Caught up — following live" before the harness's window expires, and with no snapshots
# (doc/todo.md) the history it must walk is the whole trading day. It was written to chase a cold start
# that never converged at all past ~32 MiB of history — the shared-replay-stream flow-control wedge
# ReplayerStreamReceiver::openReplaySubscription now documents — and stays as the regression measurement
# for it: a run that reports NEVER CAUGHT UP is that class of bug, not a slow machine.
#
#   ./src/test/scripts/replay-bench.sh <preload> [load-during]
#     preload       frames flooded to cluster ingress to build the archive before the cold start
#     load-during   1 = keep flooding while the replica catches up (moving-target case), default 0
#
# Java only — no C++ binary is built or launched (doc/future-arch.md §11 step 5). The load is
# ClusterProbe submit and the cold replica is ClusterProbe follow; the cluster comes up in
# start-three-node-cluster.sh's default Java-only mode. FILLER_BYTES below is what keeps the MB/s
# number meaningful: a bare ProbeMarker is tiny, so it is padded to the size of the NewOrderSingle
# this flood replaced. Numbers from before that change are still not comparable frame-for-frame.
#
#   ./src/test/scripts/replay-bench.sh 20000        # small archive, quiet
#   ./src/test/scripts/replay-bench.sh 400000       # ~70 MB of history — the case that used to hang
#
# Prints one result line: archive size, elapsed seconds, MB/s, and whether it caught up at all.
# Elapsed includes the cold client's own start-up, which is now a JVM's rather than a C++ binary's —
# a fixed cost that dominates at the small preloads and is noise at the 400k this was written for.
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1
source src/main/scripts/ports.sh
source src/main/scripts/paths.sh

PRELOAD="${1:-20000}"
LOAD_DURING="${2:-0}"
CAP_SECONDS=120
# NewOrderSingle is a 185-byte SBE block plus its own 8-byte messageHeader; a ProbeMarker is that
# header plus an 8-byte seqNo and a 2-byte length, so 175 bytes of filler makes the two the same
# size on the wire and in the archive.
FILLER_BYTES="${FILLER_BYTES:-175}"

BENCH_LOG_DIR="/tmp/seqeron-replay-bench"
mkdir -p "$BENCH_LOG_DIR"
START_LOG="$BENCH_LOG_DIR/cluster.log"
COLD_LOG="$BENCH_LOG_DIR/cold-replica.log"

cleanup() {
    # Never `kill ${PID:-0}` here: an unset pid defaulting to 0 signals the whole process group,
    # which takes this script (and its caller) down with it.
    [[ -n "${COLD_PID:-}" ]] && kill "$COLD_PID" 2>/dev/null
    [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null
    pkill -f ClusterProbe 2>/dev/null
    ./src/main/scripts/stop-cluster.sh >/dev/null 2>&1
    pkill -9 -f "uber.jar" 2>/dev/null
    pkill -9 -f aeronmd 2>/dev/null
}
trap cleanup EXIT

# Stale recordings are not merely noise here — they are counted in the archive size and replayed by
# the cold replica, so the number this prints would be measuring the wrong history.
./src/main/scripts/purgelog.sh --force >/dev/null 2>&1
./src/test/scripts/start-three-node-cluster.sh > "$START_LOG" 2>&1 &
W=0
until grep -q "READY" "$START_LOG" 2>/dev/null; do
    sleep 2
    W=$((W + 1))
    ((W > 90)) && { echo "cluster did not come up — see $START_LOG"; exit 1; }
done

# Build the archive.
JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
JAR="build/libs/seqeron-0.1.0-uber.jar"
java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.count="$PRELOAD" -Dprobe.fillerBytes="$FILLER_BYTES" \
    -cp "$JAR" org.limitless.seqeron.tools.ClusterProbe submit > "$BENCH_LOG_DIR/preload.log" 2>&1
sleep 3

ARCHIVE_BYTES=$(du -sk "${TMP_DIR}/seqeron-seq3/archive-1" 2>/dev/null | awk '{print $1*1024}')

if [[ "$LOAD_DURING" == "1" ]]; then
    ( java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.count=$((PRELOAD * 4)) \
        -Dprobe.fillerBytes="$FILLER_BYTES" \
        -cp "$JAR" org.limitless.seqeron.tools.ClusterProbe submit > "$BENCH_LOG_DIR/load.log" 2>&1 ) &
    LOAD_PID=$!
    disown "$LOAD_PID"  # cleanup kills it by pid; this just keeps bash from reporting the kill
fi

# ADD a cold replica rather than restarting a running one: start-three-node-cluster.sh monitors its
# children and tears the whole cluster down the moment any of them exits. This one attaches to
# member 1's Replayer under its own client id, so it walks the entire recording chain from scratch
# exactly as a restarted replica does, while everything else keeps running.
: > "$COLD_LOG"
START=$(python3 -c 'import time; print(time.time())')
java "${JAVA_OPTS[@]}" -Dprobe.memberId=1 -Dprobe.clientId=7 -cp "$JAR" \
    org.limitless.seqeron.tools.ClusterProbe follow > "$COLD_LOG" 2>&1 &
COLD_PID=$!

W=0
CAUGHT=0
while (( W < CAP_SECONDS * 4 )); do
    grep -q "Caught up" "$COLD_LOG" 2>/dev/null && { CAUGHT=1; break; }
    kill -0 "$COLD_PID" 2>/dev/null || break
    sleep 0.25
    W=$((W + 1))
done
END=$(python3 -c 'import time; print(time.time())')

python3 - "$START" "$END" "$CAUGHT" "${ARCHIVE_BYTES:-0}" "$LOAD_DURING" "$CAP_SECONDS" <<'PY'
import sys
start, end, caught, size, during, cap = sys.argv[1:7]
elapsed = float(end) - float(start)
mb = int(size) / 1e6
status = "caught up" if caught == "1" else f"NEVER CAUGHT UP ({cap}s cap)"
rate = f"{mb / elapsed:.1f} MB/s" if caught == "1" and elapsed > 0 else "—"
print(f"archive={mb:.1f} MB  load-during={'yes' if during == '1' else 'no'}  "
      f"elapsed={elapsed:.2f}s  rate={rate}  {status}")
PY
