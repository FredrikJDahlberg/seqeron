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
#     preload       orders flooded to cluster ingress to build the archive before the cold start
#     load-during   1 = keep flooding while the replica catches up (moving-target case), default 0
#
#   ./src/test/scripts/replay-bench.sh 20000        # small archive, quiet
#   ./src/test/scripts/replay-bench.sh 400000       # ~70 MB of history — the case that used to hang
#
# Prints one result line: archive size, elapsed seconds, MB/s, and whether it caught up at all.
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1
source src/main/scripts/ports.sh
source src/main/scripts/paths.sh

PRELOAD="${1:-20000}"
LOAD_DURING="${2:-0}"
CAP_SECONDS=120

BENCH_LOG_DIR="/tmp/phixeron-replay-bench"
mkdir -p "$BENCH_LOG_DIR"
START_LOG="$BENCH_LOG_DIR/cluster.log"
COLD_LOG="$BENCH_LOG_DIR/cold-replica.log"

cleanup() {
    # Never `kill ${PID:-0}` here: an unset pid defaulting to 0 signals the whole process group,
    # which takes this script (and its caller) down with it.
    [[ -n "${COLD_PID:-}" ]] && kill "$COLD_PID" 2>/dev/null
    [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null
    pkill -f fix_test_server 2>/dev/null
    ./src/main/scripts/stop-cluster.sh >/dev/null 2>&1
    pkill -9 -f "uber.jar" 2>/dev/null
    pkill -9 -f aeronmd 2>/dev/null
}
trap cleanup EXIT

# Stale recordings are not merely noise here — they are counted in the archive size and replayed by
# the cold replica, so the number this prints would be measuring the wrong history.
./src/main/scripts/purgelog.sh --force >/dev/null 2>&1
./src/main/scripts/start-three-node-cluster.sh > "$START_LOG" 2>&1 &
W=0
until grep -q "READY" "$START_LOG" 2>/dev/null; do
    sleep 2
    W=$((W + 1))
    ((W > 90)) && { echo "cluster did not come up — see $START_LOG"; exit 1; }
done

# Build the archive.
PHIXERON_FLOOD_ORDERS="$PRELOAD" ./cmake-build-release/fix_test_server 127.0.0.1 "$(fix_tcp_port)" \
    > "$BENCH_LOG_DIR/preload.log" 2>&1
sleep 3

ARCHIVE_BYTES=$(du -sk "${TMP_DIR}/phixeron-seq3/archive-1" 2>/dev/null | awk '{print $1*1024}')

if [[ "$LOAD_DURING" == "1" ]]; then
    ( PHIXERON_FLOOD_ORDERS=$((PRELOAD * 4)) \
        ./cmake-build-release/fix_test_server 127.0.0.1 "$(fix_tcp_port)" \
        > "$BENCH_LOG_DIR/load.log" 2>&1 ) &
    LOAD_PID=$!
    disown "$LOAD_PID"  # cleanup kills it by pid; this just keeps bash from reporting the kill
fi

# ADD a cold replica rather than restarting a running one: start-three-node-cluster.sh monitors its
# children and tears the whole cluster down the moment any of them exits. This one attaches to
# member 1's Replayer under its own client id and egress port, so it walks the entire recording
# chain from scratch exactly as a restarted replica does, while everything else keeps running.
: > "$COLD_LOG"
START=$(python3 -c 'import time; print(time.time())')
PHIXERON_ORDER_EXEC_AERON_DIR="${TMP_DIR}/phixeron-seq-aeron-1" \
    PHIXERON_NODE_MEMBER_ID=1 \
    PHIXERON_REPLAYER_CLIENT_ID=7 \
    PHIXERON_CLUSTER_EGRESS_ENDPOINT="localhost:${REPLAY_BENCH_EGRESS_PORT}" \
    stdbuf -oL -eL ./cmake-build-release/OrderExecServer > "$COLD_LOG" 2>&1 &
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
