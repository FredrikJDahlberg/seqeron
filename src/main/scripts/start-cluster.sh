#!/usr/bin/env bash
# start-cluster.sh — start the single-node cluster and a co-located consumer replica.
#
# The cluster tier alone, each process to its own log file — this repo builds no product binary and
# names none (the same split start-three-node-cluster.sh makes):
#
#   1. SequencerServer  (Java, single-node Aeron Cluster, member 0)
#   2. ReplayerServer   (Java, co-located with member 0: serves archive replay to co-located apps
#                        over aeron:ipc; apps read the tap directly for live)
#   3. ClusterProbe follow (Java, the edge-neutral consumer replica)
#
# SEQERON_NO_CONSUMERS=1 leaves out the probe replica, for a caller that supplies its own consumer —
# it launches, waits for and stops that itself, and stop-cluster.sh sweeps it through
# SEQERON_EXTRA_PROCESSES.
#
# Ctrl-C (or kill $$ / kill -- -$$) stops all of them cleanly.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#
# Usage:
#   ./start-cluster.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/paths.sh"

usage() {
    echo "Usage: $0"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# ── Config ────────────────────────────────────────────────────────────────────

JAR="build/libs/seqeron-0.1.0-uber.jar"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

# 1 = the caller supplies the consumer replica, so don't start the probe follower.
NO_CONSUMERS="${SEQERON_NO_CONSUMERS:-0}"

LOG_DIR="logs"
SEQ_LOG="${LOG_DIR}/sequencer.log"
REPLAYER_LOG="${LOG_DIR}/ReplayerServer.log"

# SequencerServer (member 0)'s own embedded media driver directory — matches its default when
# -Dsequencer.aeronDir isn't overridden. A consumer co-located with this member shares the directory,
# so archive/replay/ingress use aeron:ipc rather than a standalone driver.

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"

# ── Launch ────────────────────────────────────────────────────────────────────

echo "[cluster.sh] Starting SequencerServer (member 0) → ${SEQ_LOG}"
java "${JAVA_OPTS[@]}" \
    -Dsequencer.memberId=0 \
    -jar "${JAR}" \
    > "${SEQ_LOG}" 2>&1 &
SEQ_PID=$!

# Give the cluster time to elect a leader and open its archive before clients connect.
echo "[cluster.sh] Waiting for cluster to become ready…"
WAIT=0
until grep -q "Running" "${SEQ_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 40 )); then
        echo "ERROR: SequencerServer did not reach Running state after 20 s" >&2
        kill "${SEQ_PID}" 2>/dev/null
        exit 1
    fi
done
echo "[cluster.sh] SequencerServer is running"

echo "[cluster.sh] Starting ReplayerServer (co-located with SequencerServer member 0) → ${REPLAYER_LOG}"
java "${JAVA_OPTS[@]}" \
    -Dreplayer.memberId=0 \
    -cp "${JAR}" \
    org.limitless.seqeron.replayer.server.ReplayerServer \
    > "${REPLAYER_LOG}" 2>&1 &
REPLAYER_PID=$!

echo "[cluster.sh] Waiting for ReplayerServer to start serving replay…"
WAIT=0
until grep -q "serving replay" "${REPLAYER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 40 )); then
        echo "[cluster.sh] WARN: ReplayerServer not serving after 20s — starting the consumer anyway" >&2
        break
    fi
done

APP_PID=""
APP_LOG="${LOG_DIR}/ClusterProbe.log"
if [[ "${NO_CONSUMERS}" != "1" ]]; then
    echo "[cluster.sh] Starting ClusterProbe follower (replica on member 0) → ${APP_LOG}"
    java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.clientId=1 -Dprobe.latencyStats=true \
        -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${APP_LOG}" 2>&1 &
    APP_PID=$!
fi

ALL_PIDS=("${SEQ_PID}" "${REPLAYER_PID}")
[[ -n "${APP_PID}" ]] && ALL_PIDS+=("${APP_PID}")

echo "[cluster.sh] All processes started"
echo "  SequencerServer   pid=${SEQ_PID}  log=${SEQ_LOG}"
echo "  ReplayerServer    pid=${REPLAYER_PID}  log=${REPLAYER_LOG}"
if [[ -n "${APP_PID}" ]]; then
    echo "  ClusterProbe      pid=${APP_PID}  log=${APP_LOG}"
else
    echo "  ClusterProbe      (none — SEQERON_NO_CONSUMERS=1, the caller supplies its own)"
fi
echo "[cluster.sh] Press Ctrl-C to stop"

# ── Shutdown on Ctrl-C ────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "[cluster.sh] Stopping…"
    kill "${ALL_PIDS[@]}" 2>/dev/null
    wait "${ALL_PIDS[@]}" 2>/dev/null
    echo "[cluster.sh] Done"
}
trap cleanup INT TERM

# ── Monitor ───────────────────────────────────────────────────────────────────
# Wait until any child exits unexpectedly, then stop the rest.

wait_any() {
    while true; do
        for pid in "${ALL_PIDS[@]}"; do
            if ! kill -0 "${pid}" 2>/dev/null; then
                echo "[cluster.sh] Process ${pid} exited — shutting down"
                return
            fi
        done
        sleep 1
    done
}

wait_any
cleanup
