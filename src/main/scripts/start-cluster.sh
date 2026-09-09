#!/usr/bin/env bash
# start-cluster.sh — start the single-node cluster and a co-located consumer replica.
#
# TWO MODES, and the default is the cluster tier alone — this repo builds no product binary, so its
# own dev cluster must come up without one (the same split start-three-node-cluster.sh makes):
#
#   default (SEQERON_PRODUCT_APPS unset) — Java only, each process to its own log file:
#     1. SequencerServer  (Java, single-node Aeron Cluster, member 0)
#     2. ReplayerServer   (Java, co-located with member 0: serves archive replay to co-located apps
#                          over aeron:ipc; apps read the tap directly for live)
#     3. ClusterProbe follow (Java, the edge-neutral consumer replica standing in for a product one)
#
#   SEQERON_PRODUCT_APPS=1 — additionally the product repo's C++ processes, which must already be
#   built into ${BUILD_DIR} by that repo:
#     4. aeronmd          (standalone Aeron media driver, for C++ clients that do NOT co-locate)
#     5. FixGateway       (C++, FIX TCP gateway on port 9000)
#     6. OrderExecServer  (C++, a replica co-located with member 0: reads the tap directly, tracks
#                          positions, answers risk queries while its node is leader) — REPLACES the
#                          probe replica above
#
# Ctrl-C (or kill $$ / kill -- -$$) stops all of them cleanly.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#   cmake --build cmake-build-release              # product mode only, in the product repo
#
# Usage:
#   ./start-cluster.sh [debug|release]     default: release

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/paths.sh"

usage() {
    echo "Usage: $0 [debug|release]"
    echo "  default: release"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# ── Config ────────────────────────────────────────────────────────────────────

BUILD_TYPE="${1:-release}"
BUILD_DIR="cmake-build-${BUILD_TYPE}"
JAR="build/libs/seqeron-0.1.0-uber.jar"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

PRODUCT_APPS="${SEQERON_PRODUCT_APPS:-}"

LOG_DIR="logs"
SEQ_LOG="${LOG_DIR}/sequencer.log"
MD_LOG="${LOG_DIR}/aeronmd.log"
FIX_LOG="${LOG_DIR}/FixGateway.log"
REPLAYER_LOG="${LOG_DIR}/ReplayerServer.log"
APP_LOG="${LOG_DIR}/OrderExecServer.log"

# Default Aeron directory used by the standalone aeronmd and by FixGateway.
AERON_DIR="$(aeron_default_dir)"

# SequencerServer (member 0)'s own embedded media driver directory — matches its default
# when -Dsequencer.aeronDir isn't overridden. OrderExecServer is co-located with this
# member (shares its Aeron directory) so archive/replay/ingress can use aeron:ipc instead
# of looping through the standalone aeronmd above — see ClusterStreamSender::connectColocated.
SEQ_AERON_DIR="${TMP_DIR}/seqeron-seq-aeron-0"

# Prefer a system-installed aeronmd (e.g. Homebrew or a system package) on PATH;
# fall back to the CMake FetchContent build-tree copy if none is found there.
if command -v aeronmd >/dev/null 2>&1; then
    AERONMD="$(command -v aeronmd)"
else
    AERONMD="${BUILD_DIR}/_deps/aeron-build/binaries/aeronmd"
fi


# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

if [[ -n "${PRODUCT_APPS}" ]]; then
    for bin in FixGateway OrderExecServer; do
        if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
            echo "ERROR: ${BUILD_DIR}/${bin} not found — it is the product repo's binary, built there" >&2
            exit 1
        fi
    done

    if [[ ! -x "${AERONMD}" ]]; then
        echo "ERROR: ${AERONMD} not found — run: cmake --build ${BUILD_DIR}" >&2
        exit 1
    fi
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

MD_PID=""
FIX_PID=""
if [[ -n "${PRODUCT_APPS}" ]]; then
    echo "[cluster.sh] Starting Aeron media driver → ${MD_LOG}"
    AERON_DIR="${AERON_DIR}" "${AERONMD}" > "${MD_LOG}" 2>&1 &
    MD_PID=$!

    # Wait for aeronmd to create its CnC file so C++ clients can attach.
    echo "[cluster.sh] Waiting for media driver CnC file…"
    WAIT=0
    until [[ -f "${AERON_DIR}/cnc.dat" ]]; do
        sleep 0.2
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 25 )); then
            echo "ERROR: aeronmd CnC file not created after 5 s at ${AERON_DIR}/cnc.dat" >&2
            kill "${MD_PID}" "${SEQ_PID}" 2>/dev/null
            exit 1
        fi
    done
    echo "[cluster.sh] Media driver ready"

    echo "[cluster.sh] Starting FixGateway → ${FIX_LOG}"
    stdbuf -oL -eL "${BUILD_DIR}/FixGateway" > "${FIX_LOG}" 2>&1 &
    FIX_PID=$!
fi

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
        echo "[cluster.sh] WARN: ReplayerServer not serving after 20s — starting OrderExecServer anyway" >&2
        break
    fi
done

if [[ -n "${PRODUCT_APPS}" ]]; then
    echo "[cluster.sh] Starting OrderExecServer (replica co-located with member 0) → ${APP_LOG}"
    SEQERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
        stdbuf -oL -eL "${BUILD_DIR}/OrderExecServer" > "${APP_LOG}" 2>&1 &
    APP_PID=$!
else
    APP_LOG="${LOG_DIR}/ClusterProbe.log"
    echo "[cluster.sh] Starting ClusterProbe follower (replica on member 0) → ${APP_LOG}"
    java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.clientId=1 -Dprobe.latencyStats=true \
        -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${APP_LOG}" 2>&1 &
    APP_PID=$!
fi

ALL_PIDS=("${SEQ_PID}" "${REPLAYER_PID}" "${APP_PID}")
for pid in "${MD_PID}" "${FIX_PID}"; do
    [[ -n "${pid}" ]] && ALL_PIDS+=("${pid}")
done

echo "[cluster.sh] All processes started"
echo "  SequencerServer   pid=${SEQ_PID}  log=${SEQ_LOG}"
echo "  ReplayerServer    pid=${REPLAYER_PID}  log=${REPLAYER_LOG}"
if [[ -n "${PRODUCT_APPS}" ]]; then
    echo "  aeronmd           pid=${MD_PID}   log=${MD_LOG}"
    echo "  FixGateway        pid=${FIX_PID}  log=${FIX_LOG}"
    echo "  OrderExecServer   pid=${APP_PID}  log=${APP_LOG}"
else
    echo "  ClusterProbe      pid=${APP_PID}  log=${APP_LOG}   (Java-only mode)"
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
