#!/usr/bin/env bash
# cluster.sh — start the phixeron single-node cluster and its C++ clients.
#
# Launches four processes in the background, each writing to its own log file:
#   1. SequencerNode  (Java, single-node Aeron Cluster, member 0)
#   2. FixSessionClient  (C++, FIX TCP gateway on port 9000)
#   3. RouterNode  (Java, co-located with member 0: sole global-stream reader on the node; fans it
#                   out over aeron:ipc and serves replays — doc/router-design.md)
#   4. OrderExecClient  (C++, a replica behind RouterNode: consumes its IPC fan-out, tracks
#                        positions, answers risk queries while its node is leader)
#
# Ctrl-C (or kill $$ / kill -- -$$) stops all four cleanly.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#   cmake --build cmake-build-release              # build C++ targets
#
# Usage:
#   ./cluster.sh [debug|release]     default: release

set -euo pipefail

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
JAR="build/libs/phixeron-0.1.0-uber.jar"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

LOG_DIR="logs"
SEQ_LOG="${LOG_DIR}/sequencer.log"
MD_LOG="${LOG_DIR}/aeronmd.log"
FIX_LOG="${LOG_DIR}/FixSessionClient.log"
ROUTER_LOG="${LOG_DIR}/RouterNode.log"
APP_LOG="${LOG_DIR}/OrderExecClient.log"

# Default Aeron directory used by the standalone aeronmd and by FixSessionClient.
AERON_DIR="${TMPDIR}aeron-$(whoami)"

# SequencerNode (member 0)'s own embedded media driver directory — matches its default
# when -Dsequencer.aeronDir isn't overridden. OrderExecClient is co-located with this
# member (shares its Aeron directory) so archive/replay/ingress can use aeron:ipc instead
# of looping through the standalone aeronmd above — see ClusterIngressSender::connectColocated.
SEQ_AERON_DIR="${TMPDIR}phixeron-seq-aeron-0"

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

for bin in FixSessionClient OrderExecClient; do
    if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
        echo "ERROR: ${BUILD_DIR}/${bin} not found — run: cmake --build ${BUILD_DIR}" >&2
        exit 1
    fi
done

if [[ ! -x "${AERONMD}" ]]; then
    echo "ERROR: ${AERONMD} not found — run: cmake --build ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"

# ── Launch ────────────────────────────────────────────────────────────────────

echo "[cluster.sh] Starting SequencerNode (member 0) → ${SEQ_LOG}"
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
        echo "ERROR: SequencerNode did not reach Running state after 20 s" >&2
        kill "${SEQ_PID}" 2>/dev/null
        exit 1
    fi
done
echo "[cluster.sh] SequencerNode is running"

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

echo "[cluster.sh] Starting FixSessionClient → ${FIX_LOG}"
stdbuf -oL -eL "${BUILD_DIR}/FixSessionClient" > "${FIX_LOG}" 2>&1 &
FIX_PID=$!

echo "[cluster.sh] Starting RouterNode (co-located with SequencerNode member 0) → ${ROUTER_LOG}"
java "${JAVA_OPTS[@]}" \
    -Drouter.memberId=0 \
    -cp "${JAR}" \
    org.limitless.phixeron.router.RouterNode \
    > "${ROUTER_LOG}" 2>&1 &
ROUTER_PID=$!

echo "[cluster.sh] Waiting for RouterNode to follow the global-stream recording…"
WAIT=0
until grep -q "following recording" "${ROUTER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 40 )); then
        echo "[cluster.sh] WARN: RouterNode not following after 20s — starting OrderExecClient anyway" >&2
        break
    fi
done

echo "[cluster.sh] Starting OrderExecClient (replica behind member 0's RouterNode) → ${APP_LOG}"
PHIXERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
    stdbuf -oL -eL "${BUILD_DIR}/OrderExecClient" > "${APP_LOG}" 2>&1 &
APP_PID=$!

echo "[cluster.sh] All processes started"
echo "  SequencerNode     pid=${SEQ_PID}  log=${SEQ_LOG}"
echo "  aeronmd           pid=${MD_PID}   log=${MD_LOG}"
echo "  FixSessionClient  pid=${FIX_PID}  log=${FIX_LOG}"
echo "  RouterNode        pid=${ROUTER_PID}  log=${ROUTER_LOG}"
echo "  OrderExecClient   pid=${APP_PID}  log=${APP_LOG}"
echo "[cluster.sh] Press Ctrl-C to stop"

# ── Shutdown on Ctrl-C ────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "[cluster.sh] Stopping…"
    kill "${APP_PID}" "${ROUTER_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PID}" 2>/dev/null
    wait "${APP_PID}" "${ROUTER_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PID}" 2>/dev/null
    echo "[cluster.sh] Done"
}
trap cleanup INT TERM

# ── Monitor ───────────────────────────────────────────────────────────────────
# Wait until any child exits unexpectedly, then stop the rest.

wait_any() {
    while true; do
        for pid in "${SEQ_PID}" "${MD_PID}" "${FIX_PID}" "${ROUTER_PID}" "${APP_PID}"; do
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
