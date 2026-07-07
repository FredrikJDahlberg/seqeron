#!/usr/bin/env bash
# three-node-cluster.sh — bring up a local 3-node Aeron Cluster with one
# FixSessionClient, one OrderExecClient, then run fix_test_server against it.
#
# Launches, each writing to its own log file under logs/:
#   1. SequencerNode  x3  (Java, Raft members 0/1/2, all on localhost)
#   2. aeronmd            (shared Aeron media driver for the C++ clients)
#   3. FixSessionClient   (C++, FIX TCP gateway on port 9000)
#   4. OrderExecClient    (C++, replays global stream, tracks positions, answers risk queries)
# then runs fix_test_server once to completion and reports its result.
#
# FixSessionClient only ever *bootstraps* against member 0's archive (9301) and
# ingress (9302) endpoints, but follows SessionEvent REDIRECT/NewLeaderEvent to
# the real leader afterwards — so this works regardless of which member wins the
# Raft election, as long as member 0 is reachable at startup.
#
# OrderExecClient is co-located with member 0 (shares its Aeron directory) and
# always reads member 0's own archive over aeron:ipc — safe regardless of which
# member is leader, since every member's archive replicates the full global
# stream (see SequencerService's standby-follow). Cluster ingress tries
# aeron:ipc first and falls back to the same UDP bootstrap/redirect path as
# FixSessionClient when member 0 isn't currently leader.
#
# Uses a dedicated baseDir (${TMPDIR}phixeron-seq3) so it doesn't collide
# with single-node dev state left behind by start-cluster.sh.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#   cmake --build cmake-build-release              # build C++ targets
#
# Usage:
#   ./three-node-cluster.sh [debug|release]     default: release

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
MD_LOG="${LOG_DIR}/aeronmd.log"
FIX_LOG="${LOG_DIR}/FixSessionClient.log"
APP_LOG="${LOG_DIR}/OrderExecClient.log"
TEST_LOG="${LOG_DIR}/fix_test_server.log"

BASE_DIR="${TMPDIR:-/tmp}phixeron-seq3"

CLUSTER_MEMBERS="0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301"
CLUSTER_MEMBERS+="|1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311"
CLUSTER_MEMBERS+="|2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321"

# Default Aeron directory used by the standalone aeronmd and by FixSessionClient.
AERON_DIR="${TMPDIR}aeron-$(whoami)"

# SequencerNode member 0's own embedded media driver directory — matches its default
# when -Dsequencer.aeronDir isn't overridden (it isn't, below). OrderExecClient is
# co-located with member 0, sharing this directory instead of the standalone aeronmd's.
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

for bin in FixSessionClient OrderExecClient fix_test_server; do
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

# ── Launch cluster ────────────────────────────────────────────────────────────

SEQ_PIDS=()
SEQ_LOGS=()

for member in 0 1 2; do
    SEQ_LOG="${LOG_DIR}/sequencer-${member}.log"
    SEQ_LOGS+=("${SEQ_LOG}")
    echo "[three-node-cluster.sh] Starting SequencerNode (member ${member}) → ${SEQ_LOG}"
    java "${JAVA_OPTS[@]}" \
        -Dsequencer.memberId="${member}" \
        -Dsequencer.baseDir="${BASE_DIR}" \
        -Dsequencer.clusterMembers="${CLUSTER_MEMBERS}" \
        -jar "${JAR}" \
        > "${SEQ_LOG}" 2>&1 &
    SEQ_PIDS+=("$!")
done

# Give the cluster time to elect a leader and open each archive before clients connect.
echo "[three-node-cluster.sh] Waiting for cluster members to become ready…"
for SEQ_LOG in "${SEQ_LOGS[@]}"; do
    WAIT=0
    until grep -q "Running" "${SEQ_LOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "ERROR: ${SEQ_LOG} did not reach Running state after 30 s" >&2
            kill "${SEQ_PIDS[@]}" 2>/dev/null
            exit 1
        fi
    done
done
echo "[three-node-cluster.sh] All 3 cluster members are running"

echo "[three-node-cluster.sh] Starting Aeron media driver → ${MD_LOG}"
AERON_DIR="${AERON_DIR}" "${AERONMD}" > "${MD_LOG}" 2>&1 &
MD_PID=$!

# Wait for aeronmd to create its CnC file so C++ clients can attach.
echo "[three-node-cluster.sh] Waiting for media driver CnC file…"
WAIT=0
until [[ -f "${AERON_DIR}/cnc.dat" ]]; do
    sleep 0.2
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 25 )); then
        echo "ERROR: aeronmd CnC file not created after 5 s at ${AERON_DIR}/cnc.dat" >&2
        kill "${MD_PID}" "${SEQ_PIDS[@]}" 2>/dev/null
        exit 1
    fi
done
echo "[three-node-cluster.sh] Media driver ready"

echo "[three-node-cluster.sh] Starting FixSessionClient → ${FIX_LOG}"
stdbuf -oL -eL "${BUILD_DIR}/FixSessionClient" > "${FIX_LOG}" 2>&1 &
FIX_PID=$!

echo "[three-node-cluster.sh] Starting OrderExecClient (co-located with SequencerNode member 0) → ${APP_LOG}"
PHIXERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
    stdbuf -oL -eL "${BUILD_DIR}/OrderExecClient" > "${APP_LOG}" 2>&1 &
APP_PID=$!

# ── Shutdown handling ─────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "[three-node-cluster.sh] Stopping…"
    kill "${APP_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PIDS[@]}" 2>/dev/null || true
    wait "${APP_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PIDS[@]}" 2>/dev/null || true
    echo "[three-node-cluster.sh] Done"
    return 0
}
# Only signal-driven interrupts run cleanup via the trap; the normal exit path
# below calls cleanup explicitly so fix_test_server's pass/fail status isn't
# clobbered by wait's exit code for the just-killed background processes.
trap cleanup INT TERM

# ── Wait for the gateway, then run fix_test_server ───────────────────────────

echo "[three-node-cluster.sh] Waiting for FixSessionClient gateway on 127.0.0.1:9000…"
WAIT=0
until nc -z 127.0.0.1 9000 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 40 )); then
        echo "ERROR: 127.0.0.1:9000 not reachable after 20 s — see ${FIX_LOG}" >&2
        cleanup
        exit 1
    fi
done
echo "[three-node-cluster.sh] Gateway is up"

echo "[three-node-cluster.sh] Running fix_test_server → ${TEST_LOG}"
if stdbuf -oL -eL "${BUILD_DIR}/fix_test_server" 127.0.0.1 9000 2>&1 | tee "${TEST_LOG}"; then
    echo "[three-node-cluster.sh] fix_test_server PASSED"
    STATUS=0
else
    echo "[three-node-cluster.sh] fix_test_server FAILED — see ${TEST_LOG}" >&2
    STATUS=1
fi

cleanup
exit "${STATUS}"
