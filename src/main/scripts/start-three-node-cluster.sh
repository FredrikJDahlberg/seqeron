#!/usr/bin/env bash
# start-three-node-cluster.sh — bring up a local 3-node Aeron Cluster with a
# FixSessionClient and a per-node RouterNode + OrderExecClient replica on every
# member, then keep it running until interrupted.
#
# Launches, each writing to its own log file under logs/:
#   1. SequencerNode  x3  (Java, Raft members 0/1/2, all on localhost)
#   2. aeronmd            (shared Aeron media driver for the C++ clients)
#   3. FixSessionClient   (C++, FIX TCP gateway on port 9000)
#   4. RouterNode     x3  (Java, one co-located with each member: the only global-stream reader on
#                          that node; fans it out over aeron:ipc and serves replays — router-design.md)
#   5. OrderExecClient x3 (C++, one per-node replica behind each RouterNode: all track positions from
#                          the same ordered stream; only the leader node's replica answers risk queries)
# then blocks, monitoring the launched processes; Ctrl-C (or kill) stops all of them cleanly. This
# script only starts and stops the cluster — it runs no tests. To drive it end-to-end, use the test
# scripts under src/test/scripts/ (e.g. three-node-e2e-test.sh), which start it via this script and
# tear it down via stop-cluster.sh.
#
# FixSessionClient is co-located with member 0 (shares its Aeron directory, SEQ_AERON_DIR): it
# follows that node's Router fan-out over aeron:ipc and reaches member 0's local archive over
# aeron:ipc for FIX-session resend recovery. Cluster ingress tries aeron:ipc first and falls back to
# UDP + SessionEvent REDIRECT/NewLeaderEvent to the real leader, so this works regardless of which
# member wins the Raft election.
#
# Each OrderExecClient is co-located with one member (shares that member's Aeron directory) and
# reads that node's RouterNode fan-out over aeron:ipc. Because a replica runs on every node, whichever
# member is elected leader has a local replica ready to answer risk queries (leader-only emission,
# design §3) — no dependence on which member wins the election. Each replica uses a distinct cluster
# egress port (9330 + memberId) so the three co-located clients don't collide on one host.
#
# Uses a dedicated baseDir (${TMPDIR}phixeron-seq3) so it doesn't collide
# with single-node dev state left behind by start-cluster.sh.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#   cmake --build cmake-build-release              # build C++ targets
#
# Usage:
#   ./start-three-node-cluster.sh [debug|release]     default: release

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
ROUTER_LOG="${LOG_DIR}/RouterNode.log"
APP_LOG="${LOG_DIR}/OrderExecClient.log"

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

# ── Launch cluster ────────────────────────────────────────────────────────────

SEQ_PIDS=()
SEQ_LOGS=()

for member in 0 1 2; do
    SEQ_LOG="${LOG_DIR}/sequencer-${member}.log"
    SEQ_LOGS+=("${SEQ_LOG}")
    echo "[start-three-node-cluster.sh] Starting SequencerNode (member ${member}) → ${SEQ_LOG}"
    java "${JAVA_OPTS[@]}" \
        -Dsequencer.memberId="${member}" \
        -Dsequencer.baseDir="${BASE_DIR}" \
        -Dsequencer.clusterMembers="${CLUSTER_MEMBERS}" \
        -jar "${JAR}" \
        > "${SEQ_LOG}" 2>&1 &
    SEQ_PIDS+=("$!")
done

# Give the cluster time to elect a leader and open each archive before clients connect.
echo "[start-three-node-cluster.sh] Waiting for cluster members to become ready…"
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
echo "[start-three-node-cluster.sh] All 3 cluster members are running"

echo "[start-three-node-cluster.sh] Starting Aeron media driver → ${MD_LOG}"
AERON_DIR="${AERON_DIR}" "${AERONMD}" > "${MD_LOG}" 2>&1 &
MD_PID=$!

# Wait for aeronmd to create its CnC file so C++ clients can attach.
echo "[start-three-node-cluster.sh] Waiting for media driver CnC file…"
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
echo "[start-three-node-cluster.sh] Media driver ready"

echo "[start-three-node-cluster.sh] Starting RouterNode (co-located with SequencerNode member 0) → ${ROUTER_LOG}"
# Attaches to member 0's embedded media driver (router.memberId=0 → phixeron-seq-aeron-0, i.e.
# SEQ_AERON_DIR) and reads that node's local global-stream recording over aeron:ipc, fanning it out
# to co-located replicas. OrderExecClient (below) shares the same directory and consumes that fan-out.
java "${JAVA_OPTS[@]}" \
    -Drouter.memberId=0 \
    -cp "${JAR}" \
    org.limitless.phixeron.router.RouterNode \
    > "${ROUTER_LOG}" 2>&1 &
ROUTER_PID=$!

# Wait until the Router's node-local IPC tap is live, so OrderExecClient's first replay request lands
# on a Router that is already relaying (it retries regardless, but this avoids a noisy startup and a
# needless extra replay).
echo "[start-three-node-cluster.sh] Waiting for RouterNode's IPC tap to go live…"
WAIT=0
until grep -q "IPC tap live" "${ROUTER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 60 )); then
        echo "[start-three-node-cluster.sh] WARN: RouterNode not following after 30s — starting OrderExecClient anyway" >&2
        break
    fi
done

# FixSessionClient is co-located with member 0 too (shares SEQ_AERON_DIR): it follows member 0's
# Router fan-out over aeron:ipc and uses member 0's local archive over aeron:ipc for FIX-session
# resend recovery. Started only now — after member 0's Router is following — so the fan-out exists
# and the local archive already holds the global-stream recording connectLocalArchive needs.
# PHIXERON_ROUTER_CLIENT_ID=2 keeps it distinct from the co-located OrderExecClient replica (id 1);
# its cluster egress port defaults to 9340+memberId, clear of the replica's 9330+memberId.
echo "[start-three-node-cluster.sh] Starting FixSessionClient (co-located with member 0) → ${FIX_LOG}"
PHIXERON_FIX_GATEWAY_AERON_DIR="${SEQ_AERON_DIR}" \
    PHIXERON_NODE_MEMBER_ID=0 \
    PHIXERON_ROUTER_CLIENT_ID=2 \
    stdbuf -oL -eL "${BUILD_DIR}/FixSessionClient" > "${FIX_LOG}" 2>&1 &
FIX_PID=$!

echo "[start-three-node-cluster.sh] Starting OrderExecClient (replica on member 0) → ${APP_LOG}"
# Member 0's replica is the latency-instrumented one: PHIXERON_LATENCY_STATS=1 makes it record the
# post-consensus delivery latency (cluster-commit → its aeron:ipc Router-fan-out tail) of every
# caught-up global-stream message and print p50/p99/p99.9 on shutdown — the Variant-B
# "record→(replicate→)replay" latency the Router design's per-node consumers inherit. Harmless when
# unused (it only prints on shutdown); the S4 wedge test reads it. The members 1 & 2 replicas below
# run without it (one measurement is enough).
PHIXERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
    PHIXERON_NODE_MEMBER_ID=0 \
    PHIXERON_LATENCY_STATS=1 \
    stdbuf -oL -eL "${BUILD_DIR}/OrderExecClient" > "${APP_LOG}" 2>&1 &
APP_PID=$!

# A replica on every node (design §3): start a RouterNode + OrderExecClient co-located with members 1
# and 2 too. Each attaches to its own member's media driver (phixeron-seq-aeron-<m>), reads that
# node's Router fan-out over aeron:ipc, and uses a distinct cluster egress port (9330 + m, derived
# from PHIXERON_NODE_MEMBER_ID) so the three co-located clients don't collide on one host. Whichever
# member is leader then has a local replica ready to answer risk queries.
EXTRA_ROUTER_PIDS=()
EXTRA_APP_PIDS=()
EXTRA_APP_LOGS=()
for m in 1 2; do
    RLOG="${LOG_DIR}/RouterNode-${m}.log"
    ALOG="${LOG_DIR}/OrderExecClient-${m}.log"
    MDIR="${TMPDIR}phixeron-seq-aeron-${m}"
    echo "[start-three-node-cluster.sh] Starting RouterNode + OrderExecClient (replica on member ${m})"
    java "${JAVA_OPTS[@]}" -Drouter.memberId="${m}" -cp "${JAR}" \
        org.limitless.phixeron.router.RouterNode > "${RLOG}" 2>&1 &
    EXTRA_ROUTER_PIDS+=("$!")
    WAIT=0
    until grep -q "IPC tap live" "${RLOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        (( WAIT > 60 )) && { echo "[start-three-node-cluster.sh] WARN: RouterNode-${m} IPC tap not live after 30s" >&2; break; }
    done
    PHIXERON_ORDER_EXEC_AERON_DIR="${MDIR}" \
        PHIXERON_NODE_MEMBER_ID="${m}" \
        stdbuf -oL -eL "${BUILD_DIR}/OrderExecClient" > "${ALOG}" 2>&1 &
    EXTRA_APP_PIDS+=("$!")
    EXTRA_APP_LOGS+=("${ALOG}")
done

ALL_PIDS=("${APP_PID}" "${ROUTER_PID}" "${EXTRA_APP_PIDS[@]}" "${EXTRA_ROUTER_PIDS[@]}" \
          "${FIX_PID}" "${MD_PID}" "${SEQ_PIDS[@]}")

# ── Shutdown handling ─────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "[start-three-node-cluster.sh] Stopping…"
    kill "${ALL_PIDS[@]}" 2>/dev/null || true
    wait "${ALL_PIDS[@]}" 2>/dev/null || true
    echo "[start-three-node-cluster.sh] Done"
}
trap cleanup INT TERM

# ── Wait until fully ready ────────────────────────────────────────────────────

echo "[start-three-node-cluster.sh] Waiting for FixSessionClient gateway on 127.0.0.1:9000…"
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
echo "[start-three-node-cluster.sh] Gateway is up"

# Only a caught-up replica answers a PortfolioQueryRequest (the isCaughtUp gate against re-answering
# replayed history, design §3). We don't know which member won the election, so wait for ALL three
# replicas to reach the live tail — that guarantees the leader's replica is ready to answer.
echo "[start-three-node-cluster.sh] Waiting for all OrderExecClient replicas to catch up to the live tail…"
for LOG in "${APP_LOG}" "${EXTRA_APP_LOGS[@]}"; do
    WAIT=0
    until grep -q "following live" "${LOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "[start-three-node-cluster.sh] WARN: replica ${LOG} not caught up after 30s — proceeding anyway" >&2
            break
        fi
    done
done

echo "[start-three-node-cluster.sh] READY — cluster is up and all replicas are following the live tail"
echo "  SequencerNode     pids=${SEQ_PIDS[*]}"
echo "  aeronmd           pid=${MD_PID}"
echo "  FixSessionClient  pid=${FIX_PID}   log=${FIX_LOG}"
echo "  RouterNode        pids=${ROUTER_PID} ${EXTRA_ROUTER_PIDS[*]}"
echo "  OrderExecClient   pids=${APP_PID} ${EXTRA_APP_PIDS[*]}"
echo "[start-three-node-cluster.sh] Press Ctrl-C to stop"

# ── Monitor ───────────────────────────────────────────────────────────────────
# Block until any child exits unexpectedly, then stop the rest.

while true; do
    for pid in "${ALL_PIDS[@]}"; do
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "[start-three-node-cluster.sh] Process ${pid} exited — shutting down"
            cleanup
            exit 0
        fi
    done
    sleep 1
done
