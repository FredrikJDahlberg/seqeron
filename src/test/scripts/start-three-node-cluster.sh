#!/usr/bin/env bash
# start-three-node-cluster.sh — bring up a local 3-node Aeron Cluster with a per-node ReplayerServer
# and a per-node consumer replica, then keep it running until interrupted.
#
# TWO MODES, and the default is the cluster tier alone. This script is
# core's, and core's own e2e must run with no C++ binary built:
#
#   default (SEQERON_PRODUCT_APPS unset)  — Java only:
#     1. SequencerServer  x3  (Java, Raft members 0/1/2, all on localhost)
#     2. ReplayerServer   x3  (Java, one co-located with each member: serves archive replay to co-located
#                              apps over aeron:ipc; apps read the tap directly for live)
#     3. ClusterProbe follow x3 (Java, one per-node consumer replica — the edge-neutral probe, standing
#                              in for the product replicas so READY still means "every node is following
#                              the live tail")
#
#   SEQERON_PRODUCT_APPS=1 — additionally the C++ edge's processes, which is what the product e2e
#   scripts (src/test/scripts/three-node-e2e-test.sh) set:
#     4. aeronmd            (standalone Aeron media driver, for C++ clients that DON'T co-locate with a
#                            member — in practice fix_test_server, which those scripts launch against
#                            this cluster. Every long-running app below uses a member's embedded driver.)
#     5. FixGateway   (C++, FIX TCP gateway on port 9000)
#     6. OrderExecServer x3 (C++, one per-node replica co-located with each member: all track positions
#                            from the same ordered stream; only the leader node's replica answers risk
#                            queries) — these REPLACE the probe replicas above
#     7. BasicDataServer x3 (C++, the reference-data gateway)
#
# then blocks, monitoring the launched processes; Ctrl-C (or kill) stops all of them cleanly. This
# script only starts and stops the cluster — it runs no tests.
#
# The product half is still written here rather than in a launcher the C++ edge owns. That is the
# remaining half of the cross-module reference §11 step 7a.2 recorded: the flag makes core's own suite
# independent of it, and 7b.6's carve is what deletes it from seqeron's copy.
#
# In product mode, FixGateway is co-located with member 0 (shares its Aeron directory, SEQ_AERON_DIR): it
# follows that node's SequencerService tap over aeron:ipc and reaches member 0's local archive over
# aeron:ipc for FIX-session resend recovery. Cluster ingress tries aeron:ipc first and falls back to
# UDP + SessionEvent REDIRECT/NewLeaderEvent to the real leader, so this works regardless of which
# member wins the Raft election.
#
# Each OrderExecServer is co-located with one member (shares that member's Aeron directory) and
# reads that node's SequencerService tap over aeron:ipc directly. Because a replica runs on every node,
# whichever member is elected leader has a local replica ready to answer risk queries (leader-only
# emission, design §3) — no dependence on which member wins the election. Each replica uses a distinct
# cluster egress port (9330 + memberId) so the three co-located clients don't collide on one host.
#
# Uses a dedicated baseDir (seqeron-seq3 under the temp dir) so it doesn't collide
# with single-node dev state left behind by start-cluster.sh.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#   cmake --build cmake-build-release              # C++ targets — SEQERON_PRODUCT_APPS=1 only
#
# Usage:
#   ./start-three-node-cluster.sh [debug|release]     default: release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_SCRIPTS="${SCRIPT_DIR}/../../main/scripts"
source "${MAIN_SCRIPTS}/ports.sh"
source "${MAIN_SCRIPTS}/paths.sh"

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

# 1 = also launch the C++ edge's processes (see the header). Default off: core's e2e runs Java-only.
PRODUCT_APPS="${SEQERON_PRODUCT_APPS:-0}"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

LOG_DIR="logs"
MD_LOG="${LOG_DIR}/aeronmd.log"
FIX_LOG="${LOG_DIR}/FixGateway.log"
REPLAYER_LOG="${LOG_DIR}/ReplayerServer.log"
APP_LOG="${LOG_DIR}/OrderExecServer.log"
BASICDATA_LOG="${LOG_DIR}/BasicDataServer.log"

BASE_DIR="${TMP_DIR}/seqeron-seq3"

CLUSTER_MEMBERS="$(cluster_members_string 3)"
FIX_TCP_PORT="$(fix_tcp_port)"

# Default Aeron directory: the standalone aeronmd's own, and what a C++ client that sets no
# SEQERON_*_AERON_DIR attaches to. NOT FixGateway's — that is pointed at SEQ_AERON_DIR below, as are
# OrderExecServer and BasicDataServer, so nothing this script launches uses this directory.
AERON_DIR="$(aeron_default_dir)"

# SequencerServer member 0's own embedded media driver directory — matches its default
# when -Dsequencer.aeronDir isn't overridden (it isn't, below). OrderExecServer is
# co-located with member 0, sharing this directory instead of the standalone aeronmd's.
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

if [[ "${PRODUCT_APPS}" == "1" ]]; then
    for bin in FixGateway OrderExecServer BasicDataServer; do
        if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
            echo "ERROR: ${BUILD_DIR}/${bin} not found — run: cmake --build ${BUILD_DIR}" >&2
            exit 1
        fi
    done

    if [[ ! -x "${AERONMD}" ]]; then
        echo "ERROR: ${AERONMD} not found — run: cmake --build ${BUILD_DIR}" >&2
        exit 1
    fi
fi

mkdir -p "${LOG_DIR}"

# ── Launch cluster ────────────────────────────────────────────────────────────

SEQ_PIDS=()
SEQ_LOGS=()

for member in 0 1 2; do
    SEQ_LOG="${LOG_DIR}/sequencer-${member}.log"
    SEQ_LOGS+=("${SEQ_LOG}")
    echo "[start-three-node-cluster.sh] Starting SequencerServer (member ${member}) → ${SEQ_LOG}"
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

# The gateway list. Not reference data and not BasicDataServer's: it is a
# deployment assertion an operator makes, so it comes in through clusterctl. It has to be in the log
# before the reference-data load — a gateway that has not resolved its own gatewaySourceId from the
# list drops every session row on ingest — and the bootstrap GatewayActive that designates each
# pair's primary is synthesized behind the list's last row.
#
# Java-only mode loads no list, and deliberately: a list is a deployment assertion naming instances
# this deployment actually runs, and there is no gateway process here to name. Listing one anyway
# makes the sequencer designate it, time out after GATEWAY_ACTIVATION_TIMEOUT_MS, hand the role to its
# standby and time out again — a GatewayActive every 5s, forever. The list itself is a product file
# (src/test/resources/topology-gw.xml names the C++ pair), which is the other reason. The election path
# stays covered by SequencerTest/SequencerServerTest and by the product e2e.
if [[ "${PRODUCT_APPS}" == "1" ]]; then
    echo "[start-three-node-cluster.sh] Loading the gateway list"
    if ! CLUSTERCTL_AERON_DIR="${SEQ_AERON_DIR}" CLUSTERCTL_INGRESS_ENDPOINTS="$(ingress_endpoints_string 3)" \
            "${MAIN_SCRIPTS}/clusterctl.sh" load-topology src/test/resources/topology-gw.xml \
            > "${LOG_DIR}/clusterctl-load-topology.log" 2>&1; then
        echo "ERROR: clusterctl load-topology failed — see ${LOG_DIR}/clusterctl-load-topology.log" >&2
        kill "${SEQ_PIDS[@]}" 2>/dev/null
        exit 1
    fi
fi

# The standalone driver serves only the C++ clients that do not co-locate with a member; every Java
# process here attaches to a member's own embedded driver, so it is product mode's alone.
MD_PID=""
if [[ "${PRODUCT_APPS}" == "1" ]]; then
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
fi

echo "[start-three-node-cluster.sh] Starting ReplayerServer (co-located with SequencerServer member 0) → ${REPLAYER_LOG}"
# Attaches to member 0's embedded media driver (replayer.memberId=0 → seqeron-seq-aeron-0, i.e.
# SEQ_AERON_DIR) and serves archive replay of that node's local tap recording over aeron:ipc.
# OrderExecServer (below) shares the same directory, reads the tap directly for live, and asks this
# ReplayerService to replay on a gap / for cold-start history.
java "${JAVA_OPTS[@]}" \
    -Dreplayer.memberId=0 \
    -cp "${JAR}" \
    org.limitless.seqeron.replayer.server.ReplayerServer \
    > "${REPLAYER_LOG}" 2>&1 &
REPLAYER_PID=$!

# Wait until the ReplayerService is serving replay (its local tap recording is visible on the archive), which
# also confirms the tap is live for the direct-read consumers. OrderExecServer retries regardless, but
# this avoids a noisy startup and a needless extra cold-start replay.
echo "[start-three-node-cluster.sh] Waiting for ReplayerServer to start serving replay…"
WAIT=0
until grep -q "serving replay" "${REPLAYER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 60 )); then
        echo "[start-three-node-cluster.sh] WARN: ReplayerServer not serving after 30s — starting OrderExecServer anyway" >&2
        break
    fi
done

# FixGateway is co-located with member 0 too (shares SEQ_AERON_DIR): it follows member 0's
# SequencerService tap over aeron:ipc and uses member 0's local archive over aeron:ipc for FIX-session
# resend recovery. Started only now — after member 0's ReplayerService is serving replay — so the tap exists
# and the local archive already holds the tap recording connectLocalArchive needs.
# SEQERON_REPLAYER_CLIENT_ID=2 keeps it distinct from the co-located OrderExecServer replica (id 1);
# its cluster egress port defaults to 9340+memberId, clear of the replica's 9330+memberId.
# SEQERON_SKIP_FIX_GATEWAY lets a caller own the FIX gateway itself instead of having this script
# launch and monitor one — needed by the two-gateway failover harness, which runs a primary and a hot
# standby and must kill the primary WITHOUT this script's monitor tearing down the whole cluster.
FIX_PID=""
if [[ "${PRODUCT_APPS}" == "1" && -z "${SEQERON_SKIP_FIX_GATEWAY:-}" ]]; then
    echo "[start-three-node-cluster.sh] Starting FixGateway (co-located with member 0) → ${FIX_LOG}"
    SEQERON_FIX_GATEWAY_AERON_DIR="${SEQ_AERON_DIR}" \
        SEQERON_NODE_MEMBER_ID=0 \
        SEQERON_REPLAYER_CLIENT_ID=2 \
        SEQERON_FIX_GATEWAY_NAME=GW-A \
        SEQERON_FIX_TCP_PORT="${FIX_TCP_PORT}" \
        stdbuf -oL -eL "${BUILD_DIR}/FixGateway" > "${FIX_LOG}" 2>&1 &
    FIX_PID=$!
elif [[ "${PRODUCT_APPS}" == "1" ]]; then
    echo "[start-three-node-cluster.sh] SEQERON_SKIP_FIX_GATEWAY set — not launching the FIX gateway (caller-owned)"
fi

# The per-node consumer replica. Member 0's is the latency-instrumented one: it records the
# post-consensus delivery latency (cluster-commit → its aeron:ipc tap tail) of every caught-up
# sequenced message and prints p50/p99/p99.9 on shutdown — the Variant-B "record→deliver" latency the
# ReplayerService design's per-node consumers inherit. Harmless when unused (it only prints on
# shutdown); the S4 wedge test reads it. The members 1 & 2 replicas run without it (one is enough).
BASICDATA_PID=""
if [[ "${PRODUCT_APPS}" == "1" ]]; then
    echo "[start-three-node-cluster.sh] Starting OrderExecServer (replica on member 0) → ${APP_LOG}"
    SEQERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
        SEQERON_NODE_MEMBER_ID=0 \
        SEQERON_LATENCY_STATS=1 \
        stdbuf -oL -eL "${BUILD_DIR}/OrderExecServer" > "${APP_LOG}" 2>&1 &
    APP_PID=$!

    # BasicDataServer (replica on member 0) — the reference-data gateway. Dual-role: the replica on
    # whichever member is leader produces the session/trading-day rows into the sequenced log once; every
    # replica consumes them back off the tap. The FIX gateway builds its SessionMap from those
    # BasicDataSession rows, so without this running every Logon is refused "Unknown SenderCompID".
    # SEQERON_REPLAYER_CLIENT_ID=3 keeps it distinct from the co-located OrderExecServer (1) and
    # FixGateway (2). Its cluster egress port must be given explicitly: the default is 9340+memberId,
    # which is exactly FixGateway's, so co-locating both on member 0 would collide — use 9350+memberId
    # (clear of the 9300-9329 cluster block, the replica's 9330+m and the gateway's 9340+m).
    echo "[start-three-node-cluster.sh] Starting BasicDataServer (replica on member 0) → ${BASICDATA_LOG}"
    SEQERON_BASICDATA_AERON_DIR="${SEQ_AERON_DIR}" \
        SEQERON_NODE_MEMBER_ID=0 \
        SEQERON_REPLAYER_CLIENT_ID=3 \
        SEQERON_BASICDATA_EGRESS_ENDPOINT="localhost:$(basicdata_egress_port 0)" \
        stdbuf -oL -eL "${BUILD_DIR}/BasicDataServer" > "${BASICDATA_LOG}" 2>&1 &
    BASICDATA_PID=$!
else
    APP_LOG="${LOG_DIR}/ClusterProbe.log"
    echo "[start-three-node-cluster.sh] Starting ClusterProbe follower (replica on member 0) → ${APP_LOG}"
    java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.clientId=1 -Dprobe.latencyStats=true \
        -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${APP_LOG}" 2>&1 &
    APP_PID=$!
fi

# A replica on every node (design §3): start a ReplayerServer + OrderExecServer co-located with members 1
# and 2 too. Each attaches to its own member's media driver (seqeron-seq-aeron-<m>), reads that
# node's SequencerService tap over aeron:ipc, and uses a distinct cluster egress port (9330 + m, derived
# from SEQERON_NODE_MEMBER_ID) so the three co-located clients don't collide on one host. Whichever
# member is leader then has a local replica ready to answer risk queries.
EXTRA_REPLAYER_PIDS=()
EXTRA_APP_PIDS=()
EXTRA_APP_LOGS=()
EXTRA_BASICDATA_PIDS=()
for m in 1 2; do
    RLOG="${LOG_DIR}/ReplayerServer-${m}.log"
    ALOG="${LOG_DIR}/OrderExecServer-${m}.log"
    BDLOG="${LOG_DIR}/BasicDataServer-${m}.log"
    MDIR="${TMP_DIR}/seqeron-seq-aeron-${m}"
    echo "[start-three-node-cluster.sh] Starting ReplayerServer + consumer replica (member ${m})"
    java "${JAVA_OPTS[@]}" -Dreplayer.memberId="${m}" -cp "${JAR}" \
        org.limitless.seqeron.replayer.server.ReplayerServer > "${RLOG}" 2>&1 &
    EXTRA_REPLAYER_PIDS+=("$!")
    WAIT=0
    until grep -q "serving replay" "${RLOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        (( WAIT > 60 )) && { echo "[start-three-node-cluster.sh] WARN: ReplayerServer-${m} not serving after 30s" >&2; break; }
    done
    if [[ "${PRODUCT_APPS}" == "1" ]]; then
        SEQERON_ORDER_EXEC_AERON_DIR="${MDIR}" \
            SEQERON_NODE_MEMBER_ID="${m}" \
            stdbuf -oL -eL "${BUILD_DIR}/OrderExecServer" > "${ALOG}" 2>&1 &
        EXTRA_APP_PIDS+=("$!")
        EXTRA_APP_LOGS+=("${ALOG}")
        # One BasicDataServer replica per node too, so whichever member is elected leader has a local
        # replica able to produce the load (and every node keeps the consumed tables warm for failover).
        SEQERON_BASICDATA_AERON_DIR="${MDIR}" \
            SEQERON_NODE_MEMBER_ID="${m}" \
            SEQERON_REPLAYER_CLIENT_ID=3 \
            SEQERON_BASICDATA_EGRESS_ENDPOINT="localhost:$(basicdata_egress_port "${m}")" \
            stdbuf -oL -eL "${BUILD_DIR}/BasicDataServer" > "${BDLOG}" 2>&1 &
        EXTRA_BASICDATA_PIDS+=("$!")
    else
        ALOG="${LOG_DIR}/ClusterProbe-${m}.log"
        java "${JAVA_OPTS[@]}" -Dprobe.memberId="${m}" -Dprobe.clientId=1 \
            -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${ALOG}" 2>&1 &
        EXTRA_APP_PIDS+=("$!")
        EXTRA_APP_LOGS+=("${ALOG}")
    fi
done

ALL_PIDS=("${APP_PID}" "${REPLAYER_PID}" "${EXTRA_APP_PIDS[@]}" "${EXTRA_REPLAYER_PIDS[@]}" \
          "${SEQ_PIDS[@]}")
for pid in "${EXTRA_BASICDATA_PIDS[@]+"${EXTRA_BASICDATA_PIDS[@]}"}" "${BASICDATA_PID}" "${MD_PID}" "${FIX_PID}"; do
    [[ -n "${pid}" ]] && ALL_PIDS+=("${pid}")
done

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

if [[ -n "${FIX_PID}" ]]; then
    echo "[start-three-node-cluster.sh] Waiting for FixGateway on 127.0.0.1:${FIX_TCP_PORT}…"
    WAIT=0
    until nc -z 127.0.0.1 "${FIX_TCP_PORT}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 40 )); then
            echo "ERROR: 127.0.0.1:${FIX_TCP_PORT} not reachable after 20 s — see ${FIX_LOG}" >&2
            cleanup
            exit 1
        fi
    done
    echo "[start-three-node-cluster.sh] Gateway is up"
fi

# Only a caught-up replica answers a PortfolioQueryRequest (the isCaughtUp gate against re-answering
# replayed history, design §3). We don't know which member won the election, so wait for ALL three
# replicas to reach the live tail — that guarantees the leader's replica is ready to answer.
echo "[start-three-node-cluster.sh] Waiting for all consumer replicas to catch up to the live tail…"
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

# The gateway gates logons on EndBasicData itself, so a client that
# connects early queues in the listen backlog rather than being refused — this wait is not needed for
# correctness. It is here so READY means "a Logon will be answered now", keeping the harness's
# failure modes distinguishable: a genuine hang shows up here, not as a client-side connect timeout.
if [[ -n "${FIX_PID}" ]]; then
    echo "[start-three-node-cluster.sh] Waiting for the FIX gateway to load basic data…"
    WAIT=0
    until grep -q "Basic data loaded" "${FIX_LOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "[start-three-node-cluster.sh] WARN: gateway has not seen EndBasicData after 30s —" \
                 "its logon gate is still shut, so clients will connect but get no Logon reply" >&2
            break
        fi
    done
fi

echo "[start-three-node-cluster.sh] READY — cluster is up and all replicas are following the live tail"
echo "  SequencerServer     pids=${SEQ_PIDS[*]}"
echo "  ReplayerServer      pids=${REPLAYER_PID} ${EXTRA_REPLAYER_PIDS[*]}"
if [[ "${PRODUCT_APPS}" == "1" ]]; then
    echo "  aeronmd           pid=${MD_PID}"
    if [[ -n "${FIX_PID}" ]]; then
        echo "  FixGateway  pid=${FIX_PID}   log=${FIX_LOG}"
    else
        echo "  FixGateway  (skipped — caller-owned)"
    fi
    echo "  OrderExecServer   pids=${APP_PID} ${EXTRA_APP_PIDS[*]}"
    echo "  BasicDataServer   pids=${BASICDATA_PID} ${EXTRA_BASICDATA_PIDS[*]}"
else
    echo "  ClusterProbe      pids=${APP_PID} ${EXTRA_APP_PIDS[*]}   (Java-only mode)"
fi
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
