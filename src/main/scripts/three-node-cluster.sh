#!/usr/bin/env bash
# three-node-cluster.sh — bring up a local 3-node Aeron Cluster with one
# FixSessionClient, one OrderExecClient, then run fix_test_server against it.
#
# Launches, each writing to its own log file under logs/:
#   1. SequencerNode  x3  (Java, Raft members 0/1/2, all on localhost)
#   2. aeronmd            (shared Aeron media driver for the C++ clients)
#   3. FixSessionClient   (C++, FIX TCP gateway on port 9000)
#   4. RouterNode         (Java, co-located with member 0: the only global-stream reader on that
#                          node; fans it out over aeron:ipc and serves replays — doc/router-design.md)
#   5. OrderExecClient    (C++, a per-node replica behind RouterNode: consumes its IPC fan-out,
#                          tracks positions, answers risk queries iff its node is leader)
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
ROUTER_LOG="${LOG_DIR}/RouterNode.log"
APP_LOG="${LOG_DIR}/OrderExecClient.log"
PROBE_LOG="${LOG_DIR}/GlobalStreamLatencyProbe.log"
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

# ── S4 wedge smoke test (opt-in via PHIXERON_FLOOD_ORDERS) ───────────────────
# When PHIXERON_FLOOD_ORDERS=<N> is set, this run becomes the audit S4 decoupling test instead of
# the normal end-to-end pass:
#   * each SequencerNode gets -Dphixeron.globalStream.termLength (default 65536) so the global
#     stream's flow-control window is tiny — back-pressure from an un-drained global-stream
#     subscriber (every client subscribes a live MDC sub but never polls it in steady state,
#     following via archive replay instead) manifests after a few hundred messages rather than a
#     full default term buffer;
#   * fix_test_server runs its direct-cluster-ingress flood (PHIXERON_FLOOD_ORDERS messages, see
#     runIngressFloodTest) instead of the functional suite — it bypasses the FIX gateway entirely,
#     so FIX heartbeats/resends can't confound the result; PHIXERON_FLOOD_ORDERS passes through the
#     environment automatically.
# Expected: a tether=false (post-fix) build sequences the whole flood with zero back-pressure
# alerts; a tethered build wedges ("ALERT: global stream back-pressure" in sequencer-*.log). See
# the S4 summary printed after the run below.
SEQ_S4_OPTS=()
if [[ -n "${PHIXERON_FLOOD_ORDERS:-}" ]]; then
    SEQ_S4_OPTS+=("-Dphixeron.globalStream.termLength=${PHIXERON_GLOBAL_TERM_LENGTH:-65536}")
    echo "[three-node-cluster.sh] S4 wedge test ENABLED — flood ${PHIXERON_FLOOD_ORDERS} orders," \
         "global-stream term-length=${PHIXERON_GLOBAL_TERM_LENGTH:-65536}"
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
        ${SEQ_S4_OPTS[@]+"${SEQ_S4_OPTS[@]}"} \
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

echo "[three-node-cluster.sh] Starting RouterNode (co-located with SequencerNode member 0) → ${ROUTER_LOG}"
# Attaches to member 0's embedded media driver (router.memberId=0 → phixeron-seq-aeron-0, i.e.
# SEQ_AERON_DIR) and reads that node's local global-stream recording over aeron:ipc, fanning it out
# to co-located replicas. OrderExecClient (below) shares the same directory and consumes that fan-out.
java "${JAVA_OPTS[@]}" \
    -Drouter.memberId=0 \
    -cp "${JAR}" \
    org.limitless.phixeron.router.RouterNode \
    > "${ROUTER_LOG}" 2>&1 &
ROUTER_PID=$!

# Wait until the Router has located member 0's recording and begun following it, so OrderExecClient's
# first replay request lands on a Router that can serve it (it retries regardless, but this avoids a
# noisy startup and a needless extra replay).
echo "[three-node-cluster.sh] Waiting for RouterNode to follow the global-stream recording…"
WAIT=0
until grep -q "following recording" "${ROUTER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 60 )); then
        echo "[three-node-cluster.sh] WARN: RouterNode not following after 30s — starting OrderExecClient anyway" >&2
        break
    fi
done

echo "[three-node-cluster.sh] Starting OrderExecClient (replica behind member 0's RouterNode) → ${APP_LOG}"
# PHIXERON_LATENCY_STATS=1 makes OrderExecClient record the post-consensus delivery latency
# (cluster-commit → its aeron:ipc archive-replay tail) of every caught-up global-stream message and
# print p50/p99/p99.9 on shutdown — the Variant-B "record→(replicate→)replay" latency the Router
# design's per-node consumers would inherit. Surfaced in the flood summary below.
PHIXERON_ORDER_EXEC_AERON_DIR="${SEQ_AERON_DIR}" \
    PHIXERON_LATENCY_STATS=1 \
    stdbuf -oL -eL "${BUILD_DIR}/OrderExecClient" > "${APP_LOG}" 2>&1 &
APP_PID=$!

# Variant-A latency probe (flood mode only): a direct live-MDC subscriber to the global stream,
# measured side-by-side with OrderExecClient's Variant-B (archive IPC replay) stat for an A-vs-B
# delta on the same box under the same flood. Attaches to the standalone media driver like
# FixSessionClient (default Aeron dir), so it needs no co-location.
PROBE_PID=""
if [[ -n "${PHIXERON_FLOOD_ORDERS:-}" ]]; then
    echo "[three-node-cluster.sh] Starting GlobalStreamLatencyProbe (Variant-A) → ${PROBE_LOG}"
    stdbuf -oL -eL "${BUILD_DIR}/GlobalStreamLatencyProbe" > "${PROBE_LOG}" 2>&1 &
    PROBE_PID=$!
fi

# ── Shutdown handling ─────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "[three-node-cluster.sh] Stopping…"
    kill ${PROBE_PID:+"${PROBE_PID}"} "${APP_PID}" "${ROUTER_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PIDS[@]}" 2>/dev/null || true
    wait ${PROBE_PID:+"${PROBE_PID}"} "${APP_PID}" "${ROUTER_PID}" "${FIX_PID}" "${MD_PID}" "${SEQ_PIDS[@]}" 2>/dev/null || true
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

# For the delivery-latency measurement (flood mode), OrderExecClient must be caught up and
# following the live tail BEFORE the flood — otherwise its samples measure the age of a drained
# backlog rather than real-time record→replicate→replay latency. Wait for its "following live"
# marker (its cluster-ingress connect has a multi-second IPC-timeout fallback on a follower node).
if [[ -n "${PHIXERON_FLOOD_ORDERS:-}" ]]; then
    echo "[three-node-cluster.sh] Waiting for OrderExecClient to catch up to the live tail…"
    WAIT=0
    until grep -q "following live" "${APP_LOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "[three-node-cluster.sh] WARN: OrderExecClient not caught up after 30s — measuring anyway" >&2
            break
        fi
    done
    echo "[three-node-cluster.sh] Waiting for GlobalStreamLatencyProbe (Variant-A) live subscription…"
    WAIT=0
    until grep -q "subscription connected" "${PROBE_LOG}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "[three-node-cluster.sh] WARN: probe not connected after 30s — measuring anyway" >&2
            break
        fi
    done
fi

echo "[three-node-cluster.sh] Running fix_test_server → ${TEST_LOG}"
if stdbuf -oL -eL "${BUILD_DIR}/fix_test_server" 127.0.0.1 9000 2>&1 | tee "${TEST_LOG}"; then
    echo "[three-node-cluster.sh] fix_test_server PASSED"
    STATUS=0
else
    echo "[three-node-cluster.sh] fix_test_server FAILED — see ${TEST_LOG}" >&2
    STATUS=1
fi

# ── S4 wedge test summary ────────────────────────────────────────────────────
if [[ -n "${PHIXERON_FLOOD_ORDERS:-}" ]]; then
    # Best-effort reporting: never let a zero-match grep (e.g. no back-pressure alerts on a
    # non-wedging run) abort under `set -e` before cleanup runs.
    set +e
    # Let the last sequenced messages drain through record→replicate→replay to OrderExecClient
    # (it is following live, so this is a short tail, not a full backlog) before stopping it.
    sleep 3
    # Stop OrderExecClient (Variant B) and the probe (Variant A) so each flushes its delivery-latency
    # report (printed on SIGTERM), then wait for both lines to land in their logs.
    kill -TERM "${APP_PID}" 2>/dev/null || true
    kill -TERM ${PROBE_PID:+"${PROBE_PID}"} 2>/dev/null || true
    WAIT=0
    until grep -q "delivery latency" "${APP_LOG}" 2>/dev/null \
          && grep -q "delivery latency" "${PROBE_LOG}" 2>/dev/null; do
        sleep 0.25
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 20 )); then
            break
        fi
    done

    ALERTS=$(grep -h "global stream back-pressure" "${LOG_DIR}"/sequencer-*.log 2>/dev/null | wc -l | tr -d ' ')
    LEADER=$(grep -h "new leader is memberId=" "${LOG_DIR}"/sequencer-*.log 2>/dev/null \
             | tail -1 | grep -oE 'memberId=[0-9]+' | head -1 | cut -d= -f2 || true)
    LAT=$(grep -h "delivery latency" "${APP_LOG}" 2>/dev/null | tail -1 || true)
    PROBE_LAT=$(grep -h "delivery latency" "${PROBE_LOG}" 2>/dev/null | tail -1 || true)
    echo ""
    echo "[three-node-cluster.sh] ── S4 wedge test summary ─────────────────────────────────"
    echo "[three-node-cluster.sh]   flood exit status : ${STATUS} (0 = flood completed)"
    echo "[three-node-cluster.sh]   back-pressure alerts across sequencer logs : ${ALERTS}"
    if [[ "${STATUS}" -eq 0 && "${ALERTS}" -eq 0 ]]; then
        echo "[three-node-cluster.sh]   S4 RESULT: DECOUPLED — sequencer stayed live under a stalled subscriber."
    else
        echo "[three-node-cluster.sh]   S4 RESULT: WEDGED — the stalled subscriber back-pressured the sequencer."
        echo "[three-node-cluster.sh]     (tethered control build: this is the expected pre-fix behavior;"
        echo "[three-node-cluster.sh]      tether=false build: indicates |ssc=true is also required.)"
    fi

    echo ""
    echo "[three-node-cluster.sh] ── Delivery latency: Variant A vs Variant B (cluster-commit → consumer) ──"
    echo "[three-node-cluster.sh]   A / direct live MDC       : ${PROBE_LAT:-<no line in ${PROBE_LOG} — did the probe connect?>}"
    echo "[three-node-cluster.sh]   B / archive IPC replay    : ${LAT:-<no line in ${APP_LOG} — did OrderExecClient catch up?>}"
    if [[ "${LEADER}" == "0" ]]; then
        echo "[three-node-cluster.sh]   NOTE: member 0 (OrderExecClient's co-located member) is the LEADER this run,"
        echo "[three-node-cluster.sh]         so its archive records directly — this is the record→replay path"
        echo "[three-node-cluster.sh]         WITHOUT the replication hop (a LOWER BOUND). Re-run until member 0 is a"
        echo "[three-node-cluster.sh]         follower to measure the full record→replicate→replay gateway path."
    else
        echo "[three-node-cluster.sh]   NOTE: leader is member ${LEADER:-?}; OrderExecClient's member 0 is a FOLLOWER,"
        echo "[three-node-cluster.sh]         so this INCLUDES the record→replicate→replay hop (the full Variant-B"
        echo "[three-node-cluster.sh]         gateway path). Trust the p99/p99.9 tail — p50 is ms-quantization noise."
    fi
fi

cleanup
exit "${STATUS}"
