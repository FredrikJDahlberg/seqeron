#!/usr/bin/env bash
# start-three-node-cluster.sh — bring up a local 3-node Aeron Cluster with a per-node ReplayerServer
# and a per-node consumer replica, then keep it running until interrupted.
#
# The cluster tier alone, in every mode. This script is core's, and it launches core's processes only:
#
#   1. SequencerServer  x3  (Java, Raft members 0/1/2, all on localhost)
#   2. ReplayerServer   x3  (Java, one co-located with each member: serves archive replay to co-located
#                            apps over aeron:ipc; apps read the tap directly for live)
#   3. ClusterProbe follow x3 (Java, one per-node consumer replica — the edge-neutral probe, so READY
#                            still means "every node is following the live tail")
#
# then blocks, monitoring the launched processes; Ctrl-C (or kill) stops all of them cleanly. This
# script only starts and stops the cluster — it runs no tests.
#
# SEQERON_NO_CONSUMERS=1 starts the cluster tier WITHOUT the probe followers, for a caller that supplies
# its own node consumers — its own replicas, its own gateways, its own media driver if it needs one.
# The caller launches, waits for and stops those itself, after this script prints READY; core names no
# process it does not start. READY then means the cluster is up, not that any consumer is caught up.
#
# Uses a dedicated baseDir (seqeron-seq3 under the temp dir) so it doesn't collide
# with single-node dev state left behind by start-cluster.sh.
#
# Prerequisites:
#   ./gradlew uberJar                              # build the fat jar
#
# Usage:
#   ./start-three-node-cluster.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_SCRIPTS="${SCRIPT_DIR}/../../main/scripts"
source "${MAIN_SCRIPTS}/ports.sh"
source "${MAIN_SCRIPTS}/paths.sh"

usage() {
    echo "Usage: $0"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# ── Config ────────────────────────────────────────────────────────────────────

JAR="build/libs/seqeron-0.1.0-uber.jar"

# 1 = the caller supplies the node consumers, so don't start the probe followers (see the header).
NO_CONSUMERS="${SEQERON_NO_CONSUMERS:-0}"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

LOG_DIR="logs"
REPLAYER_LOG="${LOG_DIR}/ReplayerServer.log"
APP_LOG="${LOG_DIR}/ClusterProbe.log"

BASE_DIR="${TMP_DIR}/seqeron-seq3"

CLUSTER_MEMBERS="$(cluster_members_string 3)"



# SequencerServer member 0's own embedded media driver directory — matches its default when
# -Dsequencer.aeronDir isn't overridden (it isn't, below). A co-located consumer shares this directory.
SEQ_AERON_DIR="${TMP_DIR}/seqeron-seq-aeron-0"


# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
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

echo "[start-three-node-cluster.sh] Starting ReplayerServer (co-located with SequencerServer member 0) → ${REPLAYER_LOG}"
# Attaches to member 0's embedded media driver (replayer.memberId=0 → seqeron-seq-aeron-0, i.e.
# SEQ_AERON_DIR) and serves archive replay of that node's local tap recording over aeron:ipc.
# A consumer co-located with member 0 shares the same directory, reads the tap directly for live, and
# asks this ReplayerService to replay on a gap / for cold-start history.
java "${JAVA_OPTS[@]}" \
    -Dreplayer.memberId=0 \
    -cp "${JAR}" \
    org.limitless.seqeron.replayer.server.ReplayerServer \
    > "${REPLAYER_LOG}" 2>&1 &
REPLAYER_PID=$!

# Wait until the ReplayerService is serving replay (its local tap recording is visible on the archive), which
# also confirms the tap is live for the direct-read consumers. A consumer retries regardless, but this
# avoids a noisy startup and a needless extra cold-start replay.
echo "[start-three-node-cluster.sh] Waiting for ReplayerServer to start serving replay…"
WAIT=0
until grep -q "serving replay" "${REPLAYER_LOG}" 2>/dev/null; do
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 60 )); then
        echo "[start-three-node-cluster.sh] WARN: ReplayerServer not serving after 30s — starting the consumer anyway" >&2
        break
    fi
done

# The per-node consumer replica: ClusterProbe following the tap, one per member. Member 0's is the
# latency-instrumented one — it records the post-consensus delivery latency (cluster-commit → its
# aeron:ipc tap tail) of every caught-up sequenced message and prints p50/p99/p99.9 on shutdown.
APP_PID=""
if [[ "${NO_CONSUMERS}" != "1" ]]; then
    echo "[start-three-node-cluster.sh] Starting ClusterProbe follower (replica on member 0) → ${APP_LOG}"
    java "${JAVA_OPTS[@]}" -Dprobe.memberId=0 -Dprobe.clientId=1 -Dprobe.latencyStats=true \
        -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${APP_LOG}" 2>&1 &
    APP_PID=$!
fi

# A replica on every node: start a ReplayerServer and a consumer co-located with members 1 and 2 too.
# Each attaches to its own member's media driver (seqeron-seq-aeron-<m>) and reads that node's
# SequencerService tap over aeron:ipc, so whichever member is elected leader has a local replica
# already following the live tail.
EXTRA_REPLAYER_PIDS=()
EXTRA_APP_PIDS=()
EXTRA_APP_LOGS=()
for m in 1 2; do
    RLOG="${LOG_DIR}/ReplayerServer-${m}.log"
    ALOG="${LOG_DIR}/ClusterProbe-${m}.log"
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
    if [[ "${NO_CONSUMERS}" != "1" ]]; then
        ALOG="${LOG_DIR}/ClusterProbe-${m}.log"
        java "${JAVA_OPTS[@]}" -Dprobe.memberId="${m}" -Dprobe.clientId=1 \
            -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${ALOG}" 2>&1 &
        EXTRA_APP_PIDS+=("$!")
        EXTRA_APP_LOGS+=("${ALOG}")
    fi
done

ALL_PIDS=("${REPLAYER_PID}" "${EXTRA_REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}")
for pid in "${APP_PID}" "${EXTRA_APP_PIDS[@]+"${EXTRA_APP_PIDS[@]}"}"; do
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


# READY means every node is following the live tail, so a caller's own consumers start against a
# cluster that is fully up. With no consumers of core's own there is nothing to wait for.
if [[ "${NO_CONSUMERS}" != "1" ]]; then
    echo "[start-three-node-cluster.sh] Waiting for all consumer replicas to catch up to the live tail…"
    for LOG in "${APP_LOG}" "${EXTRA_APP_LOGS[@]}"; do
        WAIT=0
        until grep -q "following live" "${LOG}" 2>/dev/null; do
            sleep 0.5
            WAIT=$(( WAIT + 1 ))
            if (( WAIT > 60 )); then
                echo "[start-three-node-cluster.sh] WARN: replica ${LOG} not caught up after 30s —" \
                     "proceeding anyway" >&2
                break
            fi
        done
    done
fi


echo "[start-three-node-cluster.sh] READY — the cluster tier is up"
echo "  SequencerServer     pids=${SEQ_PIDS[*]}"
echo "  ReplayerServer      pids=${REPLAYER_PID} ${EXTRA_REPLAYER_PIDS[*]}"
if [[ "${NO_CONSUMERS}" == "1" ]]; then
    echo "  ClusterProbe      (none — SEQERON_NO_CONSUMERS=1, the caller supplies its own consumers)"
else
    echo "  ClusterProbe      pids=${APP_PID} ${EXTRA_APP_PIDS[*]}   (per-node consumer replicas)"
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
