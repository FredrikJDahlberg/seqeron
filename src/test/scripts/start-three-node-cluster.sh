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
source "${MAIN_SCRIPTS}/seqeron-home.sh"

usage() {
    echo "Usage: $0"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# ── Config ────────────────────────────────────────────────────────────────────

# Resolved the way the operator scripts resolve it (seqeron-home.sh), not as a path relative to the
# caller's cwd with the version written into it. The old form resolved only when the cwd happened to be
# this repo's root, so the launcher could not be driven from anywhere else — and a consuming product's
# end-to-end suite is exactly a caller that lives somewhere else. SEQERON_JAR overrides it, which is how
# such a product points this launcher at its own uber jar.
seqeron_require_jar
JAR="${SEQERON_JAR}"

# 1 = the caller supplies the node consumers, so don't start the probe followers (see the header).
NO_CONSUMERS="${SEQERON_NO_CONSUMERS:-0}"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

LOG_DIR="logs"

BASE_DIR="${TMP_DIR}/seqeron-seq3"

CLUSTER_MEMBERS="$(cluster_members_string 3)"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

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
    wait_for_log "${SEQ_LOG}" "Running" 30 || {
        echo "ERROR: ${SEQ_LOG} did not reach Running state after 30 s" >&2
        kill "${SEQ_PIDS[@]}" 2>/dev/null
        exit 1
    }
done
echo "[start-three-node-cluster.sh] All 3 cluster members are running"

# A ReplayerServer and a consumer replica on every node. Each attaches to its own member's media driver
# (seqeron-seq-aeron-<m>): the ReplayerServer serves replay of that node's tap recording over aeron:ipc,
# and the replica reads the tap directly for live, so whichever member is elected leader has one already
# following the live tail. Member 0's replica also records post-consensus delivery latency and prints
# p50/p99/p99.9 on shutdown. Waiting for "serving replay" avoids a needless extra cold-start replay.
REPLAYER_PIDS=()
APP_PIDS=()
APP_LOGS=()
for m in 0 1 2; do
    suffix="-${m}" latency=()
    if [[ "${m}" == 0 ]]; then suffix="" latency=(-Dprobe.latencyStats=true); fi
    RLOG="${LOG_DIR}/ReplayerServer${suffix}.log"
    echo "[start-three-node-cluster.sh] Starting ReplayerServer (member ${m}) → ${RLOG}"
    java "${JAVA_OPTS[@]}" -Dreplayer.memberId="${m}" -cp "${JAR}" \
        org.limitless.seqeron.replayer.server.ReplayerServer > "${RLOG}" 2>&1 &
    REPLAYER_PIDS+=("$!")
    wait_for_log "${RLOG}" "serving replay" 30 ||
        echo "[start-three-node-cluster.sh] WARN: ${RLOG} not serving after 30s — starting the consumer anyway" >&2
    if [[ "${NO_CONSUMERS}" != "1" ]]; then
        ALOG="${LOG_DIR}/ClusterProbe${suffix}.log"
        echo "[start-three-node-cluster.sh] Starting ClusterProbe follower (replica on member ${m}) → ${ALOG}"
        java "${JAVA_OPTS[@]}" -Dprobe.memberId="${m}" -Dprobe.clientId=1 "${latency[@]+"${latency[@]}"}" \
            -cp "${JAR}" org.limitless.seqeron.tools.ClusterProbe follow > "${ALOG}" 2>&1 &
        APP_PIDS+=("$!")
        APP_LOGS+=("${ALOG}")
    fi
done

ALL_PIDS=("${REPLAYER_PIDS[@]}" "${SEQ_PIDS[@]}" "${APP_PIDS[@]+"${APP_PIDS[@]}"}")

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
    for LOG in "${APP_LOGS[@]}"; do
        wait_for_log "${LOG}" "following live" 30 ||
            echo "[start-three-node-cluster.sh] WARN: replica ${LOG} not caught up after 30s — proceeding anyway" >&2
    done
fi

echo "[start-three-node-cluster.sh] READY — the cluster tier is up"
echo "  SequencerServer     pids=${SEQ_PIDS[*]}"
echo "  ReplayerServer      pids=${REPLAYER_PIDS[*]}"
if [[ "${NO_CONSUMERS}" == "1" ]]; then
    echo "  ClusterProbe      (none — SEQERON_NO_CONSUMERS=1, the caller supplies its own consumers)"
else
    echo "  ClusterProbe      pids=${APP_PIDS[*]}   (per-node consumer replicas)"
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
