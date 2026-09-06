#!/usr/bin/env bash
# stop-cluster.sh — stop all cluster processes started by start-cluster.sh or
# start-three-node-cluster.sh.
#
# Sends SIGTERM to the consumer replicas (ClusterProbe, TestGateway), then to the cluster itself
# (SequencerServer, ReplayerServer, aeronmd) and to the product processes either start script
# launches under PHIXERON_PRODUCT_APPS=1 (FixGateway, OrderExecServer, BasicDataServer) — those
# are another repo's binaries, kept here so a run that launched them leaves nothing behind. Then
# waits up to 10 s for each to exit before sending SIGKILL to any survivors.
#
# Usage:
#   ./stop-cluster.sh

set -euo pipefail

usage() {
    echo "Usage: $0"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# Each entry is "label|pgrep-pattern", clients first: killing the consumer replicas ahead of the
# cluster keeps a stop from tripping their die-on-driver-loss path, which is a real failure signal
# everywhere else. ClusterProbe and TestGateway run as `java … -cp <jar> <fully-qualified class>`, so
# the class name is on the command line and pgrep -f finds it. SequencerServer is launched via
# `java … -jar …uber.jar`, so its Main-Class name is in the manifest, not on the command line — match
# its -Dsequencer.memberId system property instead (present on every SequencerServer launch, and on
# no other process).
PROCESSES=(
    "ClusterProbe|ClusterProbe"
    "TestGateway|TestGateway"
    "SequencerServer|sequencer.memberId"
    "aeronmd|aeronmd"
    "FixGateway|FixGateway"
    "ReplayerServer|ReplayerServer"
    "OrderExecServer|OrderExecServer"
    "BasicDataServer|BasicDataServer"
)
TIMEOUT=10

kill_by_name() {
    local label="$1" pattern="$2"
    local pids
    pids=$(pgrep -f "${pattern}" 2>/dev/null) || true
    if [[ -z "${pids}" ]]; then
        return 0
    fi
    echo "[stop-cluster.sh] Stopping ${label} (pid(s): ${pids})"
    kill -TERM ${pids} 2>/dev/null || true
}

wait_for_exit() {
    local label="$1" pattern="$2"
    local elapsed=0
    while pgrep -f "${pattern}" > /dev/null 2>&1; do
        sleep 0.5
        elapsed=$(( elapsed + 1 ))
        if (( elapsed > TIMEOUT * 2 )); then
            local pids
            pids=$(pgrep -f "${pattern}" 2>/dev/null) || true
            if [[ -n "${pids}" ]]; then
                echo "[stop-cluster.sh] Force-killing ${label} (pid(s): ${pids})"
                kill -KILL ${pids} 2>/dev/null || true
            fi
            return
        fi
    done
}

for proc in "${PROCESSES[@]}"; do
    kill_by_name "${proc%%|*}" "${proc#*|}"
done

for proc in "${PROCESSES[@]}"; do
    wait_for_exit "${proc%%|*}" "${proc#*|}"
done

echo "[stop-cluster.sh] All cluster processes stopped"
