#!/usr/bin/env bash
# stop-cluster.sh — stop all cluster processes started by start-cluster.sh or
# start-three-node-cluster.sh.
#
# Sends SIGTERM to the consumer replicas (ClusterProbe, TestGateway), then to the cluster itself
# (SequencerServer, ReplayerServer, aeronmd), then waits up to 10 s for each to exit before sending
# SIGKILL to any survivors.
#
# Core's own processes, and no others: a caller that starts its own consumers alongside the cluster
# (SEQERON_NO_CONSUMERS=1, see start-three-node-cluster.sh) names them in SEQERON_EXTRA_PROCESSES and
# they are swept first, with the replicas. The format is one "label|pgrep-pattern" per entry,
# semicolon-separated:
#
#   SEQERON_EXTRA_PROCESSES="FixGateway|FixGateway;OrderExecServer|OrderExecServer" ./stop-cluster.sh
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
    "ReplayerServer|ReplayerServer"
)

# A caller's own processes, swept ahead of the cluster for the same reason the replicas are.
if [[ -n "${SEQERON_EXTRA_PROCESSES:-}" ]]; then
    IFS=';' read -r -a EXTRA <<< "${SEQERON_EXTRA_PROCESSES}"
    for entry in "${EXTRA[@]}"; do
        [[ -n "${entry}" ]] && PROCESSES=("${entry}" "${PROCESSES[@]}")
    done
fi
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
