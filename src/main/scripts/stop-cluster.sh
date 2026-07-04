#!/usr/bin/env bash
# stop-cluster.sh — stop all phixeron cluster processes started by cluster.sh.
#
# Sends SIGTERM to SequencerNode, aeronmd, FixSessionClient, and
# OrderExecClient, then waits up to 10 s for them to exit before
# sending SIGKILL to any survivors.
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

PROCESSES=(SequencerNode aeronmd FixSessionClient OrderExecClient)
TIMEOUT=10

kill_by_name() {
    local name="$1"
    local pids
    pids=$(pgrep -f "${name}" 2>/dev/null) || true
    if [[ -z "${pids}" ]]; then
        return 0
    fi
    echo "[stop-cluster.sh] Stopping ${name} (pid(s): ${pids})"
    kill -TERM ${pids} 2>/dev/null || true
}

wait_for_exit() {
    local name="$1"
    local elapsed=0
    while pgrep -f "${name}" > /dev/null 2>&1; do
        sleep 0.5
        elapsed=$(( elapsed + 1 ))
        if (( elapsed > TIMEOUT * 2 )); then
            local pids
            pids=$(pgrep -f "${name}" 2>/dev/null) || true
            if [[ -n "${pids}" ]]; then
                echo "[stop-cluster.sh] Force-killing ${name} (pid(s): ${pids})"
                kill -KILL ${pids} 2>/dev/null || true
            fi
            return
        fi
    done
}

for proc in "${PROCESSES[@]}"; do
    kill_by_name "${proc}"
done

for proc in "${PROCESSES[@]}"; do
    wait_for_exit "${proc}"
done

echo "[stop-cluster.sh] All cluster processes stopped"
