#!/usr/bin/env bash
# purgelog.sh — delete all phixeron transaction logs and text log files.
#
# Removes:
#   /tmp/phixeron-seq/archive-*   Aeron Archive recordings (transaction log)
#   /tmp/phixeron-seq/cluster-*   Aeron Cluster consensus log
#   logs/                         stdout/stderr captures from cluster.sh
#
# The cluster must be stopped before running this script.
#
# Usage:
#   ./purgelog.sh [--force]    --force skips the confirmation prompt

set -euo pipefail

usage() {
    echo "Usage: $0 [--force]"
    echo "  --force    skip the confirmation prompt"
}

FORCE=0
case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    --force)
        FORCE=1
        ;;
esac

# start-cluster.sh (single node) uses the SequencerNode default baseDir (phixeron-seq);
# start-three-node-cluster.sh overrides it to phixeron-seq3. Purge both, else stale per-tenure
# recordings accumulate across runs (a fresh run recovers them and grows the catalog).
BASE_DIRS=("${TMPDIR:-/tmp}phixeron-seq" "${TMPDIR:-/tmp}phixeron-seq3")
LOG_DIR="logs"

echo "[purgelog.sh] The following will be deleted:"
for BASE_DIR in "${BASE_DIRS[@]}"; do
    echo "  ${BASE_DIR}/archive-*"
    echo "  ${BASE_DIR}/cluster-*"
done
echo "  ${LOG_DIR}/"

if (( FORCE == 0 )); then
    read -r -p "[purgelog.sh] Proceed? [y/N] " answer
    if [[ "${answer}" != "y" && "${answer}" != "Y" ]]; then
        echo "[purgelog.sh] Aborted"
        exit 0
    fi
fi

for BASE_DIR in "${BASE_DIRS[@]}"; do
    if ls "${BASE_DIR}"/archive-* > /dev/null 2>&1; then
        rm -rf "${BASE_DIR}"/archive-*
        echo "[purgelog.sh] Removed ${BASE_DIR}/archive-*"
    fi
    if ls "${BASE_DIR}"/cluster-* > /dev/null 2>&1; then
        rm -rf "${BASE_DIR}"/cluster-*
        echo "[purgelog.sh] Removed ${BASE_DIR}/cluster-*"
    fi
done

if [[ -d "${LOG_DIR}" ]]; then
    rm -rf "${LOG_DIR}"
    echo "[purgelog.sh] Removed ${LOG_DIR}/"
else
    echo "[purgelog.sh] No ${LOG_DIR}/ directory found"
fi

echo "[purgelog.sh] Done"
