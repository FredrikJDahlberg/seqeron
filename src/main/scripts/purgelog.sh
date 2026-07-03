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

BASE_DIR="${TMPDIR:-/tmp}phixeron-seq"
LOG_DIR="logs"

echo "[purgelog.sh] The following will be deleted:"
echo "  ${BASE_DIR}/archive-*"
echo "  ${BASE_DIR}/cluster-*"
echo "  ${LOG_DIR}/"

if (( FORCE == 0 )); then
    read -r -p "[purgelog.sh] Proceed? [y/N] " answer
    if [[ "${answer}" != "y" && "${answer}" != "Y" ]]; then
        echo "[purgelog.sh] Aborted"
        exit 0
    fi
fi

if ls "${BASE_DIR}"/archive-* > /dev/null 2>&1; then
    rm -rf "${BASE_DIR}"/archive-*
    echo "[purgelog.sh] Removed archive dirs"
else
    echo "[purgelog.sh] No archive dirs found"
fi

if ls "${BASE_DIR}"/cluster-* > /dev/null 2>&1; then
    rm -rf "${BASE_DIR}"/cluster-*
    echo "[purgelog.sh] Removed cluster dirs"
else
    echo "[purgelog.sh] No cluster dirs found"
fi

if [[ -d "${LOG_DIR}" ]]; then
    rm -rf "${LOG_DIR}"
    echo "[purgelog.sh] Removed ${LOG_DIR}/"
else
    echo "[purgelog.sh] No ${LOG_DIR}/ directory found"
fi

echo "[purgelog.sh] Done"
