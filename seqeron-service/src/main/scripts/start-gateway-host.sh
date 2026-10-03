#!/usr/bin/env bash
# start-gateway-host.sh — make this host, which runs no cluster member, one that clients can run on.
#
# Starts ReplayerServer in gateway-host mode: its own media driver and archive, and a relay that copies a
# member's tap onto this host's own over UDP, moving to the next member when that one is lost. Gateways,
# consumers and ClusterProbe on this host then attach to its Aeron directory exactly as they would on a
# member, and submit over UDP ingress.
#
# Config:
#   SEQERON_NODE_ID            this host's node id, one no member uses (3 suits a three-member cluster); names its
#                              Aeron directory, $TMPDIR/seqeron-seq-aeron-<id>, and archive  (default 3)
#   SEQERON_HOSTS              every member's host, in member-id order; ReplayerServer and every client on
#                              this host read it
#   SEQERON_ARCHIVE_ENDPOINTS  the members' archive control endpoints, host:port, comma-separated
#                              (default each member's in SEQERON_HOSTS, else the three localhost members)
#   SEQERON_HOST               this host's name as the members reach it          (default localhost)
#   SEQERON_BASE_DIR           data directory root                               (default $TMPDIR/seqeron-seq)
#
# Clients on this host use the node id where they take a member id (probe.memberId, a Gateway's memberId).
#
# Ctrl-C (or kill $$) stops it; so does stop-cluster.sh.
#
# Prerequisites:
#   ./gradlew uberJar
#
# Usage:
#   ./start-gateway-host.sh

set -euo pipefail

_seqeron_scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_seqeron_scripts}/paths.sh"
source "${_seqeron_scripts}/ports.sh"
source "${_seqeron_scripts}/seqeron-home.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    echo "Usage: $0"
    exit 0
fi

seqeron_require_jar
JAR="${SEQERON_JAR}"
JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

NODE_ID="${SEQERON_NODE_ID:-3}"
ENDPOINTS="${SEQERON_ARCHIVE_ENDPOINTS:-}"
if [[ -z "${ENDPOINTS}" && -z "${SEQERON_HOSTS:-}" ]]; then
    ENDPOINTS="localhost:$(archive_port 0),localhost:$(archive_port 1),localhost:$(archive_port 2)"
fi
HOST="${SEQERON_HOST:-localhost}"
BASE_DIR="${SEQERON_BASE_DIR:-${TMP_DIR}/seqeron-seq}"

LOG_DIR="logs"
LOG="${LOG_DIR}/ReplayerServer-${NODE_ID}.log"
mkdir -p "${LOG_DIR}"

echo "[gateway-host.sh] Starting ReplayerServer (gateway host, node ${NODE_ID}) → ${LOG}"
java "${JAVA_OPTS[@]}" \
    -Dreplayer.memberId="${NODE_ID}" \
    ${ENDPOINTS:+-Dreplayer.archiveEndpoints="${ENDPOINTS}"} \
    -Dreplayer.host="${HOST}" \
    -Dreplayer.baseDir="${BASE_DIR}" \
    -cp "${JAR}" \
    org.limitless.seqeron.replayer.server.ReplayerServer \
    > "${LOG}" 2>&1 &
PID=$!

cleanup() {
    echo ""
    echo "[gateway-host.sh] Stopping…"
    kill "${PID}" 2>/dev/null
    wait "${PID}" 2>/dev/null
    echo "[gateway-host.sh] Done"
}
trap cleanup INT TERM

echo "[gateway-host.sh] Waiting for it to relay the log and serve replay…"
wait_for_log "${LOG}" "ready —" 60 ||
    echo "[gateway-host.sh] WARN: not serving after 60s — is a member reachable at ${ENDPOINTS:-${SEQERON_HOSTS}}?" >&2
echo "  ReplayerServer    pid=${PID}  log=${LOG}  aeronDir=${TMP_DIR}/seqeron-seq-aeron-${NODE_ID}"
echo "[gateway-host.sh] Press Ctrl-C to stop"
wait "${PID}"
