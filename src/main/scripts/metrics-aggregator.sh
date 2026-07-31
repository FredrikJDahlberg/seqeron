#!/usr/bin/env bash
# metrics-aggregator.sh — central ops-server aggregator for phixeron's node-local exporters.
#
# Thin launcher for org.limitless.phixeron.tools.MetricsAggregator. Pulls every node's /metrics
# (see metrics-exporter.sh) over HTTP and re-exposes one combined /metrics endpoint — the
# aggregating-proxy topology, so Prometheus only needs network reach to this one process. Also
# synthesizes phixeron_node_up{member="N"} from its own per-node scrape success/failure. No Aeron
# dependency, so unlike the node-local exporter this needs no --add-opens flags and can run anywhere
# with HTTP reach to the node exporters.
#
#   metrics-aggregator.sh          # serve combined /metrics on port 9500
#
# Config:
#   METRICS_AGGREGATOR_PORT       HTTP port to serve /metrics on          (default 9500)
#   METRICS_AGGREGATOR_TARGETS    memberId=host:port, comma-separated     (default 0=localhost:9400)
#   PHIXERON_JAR                  path to the uber jar                    (default build/libs/phixeron-<v>-uber.jar)
#
# Prerequisite: ./gradlew uberJar

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
JAR="${PHIXERON_JAR:-${REPO_ROOT}/build/libs/phixeron-0.1.0-uber.jar}"

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

DPROPS=()
[[ -n "${METRICS_AGGREGATOR_PORT:-}" ]]    && DPROPS+=( "-DmetricsAggregator.port=${METRICS_AGGREGATOR_PORT}" )
[[ -n "${METRICS_AGGREGATOR_TARGETS:-}" ]] && DPROPS+=( "-DmetricsAggregator.targets=${METRICS_AGGREGATOR_TARGETS}" )

exec java "${DPROPS[@]+"${DPROPS[@]}"}" -cp "${JAR}" org.limitless.phixeron.tools.MetricsAggregator "$@"
