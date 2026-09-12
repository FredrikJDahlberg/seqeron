#!/usr/bin/env bash
# metrics-aggregator.sh — central ops-server aggregator for seqeron's node-local exporters.
#
# Thin launcher for org.limitless.seqeron.metrics.MetricsAggregator. Pulls every node's /metrics
# (see metrics-exporter.sh) over HTTP and re-exposes one combined /metrics endpoint — the
# aggregating-proxy topology, so Prometheus only needs network reach to this one process. Also
# synthesizes seqeron_node_up{member="N"} from its own per-node scrape success/failure. No Aeron
# dependency, so unlike the node-local exporter this needs no --add-opens flags and can run anywhere
# with HTTP reach to the node exporters.
#
#   metrics-aggregator.sh          # serve combined /metrics on port 9500
#
# Config:
#   METRICS_AGGREGATOR_PORT       HTTP port to serve /metrics on          (default 9500)
#   METRICS_AGGREGATOR_TARGETS    memberId=host:port, comma-separated     (default 0=localhost:9400)
#   SEQERON_JAR                  path to the uber jar                    (default build/libs/seqeron-<v>-uber.jar)
#
# Prerequisite: ./gradlew uberJar

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/seqeron-home.sh"
seqeron_require_jar
JAR="${SEQERON_JAR}"

DPROPS=()
[[ -n "${METRICS_AGGREGATOR_PORT:-}" ]]    && DPROPS+=( "-DmetricsAggregator.port=${METRICS_AGGREGATOR_PORT}" )
[[ -n "${METRICS_AGGREGATOR_TARGETS:-}" ]] && DPROPS+=( "-DmetricsAggregator.targets=${METRICS_AGGREGATOR_TARGETS}" )

exec java "${DPROPS[@]+"${DPROPS[@]}"}" -cp "${JAR}" org.limitless.seqeron.metrics.MetricsAggregator "$@"
