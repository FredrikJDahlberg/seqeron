#!/usr/bin/env bash
# metrics-exporter.sh — node-local Prometheus exporter for phixeron's operator counters.
#
# Thin launcher for org.limitless.phixeron.metrics.MetricsExporter. Node-local: run co-located on a
# SequencerServer/ReplayerServer host — it shares that node's Aeron directory (the same connection-less
# access pattern as `clusterctl counters`) and stays resident, serving /metrics for a scraper to
# poll on an interval.
#
#   metrics-exporter.sh          # serve /metrics on port 9400 + memberId
#
# Config (override the SequencerServer-mirroring defaults for multi-node / custom dirs):
#   METRICS_EXPORTER_MEMBER_ID    co-located member id             (default 0)
#   METRICS_EXPORTER_AERON_DIR    co-located member's Aeron dir     (default $TMPDIR/phixeron-seq-aeron-<id>)
#   METRICS_EXPORTER_PORT         HTTP port to serve /metrics on    (default 9400 + memberId)
#   PHIXERON_JAR                  path to the uber jar             (default build/libs/phixeron-<v>-uber.jar)
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

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

# Map METRICS_EXPORTER_* env onto -DmetricsExporter.* system properties; unset ones fall back to
# MetricsExporter's SequencerServer-mirroring defaults.
DPROPS=( "-DmetricsExporter.memberId=${METRICS_EXPORTER_MEMBER_ID:-0}" )
[[ -n "${METRICS_EXPORTER_AERON_DIR:-}" ]] && DPROPS+=( "-DmetricsExporter.aeronDir=${METRICS_EXPORTER_AERON_DIR}" )
[[ -n "${METRICS_EXPORTER_PORT:-}" ]]      && DPROPS+=( "-DmetricsExporter.port=${METRICS_EXPORTER_PORT}" )

exec java "${JAVA_OPTS[@]}" "${DPROPS[@]}" -cp "${JAR}" org.limitless.phixeron.metrics.MetricsExporter "$@"
