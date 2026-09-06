#!/usr/bin/env bash
# clusterctl.sh — operator cluster life-cycle tool (start / shutdown / passthrough).
#
# Thin launcher for org.limitless.phixeron.tools.ClusterCtl (see clusterctl.md). Node-local:
# run co-located on a SequencerServer host — it shares that node's Aeron directory (to reach the
# co-located tap over aeron:ipc) and its clusterDir (for io.aeron.cluster.ClusterTool).
#
#   clusterctl.sh start             # record a "system started" marker (requires an elected leader)
#   clusterctl.sh shutdown          # orderly stop; safe on every node, no-op on followers
#   clusterctl.sh activate <id>     # manual standby promotion (requires an elected leader)
#   clusterctl.sh load-topology <file>
#                                   # publish the topology document — the gateway list and the
#                                   # protocol registry (src/main/resources/topology.xml); run once
#                                   # per cluster lifetime, BEFORE the reference-data load
#   clusterctl.sh help
#   clusterctl.sh describe …        # anything else → ClusterTool passthrough
#
# Config (override the SequencerServer-mirroring defaults for multi-node / custom dirs):
#   CLUSTERCTL_MEMBER_ID          co-located member id             (default 0)
#   CLUSTERCTL_BASE_DIR           cluster data dir root            (default $TMPDIR/phixeron-seq)
#   CLUSTERCTL_AERON_DIR          co-located member's Aeron dir     (default $TMPDIR/phixeron-seq-aeron-<id>)
#   CLUSTERCTL_INGRESS_ENDPOINTS  member ingress endpoints          (default 0=localhost:9302)
#   PHIXERON_JAR                  path to the uber jar             (default build/libs/phixeron-<v>-uber.jar)
#
# Prerequisite: ./gradlew uberJar

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
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

# Map CLUSTERCTL_* env onto -Dclusterctl.* system properties; unset ones fall back to ClusterCtl's
# SequencerServer-mirroring defaults.
DPROPS=( "-Dclusterctl.memberId=${CLUSTERCTL_MEMBER_ID:-0}" )
[[ -n "${CLUSTERCTL_BASE_DIR:-}" ]]          && DPROPS+=( "-Dclusterctl.baseDir=${CLUSTERCTL_BASE_DIR}" )
[[ -n "${CLUSTERCTL_AERON_DIR:-}" ]]         && DPROPS+=( "-Dclusterctl.aeronDir=${CLUSTERCTL_AERON_DIR}" )
[[ -n "${CLUSTERCTL_INGRESS_ENDPOINTS:-}" ]] && DPROPS+=( "-Dclusterctl.ingressEndpoints=${CLUSTERCTL_INGRESS_ENDPOINTS}" )

exec java "${JAVA_OPTS[@]}" "${DPROPS[@]}" -cp "${JAR}" org.limitless.phixeron.tools.ClusterCtl "$@"
