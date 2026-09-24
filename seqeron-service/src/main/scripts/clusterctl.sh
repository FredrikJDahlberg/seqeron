#!/usr/bin/env bash
# clusterctl.sh — operator cluster life-cycle tool (start / shutdown / passthrough).
#
# Thin launcher for org.limitless.seqeron.tools.ClusterCtl (see clusterctl.md). Node-local:
# run co-located on a SequencerServer host — it shares that node's Aeron directory (to reach the
# co-located tap over aeron:ipc) and its clusterDir (for io.aeron.cluster.ClusterTool).
#
#   clusterctl.sh start             # record a "system started" marker (requires an elected leader)
#   clusterctl.sh shutdown          # orderly stop; safe on every node, no-op on followers
#   clusterctl.sh activate <id>     # manual standby promotion (requires an elected leader)
#   clusterctl.sh load-topology <file>
#                                   # publish the topology document — the gateway list and the
#                                   # protocol registry. <file> is the deployment's own, validated
#                                   # against the topology.xsd the jar carries; this repo ships only
#                                   # the test one. Run once per cluster lifetime, after start and
#                                   # before any gateway starts
#   clusterctl.sh help
#   clusterctl.sh describe …        # anything else → ClusterTool passthrough
#
# Config (override the SequencerServer-mirroring defaults for multi-node / custom dirs):
#   CLUSTERCTL_MEMBER_ID          co-located member id             (default 0)
#   CLUSTERCTL_BASE_DIR           cluster data dir root            (default $TMPDIR/seqeron-seq)
#   CLUSTERCTL_AERON_DIR          co-located member's Aeron dir     (default $TMPDIR/seqeron-seq-aeron-<id>)
#   CLUSTERCTL_INGRESS_ENDPOINTS  member ingress endpoints          (default 0=localhost:9302)
#   CLUSTERCTL_EGRESS_HOST        host the leader replies to        (default localhost)
#   SEQERON_JAR                  path to the uber jar             (default build/libs/seqeron-<v>-uber.jar)
#
# Prerequisite: ./gradlew uberJar

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/seqeron-home.sh"
seqeron_require_jar
JAR="${SEQERON_JAR}"

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

# Map CLUSTERCTL_* env onto -Dclusterctl.* system properties; unset ones fall back to ClusterCtl's
# SequencerServer-mirroring defaults.
DPROPS=( "-Dclusterctl.memberId=${CLUSTERCTL_MEMBER_ID:-0}" )
[[ -n "${CLUSTERCTL_BASE_DIR:-}" ]]          && DPROPS+=( "-Dclusterctl.baseDir=${CLUSTERCTL_BASE_DIR}" )
[[ -n "${CLUSTERCTL_AERON_DIR:-}" ]]         && DPROPS+=( "-Dclusterctl.aeronDir=${CLUSTERCTL_AERON_DIR}" )
[[ -n "${CLUSTERCTL_INGRESS_ENDPOINTS:-}" ]] && DPROPS+=( "-Dclusterctl.ingressEndpoints=${CLUSTERCTL_INGRESS_ENDPOINTS}" )
[[ -n "${CLUSTERCTL_EGRESS_HOST:-}" ]]       && DPROPS+=( "-Dclusterctl.egressHost=${CLUSTERCTL_EGRESS_HOST}" )

exec java "${JAVA_OPTS[@]}" "${DPROPS[@]}" -cp "${JAR}" org.limitless.seqeron.tools.ClusterCtl "$@"
