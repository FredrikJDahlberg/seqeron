#!/usr/bin/env bash
# cluster.sh — a three-member cluster across the hosts common.sh names, for running by hand.
#
#   cluster.sh up [topology]   wipe every host, start the members, load [topology], an absolute path on member 0's
#                              host (default topology-test-groups.xml)
#   cluster.sh leader          the member id every member records as leader
#   cluster.sh down            stop everything on every host and fetch the logs
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./cluster.sh up

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

case "${1:-}" in
up)
    cluster_up "${2:-${RESOURCES}/topology-test-groups.xml}"
    mh_log "cluster up"
    ;;
leader)
    leader
    ;;
down)
    cluster_down
    ;;
*)
    sed -n '2,/^$/p' "$0" >&2
    exit 2
    ;;
esac
