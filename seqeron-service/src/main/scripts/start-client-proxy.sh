#!/usr/bin/env bash
# start-client-proxy.sh — run the host of one hosted client: org.limitless.seqeron.proxy.ClientProxy, attached to the
# driver of the member or gateway host it sits beside (doc/hosted-facade-draft.md section 17).
#
# The host is configured by proxy.* system properties, which this passes through: the arguments are -Dname=value
# pairs. doc/running-a-cluster.md, "Client proxy", lists them; proxy.snapshotDir is required, and proxy.memberId
# picks the member whose driver it uses (default 0).
#
# Ctrl-C (or kill $$) stops it.
#
# Prerequisites:
#   ./gradlew uberJar
#
# Usage:
#   ./start-client-proxy.sh -Dproxy.sourceId=12 -Dproxy.clientId=14 -Dproxy.snapshotDir=/tmp/colocated-example

set -euo pipefail

_seqeron_scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_seqeron_scripts}/paths.sh"
source "${_seqeron_scripts}/ports.sh"
source "${_seqeron_scripts}/seqeron-home.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    echo "Usage: $0 [-Dproxy.name=value ...]"
    exit 0
fi

seqeron_require_jar

exec java "${SEQERON_JAVA_OPTS[@]}" "$@" -cp "${SEQERON_JAR}" org.limitless.seqeron.proxy.ClientProxy
