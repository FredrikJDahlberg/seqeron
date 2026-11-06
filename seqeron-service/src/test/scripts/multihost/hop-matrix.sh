#!/usr/bin/env bash
# hop-matrix.sh — the hosted-façade hop across three hosts (doc/ec2-tests.md, test 1): the benches' arms on the
# leader's host, each run a line in <results>/hop-<stamp>.txt.
#
# For each rate a fresh cluster loads topology-test-hosted-quiet.xml, and each repeat runs every mode in turn on the
# leader's host, so the arms alternate within a rate:
#
#   direct          hop_bench direct, built on the tag: the C++ Application on the leader's driver
#   hosted-spin     hosted_hop_bench against ClientProxy with proxy.spin=true
#   hosted-backoff  the same with the host backing off between messages
#
# A run is HOP_SECONDS at its rate; each bench drops the first 20% of it. A run whose leader is not the block's fails
# the matrix rather than measure an election.
#
#   HOP_RATES     msg/s, space-separated            (default "10000 20000 50000")
#   HOP_REPEATS   runs of each mode at each rate     (default 3)
#   HOP_SECONDS   length of a run                    (default 30)
#   HOP_MODES     the modes, space-separated         (default "direct hosted-spin hosted-backoff")
#   MULTIHOST_HOST_CPUS, MULTIHOST_BENCH_CPUS   taskset CPU lists for the host JVM and hop_bench (default unpinned)
#   MULTIHOST_PAYLOAD   the marker's length in bytes, hosted arm only            (default 18)
#
# Prerequisites on every host: ./gradlew uberJar, hosted_hop_bench built Release and, for the direct arm,
# MULTIHOST_DIRECT_BENCH (doc/ec2-tests.md, "Image").
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./hop-matrix.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RATES="${HOP_RATES:-10000 20000 50000}"
REPEATS="${HOP_REPEATS:-3}"
RUN_SECONDS="${HOP_SECONDS:-30}"
MODES="${HOP_MODES:-direct hosted-spin hosted-backoff}"
HOST_CLASS=org.limitless.seqeron.proxy.ClientProxy

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/hop-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# hop-matrix $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} seconds=${RUN_SECONDS}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}"
    echo "# MULTIHOST_MEMBER_CPUS=${MULTIHOST_MEMBER_CPUS:-} MULTIHOST_HOST_CPUS=${MULTIHOST_HOST_CPUS:-}" \
         "MULTIHOST_BENCH_CPUS=${MULTIHOST_BENCH_CPUS:-}"
} > "${RESULTS}"

trap cluster_down EXIT

# run_mode <mode> <leader> <rate> <count> — one hop_bench run; prints its result line.
run_mode() {
    local mode="$1" id="$2" rate="$3" count="$4" host="${MEMBERS[$2]}" spin arm=hosted output extra=()
    case "${mode}" in
    direct)
        arm=direct
        extra=(--client-id 12)
        ;;
    hosted-spin | hosted-backoff)
        spin=false
        [[ "${mode}" == hosted-spin ]] && spin=true
        node "${host}" start --cpus "${MULTIHOST_HOST_CPUS:-}" host "${HOST_CLASS}" \
            -Dproxy.memberId="${id}" -Dproxy.sourceId=16 -Dproxy.clientId=31 \
            -Dproxy.snapshotDir='{data}/snap' -Dproxy.spin="${spin}"
        extra=(--payload "${MULTIHOST_PAYLOAD:-18}")
        node "${host}" wait-log host "host ready" 60 || {
            echo "ERROR: the host on member ${id} not ready after 60 s" >&2
            return 1
        }
        ;;
    *)
        echo "ERROR: unknown mode ${mode}" >&2
        return 2
        ;;
    esac
    output="$(node "${host}" bench --cpus "${MULTIHOST_BENCH_CPUS:-}" "${arm}" --member "${id}" --rate "${rate}" \
              --count "${count}" "${extra[@]}")" || true
    [[ "${arm}" == hosted ]] && node "${host}" kill host
    grep '^arm=' <<< "${output}" || echo "FAILED: ${output//$'\n'/ | }"
}

for rate in ${RATES}; do
    mh_log "rate ${rate} msg/s: fresh cluster"
    cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
    block_leader="$(await_leader 60)"
    count=$(( rate * RUN_SECONDS ))
    for (( repeat = 1; repeat <= REPEATS; repeat++ )); do
        for mode in ${MODES}; do
            current="$(leader)" || current=none
            if [[ "${current}" != "${block_leader}" ]]; then
                echo "ERROR: leader moved from member ${block_leader} to ${current} during rate ${rate}" >&2
                exit 1
            fi
            mh_log "rate ${rate} repeat ${repeat} ${mode} on member ${block_leader} (${count} markers)"
            line="$(run_mode "${mode}" "${block_leader}" "${rate}" "${count}")"
            echo "mode=${mode} repeat=${repeat} leader=${block_leader} ${line}" | tee -a "${RESULTS}"
        done
    done
done

mh_log "results in ${RESULTS}"
