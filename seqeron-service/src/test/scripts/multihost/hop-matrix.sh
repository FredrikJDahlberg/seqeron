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
#   HOP_PAYLOADS  marker lengths in bytes, 18 to 8875, space-separated (default "18"); every arm takes each
#
# Prerequisites on every host: ./gradlew uberJar, hosted_hop_bench built Release and, for the direct arm,
# MULTIHOST_DIRECT_BENCH, hop_bench of the tag with direct-hop-bench-payload.patch applied (doc/ec2-tests.md,
# "Image").
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./hop-matrix.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

RATES="${HOP_RATES:-10000 20000 50000}"
REPEATS="${HOP_REPEATS:-3}"
RUN_SECONDS="${HOP_SECONDS:-30}"
PAYLOADS="${HOP_PAYLOADS:-18}"
MODES="${HOP_MODES:-direct hosted-spin hosted-backoff}"

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/hop-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# hop-matrix $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} seconds=${RUN_SECONDS}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}"
    echo "# MULTIHOST_MEMBER_CPUS=${MULTIHOST_MEMBER_CPUS:-} MULTIHOST_HOST_CPUS=${MULTIHOST_HOST_CPUS:-}" \
         "MULTIHOST_BENCH_CPUS=${MULTIHOST_BENCH_CPUS:-}"
} > "${RESULTS}"

trap cluster_down EXIT

for rate in ${RATES}; do
    mh_log "rate ${rate} msg/s: fresh cluster"
    cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
    block_leader="$(await_leader 60)"
    count=$(( rate * RUN_SECONDS ))
    for (( repeat = 1; repeat <= REPEATS; repeat++ )); do
        for payload in ${PAYLOADS}; do
            for mode in ${MODES}; do
                current="$(leader)" || current=none
                if [[ "${current}" != "${block_leader}" ]]; then
                    echo "ERROR: leader moved from member ${block_leader} to ${current} during rate ${rate}" >&2
                    exit 1
                fi
                mh_log "rate ${rate} repeat ${repeat} ${mode} payload ${payload} on member ${block_leader} (${count} markers)"
                line="$(run_mode "${mode}" "${block_leader}" "${rate}" "${count}" "${payload}")"
                echo "mode=${mode} repeat=${repeat} leader=${block_leader} ${line}" | tee -a "${RESULTS}"
            done
        done
    done
done

mh_log "results in ${RESULTS}"
