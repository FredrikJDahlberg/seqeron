#!/usr/bin/env bash
# shadow-lag.sh — the shadow queue's lag across three hosts (doc/ec2-tests.md, test 7): the members run with
# sequencer.shadowQueues=true, the direct arm's bench writes markers on the leader's host, and ClusterProbe shadow on
# every member reports how much later each frame lands on the shadow queue than on the service's. Each run is a line per
# member in <results>/shadow-<stamp>.txt.
#
# For each rate a fresh cluster loads topology-test-hosted-quiet.xml. A run starts a probe on every member, runs the
# bench for SHADOW_SECONDS, and stops the probes, whose SIGTERM makes each print its line. A run whose leader is not the
# rate's fails the script rather than measure an election. Every member's /proc counters (node.sh sample) are taken
# before and after each run into shadow-<stamp>.samples, and each member's driver counters every MULTIHOST_COUNTER_MS
# (default 10, 0 for none) by CounterSampler, into its counter-sampler.log in the rate's logs. Before a rate's cluster
# goes down the members' queues are compared (hop-order-check.py), into a "# order" line of the results.
#
# MULTIHOST_IDLE is the arm: busyspin, or backoff (the default), every member agent's idle strategy, the shadow
# thread's included.
#
#   SHADOW_RATES          msg/s, space-separated            (default "20000 100000 400000")
#   SHADOW_RUNS           runs at each rate                 (default 3)
#   SHADOW_SECONDS        length of a run                   (default 30)
#   SHADOW_PAYLOAD        marker length in bytes            (default 100)
#   MULTIHOST_PROBE_CPUS  taskset CPU list for each probe   (default unpinned)
#   MULTIHOST_BENCH_CPUS  taskset CPU list for hop_bench    (default unpinned)
#
# Prerequisites on every host: ./gradlew uberJar and MULTIHOST_DIRECT_BENCH, as hop-matrix.sh's direct arm needs.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./shadow-lag.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

RATES="${SHADOW_RATES:-20000 100000 400000}"
RUNS="${SHADOW_RUNS:-3}"
RUN_SECONDS="${SHADOW_SECONDS:-30}"
PAYLOAD="${SHADOW_PAYLOAD:-100}"
PROBE=org.limitless.seqeron.tools.ClusterProbe
export MULTIHOST_MEMBER_OPTS="${MULTIHOST_MEMBER_OPTS:+${MULTIHOST_MEMBER_OPTS} }-Dsequencer.shadowQueues=true"

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/shadow-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# shadow-lag $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} seconds=${RUN_SECONDS}" \
         "payload=${PAYLOAD}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS}"
    echo "# MULTIHOST_MEMBER_CPUS=${MULTIHOST_MEMBER_CPUS:-} MULTIHOST_PROBE_CPUS=${MULTIHOST_PROBE_CPUS:-}" \
         "MULTIHOST_BENCH_CPUS=${MULTIHOST_BENCH_CPUS:-}"
} > "${RESULTS}"
SAMPLES="${RESULTS%.txt}.samples"

# sample_members <run> <before|after> — each member's node.sh sample line, appended to <results>/shadow-<stamp>.samples.
sample_members() {
    local host
    for host in "${MEMBERS[@]}"; do
        echo "run=$1 $2 host=${host} $(node "${host}" sample "$1" || true)" >> "${SAMPLES}"
    done
}

# start_probes — ClusterProbe shadow on every member, each once it reads both streams.
start_probes() {
    local id
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" start --cpus "${MULTIHOST_PROBE_CPUS:-}" "shadow-${id}" "${PROBE}" \
            -Dprobe.memberId="${id}" -Dprobe.idleStrategy=busyspin -- shadow
    done
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" wait-log "shadow-${id}" "shadow: reads group" 60 || {
            echo "ERROR: the shadow probe on member ${id} not reading after 60 s" >&2
            return 1
        }
    done
}

# stop_probes <run> <leader> — stops every probe and appends its report, and an unmatched-frames error if it logged one.
stop_probes() {
    local id role line unmatched
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" kill "shadow-${id}"
    done
    for id in 0 1 2; do
        role=follower
        [[ "${id}" == "$2" ]] && role=leader
        line="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "shadow lag")"
        [[ -n "${line}" ]] || line="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "no frame seen")"
        unmatched="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "have no match")"
        echo "$1 member=${id} role=${role} ${line:-FAILED: no report}${unmatched:+ | ${unmatched}}" | tee -a "${RESULTS}"
    done
}

trap cluster_down EXIT

for rate in ${RATES}; do
    mh_log "rate ${rate} msg/s: fresh cluster"
    cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
    rate_leader="$(await_leader 60)"
    if [[ "${MULTIHOST_COUNTER_MS:-10}" != 0 ]]; then
        for id in 0 1 2; do
            node "${MEMBERS[$id]}" counter-sampler "${id}"
        done
    fi
    count=$(( rate * RUN_SECONDS ))
    for (( run = 1; run <= RUNS; run++ )); do
        current="$(leader)" || current=none
        if [[ "${current}" != "${rate_leader}" ]]; then
            echo "ERROR: leader moved from member ${rate_leader} to ${current} during rate ${rate}" >&2
            exit 1
        fi
        mh_log "rate ${rate} run ${run} on member ${rate_leader} (${count} markers)"
        label="rate=${rate} run=${run}"
        start_probes
        sample_members "${label// /-}" before
        line="$(run_mode direct "${rate_leader}" "${rate}" "${count}" "${PAYLOAD}")"
        sample_members "${label// /-}" after
        echo "${label} bench ${line}" | tee -a "${RESULTS}"
        stop_probes "${label}" "${rate_leader}"
    done
    order_line="$(check_order "rate=${rate}" || true)"
    echo "${order_line}" | tee -a "${RESULTS}"
    cluster_down
done
trap - EXIT

mh_log "results in ${RESULTS}"
