#!/usr/bin/env bash
# shadow-lag.sh — the shadow queue's lag across three hosts (doc/ec2-tests.md, test 7): the members run with
# sequencer.shadowQueues=true, the direct arm's bench writes markers on the leader's host, and ClusterProbe shadow on
# every member reports how much later each frame lands on the shadow queue than on the service's. Each run is a line per
# member in <results>/shadow-<stamp>.txt.
#
# One cluster loads topology-test-hosted-quiet.xml and serves every rate, lowest first, after a 10 s warm-up at the
# first rate, which is reported and not judged. A run starts a probe on every member, runs the bench for SHADOW_SECONDS,
# and stops the probes, whose SIGTERM makes each print its line. A run whose leader is not the cluster's first fails the
# script rather than measure an election. Every member's /proc counters (node.sh sample) are taken before and after each
# run into shadow-<stamp>.samples, and each member's driver counters every MULTIHOST_COUNTER_MS (default 10, 0 for none)
# by CounterSampler, into its counter-sampler.log in the logs.
#
# MULTIHOST_IDLE is every member agent's idle strategy, the shadow thread's included; the test's is busyspin.
#
#   SHADOW_RATES          msg/s, space-separated            (default "20000 100000 400000")
#   SHADOW_RUNS           runs at each rate                 (default 2)
#   SHADOW_SECONDS        length of a run                   (default 30)
#   SHADOW_PAYLOAD        marker length in bytes            (default 100)
#   MULTIHOST_PROBE_CPUS  taskset CPU list for each probe   (default unpinned)
#   MULTIHOST_BENCH_CPUS  taskset CPU list for hop_bench    (default unpinned)
#
# Prerequisites on every host: ./gradlew uberJar and MULTIHOST_DIRECT_BENCH, as hop-matrix.sh's direct arm needs.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 MULTIHOST_IDLE=busyspin ./shadow-lag.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

RATES="${SHADOW_RATES:-20000 100000 400000}"
RUNS="${SHADOW_RUNS:-2}"
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

# stop_probes <run> — stops every probe and appends its report, and an unmatched-frames error if it logged one.
stop_probes() {
    local id role line unmatched
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" kill "shadow-${id}"
    done
    for id in 0 1 2; do
        role=follower
        [[ "${id}" == "${LEADER}" ]] && role=leader
        line="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "shadow lag")"
        [[ -n "${line}" ]] || line="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "no frame seen")"
        unmatched="$(node "${MEMBERS[$id]}" last-line "shadow-${id}" "have no match")"
        echo "$1 member=${id} role=${role} ${line:-FAILED: no report}${unmatched:+ | ${unmatched}}" | tee -a "${RESULTS}"
    done
}

# measure <label> <rate> <seconds> — one run: the probes, the bench on the leader, each member's report.
measure() {
    local current line
    current="$(leader)" || current=none
    if [[ "${current}" != "${LEADER}" ]]; then
        echo "ERROR: leader moved from member ${LEADER} to ${current} before $1" >&2
        exit 1
    fi
    mh_log "$1 on member ${LEADER} ($(( $2 * $3 )) markers)"
    start_probes
    sample_members "${1// /-}" before
    line="$(run_mode direct "${LEADER}" "$2" "$(( $2 * $3 ))" "${PAYLOAD}")"
    sample_members "${1// /-}" after
    echo "$1 bench ${line}" | tee -a "${RESULTS}"
    stop_probes "$1"
}

trap cluster_down EXIT

cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
LEADER="$(await_leader 60)"
if [[ "${MULTIHOST_COUNTER_MS:-10}" != 0 ]]; then
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" counter-sampler "${id}"
    done
fi
read -r first_rate _ <<< "${RATES}"
measure "warmup rate=${first_rate}" "${first_rate}" 10
for rate in ${RATES}; do
    for (( run = 1; run <= RUNS; run++ )); do
        measure "rate=${rate} run=${run}" "${rate}" "${RUN_SECONDS}"
    done
done
cluster_down
trap - EXIT

mh_log "results in ${RESULTS}"
