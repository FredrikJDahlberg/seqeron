#!/usr/bin/env bash
# trading-day.sh — a trading day's message rates against three hosts (doc/ec2-tests.md, test 4): TradingDayBench on the
# leader's host replays a profile's phases, every message 50 bytes, and prints a line per phase into
# <results>/day-<stamp>.txt. Each run takes a fresh cluster loading topology-test-hosted-quiet.xml. Each member's
# driver counters are sampled every MULTIHOST_COUNTER_MS (default 10, 0 for none) by CounterSampler, into its
# counter-sampler.log in the run's logs, and before the cluster goes down the members' queues are compared
# (hop-order-check.py) into a "# order" line.
#
#   DAY_PROFILE   the profile, relative to the checkout (default seqeron-service/src/test/resources/trading-day-us.txt)
#   DAY_SCALE     multiplies every rate                   (default 1)
#   DAY_SEED      seeds the first run's arrivals, each later run the next  (default 1)
#   DAY_RUNS      runs                                    (default 1)
#   MULTIHOST_BENCH_CPUS   taskset CPU list for the bench (default unpinned)
#
# Prerequisites on every host: ./gradlew uberJar :seqeron-service:compileTestJava.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./trading-day.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

PROFILE="${DAY_PROFILE:-seqeron-service/src/test/resources/trading-day-us.txt}"
SCALE="${DAY_SCALE:-1}"
SEED="${DAY_SEED:-1}"
RUNS="${DAY_RUNS:-1}"

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/day-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# trading-day $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} profile=${PROFILE} scale=${SCALE}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}"
    echo "# MULTIHOST_MEMBER_CPUS=${MULTIHOST_MEMBER_CPUS:-} MULTIHOST_BENCH_CPUS=${MULTIHOST_BENCH_CPUS:-}"
} > "${RESULTS}"

trap cluster_down EXIT

status=0
for (( run = 1; run <= RUNS; run++ )); do
    mh_log "run ${run}: fresh cluster"
    cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
    leader_id="$(await_leader 60)"
    if [[ "${MULTIHOST_COUNTER_MS:-10}" != 0 ]]; then
        for id in 0 1 2; do
            node "${MEMBERS[$id]}" counter-sampler "${id}"
        done
    fi
    seed=$(( SEED + run - 1 ))
    mh_log "run ${run}: ${PROFILE} on member ${leader_id}, scale ${SCALE}, seed ${seed}"
    echo "# run=${run} leader=${leader_id} seed=${seed} started=$(date +%s)" >> "${RESULTS}"
    output="$(node "${MEMBERS[$leader_id]}" trading-day "${leader_id}" "${PROFILE}" "${SCALE}" "${seed}")" || status=1
    grep -E '^(phase=|FAILED)' <<< "${output}" | sed "s/^/run=${run} /" | tee -a "${RESULTS}" ||
        echo "run=${run} FAILED: ${output//$'\n'/ | }" | tee -a "${RESULTS}"
    current="$(leader)" || current=none
    if [[ "${current}" != "${leader_id}" ]]; then
        echo "run=${run} FAILED: the leader moved from member ${leader_id} to ${current}" | tee -a "${RESULTS}"
        status=1
    fi
    check_order "run=${run}" | tee -a "${RESULTS}" || status=1
    cluster_down
done
trap - EXIT

mh_log "results in ${RESULTS}"
exit "${status}"
