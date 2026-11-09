#!/usr/bin/env bash
# hop-knee.sh — the knee of each arm (doc/ec2-tests.md, test 1b): the rate at which the whole loop saturates, found by
# stepping the rate up in short runs on one fresh cluster per ramp. Each step is a line in <results>/knee-<stamp>.txt, in
# hop-matrix.sh's format, and each ramp ends in a line
#
#   knee mode=<mode> payload=<B> repeat=<n> last_ok=<msg/s> saturated_at=<msg/s> reason=<why>
#
# A step saturates when its p99 exceeds KNEE_P99_US, when it loses a marker or fails, or when the leader moved during
# it. last_ok is the highest rate below the first saturated step; saturated_at is none when KNEE_MAX was reached first.
#
#   KNEE_START     first rate, msg/s                    (default 20000)
#   KNEE_FACTOR    percent each step raises the rate    (default 125)
#   KNEE_MAX       the last rate tried                  (default 1000000)
#   KNEE_SECONDS   length of a step; each bench drops its first 20%  (default 20)
#   KNEE_P99_US    the p99 that counts as saturated, in microseconds (default 10000)
#   KNEE_RUNS      ramps of each mode and payload       (default 2)
#   KNEE_PAYLOADS  marker lengths in bytes, space-separated (default "18 1024")
#   HOP_MODES      the modes, space-separated           (default "direct hosted-spin")
#
# A bench keeps every sample of a run, so KNEE_MAX times KNEE_SECONDS is its memory: 8 bytes a marker.
# The prerequisites and the pinning variables are hop-matrix.sh's.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./hop-knee.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

START="${KNEE_START:-20000}"
FACTOR="${KNEE_FACTOR:-125}"
MAX="${KNEE_MAX:-1000000}"
STEP_SECONDS="${KNEE_SECONDS:-20}"
P99_LIMIT="${KNEE_P99_US:-10000}"
RUNS="${KNEE_RUNS:-2}"
PAYLOADS="${KNEE_PAYLOADS:-100 1024}"
MODES="${HOP_MODES:-direct hosted-spin}"

if (( FACTOR <= 100 )); then
    echo "ERROR: KNEE_FACTOR must exceed 100" >&2
    exit 2
fi

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/knee-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# hop-knee $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} seconds=${STEP_SECONDS}"
    echo "# start=${START} factor=${FACTOR}% max=${MAX} p99_limit_us=${P99_LIMIT}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}"
    echo "# MULTIHOST_MEMBER_CPUS=${MULTIHOST_MEMBER_CPUS:-} MULTIHOST_HOST_CPUS=${MULTIHOST_HOST_CPUS:-}" \
         "MULTIHOST_BENCH_CPUS=${MULTIHOST_BENCH_CPUS:-}"
} > "${RESULTS}"

trap cluster_down EXIT

# saturated <line> <count> — prints why the step's result line shows saturation, nothing when it does not.
saturated() {
    local line="$1" count="$2" seen p99
    if [[ "${line}" != arm=* ]]; then
        echo "failed"
        return
    fi
    seen="$(sed -n 's/.* seen=\([0-9]*\) .*/\1/p' <<< "${line}")"
    p99="$(sed -n 's/.* p99=\([0-9.]*\) .*/\1/p' <<< "${line}")"
    if [[ -z "${seen}" || -z "${p99}" ]] || (( seen < count )); then
        echo "lost-markers"
    elif awk -v p99="${p99}" -v limit="${P99_LIMIT}" 'BEGIN { exit !(p99 > limit) }'; then
        echo "p99=${p99}us"
    fi
}

for payload in ${PAYLOADS}; do
    for (( run = 1; run <= RUNS; run++ )); do
        for mode in ${MODES}; do
            mh_log "ramp ${mode} payload ${payload} run ${run}: fresh cluster"
            cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
            block_leader="$(await_leader 60)"
            rate="${START}"
            last_ok=none
            saturated_at=none
            reason=none
            while (( rate <= MAX )); do
                count=$(( rate * STEP_SECONDS ))
                current="$(leader)" || current=none
                if [[ "${current}" != "${block_leader}" ]]; then
                    saturated_at="${rate}"
                    reason=leader-moved
                    break
                fi
                mh_log "${mode} payload ${payload} run ${run}: ${rate} msg/s (${count} markers)"
                line="$(run_mode "${mode}" "${block_leader}" "${rate}" "${count}" "${payload}")"
                echo "mode=${mode} repeat=${run} leader=${block_leader} ${line}" | tee -a "${RESULTS}"
                reason="$(saturated "${line}" "${count}")"
                if [[ -n "${reason}" ]]; then
                    saturated_at="${rate}"
                    break
                fi
                last_ok="${rate}"
                reason=none
                rate=$(( rate * FACTOR / 100 ))
            done
            echo "knee mode=${mode} payload=${payload} repeat=${run} last_ok=${last_ok}" \
                 "saturated_at=${saturated_at} reason=${reason}" | tee -a "${RESULTS}"
            for host in $(all_hosts); do
                node "${host}" stop > /dev/null
            done
        done
    done
done

mh_log "results in ${RESULTS}"
