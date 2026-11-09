#!/usr/bin/env bash
# hop-writeback.sh — the kernel's writeback timers against the hop's tail (doc/ec2-tests.md, test 1, "Archive writes on
# EBS"): for each pair of vm.dirty_expire_centisecs and vm.dirty_writeback_centisecs, the members are set to it and
# each rate runs on a fresh cluster, so every run starts with an empty archive. Each run is a line in
# <results>/writeback-<stamp>.txt, in hop-matrix's format with the setting appended
#
#   mode=<mode> repeat=<n> leader=<id> arm=... expire=<cs> writeback=<cs>
#
# and each setting and rate ends in a line
#
#   summary expire=<cs> writeback=<cs> rate=<msg/s> payload=<B> runs=<n> stalled=<n> worst_p99.9=<us> worst_max=<us>
#
# A run stalls when its p99.9 reaches WB_STALL_US, the threshold hop-analyze.py uses. The members' values from before
# are put back when the script ends.
#
#   WB_EXPIRE      vm.dirty_expire_centisecs values, space-separated       (default "25 50 100 3000")
#   WB_WAKEUP      vm.dirty_writeback_centisecs values, space-separated    (default "10 20 50 500")
#   WB_RATES       msg/s, space-separated                                  (default "50000 100000")
#   WB_PAYLOAD     marker length in bytes                                  (default 100)
#   WB_REPEATS     runs at each setting and rate                           (default 5)
#   WB_SECONDS     length of a run; each bench drops its first 20%         (default 30)
#   WB_STALL_US    the p99.9 that counts as a stall, in microseconds       (default 5000)
#   WB_MODE        the mode, as hop-matrix.sh's                            (default direct)
#
# Every expire value is run with every wakeup value; 3000 and 500 are the kernel's defaults. The members need
# passwordless sudo for sysctl. The prerequisites and the pinning variables are hop-matrix.sh's.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 ./hop-writeback.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

EXPIRES="${WB_EXPIRE:-25 50 100 3000}"
WAKEUPS="${WB_WAKEUP:-10 20 50 500}"
RATES="${WB_RATES:-50000 100000}"
PAYLOAD="${WB_PAYLOAD:-100}"
REPEATS="${WB_REPEATS:-5}"
SECONDS_PER_RUN="${WB_SECONDS:-30}"
STALL_US="${WB_STALL_US:-5000}"
MODE="${WB_MODE:-direct}"

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/writeback-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# hop-writeback $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} seconds=${SECONDS_PER_RUN}"
    echo "# expire=${EXPIRES} wakeup=${WAKEUPS} rates=${RATES} payload=${PAYLOAD} repeats=${REPEATS} stall_us=${STALL_US}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}"
} > "${RESULTS}"

# on_member <host> <command> — runs <command> on <host>, here when it is localhost.
on_member() {
    if [[ "$1" == localhost || "$1" == 127.0.0.1 ]]; then
        bash -c "$2"
    else
        ${MULTIHOST_SSH} "$1" "$2"
    fi
}

# members — each member host once.
members() {
    local host seen=" "
    for host in "${MEMBERS[@]}"; do
        if [[ "${seen}" != *" ${host} "* ]]; then
            seen+="${host} "
            echo "${host}"
        fi
    done
}

# set_writeback <expire> <writeback> — sets both timers on every member and checks they read back.
set_writeback() {
    local host got
    for host in $(members); do
        on_member "${host}" "sudo -n sysctl -qw vm.dirty_expire_centisecs=$1 vm.dirty_writeback_centisecs=$2"
        got="$(on_member "${host}" "sysctl -n vm.dirty_expire_centisecs vm.dirty_writeback_centisecs" | tr '\n' ' ')"
        if [[ "${got}" != "$1 $2 " ]]; then
            echo "ERROR: ${host} reads back '${got}', not '$1 $2'" >&2
            return 1
        fi
    done
}

ORIGINAL="$(on_member "$(members | head -n 1)" "sysctl -n vm.dirty_expire_centisecs vm.dirty_writeback_centisecs" |
            tr '\n' ' ')"
restore() {
    cluster_down
    set_writeback ${ORIGINAL} || true
}
trap restore EXIT

# field <name> <line> — the number after <name>= in the bench's line, empty when absent.
field() {
    sed -n "s/.* $1=\([0-9.]*\).*/\1/p" <<< "$2"
}

for expire in ${EXPIRES}; do
    for wakeup in ${WAKEUPS}; do
        set_writeback "${expire}" "${wakeup}"
        for rate in ${RATES}; do
            count=$(( rate * SECONDS_PER_RUN ))
            runs=0
            stalled=0
            worst_p999=0
            worst_max=0
            for (( repeat = 1; repeat <= REPEATS; repeat++ )); do
                mh_log "expire ${expire} writeback ${wakeup}: ${rate} msg/s run ${repeat}: fresh cluster"
                cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
                leader_id="$(await_leader 60)"
                line="$(run_mode "${MODE}" "${leader_id}" "${rate}" "${count}" "${PAYLOAD}")"
                echo "mode=${MODE} repeat=${repeat} leader=${leader_id} ${line} expire=${expire} writeback=${wakeup}" |
                    tee -a "${RESULTS}"
                for host in $(all_hosts); do
                    node "${host}" stop > /dev/null
                done
                runs=$(( runs + 1 ))
                p999="$(field p99.9 "${line}")"
                max="$(field max "${line}")"
                if [[ -z "${p999}" ]]; then
                    stalled=$(( stalled + 1 ))
                    continue
                fi
                if awk -v v="${p999}" -v limit="${STALL_US}" 'BEGIN { exit !(v >= limit) }'; then
                    stalled=$(( stalled + 1 ))
                fi
                worst_p999="$(awk -v a="${worst_p999}" -v b="${p999}" 'BEGIN { print (b > a ? b : a) }')"
                worst_max="$(awk -v a="${worst_max}" -v b="${max:-0}" 'BEGIN { print (b > a ? b : a) }')"
            done
            echo "summary expire=${expire} writeback=${wakeup} rate=${rate} payload=${PAYLOAD} runs=${runs}" \
                 "stalled=${stalled} worst_p99.9=${worst_p999} worst_max=${worst_max}" | tee -a "${RESULTS}"
        done
    done
done

mh_log "results in ${RESULTS}"
