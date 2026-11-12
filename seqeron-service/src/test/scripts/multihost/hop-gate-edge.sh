#!/usr/bin/env bash
# hop-gate-edge.sh — the gate-edge set of doc/ec2-tests.md, test 1b: does the hosted stub lose a command when the
# leader is paused until it is deposed, and how long does the resend take (spec H-17), against the direct client, which
# does not resend.
#
# Each run is a fresh cluster, and each repeat runs the arms in turn, in the opposite order every other repeat. Both
# arms are off the cluster, on a gateway host on MULTIHOST_CLIENT, so the gate closes at the leadership change and
# reopens once it is caught up, whoever leads; a client on the leader's member would not lead again, and both pay the
# gateway host's queue relay and UDP ingress, so the arms' times are not the matrix's and are not judged. hosted is
# hosted_hop_bench through a host inside the gateway host; direct is hop_bench of the tag (MULTIHOST_DIRECT_BENCH, on
# the fourth host), an app::Application on the gateway host's driver. The arm floods for HOP_EDGE_FLOOD_MS, the
# leader's member is stopped (SIGSTOP) HOP_EDGE_BEFORE_S seconds into the flood for HOP_EDGE_PAUSE_S seconds, and the
# bench ends once every marker it offered is sequenced, or HOP_EDGE_SETTLE_MS after the last that was. Its line, the
# hosted host's epochDrops and retained count, and the members' queue comparison (hop-order-check.py) are a run's
# line in <results>/hop-edge-<stamp>.txt.
#
# A hosted run passes when nothing offered was lost, none was sequenced twice or out of order, the stub retains
# nothing, the gate shut and reopened, and the host dropped at least one command at the edge (otherwise the edge
# proved nothing). A direct run passes when none was sequenced twice or out of order and the gate shut and reopened;
# its lost and declined counts are the baseline the hosted arm's drops are read against, and are not judged. A direct
# client the cluster fences has no result and fails. Exits 1 if any run fails.
#
#   HOP_EDGE_REPEATS     repeats                                         (default 5)
#   HOP_EDGE_ARMS        the arms, space-separated                       (default "hosted direct")
#   HOP_EDGE_SETTLE_MS   how long after the last sequenced marker the bench waits for the rest  (default 20000)
#   HOP_EDGE_COUNT       the most markers a run may offer; a run that reaches it fails, for the pause may have come
#                        after the last (default 60000000)
#   HOP_EDGE_FLOOD_MS    how long the bench offers markers               (default 20000)
#   HOP_EDGE_BEFORE_S    flood seconds before the leader is stopped      (default 5)
#   HOP_EDGE_PAUSE_S     seconds the leader is stopped                   (default 4.5)
#   HOP_EDGE_HOST_OPTS   extra JVM properties for the host, space-separated    (default none: the host's defaults, which
#                        drop its command window's worth at an edge; HostedCppStubTest's throttles are
#                        -Dproxy.commandWindow=1024 -Dproxy.pendingCapacity=4 -Dproxy.placePerCycle=2)
#   MULTIHOST_HOST_OPTS  extra JVM flags for the host in both arms: -Dreplayer.deriveFromLog=true beside members that
#                        record no queue
#   HOP_EDGE_SPIN        proxy.spin of the host: the fourth host has too few cores to spin it  (default false)
#   MULTIHOST_BENCH_CPUS taskset CPU list for hosted_hop_bench (default unpinned)
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 MULTIHOST_CLIENT=10.0.0.14 ./hop-gate-edge.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
source "${MH_DIR}/hop-common.sh"

REPEATS="${HOP_EDGE_REPEATS:-5}"
SETTLE_MS="${HOP_EDGE_SETTLE_MS:-20000}"
read -r -a ARMS <<< "${HOP_EDGE_ARMS:-hosted direct}"
FLOOD_MS="${HOP_EDGE_FLOOD_MS:-20000}"
COUNT="${HOP_EDGE_COUNT:-60000000}"
BEFORE_S="${HOP_EDGE_BEFORE_S:-5}"
PAUSE_S="${HOP_EDGE_PAUSE_S:-4.5}"
SPIN="${HOP_EDGE_SPIN:-false}"
read -r -a HOST_OPTS <<< "${HOP_EDGE_HOST_OPTS:-}"
read -r -a FLAG_OPTS <<< "${MULTIHOST_HOST_OPTS:-}"
GATEWAY_NODE=3
HOST_CLASS=org.limitless.seqeron.replayer.server.ReplayerServer

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/hop-edge-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
    echo "# hop-gate-edge $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} client=${MULTIHOST_CLIENT}" \
         "flood_ms=${FLOOD_MS} before_s=${BEFORE_S} pause_s=${PAUSE_S} settle_ms=${SETTLE_MS} arms=${ARMS[*]}"
    echo "# MULTIHOST_IDLE=${MULTIHOST_IDLE:-backoff} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}" \
         "host_opts=${HOST_OPTS[*]-} MULTIHOST_HOST_OPTS=${MULTIHOST_HOST_OPTS:-} spin=${SPIN}"
} > "${RESULTS}"

paused_host="" paused_name=""
resume_paused() {
    if [[ -n "${paused_host}" ]]; then
        node "${paused_host}" resume "${paused_name}" || true
        paused_host=""
    fi
}
trap 'resume_paused; cluster_down' EXIT

# number <line> <key> — the value of key=value (or "key value") in a line.
number() {
    sed -n "s/.*[ =]$2[ =]\\([0-9.-]*\\).*/\\1/p" <<< " $1" | head -n 1
}

# run_arm <arm> <repeat> — one fresh cluster, one flood across the pause; prints the run's line and its order check.
run_arm() {
    local arm="$1" repeat="$2" old new out bench_pid bench_line retained="" drops="" host_line reasons=() verdict host_fields
    local host_opts=() bench_args=() lost
    mh_log "gate edge ${arm} ${repeat}/${REPEATS}: fresh cluster"
    cluster_up "${RESOURCES}/topology-test-hosted-quiet.xml"
    old="$(await_leader 60)"
    if [[ "${arm}" == hosted ]]; then
        host_opts=(-Dproxy.hosts=gw -Dproxy.spin="${SPIN}" -Dproxy.sourceId=16 -Dproxy.clientId=31
                   -Dproxy.snapshotDir='{data}/snap' ${HOST_OPTS[@]+"${HOST_OPTS[@]}"})
        bench_args=(hosted --member "${GATEWAY_NODE}" --host-name gw --source-id 16)
    else
        bench_args=(direct --member "${GATEWAY_NODE}" --client-id 12 --off-cluster 1 --egress-host "${MULTIHOST_CLIENT}")
    fi
    node "${MULTIHOST_CLIENT}" start --cpus "${MULTIHOST_HOST_CPUS:-}" gateway-host "${HOST_CLASS}" \
        -Dreplayer.memberId="${GATEWAY_NODE}" -Dreplayer.groups=1 -Dreplayer.host="${MULTIHOST_CLIENT}" \
        -Dreplayer.baseDir='{data}' ${host_opts[@]+"${host_opts[@]}"} ${FLAG_OPTS[@]+"${FLAG_OPTS[@]}"}
    node "${MULTIHOST_CLIENT}" wait-log gateway-host "$([[ "${arm}" == hosted ]] && echo "host ready" || echo "serving replay")" 60 || {
        echo "ERROR: the gateway host not ready after 60 s" >&2
        exit 1
    }

    out="$(mktemp)"
    node "${MULTIHOST_CLIENT}" bench --cpus "${MULTIHOST_BENCH_CPUS:-}" "${bench_args[@]}" --rate 1000000 \
        --count "${COUNT}" --flood-ms "${FLOOD_MS}" --settle-ms "${SETTLE_MS}" > "${out}" 2>&1 &
    bench_pid=$!
    node "${MULTIHOST_CLIENT}" wait-log bench FLOODING 60 || {
        echo "ERROR: the bench is not flooding after 60 s" >&2
        exit 1
    }
    sleep "${BEFORE_S}"
    paused_host="${MEMBERS[$old]}" paused_name="member-${old}"
    mh_log "stopping member ${old} for ${PAUSE_S} s"
    node "${paused_host}" pause "${paused_name}"
    sleep "${PAUSE_S}"
    resume_paused
    new=none
    for _ in $(seq 60); do
        new="$(leader || true)"
        [[ "${new}" =~ ^[0-2]$ && "${new}" != "${old}" ]] && break
        sleep 1
    done
    wait "${bench_pid}" || true
    sleep 2

    bench_line="$(grep "^arm=${arm}-edge" "${out}" || echo "FAILED: $(tr '\n' '|' < "${out}")")"
    if [[ "${arm}" == hosted ]]; then
        retained="$(number "$(grep '^retained=' "${out}" || true)" retained)"
        host_line="$(node "${MULTIHOST_CLIENT}" last-line gateway-host "queue:")"
        drops="$(number "${host_line}" epochDrops)"
    fi
    rm -f "${out}"

    if [[ "${bench_line}" != arm=${arm}-edge* ]]; then
        reasons+=("no result")
    else
        lost="$(number "${bench_line}" lost)"
        [[ "$(number "${bench_line}" duplicates)" == 0 ]] || reasons+=("duplicates")
        [[ "$(number "${bench_line}" out_of_order)" == 0 ]] || reasons+=("out of order")
        (( $(number "${bench_line}" offered) < COUNT )) || reasons+=("flood ended at HOP_EDGE_COUNT")
        (( $(number "${bench_line}" edges) >= 1 )) || reasons+=("no gate edge")
        if [[ "${arm}" == hosted ]]; then
            [[ "${lost}" == 0 ]] || reasons+=("lost ${lost}")
            [[ "${retained:-x}" == 0 ]] || reasons+=("stub retains ${retained:-?}")
            (( ${drops:-0} > 0 )) || reasons+=("the host dropped nothing")
        fi
    fi
    [[ "${new}" =~ ^[0-2]$ ]] || reasons+=("no new leader")
    verdict=PASS
    if (( ${#reasons[@]} > 0 )); then
        verdict="FAIL(${reasons[*]})"
        failures=$(( failures + 1 ))
    fi
    host_fields=""
    [[ "${arm}" == hosted ]] && host_fields=" retained=${retained:-?} epoch_drops=${drops:-?}"
    echo "edge repeat=${repeat} old_leader=${old} new_leader=${new} ${bench_line}${host_fields} verdict=${verdict}" \
        | tee -a "${RESULTS}"
    check_order "edge ${arm} repeat=${repeat}" | tee -a "${RESULTS}" || failures=$(( failures + 1 ))
    cluster_down
}

failures=0
for (( repeat = 1; repeat <= REPEATS; repeat++ )); do
    if (( repeat % 2 == 1 )); then
        order=("${ARMS[@]}")
    else
        order=()
        for (( k = ${#ARMS[@]} - 1; k >= 0; k-- )); do order+=("${ARMS[$k]}"); done
    fi
    for arm in "${order[@]}"; do
        run_arm "${arm}" "${repeat}"
    done
done
trap - EXIT

mh_log "results in ${RESULTS}; ${failures} failure(s)"
(( failures == 0 ))
