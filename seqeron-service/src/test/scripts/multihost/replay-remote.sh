#!/usr/bin/env bash
# replay-remote.sh — a cold replica's replay of a member's history, member-local and from another host
# (doc/ec2-tests.md, test 3). ReplayColdStartTest's measurement, with the second replica across a network.
#
#   1. A fresh cluster; REPLAY_PRELOAD ProbeMarkers flooded at ingress from member 0's host, padded to 193 bytes as
#      ReplayColdStartTest pads them.
#   2. Local: a cold ClusterProbe follow on member 1, replayed by member 1's Replayer over aeron:ipc.
#   3. Remote: a gateway host on MULTIHOST_CLIENT, whose relay copies member 0's queue recording over UDP, and a cold
#      follow there, replayed off the host's own recording.
#
# Each phase prints its elapsed time from its JVM's start, which that time includes, so a preload that replays in
# about a second measures the JVM; 4,000,000 frames (about 700 MB) is a rate. With REPLAY_LOAD_DURING=1, four times
# the preload more is flooded through both phases.
#
#   REPLAY_PRELOAD       frames flooded before the replicas start   (default 400000)
#   REPLAY_LOAD_DURING   1 floods more while they catch up          (default 0)
#   REPLAY_TIMEOUT       seconds a phase may take                   (default 600)
#   REPLAY_COLD          1 drops every member's page cache before each phase (node.sh drop-caches: Linux, sudo), so
#                        the replay reads from the disk                (default 0)
#
# With MULTIHOST_MEMBER_OPTS=-Dreplayer.deriveFromLog=true the members record no queue (doc/disk-load-plan.md, step
# 2a): the local phase replays off member 1's Raft log, and the gateway host needs the flag too, in
# MULTIHOST_HOST_OPTS, to relay member 0's log.
#
# Usage:
#   MULTIHOST_MEMBERS=10.0.0.11,10.0.0.12,10.0.0.13 MULTIHOST_CLIENT=10.0.0.14 ./replay-remote.sh

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

PRELOAD="${REPLAY_PRELOAD:-400000}"
LOAD_DURING="${REPLAY_LOAD_DURING:-0}"
COLD="${REPLAY_COLD:-0}"
TIMEOUT="${REPLAY_TIMEOUT:-600}"
PROBE=org.limitless.seqeron.tools.ClusterProbe
FILLER=-Dprobe.fillerBytes=175

mkdir -p "${MULTIHOST_RESULTS}"
RESULTS="${MULTIHOST_RESULTS}/replay-$(date -u +%Y%m%dT%H%M%SZ).txt"
echo "# replay-remote $(date -u +%Y-%m-%dT%H:%M:%SZ) members=${MULTIHOST_MEMBERS} client=${MULTIHOST_CLIENT}" \
     "preload=${PRELOAD} load-during=${LOAD_DURING} cold=${COLD} MULTIHOST_MEMBER_OPTS=${MULTIHOST_MEMBER_OPTS:-}" \
     "MULTIHOST_HOST_OPTS=${MULTIHOST_HOST_OPTS:-}" > "${RESULTS}"

trap cluster_down EXIT

report() {
    echo "$*" | tee -a "${RESULTS}"
}

# follow <host> <name> <node id> — starts a cold follow, waits for it to catch up, stops it; prints its seconds.
follow() {
    local host="$1" name="$2" id="$3" start delivered
    start="$(now_ms)"
    node "${host}" start "${name}" "${PROBE}" -Dprobe.memberId="${id}" -Dprobe.clientId=7 -- follow
    node "${host}" wait-log "${name}" "Caught up" "${TIMEOUT}" || {
        echo "ERROR: ${name} not caught up after ${TIMEOUT} s" >&2
        return 1
    }
    local elapsed=$(( $(now_ms) - start ))
    node "${host}" kill "${name}"
    delivered="$(node "${host}" last-line "${name}" "frames delivered in order:" | sed 's/.*: //')"
    if [[ -z "${delivered}" ]] || (( delivered < PRELOAD )); then
        echo "ERROR: ${name} delivered ${delivered:-no count}, fewer than the ${PRELOAD} preloaded" >&2
        return 2
    fi
    echo "${elapsed}"
}

# cold — drops each member's page cache when REPLAY_COLD=1.
cold() {
    local host
    [[ "${COLD}" == 1 ]] || return 0
    for host in "${MEMBERS[@]}"; do
        node "${host}" drop-caches
    done
}

rate() {
    awk -v kb="$1" -v ms="$2" 'BEGIN { printf "%.1f", (kb * 1024 / 1e6) / (ms / 1000) }'
}

cluster_up "${RESOURCES}/topology-test-groups.xml"

mh_log "preloading ${PRELOAD} frames from member 0"
node "${MEMBERS[0]}" run preload "${PROBE}" -Dprobe.memberId=0 -Dprobe.count="${PRELOAD}" "${FILLER}" -- submit
sleep 3
archive_kb="$(node "${MEMBERS[1]}" archive-kb 1)"
report "archive member 1: $(awk -v kb="${archive_kb}" 'BEGIN { printf "%.1f", kb * 1024 / 1e6 }') MB"

if [[ "${LOAD_DURING}" == 1 ]]; then
    node "${MEMBERS[0]}" start load "${PROBE}" -Dprobe.memberId=0 -Dprobe.count=$(( PRELOAD * 4 )) "${FILLER}" -- submit
fi

mh_log "local: cold follow on member 1"
cold
local_ms="$(follow "${MEMBERS[1]}" cold-local 1)"
report "local  (member 1, ipc): caught up in $(( local_ms ))ms, $(rate "${archive_kb}" "${local_ms}") MB/s"

mh_log "remote: gateway host on ${MULTIHOST_CLIENT}"
cold
start="$(now_ms)"
node "${MULTIHOST_CLIENT}" gateway-host
node "${MULTIHOST_CLIENT}" wait-log gateway-host "serving replay" "${TIMEOUT}" || {
    echo "ERROR: the gateway host not serving after ${TIMEOUT} s" >&2
    exit 1
}
ready_ms=$(( $(now_ms) - start ))
# "serving replay" is logged when the host's recording exists, not when the relay has copied the history, and a cold
# follow is caught up at the recording's current end. A follow that delivered fewer than the preload started before the
# relay got there, so it is started again until the host holds it all.
attempts=1
deadline=$(( $(now_ms) + TIMEOUT * 1000 ))
while true; do
    status=0
    remote_ms="$(follow "${MULTIHOST_CLIENT}" cold-remote 3)" || status=$?
    (( status == 2 )) || break
    (( $(now_ms) < deadline )) || { echo "ERROR: the gateway host held less than the preload after ${TIMEOUT} s" >&2; exit 1; }
    attempts=$(( attempts + 1 ))
    sleep 1
done
(( status == 0 )) || exit 1
report "remote (gateway host, udp relay): serving in ${ready_ms}ms, then caught up in ${remote_ms}ms" \
       "(follow attempt ${attempts}), $(rate "${archive_kb}" $(( ready_ms + remote_ms ))) MB/s overall"

mh_log "results in ${RESULTS}"
