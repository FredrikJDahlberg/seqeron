# shellcheck shell=bash
# common.sh — sourced by the multi-host drivers (cluster.sh, hop-matrix.sh, replay-remote.sh): the host lists,
# node(), which runs node.sh on a host, and the cluster's life cycle. Meant to be sourced, not executed.
#
# Every host holds a built checkout at the same path (doc/ec2-tests.md, "Image"). The drivers run on one of them, or
# on any machine that reaches the members by the addresses below, which are also the addresses the members bind.
#
#   MULTIHOST_MEMBERS   the three members' addresses, comma-separated in member-id order (required); exported to
#                       every host as SEQERON_HOSTS. "localhost,localhost,localhost" runs everything here
#   MULTIHOST_CLIENT    the fourth host's address, where replay-remote.sh runs the gateway host  (default localhost)
#   MULTIHOST_ROOT      the checkout on every host                                (default this checkout)
#   MULTIHOST_SSH       the command that reaches a host            (default ssh -o BatchMode=yes)
#   MULTIHOST_RESULTS   where the drivers write results and fetched logs           (default build/multihost)
#
# Every SEQERON_* and MULTIHOST_* variable set here is passed on to node.sh, which documents its own.

MH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MULTIHOST_ROOT="${MULTIHOST_ROOT:-$(cd "${MH_DIR}/../../../../.." && pwd)}"
MULTIHOST_SSH="${MULTIHOST_SSH:-ssh -o BatchMode=yes}"
MULTIHOST_CLIENT="${MULTIHOST_CLIENT:-localhost}"
MULTIHOST_RESULTS="${MULTIHOST_RESULTS:-${MULTIHOST_ROOT}/build/multihost}"
export MULTIHOST_ROOT MULTIHOST_CLIENT

if [[ -z "${MULTIHOST_MEMBERS:-}" ]]; then
    echo "ERROR: MULTIHOST_MEMBERS is required: the three members' addresses, comma-separated" >&2
    exit 2
fi
IFS=',' read -r -a MEMBERS <<< "${MULTIHOST_MEMBERS}"
if (( ${#MEMBERS[@]} != 3 )); then
    echo "ERROR: MULTIHOST_MEMBERS names ${#MEMBERS[@]} hosts; the drivers run three members" >&2
    exit 2
fi
export SEQERON_HOSTS="${MULTIHOST_MEMBERS}"

NODE_SH="${MULTIHOST_ROOT}/seqeron-service/src/test/scripts/multihost/node.sh"
RESOURCES="${MULTIHOST_ROOT}/seqeron-service/src/test/resources"

mh_log() {
    echo "[$(date -u +%H:%M:%S)] $*"
}

# Milliseconds since the epoch; bash 3.2, macOS's, has no EPOCHREALTIME.
now_ms() {
    if [[ -n "${EPOCHREALTIME:-}" ]]; then
        local t="${EPOCHREALTIME/[.,]/}"
        echo $(( t / 1000 ))
    else
        perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
    fi
}

# node <host> <node.sh arguments…> — runs node.sh on <host>, here when it is localhost, with the environment above.
node() {
    local host="$1" name env=() cmd
    shift
    for name in $(compgen -e); do
        if [[ "${name}" == SEQERON_* || "${name}" == MULTIHOST_* ]]; then
            env+=("${name}=${!name}")
        fi
    done
    cmd="cd $(printf '%q' "${MULTIHOST_ROOT}") && exec env $(printf '%q ' "${env[@]}") $(printf '%q' "${NODE_SH}")"
    cmd+=" $(printf '%q ' "$@")"
    if [[ "${host}" == localhost || "${host}" == 127.0.0.1 ]]; then
        bash -c "${cmd}"
    else
        ${MULTIHOST_SSH} "${host}" "${cmd}"
    fi
}

# Every host once: the members and the client, which may repeat.
all_hosts() {
    local host seen=" "
    for host in "${MEMBERS[@]}" "${MULTIHOST_CLIENT}"; do
        if [[ "${seen}" != *" ${host} "* ]]; then
            seen+="${host} "
            echo "${host}"
        fi
    done
}

# The member every member records as leader, from its counters; fails while they disagree or none is recorded.
leader() {
    local id view agreed=""
    for id in 0 1 2; do
        view="$(node "${MEMBERS[$id]}" counters "${id}" 2>/dev/null |
                awk '$1 == "seqeron.sequencer.currentLeaderMemberId" { print $NF }')" || true
        if [[ ! "${view}" =~ ^[0-2]$ ]] || [[ -n "${agreed}" && "${view}" != "${agreed}" ]]; then
            return 1
        fi
        agreed="${view}"
    done
    echo "${agreed}"
}

await_leader() {
    local deadline=$(( SECONDS + $1 )) id
    until id="$(leader)"; do
        if (( SECONDS >= deadline )); then
            echo "ERROR: the members agree on no leader after $1 s" >&2
            return 1
        fi
        sleep 1
    done
    echo "${id}"
}

# cluster_up <topology> — wipes every host, starts the three members, loads <topology> and waits for each Replayer.
cluster_up() {
    local topology="$1" host id
    for host in $(all_hosts); do
        node "${host}" wipe
    done
    for id in 0 1 2; do
        mh_log "member ${id} on ${MEMBERS[$id]}"
        node "${MEMBERS[$id]}" member "${id}"
    done
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" wait-log "member-${id}" "Running" 60 || {
            echo "ERROR: member ${id} not Running after 60 s" >&2
            return 1
        }
    done
    id="$(await_leader 60)" || return 1
    mh_log "leader is member ${id}; loading $(basename "${topology}")"
    node "${MEMBERS[0]}" clusterctl 0 load-topology "${topology}" > /dev/null
    for id in 0 1 2; do
        node "${MEMBERS[$id]}" wait-log "member-${id}" "serving replay" 60 || {
            echo "ERROR: member ${id}'s Replayer not serving after 60 s" >&2
            return 1
        }
    done
}

# cluster_down — stops everything on every host and copies each host's logs under <results>/logs-<stamp>/<host>.
cluster_down() {
    local stamp host dir
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    for host in $(all_hosts); do
        node "${host}" stop || true
        dir="${MULTIHOST_RESULTS}/logs-${stamp}/${host}"
        mkdir -p "${dir}"
        node "${host}" logs | tar -xf - -C "${dir}" || true
    done
    mh_log "logs under ${MULTIHOST_RESULTS}/logs-${stamp}"
}
