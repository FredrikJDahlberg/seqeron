#!/usr/bin/env bash
# node.sh — the per-host half of the multi-host drivers, which run it on each host through common.sh's node().
#
#   node.sh member <id>                              start SequencerServer member <id>
#   node.sh gateway-host                             start ReplayerServer as a gateway host, node 3, relaying group 1
#   node.sh start [--cpus L] <name> <class> [-Dk=v…] [-- args…]
#                                                    start a Java main off the uber jar and the test classes
#   node.sh run   [--cpus L] <name> <class> [-Dk=v…] [-- args…]
#                                                    the same in the foreground, exiting with it
#   node.sh bench [--cpus L] <hop_bench arguments…>  run hop_bench in the foreground
#   node.sh kill <name>                              stop what start, member or gateway-host started as <name>
#   node.sh wait-log <name> <pattern> <seconds>      wait for <pattern> in <name>'s log
#   node.sh last-line <name> <pattern>               the last line of <name>'s log holding <pattern>
#   node.sh clusterctl <memberId> <arguments…>       clusterctl.sh as member <memberId>'s co-located tool
#   node.sh counters <memberId>                      clusterctl.sh counters on member <memberId>'s driver
#   node.sh archive-kb <memberId>                    the blocks member <memberId>'s archive occupies, in KiB
#   node.sh stop                                     stop every process started here, and stop-cluster.sh's
#   node.sh wipe                                     stop, then remove this host's run directory and nodes 0-3's
#                                                    Aeron directories
#   node.sh logs                                     this host's logs as a tar stream on stdout
#
# A -D value may name {data}, this host's data directory. A name's log is <run>/logs/<name>.log and its pid
# <run>/logs/<name>.pid; a gateway host's ReplayerServer logs to <run>/logs/ReplayerServer-3.log.
#
#   MULTIHOST_BASE_DIR      the run directory: data/ and logs/            (default ~/seqeron-multihost)
#   MULTIHOST_IDLE          every member agent's idle strategy            (default backoff; busyspin when pinned)
#   MULTIHOST_MEMBER_OPTS   extra JVM flags for a member, space-separated
#   MULTIHOST_MEMBER_CPUS   taskset CPU list for a member                 (default unpinned)
#   MULTIHOST_BENCH_IDLE    hop_bench's SEQERON_IDLE_STRATEGY             (default busyspin)
#   MULTIHOST_HOP_BENCH     the hop_bench binary                          (default cmake-build-release/hop_bench)
#   MULTIHOST_CLIENT        this host's address when it runs the gateway host, as the members reach it

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_SCRIPTS="${SCRIPT_DIR}/../../../main/scripts"
source "${MAIN_SCRIPTS}/paths.sh"
source "${MAIN_SCRIPTS}/seqeron-home.sh"

RUN_DIR="${MULTIHOST_BASE_DIR:-${HOME}/seqeron-multihost}"
DATA_DIR="${RUN_DIR}/data"
LOG_DIR="${RUN_DIR}/logs"
TEST_CLASSES="${SEQERON_HOME}/seqeron-service/build/classes/java/test"
HOP_BENCH="${MULTIHOST_HOP_BENCH:-${SEQERON_HOME}/cmake-build-release/hop_bench}"

mkdir -p "${DATA_DIR}" "${LOG_DIR}"
cd "${RUN_DIR}"

# pinned_command <cpus> <command…> — sets PINNED to the command, under taskset when <cpus> is not empty.
pinned_command() {
    local cpus="$1"
    shift
    PINNED=()
    if [[ -n "${cpus}" ]]; then
        PINNED=(taskset -c "${cpus}")
    fi
    PINNED+=("$@")
}

# detach <name> <cpus> <command…> — starts the command in the background, its pid in <name>.pid.
detach() {
    local name="$1" cpus="$2"
    shift 2
    pinned_command "${cpus}" "$@"
    nohup "${PINNED[@]}" > "${LOG_DIR}/${name}.log" 2>&1 < /dev/null &
    echo "$!" > "${LOG_DIR}/${name}.pid"
}

# java_main <start|run> [--cpus L] <name> <class> [-Dk=v…] [-- args…]
java_main() {
    local mode="$1" cpus="" name class props=() args=()
    shift
    if [[ "${1:-}" == "--cpus" ]]; then
        cpus="$2"
        shift 2
    fi
    name="$1" class="$2"
    shift 2
    while (( $# > 0 )) && [[ "$1" != "--" ]]; do
        props+=("${1//\{data\}/${DATA_DIR}}")
        shift
    done
    [[ "${1:-}" == "--" ]] && shift
    args=("$@")
    seqeron_require_jar
    local command=(java "${SEQERON_SERVER_JAVA_OPTS[@]}" "${props[@]+"${props[@]}"}"
                   -cp "${SEQERON_JAR}:${TEST_CLASSES}" "${class}" "${args[@]+"${args[@]}"}")
    if [[ "${mode}" == start ]]; then
        detach "${name}" "${cpus}" "${command[@]}"
    else
        pinned_command "${cpus}" "${command[@]}"
        "${PINNED[@]}" > "${LOG_DIR}/${name}.log" 2>&1
    fi
}

kill_name() {
    local pid_file="${LOG_DIR}/$1.pid" pid deadline
    [[ -f "${pid_file}" ]] || return 0
    pid="$(cat "${pid_file}")"
    rm -f "${pid_file}"
    kill -TERM "${pid}" 2>/dev/null || return 0
    deadline=$(( SECONDS + 10 ))
    while kill -0 "${pid}" 2>/dev/null; do
        if (( SECONDS >= deadline )); then
            kill -KILL "${pid}" 2>/dev/null || true
            return 0
        fi
        sleep 0.2
    done
}

stop_all() {
    local pid_file
    for pid_file in "${LOG_DIR}"/*.pid; do
        [[ -f "${pid_file}" ]] && kill_name "$(basename "${pid_file}" .pid)"
    done
    "${MAIN_SCRIPTS}/stop-cluster.sh" > /dev/null
}

action="${1:-}"
[[ $# -gt 0 ]] && shift
case "${action}" in
member)
    seqeron_require_jar
    read -r -a member_opts <<< "${MULTIHOST_MEMBER_OPTS:-}"
    detach "member-$1" "${MULTIHOST_MEMBER_CPUS:-}" \
        java "${SEQERON_SERVER_JAVA_OPTS[@]}" "${member_opts[@]+"${member_opts[@]}"}" \
        -Dsequencer.memberId="$1" \
        -Dsequencer.hosts="${SEQERON_HOSTS}" \
        -Dsequencer.baseDir="${DATA_DIR}" \
        -Dsequencer.idleStrategy="${MULTIHOST_IDLE:-backoff}" \
        -jar "${SEQERON_JAR}"
    ;;
gateway-host)
    detach gateway-host "" env SEQERON_GROUPS=1 SEQERON_NODE_ID=3 SEQERON_HOST="${MULTIHOST_CLIENT:-localhost}" \
        SEQERON_BASE_DIR="${DATA_DIR}" "${MAIN_SCRIPTS}/start-gateway-host.sh"
    ;;
start | run)
    java_main "${action}" "$@"
    ;;
bench)
    cpus=""
    if [[ "${1:-}" == "--cpus" ]]; then
        cpus="$2"
        shift 2
    fi
    pinned_command "${cpus}" env SEQERON_IDLE_STRATEGY="${MULTIHOST_BENCH_IDLE:-busyspin}" "${HOP_BENCH}" "$@"
    "${PINNED[@]}" 2>&1 | tee -a "${LOG_DIR}/bench.log"
    ;;
kill)
    kill_name "$1"
    ;;
wait-log)
    wait_for_log "${LOG_DIR}/$1.log" "$2" "$3"
    ;;
last-line)
    grep "$2" "${LOG_DIR}/$1.log" | tail -n 1 || true
    ;;
clusterctl)
    member_id="$1"
    shift
    CLUSTERCTL_MEMBER_ID="${member_id}" CLUSTERCTL_BASE_DIR="${DATA_DIR}" "${MAIN_SCRIPTS}/clusterctl.sh" "$@"
    ;;
counters)
    CLUSTERCTL_MEMBER_ID="$1" "${MAIN_SCRIPTS}/clusterctl.sh" counters
    ;;
archive-kb)
    du -sk "${DATA_DIR}/archive-$1" | cut -f1
    ;;
stop)
    stop_all
    ;;
wipe)
    stop_all
    rm -rf "${DATA_DIR}" "${LOG_DIR}"
    for id in 0 1 2 3; do
        rm -rf "$(seqeron_aeron_dir "${id}")"
    done
    ;;
logs)
    tar -C "${RUN_DIR}" -cf - logs
    ;;
*)
    sed -n '2,/^$/p' "$0" >&2
    exit 2
    ;;
esac
