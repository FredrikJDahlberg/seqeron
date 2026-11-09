#!/usr/bin/env bash
# node.sh — the per-host half of the multi-host drivers, which run it on each host through common.sh's node().
#
#   node.sh member <id>                              start SequencerServer member <id>
#   node.sh gateway-host                             start ReplayerServer as a gateway host, node 3, relaying group 1
#   node.sh start [--cpus L] <name> <class> [-Dk=v…] [-- args…]
#                                                    start a Java main off the uber jar and the test classes
#   node.sh run   [--cpus L] <name> <class> [-Dk=v…] [-- args…]
#                                                    the same in the foreground, exiting with it
#   node.sh bench [--cpus L] <direct|hosted|java|csharp> <arguments…>
#                                                    run that arm's bench in the foreground: direct is hop_bench
#                                                    of the tag cpp-direct-client-final, hosted is hosted_hop_bench,
#                                                    java is DirectHopBench, csharp is Seqeron.HopBench
#   node.sh kill <name>                              stop what start, member or gateway-host started as <name>
#   node.sh pause <name> | resume <name>             SIGSTOP / SIGCONT what start, member or gateway-host started as <name>
#   node.sh wait-log <name> <pattern> <seconds>      wait for <pattern> in <name>'s log
#   node.sh last-line <name> <pattern>               the last line of <name>'s log holding <pattern>
#   node.sh clusterctl <memberId> <arguments…>       clusterctl.sh as member <memberId>'s co-located tool
#   node.sh counters <memberId>                      clusterctl.sh counters on member <memberId>'s driver
#   node.sh archive-kb <memberId>                    the blocks member <memberId>'s archive occupies, in KiB
#   node.sh stop                                     stop every process started here, and stop-cluster.sh's
#   node.sh wipe                                     stop, then remove this host's run directory and nodes 0-3's
#                                                    Aeron directories
#   node.sh logs                                     this host's logs as a tar stream on stdout
#   node.sh counter-sampler <memberId>               start CounterSampler on member <memberId>'s driver, its changed
#                                                    counters every MULTIHOST_COUNTER_MS in <run>/logs/counter-sampler.log
#   node.sh thread-sampler <memberId>               start pidstat -t on member <memberId>'s JVM and mpstat -P ALL, once a
#                                                    second: <run>/logs/pidstat-member-<id>.log and mpstat.log. Each
#                                                    thread's CPU column shows where it ran, so a migration or two
#                                                    threads on one core is visible. Needs sysstat; stop with kill or stop
#   node.sh queue-digest <memberId>                  QueueDigest on member <memberId>'s archive: one line per recording of
#                                                    group 1's queue, for hop-order-check.py; exits 1 on a gap;
#                                                    MULTIHOST_DIGEST_UPTO caps the last groupSeqNo digested
#   node.sh sample <label>                        one line of /proc counters: time in ms, per-CPU steal, context
#                                                    switches, each java and bench process's utime+stime ticks, and the
#                                                    NIC's ENA allowance and drop counters (ethtool -S)
#
# A -D value may name {data}, this host's data directory. A name's log is <run>/logs/<name>.log and its pid
# <run>/logs/<name>.pid; a gateway host's ReplayerServer logs to <run>/logs/ReplayerServer-3.log.
#
#   MULTIHOST_BASE_DIR      the run directory: data/ and logs/            (default ~/seqeron-multihost)
#   MULTIHOST_IDLE          every member agent's idle strategy            (default backoff; busyspin when pinned)
#   MULTIHOST_MEMBER_OPTS   extra JVM flags for a member, space-separated
#   MULTIHOST_MEMBER_CPUS   taskset CPU list for a member                 (default unpinned)
#   MULTIHOST_ARCHIVE_DIR   where a member's archive-<id> lives, linked into data/  (default inside data/)
#   MULTIHOST_COUNTER_MS    CounterSampler's interval; 0 starts none          (default 10)
#   MULTIHOST_COUNTER_CPUS  taskset CPU list for it                           (default 0-1)
#   MULTIHOST_BENCH_IDLE   hop_bench's SEQERON_IDLE_STRATEGY             (default busyspin)
#   MULTIHOST_HOP_BENCH     the hosted_hop_bench binary                   (default cmake-build-release/hosted_hop_bench)
#   MULTIHOST_DIRECT_BENCH  hop_bench built on the tag, for the direct arm (required for it)
#   MULTIHOST_CSHARP_BENCH  the Seqeron.HopBench binary (dotnet publish), for the csharp arm (required for it)
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
HOP_BENCH="${MULTIHOST_HOP_BENCH:-${SEQERON_HOME}/cmake-build-release/hosted_hop_bench}"
SAFEPOINT_LOG=(-Xlog:safepoint:stdout:time,uptime)

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
    local command=(java "${SEQERON_SERVER_JAVA_OPTS[@]}" "${SAFEPOINT_LOG[@]}" "${props[@]+"${props[@]}"}"
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
    if [[ -n "${MULTIHOST_ARCHIVE_DIR:-}" ]]; then
        rm -rf "${MULTIHOST_ARCHIVE_DIR}/archive-$1" "${DATA_DIR}/archive-$1"
        mkdir -p "${MULTIHOST_ARCHIVE_DIR}/archive-$1"
        ln -s "${MULTIHOST_ARCHIVE_DIR}/archive-$1" "${DATA_DIR}/archive-$1"
    fi
    read -r -a member_opts <<< "${MULTIHOST_MEMBER_OPTS:-}"
    detach "member-$1" "${MULTIHOST_MEMBER_CPUS:-}" \
        java "${SEQERON_SERVER_JAVA_OPTS[@]}" "${SAFEPOINT_LOG[@]}" "${member_opts[@]+"${member_opts[@]}"}" \
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
    arm="$1"
    shift
    case "${arm}" in
    direct)
        bench=("${MULTIHOST_DIRECT_BENCH:?MULTIHOST_DIRECT_BENCH names hop_bench built on the tag}" direct "$@")
        ;;
    hosted)
        bench=("${HOP_BENCH}" "$@")
        ;;
    java)
        seqeron_require_jar
        bench=(java "${SEQERON_SERVER_JAVA_OPTS[@]}" "-Dbench.idle=${MULTIHOST_BENCH_IDLE:-busyspin}"
               -cp "${SEQERON_JAR}:${TEST_CLASSES}" org.limitless.seqeron.tools.DirectHopBench "$@")
        ;;
    csharp)
        bench=("${MULTIHOST_CSHARP_BENCH:?MULTIHOST_CSHARP_BENCH names the published Seqeron.HopBench}" "$@")
        ;;
    *)
        echo "ERROR: unknown arm ${arm}" >&2
        exit 2
        ;;
    esac
    pinned_command "${cpus}" env SEQERON_IDLE_STRATEGY="${MULTIHOST_BENCH_IDLE:-busyspin}" "${bench[@]}"
    "${PINNED[@]}" 2>&1 | tee -a "${LOG_DIR}/bench.log"
    ;;
kill)
    kill_name "$1"
    ;;
pause)
    kill -STOP "$(cat "${LOG_DIR}/$1.pid")"
    ;;
resume)
    kill -CONT "$(cat "${LOG_DIR}/$1.pid")"
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
    [[ -n "${MULTIHOST_ARCHIVE_DIR:-}" ]] && rm -rf "${MULTIHOST_ARCHIVE_DIR}"
    for id in 0 1 2 3; do
        rm -rf "$(seqeron_aeron_dir "${id}")"
    done
    ;;
logs)
    tar -C "${RUN_DIR}" -cf - logs
    ;;
counter-sampler)
    java_main start --cpus "${MULTIHOST_COUNTER_CPUS:-0-1}" counter-sampler org.limitless.seqeron.tools.CounterSampler \
        "-Dsampler.aeronDir=$(seqeron_aeron_dir "$1")" "-Dsampler.intervalMs=${MULTIHOST_COUNTER_MS:-10}"
    ;;
thread-sampler)
    command -v pidstat > /dev/null || { echo "ERROR: pidstat is not installed (package sysstat)" >&2; exit 1; }
    detach "pidstat-member-$1" "${MULTIHOST_COUNTER_CPUS:-0-1}" pidstat -h -t -u -w -p "$(cat "${LOG_DIR}/member-$1.pid")" 1
    detach mpstat "${MULTIHOST_COUNTER_CPUS:-0-1}" mpstat -P ALL 1
    ;;
queue-digest)
    status=0
    java_main run --cpus "${MULTIHOST_COUNTER_CPUS:-0-1}" queue-digest org.limitless.seqeron.tools.QueueDigest \
        "-Ddigest.aeronDir=$(seqeron_aeron_dir "$1")" "-Ddigest.upTo=${MULTIHOST_DIGEST_UPTO:-9223372036854775807}" || status=$?
    cat "${LOG_DIR}/queue-digest.log"
    exit "${status}"
    ;;
sample)
    steal="$(awk '/^cpu[0-9]/ { printf "%s%d", (n++ ? "," : ""), $9 }' /proc/stat)"
    ctxt="$(awk '$1 == "ctxt" { print $2 }' /proc/stat)"
    procs="$(awk '$2 ~ /^\((java|hop_bench|hosted_hop_ben.*)\)$/ { printf "%s:%s:%d ", $1, substr($2, 2, length($2) - 2), $14 + $15 }' \
             /proc/[0-9]*/stat 2>/dev/null || true)"
    nic="$(ip route get 1.1.1.1 2>/dev/null | awk '{ for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit } }')" || true
    ena="$(ethtool -S "${nic}" 2>/dev/null | awk '/allowance_exceeded|rx_drops|tx_drops/ { gsub(":", "", $1); printf "%s%s:%s", (n++ ? "," : ""), $1, $2 }' || true)"
    echo "t=$(date +%s%3N) label=$1 steal=${steal} ctxt=${ctxt} procs=${procs} nic=${nic:-none} ena=${ena}"
    ;;
*)
    sed -n '2,/^$/p' "$0" >&2
    exit 2
    ;;
esac
