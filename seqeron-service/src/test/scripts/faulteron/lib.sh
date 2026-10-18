# shellcheck shell=bash
# lib.sh — sourced by each test: the three-node docker cluster, the faulteron CLI aimed at one of its members,
# and the ClusterProbe modes that drive and judge it. The sourcing script then sets LOG_DIR.
#
# Prerequisites: docker, the operator distribution (./gradlew operatorDist), and a faulteron checkout to build the
# injector image from (FAULTERON_DIR, default ../faulteron beside this repo).

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
FAULTERON_DIR="${FAULTERON_DIR:-${REPO_ROOT}/../faulteron}"
source "${REPO_ROOT}/seqeron-service/src/main/scripts/ports.sh"
source "${REPO_ROOT}/seqeron-service/src/main/scripts/seqeron-home.sh"
COMPOSE=(docker compose -f "${REPO_ROOT}/docker/compose.yml")

NODE_COUNT=3
TIMEOUT_SCALE="${TIMEOUT_SCALE:-1}"
deadline() { echo $(($1 * TIMEOUT_SCALE)); }

INGRESS_ENDPOINTS="$(ingress_endpoints_string "${NODE_COUNT}" 'node-{id}')"
JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

# PortLayout's offsets within a member's stride.
archive_port_of() { echo $(($(cluster_member_port_base "$1") + 1)); }
consensus_port_of() { echo $(($(cluster_member_port_base "$1") + 3)); }
log_port_of() { echo $(($(cluster_member_port_base "$1") + 4)); }
transfer_port_of() { echo $(($(cluster_member_port_base "$1") + 5)); }

# Every member's archive, consensus, log and transfer port: what members speak among themselves, and not ingress.
internal_ports() {
    local m
    for ((m = 0; m < NODE_COUNT; m++)); do
        echo "$(archive_port_of "${m}") $(consensus_port_of "${m}") $(log_port_of "${m}") $(transfer_port_of "${m}")"
    done
}

cleanup() {
    local m
    for m in 0 1 2; do "${COMPOSE[@]}" logs --no-color "node-${m}" > "${LOG_DIR}/node-${m}.log" 2>&1; done
    "${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
}

# Builds the injector image and brings the cluster up healthy; exits the test if either fails.
start_cluster() {
    if [[ ! -d "${REPO_ROOT}/build/install/seqeron/lib" ]]; then
        echo "ERROR: no operator distribution under ${REPO_ROOT}/build/install/seqeron" >&2
        echo "       — run: ./gradlew operatorDist" >&2
        exit 1
    fi
    if [[ ! -f "${FAULTERON_DIR}/Dockerfile" ]]; then
        echo "ERROR: no faulteron checkout at ${FAULTERON_DIR} — set FAULTERON_DIR" >&2
        exit 1
    fi
    rm -rf "${LOG_DIR}"
    mkdir -p "${LOG_DIR}"
    trap cleanup EXIT
    if ! docker build -q -t faulteron:local "${FAULTERON_DIR}" > "${LOG_DIR}/faulteron-build.log" 2>&1; then
        echo "faulteron image did not build; see ${LOG_DIR}/faulteron-build.log"
        exit 1
    fi
    "${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
    echo "starting ${NODE_COUNT}-node cluster"
    if ! "${COMPOSE[@]}" up --build -d --wait --wait-timeout "$(deadline 120)" > "${LOG_DIR}/compose-up.log" 2>&1; then
        echo "cluster did not come up healthy; see ${LOG_DIR}/compose-up.log"
        exit 1
    fi
    load_groups
    local silent
    if ! silent="$(await_every_queue "$(deadline 60)")"; then
        echo "cluster did not form: member ${silent}'s queue does not answer; see ${LOG_DIR}/node-${silent}.log"
        exit 1
    fi
}

# await_every_queue <deadline-secs> — every member's queue answers a ping, or prints the first that does not. The
# healthcheck passes on a member whose service then fails to start, which would leave a fault judged on two members.
await_every_queue() {
    local end=$((SECONDS + $1)) m
    for ((m = 0; m < NODE_COUNT; m++)); do
        until ping_queue "${m}" >> "${LOG_DIR}/ping-start.log" 2>&1; do
            if ((SECONDS >= end)); then
                echo "${m}"
                return 1
            fi
            sleep 1
        done
    done
}

# The cluster sequences nothing before its groups are active, so the test topology's load first.
load_groups() {
    local topology="${REPO_ROOT}/seqeron-service/src/test/resources/topology-test-groups.xml"
    if ! await_leader "$(deadline 60)" > /dev/null; then
        echo "no leader emerged to load the topology"
        exit 1
    fi
    docker cp "${topology}" node-0:/tmp/topology.xml > /dev/null
    if ! docker exec node-0 clusterctl load-topology /tmp/topology.xml > "${LOG_DIR}/load-topology.log" 2>&1; then
        echo "load-topology failed; see ${LOG_DIR}/load-topology.log"
        exit 1
    fi
}

# faulteron <member> <command>... — runs the CLI in that member's network namespace.
faulteron() {
    docker run --rm --privileged --network "container:node-$1" faulteron:local faulteron "${@:2}"
}

# heal <member> — prints that member's stats and detaches in one run, so the fault ends as soon as it is read.
heal() {
    docker run --rm --privileged --network "container:node-$1" faulteron:local \
        sh -c 'faulteron stats && faulteron detach'
}

# throttle <member> <cpus> — caps that member's container at <cpus> CPUs, at once, while it runs. This is CFS bandwidth
# control: the node runs until it has spent its share of each 100 ms period, then waits out the rest.
throttle() {
    docker update --cpus "$2" "node-$1" > /dev/null
}

# unthrottle <member> — lifts the cap. --cpus 0 leaves the quota in place, so the cap becomes every CPU the docker host
# has, which no container can exceed anyway.
unthrottle() {
    docker update --cpus "$(docker info -f '{{.NCPU}}')" "node-$1" > /dev/null
}

# counter <name> [type] < stats — that counter (dropped, duplicated, delayed) summed over the rules, or over those
# of one frame type.
counter() {
    awk -v name="$1" -v type="${2:-}" '
        type == "" || $2 == type {
            for (i = 3; i <= NF; i++) { split($i, kv, "="); if (kv[1] == name) n += kv[2] }
        }
        END { print n + 0 }'
}

# Each member's own last leadership line, since a cut-off leader keeps the line that made it leader.
last_leadership() {
    "${COMPOSE[@]}" logs --no-color "node-$1" 2> /dev/null | grep "SequencerService/$1\] leadership change" | tail -1
}

# term_of <member> — the term of that member's last leadership line.
term_of() {
    last_leadership "$1" | grep -oE 'term [0-9]+' | grep -oE '[0-9]+'
}

# await_leader <deadline-secs> [excluded-member] — the member whose own last line says it leads.
await_leader() {
    local budget="$1" exclude="${2:-}" waited=0 m
    while ((waited < budget * 2)); do
        for m in 0 1 2; do
            [[ "${m}" == "${exclude}" ]] && continue
            case "$(last_leadership "${m}")" in
                *"memberId=${m} (isLeader=true)")
                    echo "${m}"
                    return 0
                    ;;
            esac
        done
        sleep 0.5
        waited=$((waited + 1))
    done
    return 1
}

# Leadership lines across every member: one each per term, so an election adds to it.
leadership_changes() {
    local m n=0
    for m in 0 1 2; do
        n=$((n + $("${COMPOSE[@]}" logs --no-color "node-${m}" 2> /dev/null | grep -c "leadership change")))
    done
    echo "${n}"
}

# await_healthy <member> <deadline-secs>
await_healthy() {
    local waited=0
    while ((waited < $2 * 2)); do
        [[ "$(docker inspect -f '{{.State.Health.Status}}' "node-$1" 2> /dev/null)" == healthy ]] && return 0
        sleep 0.5
        waited=$((waited + 1))
    done
    return 1
}

# await_follower <member> <leader> <deadline-secs>
await_follower() {
    local waited=0
    while ((waited < $3 * 2)); do
        case "$(last_leadership "$1")" in
            *"memberId=$2 (isLeader=false)") return 0 ;;
        esac
        sleep 0.5
        waited=$((waited + 1))
    done
    return 1
}

# probe <member> <mode> [-D...] — ClusterProbe co-located with that member; its exit status is the probe's.
probe() {
    docker exec "node-$1" java "${JAVA_OPTS[@]}" \
        -Dprobe.memberId="$1" -Dprobe.aeronDir="/dev/shm/aeron-$1" \
        -Dprobe.ingressEndpoints="${INGRESS_ENDPOINTS}" -Dprobe.egressHost="node-$1" \
        "${@:3}" -cp '/opt/seqeron/lib/*' org.limitless.seqeron.tools.ClusterProbe "$2"
}

# confirm <member> <clientId> [-D...] — CONFIRM_COUNT frames at CONFIRM_PACING_MICROS, judged off its own queue.
confirm() {
    probe "$1" confirm -Dprobe.clientId="$2" -Dprobe.count="${CONFIRM_COUNT:-40000}" \
        -Dprobe.pacingMicros="${CONFIRM_PACING_MICROS:-500}" "${@:3}"
}

# ping_queue <member> — one frame through consensus and back off that member's queue, within ClusterProbe's echo
# timeout. Passes only while that member's queue is current.
ping_queue() {
    probe "$1" ping
}
