#!/usr/bin/env bash
# Shared by the low-rate sizing scripts (doc/plans/low-rate-sizing-plan.md): the compose cluster with
# resource limits, the probe and the counters, and one cgroup sample per member.
#
# Every JVM that is not the member runs in a throwaway container sharing the member's /dev/shm
# (--ipc container:node-N), so its CPU and heap are not charged to the member's limit.
#
# Prerequisites: ./gradlew operatorDist, and docker with cgroup v2 (the member's cgroup is read from inside).

SIZING_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
IMAGE="${IMAGE:-seqeron/node:local}"
NET="seqeron-failover_seqnet"
MEMBERS=3
TOPOLOGY="${SIZING_ROOT}/seqeron-service/src/test/resources/topology-test-groups.xml"

COMPOSE=(docker compose -f "${SIZING_ROOT}/docker/compose.yml" -f "${SIZING_ROOT}/docker/compose.sizing.yml")

now_ms() {
    if [[ "$(date +%N)" =~ ^[0-9]+$ ]]; then
        echo $(( $(date +%s%N) / 1000000 ))
    else
        python3 -c 'import time; print(int(time.time() * 1000))'
    fi
}

# sizing_env <cpus> <heap> <mem> <shm> <threading>: the limits the next `up` applies.
sizing_env() {
    export SIZING_CPUS="$1" SIZING_HEAP="$2" SIZING_MEM="$3" SIZING_SHM="$4" SIZING_THREADING="$5"
    export SIZING_JAVA_TOOL_OPTIONS="-Xms$2 -Xmx$2 -Daeron.threading.mode=$5 -Daeron.archive.threading.mode=$5"
}

# cluster_up: starts the three members, waits for a leader and loads the topology, so the group is active.
cluster_up() {
    "${COMPOSE[@]}" up -d --build >/dev/null
    local m deadline=$(( $(now_ms) + 120000 ))
    for (( m = 0; m < MEMBERS; m++ )); do
        until [[ "$(docker inspect -f '{{.State.Health.Status}}' "node-${m}" 2>/dev/null)" == healthy ]]; do
            (( $(now_ms) < deadline )) || { echo "node-${m} not healthy" >&2; return 1; }
            sleep 1
        done
    done
    wait_leader -1 60 >/dev/null || return 1
    docker cp "${TOPOLOGY}" node-0:/tmp/topology.xml
    docker exec node-0 clusterctl load-topology /tmp/topology.xml >/dev/null
}

cluster_down() { "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true; }

# in_member <member> <command...>: runs in a sidecar container beside the member, as the member.
in_member() {
    local member="$1"; shift
    docker run --rm --network "${NET}" --ipc "container:node-${member}" -e "MEMBER_ID=${member}" \
        --entrypoint "$1" "${IMAGE}" "${@:2}"
}

# counter <member> <name>: a seqeron counter of the member, or -1 if its sequencer does not answer.
counter() {
    local value
    value="$(in_member "$1" clusterctl counters 2>/dev/null | sed -n "s/^$2 member=[0-9]* *= //p")"
    echo "${value:--1}"
}

# leader <excluded>: the member every other member records as leader, or -1.
leader() {
    local excluded="$1" agreed=-2 m view
    for (( m = 0; m < MEMBERS; m++ )); do
        (( m == excluded )) && continue
        view="$(counter "${m}" seqeron.sequencer.currentLeaderMemberId)"
        if (( view < 0 || view >= MEMBERS || view == excluded )); then echo -1; return; fi
        if (( agreed != -2 && view != agreed )); then echo -1; return; fi
        agreed="${view}"
    done
    echo "${agreed}"
}

# wait_leader <excluded> <seconds>: prints the leader once the members agree on one.
wait_leader() {
    local deadline=$(( $(now_ms) + $2 * 1000 )) l
    while (( $(now_ms) < deadline )); do
        l="$(leader "$1")"
        if (( l >= 0 )); then echo "${l}"; return 0; fi
        sleep 0.5
    done
    return 1
}

# leadership_changes: the members' applied leadership changes, summed; an election adds to it.
leadership_changes() {
    local total=0 m v
    for (( m = 0; m < MEMBERS; m++ )); do
        v="$(counter "${m}" seqeron.sequencer.leadershipChangeCount)"
        (( v > 0 )) && total=$(( total + v ))
    done
    echo "${total}"
}

# probe <member> <mode> [key=value ...]: a ClusterProbe on the member's driver, its own JVM small.
probe() {
    local member="$1" mode="$2"; shift 2
    local props=()
    for p in "$@"; do props+=("-D${p}"); done
    in_member "${member}" bash -c '
        source /opt/seqeron/docker/node-env.sh
        exec java -Xms16m -Xmx64m \
            --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
            --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
            --add-opens=java.base/java.util.zip=ALL-UNNAMED \
            -Dprobe.memberId="${MEMBER_ID}" -Dprobe.aeronDir="${AERON_DIR}" -Dprobe.egressHost="node-${MEMBER_ID}" \
            "${@:2}" -cp "/opt/seqeron/lib/*" org.limitless.seqeron.tools.ClusterProbe "$1"' _ "${mode}" ${props[@]+"${props[@]}"}
}

# cgroup <member> <file>: a cgroup v2 file of the member's container.
cgroup() { docker exec "node-$1" cat "/sys/fs/cgroup/$2" 2>/dev/null; }

# stat_field <file contents> <key>: the value on the line that starts with the key.
stat_field() { awk -v k="$2" '$1 == k { print $2 }' <<<"$1"; }

# sample <member>: one CSV row — ms, member, CPU µs used, throttled periods, throttled µs, memory now, memory
# peak, anon, shmem, /dev/shm KiB, data KiB. The anon and shmem columns are the working set; memory now
# includes page cache, which the kernel reclaims.
sample() {
    local m="$1" cpu mem
    cpu="$(cgroup "${m}" cpu.stat)"
    mem="$(cgroup "${m}" memory.stat)"
    echo "$(now_ms),${m},$(stat_field "${cpu}" usage_usec),$(stat_field "${cpu}" nr_throttled),$(stat_field "${cpu}" throttled_usec),$(cgroup "${m}" memory.current),$(cgroup "${m}" memory.peak || echo -1),$(stat_field "${mem}" anon),$(stat_field "${mem}" shmem),$(docker exec "node-${m}" du -sk "/dev/shm/aeron-${m}" 2>/dev/null | cut -f1),$(docker exec "node-${m}" du -sk /var/lib/seqeron 2>/dev/null | cut -f1)"
}
SAMPLE_HEADER="ms,member,cpu_usec,throttled_periods,throttled_usec,mem_current,mem_peak,anon,shmem,shm_kb,data_kb"

# sample_all: a row per running member.
sample_all() {
    local m
    for (( m = 0; m < MEMBERS; m++ )); do
        sample "${m}"
    done
}

# gc_max_ms <member>: the longest GC pause in the member's log since its container started.
gc_max_ms() {
    docker logs "node-$1" 2>&1 | sed -n 's/.*Pause.* \([0-9.]*\)ms$/\1/p' | sort -n | tail -1
}
