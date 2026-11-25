#!/usr/bin/env bash
# Failure injection at a production message rate (doc/plans/low-rate-sizing-plan.md, step 2). Each scenario runs
# on a fresh cluster under the chosen limits, with the order-checked load running through the fault. It reports
# failover time (the fault until the surviving members agree on a leader), rejoin time (the member's restart
# until its queue is current), the elections the fault caused, and whether the load saw every frame once, in order.
#
#   ./faults.sh                      # every scenario
#   SCENARIOS="kill-leader wipe-follower" ./faults.sh
#
# Scenarios: kill-leader, kill-follower, wipe-follower (SIGKILL, delete the member's volume, restart: the
# rejoin copies the log from its peers), throttle-leader (cap the leader at THROTTLE_CPUS for THROTTLE_SECONDS).
# A network partition needs the faulteron injector: use `./gradlew :seqeron-service:networkFaultTest`.
#
# Environment (defaults in brackets): CPUS [2], HEAP [512m], SHM [1g], MEM [3g], THREADING [SHARED], RATE [100],
# DURATION [240] (the load, which must outlast the fault and the rejoin), FAULT_AFTER [30], THROTTLE_CPUS [0.3],
# THROTTLE_SECONDS [20], OUT [build/sizing/faults-<timestamp>].
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

CPUS="${CPUS:-2}"
HEAP="${HEAP:-512m}"
SHM="${SHM:-1g}"
MEM="${MEM:-3g}"
THREADING="${THREADING:-SHARED}"
RATE="${RATE:-100}"
DURATION="${DURATION:-240}"
FAULT_AFTER="${FAULT_AFTER:-30}"
THROTTLE_CPUS="${THROTTLE_CPUS:-0.3}"
THROTTLE_SECONDS="${THROTTLE_SECONDS:-20}"
SCENARIOS="${SCENARIOS:-kill-leader kill-follower wipe-follower throttle-leader}"
OUT="${OUT:-${SIZING_ROOT}/build/sizing/faults-$(date +%Y%m%d-%H%M%S)}"

# wait_current <member> <seconds>: until a frame round-trips through consensus and off the member's queue.
wait_current() {
    local deadline=$(( $(now_ms) + $2 * 1000 ))
    until probe "$1" ping >/dev/null 2>&1; do
        (( $(now_ms) < deadline )) || return 1
        sleep 1
    done
}

# wait_healthy <member> <seconds>
wait_healthy() {
    local deadline=$(( $(now_ms) + $2 * 1000 ))
    until [[ "$(docker inspect -f '{{.State.Health.Status}}' "node-$1" 2>/dev/null)" == healthy ]]; do
        (( $(now_ms) < deadline )) || return 1
        sleep 1
    done
}

# restart <member>: starts the stopped container; prints the ms until its queue is current, or -1.
restart() {
    sleep 12   # a restart within about 10 s of a SIGKILL fails on its active mark file (ops.md)
    local began
    began="$(now_ms)"
    docker start "node-$1" >/dev/null
    if wait_healthy "$1" 180 && wait_current "$1" 600; then echo $(( $(now_ms) - began )); else echo -1; fi
}

# run_scenario <name>
run_scenario() {
    local name="$1" dir="${OUT}/$1" victim leader fault failover_ms=-1 rejoin_ms=-1 before after ok=0 volume
    mkdir -p "${dir}"
    echo "== ${name}"
    cluster_down
    sizing_env "${CPUS}" "${HEAP}" "${MEM}" "${SHM}" "${THREADING}"
    cluster_up

    # The load runs on the driver of a member the fault spares, since it must outlive the fault.
    leader="$(wait_leader -1 30)"
    case "${name}" in
        kill-leader|throttle-leader) victim="${leader}" ;;
        *) victim=$(( (leader + 1) % MEMBERS )) ;;
    esac
    local loader=$(( (victim + 1) % MEMBERS ))
    echo "leader ${leader}, fault on member ${victim}, load on member ${loader}"
    probe "${loader}" confirm "probe.count=$(( RATE * DURATION ))" "probe.pacingMicros=$(( 1000000 / RATE ))" \
        "probe.fillerBytes=${FILLER_BYTES:-100}" >"${dir}/load.log" 2>&1 &
    local load=$!
    sleep "${FAULT_AFTER}"
    before="$(leadership_changes)"

    fault="$(now_ms)"
    case "${name}" in
        kill-leader|kill-follower)
            docker kill -s KILL "node-${victim}" >/dev/null
            if (( victim == leader )); then
                wait_leader "${victim}" 60 >/dev/null && failover_ms=$(( $(now_ms) - fault ))
            else
                failover_ms=0
            fi
            rejoin_ms="$(restart "${victim}")"
            ;;
        wipe-follower)
            volume="seqeron-failover_node-${victim}-data"
            docker rm -f "node-${victim}" >/dev/null
            docker volume rm "${volume}" >/dev/null
            failover_ms=0
            fault="$(now_ms)"
            "${COMPOSE[@]}" up -d "node-${victim}" >/dev/null
            wait_healthy "${victim}" 180 && wait_current "${victim}" 900 && rejoin_ms=$(( $(now_ms) - fault ))
            ;;
        throttle-leader)
            docker update --cpus "${THROTTLE_CPUS}" "node-${victim}" >/dev/null
            sleep "${THROTTLE_SECONDS}"
            docker update --cpus "${CPUS}" "node-${victim}" >/dev/null
            sleep 20
            failover_ms=0
            ;;
    esac
    sleep 10
    after="$(leadership_changes)"

    wait "${load}" || ok=1
    # A restarted member's counters start again from zero, so the sum can fall; the figure is a lower bound there.
    echo "${name},${victim},${failover_ms},${rejoin_ms},$(( after - before )),$(( 1 - ok ))" | tee -a "${OUT}/summary.csv"
    for (( m = 0; m < MEMBERS; m++ )); do docker logs "node-${m}" >"${dir}/node-${m}.log" 2>&1 || true; done
    cluster_down
}

mkdir -p "${OUT}"
trap cluster_down EXIT
echo "scenario,member,failover_ms,rejoin_ms,elections,order_ok" >"${OUT}/summary.csv"
for scenario in ${SCENARIOS}; do
    run_scenario "${scenario}"
done
echo "results: ${OUT}/summary.csv"
