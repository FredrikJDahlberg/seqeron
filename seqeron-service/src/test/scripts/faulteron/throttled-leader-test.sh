#!/usr/bin/env bash
# Starves the leader of CPU while the cluster streams, then gives it back.
#
# THE FAULT: the leader's container is capped at CPUS CPUs while it runs (docker update --cpus). Every thread in it,
# from the consensus module to the garbage collector, now runs only until the container has spent its share of
# each 100 ms period and then waits out the rest. The leader is alive and connected but slow: a gray failure, which
# neither a kill nor a network fault produces. It is never silent for longer than one period, so whether it costs an
# election depends on the cluster's leader heartbeat timeout, and either outcome is correct.
#
# WHAT IT ASSERTS:
#   1. The cap is in place.
#   2. Either the cluster replaces the starved leader, or the starved leader keeps leading.
#   3. A ClusterProbe confirm producer on a follower, streaming throughout, sees every frame on its queue exactly once,
#      in order.
#   4. With its CPU back, the old leader's queue catches up: as a follower of the new leader, or as the leader still.
#   5. A ClusterProbe follow cold-started on the starved member at the fault's start, its Replayer answering slower
#      than the client's 500 ms resend, is caught up off its queue's history within 30 s of the producer's end, and
#      not since evicted from it, and the Replayer drops no control reply.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/throttled-leader"

CPUS="${CPUS:-0.05}"
FAULT_SECS="${FAULT_SECS:-20}"
CONFIRM_COUNT="${CONFIRM_COUNT:-60000}"

start_cluster
OLD="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
P=$(((OLD + 1) % NODE_COUNT))
echo "leader = member ${OLD}; producing on follower ${P}"

confirm "${P}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on member ${P}"

# ── fault ──────────────────────────────────────────────────────────────────────────────────────────
throttle "${OLD}" "${CPUS}"
FAULT_START=${SECONDS}
CAP="$(docker exec "node-${OLD}" cat /sys/fs/cgroup/cpu.max)"
echo "node-${OLD} capped at ${CPUS} CPUs (cpu.max ${CAP})"
probe "${OLD}" follow -Dprobe.clientId=12 > "${LOG_DIR}/follow.log" 2>&1 &
FOLLOW_PID=$!

NEW="$(await_leader "$(deadline 30)" "${OLD}")"
STILL_LEADING=0
if [[ -n "${NEW}" ]]; then
    echo "new leader = member ${NEW}, $((SECONDS - FAULT_START))s into the fault"
elif [[ "$(await_leader "$(deadline 5)")" == "${OLD}" ]]; then
    STILL_LEADING=1
    echo "no other member took over: node-${OLD} still leads"
else
    echo "no member leads"
fi
REMAINING=$((FAULT_SECS - (SECONDS - FAULT_START)))
((REMAINING > 0)) && sleep "${REMAINING}"

RUNNING="$(docker inspect -f '{{.State.Running}}' "node-${OLD}")"
[[ "${RUNNING}" == true ]] || echo "node-${OLD} exited: $(docker inspect -f '{{.State.ExitCode}}' "node-${OLD}")"
unthrottle "${OLD}"
HEALED=${SECONDS}
echo "node-${OLD} given its CPU back after $((HEALED - FAULT_START))s"

REJOINED=0
CAUGHT_UP=0
if [[ "${RUNNING}" == true ]] &&
    { [[ -n "${NEW}" ]] && await_follower "${OLD}" "${NEW}" "$(deadline 60)" || ((STILL_LEADING == 1)); }; then
    REJOINED=1
    for ((attempt = 1; attempt <= 5; attempt++)); do
        if ping_queue "${OLD}" >> "${LOG_DIR}/ping-after.log" 2>&1; then
            CAUGHT_UP=1
            break
        fi
    done
fi
echo "node-${OLD} rejoined / caught up: ${REJOINED} / ${CAUGHT_UP} ($((SECONDS - HEALED))s after)"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?

# Whether the follow's last word is that it is caught up rather than evicted from its queue.
follow_caught_up() {
    case "$(grep -E 'following live|re-converged|evicted from the queue' "${LOG_DIR}/follow.log" | tail -1)" in
        *"following live"* | *re-converged*) return 0 ;;
    esac
    return 1
}

FOLLOWED=0
FOLLOW_END=$((SECONDS + $(deadline 30)))
until follow_caught_up || ((SECONDS >= FOLLOW_END)); do sleep 0.5; done
follow_caught_up && FOLLOWED=1
kill "${FOLLOW_PID}" 2> /dev/null
DROPPED="$(docker exec "node-${OLD}" clusterctl counters 2> /dev/null |
    grep controlRepliesDroppedCount | grep -oE '[0-9]+ *$' | tr -d ' ')"

echo ""
echo "=== RESULT ==="
echo "cap                         : ${CPUS} CPUs, cpu.max ${CAP}"
echo "new leader                  : ${NEW:-none}$( ((STILL_LEADING == 1)) && echo " (node-${OLD} still leads)")"
echo "old leader running          : ${RUNNING}"
echo "old leader rejoined / queue : ${REJOINED} / ${CAUGHT_UP}"
echo "PendingSends producer       : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"
echo "follow on the old leader    : caught up ${FOLLOWED}, control replies dropped ${DROPPED:-unread}"

PASS=1
[[ "${CAP}" == "$(awk -v c="${CPUS}" 'BEGIN { printf "%d", c * 100000 }') 100000" ]] || {
    echo "FAIL: the cap was not applied"
    PASS=0
}
[[ -n "${NEW}" ]] || ((STILL_LEADING == 1)) || {
    echo "FAIL: no member leads"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}
((REJOINED == 1 && CAUGHT_UP == 1)) || {
    echo "FAIL: node-${OLD} did not rejoin, or its queue did not catch up"
    PASS=0
}
((FOLLOWED == 1)) || {
    echo "FAIL: the follow cold-started on node-${OLD} did not catch up; see ${LOG_DIR}/follow.log"
    PASS=0
}
[[ "${DROPPED}" == 0 ]] || {
    echo "FAIL: node-${OLD}'s Replayer dropped control replies: ${DROPPED:-unread}"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "THROTTLED LEADER TEST: PASS"
    exit 0
fi
echo "THROTTLED LEADER TEST: FAIL"
exit 1
