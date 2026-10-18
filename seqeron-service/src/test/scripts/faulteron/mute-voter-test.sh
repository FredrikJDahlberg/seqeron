#!/usr/bin/env bash
# Mutes one follower's ballots, kills the leader, and heals the follower after HOLD_SECS without a leader.
#
# THE FAULT: on the follower's interface, every RequestVote and Vote it sends to a peer's consensus port is dropped.
# The rest of its consensus traffic flows, so it replicates, acknowledges and canvasses like any follower, and
# nothing shows until an election: then the two survivors are up and connected, but the muted one cannot ask for a
# vote or give one, and neither reaches a quorum of two. Only a filter on the message inside the frame can produce
# this; a blocked port takes the follower out of the cluster before the leader dies.
#
# WHAT IT ASSERTS:
#   1. The fault is latent: under it, before the kill, no election runs and every member's queue answers a ping.
#   2. With the leader dead, no leader emerges in HOLD_SECS, several times a normal election.
#   3. The fault fired: ballots were dropped.
#   4. Healed, the survivors elect a leader, and the killed member, restarted, rejoins as its follower.
#   5. A ClusterProbe confirm producer on the muted follower, streaming throughout, sees every frame on its queue
#      exactly once, in order.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/mute-voter"

# The whole leaderless stretch, hold and heal, must stay under ClusterStreamSender's 5 s new-leader timeout: past it,
# the producer's session closes and nothing reopens it.
HOLD_SECS="${HOLD_SECS:-3}"
# A SIGKILLed member leaves its mark files active; a restart waits out their 10 s liveness timeout.
MARK_FILE_SETTLE_SECS=12
CONFIRM_COUNT="${CONFIRM_COUNT:-60000}"

start_cluster
OLD="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
M=$(((OLD + 1) % NODE_COUNT))
B=$(((OLD + 2) % NODE_COUNT))
echo "leader = member ${OLD}; muting follower ${M}"

confirm "${M}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on member ${M}"

# ── fault: the peers' consensus ports only, so ballots reaching the muted member still arrive ──────────────
PEER_PORTS=("$(consensus_port_of "${OLD}")" "$(consensus_port_of "${B}")")
if ! faulteron "${M}" attach || ! faulteron "${M}" block --template request-vote "${PEER_PORTS[@]}" \
    || ! faulteron "${M}" block --template vote "${PEER_PORTS[@]}"; then
    echo "faulteron could not mute node-${M}"
    exit 1
fi
echo "node-${M}: dropping RequestVote and Vote to ports ${PEER_PORTS[*]}"
CHANGES_BEFORE="$(leadership_changes)"
sleep 2

PINGS=""
for m in 0 1 2; do
    ping_queue "${m}" >> "${LOG_DIR}/ping-latent.log" 2>&1 && PINGS+="${m} "
done
CHANGES_LATENT="$(leadership_changes)"
echo "queues answering a ping under the latent fault: ${PINGS:-none}"

# ── kill the leader ────────────────────────────────────────────────────────────────────────────────
TERM_BEFORE="$(term_of "${M}")"
docker kill -s KILL "node-${OLD}" > /dev/null
KILLED_AT=${SECONDS}
echo "SIGKILLed node-${OLD} (leader)"

LEADERLESS=1
EARLY="$(await_leader "${HOLD_SECS}" "${OLD}")" && LEADERLESS=0
if ((LEADERLESS == 1)); then
    echo "no leader after ${HOLD_SECS}s"
else
    echo "member ${EARLY} was elected with the fault in place"
fi

heal "${M}" > "${LOG_DIR}/rules.txt"
HEALED_AT=${SECONDS}
echo "healed node-${M}"

NEW="$(await_leader "$(deadline 30)" "${OLD}")"
[[ -n "${NEW}" ]] && echo "new leader = member ${NEW}, $((SECONDS - HEALED_AT))s after the heal" \
    || echo "no leader emerged after the heal"

# ── restore the killed member ──────────────────────────────────────────────────────────────────────
SETTLE=$((MARK_FILE_SETTLE_SECS - (SECONDS - KILLED_AT)))
((SETTLE > 0)) && sleep "${SETTLE}"
docker start "node-${OLD}" > /dev/null
REJOINED=0
await_healthy "${OLD}" "$(deadline 120)" && [[ -n "${NEW}" ]] && await_follower "${OLD}" "${NEW}" "$(deadline 60)" \
    && REJOINED=1
echo "node-${OLD} rejoined as a follower: ${REJOINED}"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?
DROPPED="$(counter dropped < "${LOG_DIR}/rules.txt")"

echo ""
echo "=== RESULT ==="
echo "rules on node-${M}:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "leadership lines, latent    : ${CHANGES_BEFORE} / ${CHANGES_LATENT}"
echo "queues answering, latent    : ${PINGS:-none}"
echo "leaderless for ${HOLD_SECS}s          : ${LEADERLESS}"
echo "new leader / old rejoined   : ${NEW:-none} / ${REJOINED}"
echo "term before / after         : ${TERM_BEFORE} / $([[ -n "${NEW}" ]] && term_of "${NEW}")"
echo "PendingSends producer       : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"

PASS=1
((CHANGES_LATENT == CHANGES_BEFORE)) && [[ "${PINGS}" == "0 1 2 " ]] || {
    echo "FAIL: the fault showed before the leader died"
    PASS=0
}
((LEADERLESS == 1)) || {
    echo "FAIL: a leader was elected while the follower was muted"
    PASS=0
}
((DROPPED > 0)) || {
    echo "FAIL: no ballot was dropped"
    PASS=0
}
[[ -n "${NEW}" ]] || {
    echo "FAIL: no leader after the heal"
    PASS=0
}
((REJOINED == 1)) || {
    echo "FAIL: node-${OLD} did not rejoin as a follower of member ${NEW:-?}"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "MUTE VOTER TEST: PASS"
    exit 0
fi
echo "MUTE VOTER TEST: FAIL"
exit 1
