#!/usr/bin/env bash
# Slows a member archive's control messages to a gateway host's relay while the relay connects to it.
#
# THE FAULT: every archive control message (schema 101) between the host and follower F2 is held DELAY_MS, in the
# direction DIRECTION names:
#   request   (default) the relay's requests, delayed on the host's interface;
#   response  the archive's responses, delayed on F2's.
# A relay that is following sends almost no requests, so the host is then cut from the follower it reads, F1, which
# makes the relay connect to F2 under the delay. An archive connect is two exchanges, the connect and then the archive
# id, under one ARCHIVE_MESSAGE_TIMEOUT_NS, 1 s: at DELAY_MS 500 or more the relay must give F2 up and follow the
# leader, and well under it the relay must follow F2, slowly. Each request after the connect has its own 1 s.
#
# The relay tries F1, then F2, then the leader. A producer on the host submits to and hears from the leader alone, so
# rules aimed at the followers touch only the relay's traffic.
#
# WHAT IT ASSERTS:
#   1. The fault fired: the host dropped F1's packets and the rule delayed control messages.
#   2. The relay left F1 and followed F2 under the timeout, or the leader past it; the host logged no FATAL.
#   3. A ClusterProbe confirm producer on the host, streaming throughout, sees every frame on the host's queue exactly
#      once, in order.
#   4. After the heal, a fresh ClusterProbe follow on the host cold-starts through the host's Replayer, and catches up.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
DIRECTION="${DIRECTION:-request}"
DELAY_MS="${DELAY_MS:-1500}"
LOG_DIR="${REPO_ROOT}/logs/faulteron/slow-archive-control-${DIRECTION}-${DELAY_MS}"

FAULT_SECS="${FAULT_SECS:-10}"
ARCHIVE_MESSAGE_TIMEOUT_MS=1000
CONNECT_EXCHANGES=2

start_cluster
LEADER="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
F1=$(((LEADER + 1) % NODE_COUNT))
F2=$(((LEADER + 2) % NODE_COUNT))
case "${DIRECTION}" in
    request) D="${HOST_ID}" PEER="${F2}" ;;
    response) D="${F2}" PEER="${HOST_ID}" ;;
    *)
        echo "DIRECTION is request or response: ${DIRECTION}"
        exit 1
        ;;
esac
start_gateway_host "${F1}" "${F2}" "${LEADER}"
echo "leader = member ${LEADER}; gateway host relaying from follower ${F1}, then ${F2}, then ${LEADER}"

confirm "${HOST_ID}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on the gateway host"
sleep 1

# ── fault ──────────────────────────────────────────────────────────────────────────────────────────
if ! faulteron "${HOST_ID}" attach || { [[ "${D}" != "${HOST_ID}" ]] && ! faulteron "${D}" attach; } ||
    ! faulteron "${D}" rule --peer "node-${PEER}" --schema 101 --delay "${DELAY_MS}" ||
    ! faulteron "${HOST_ID}" block --peer "node-${F1}"; then
    echo "faulteron could not slow node-${F2}'s archive control"
    exit 1
fi
echo "node-${D}: archive control ${DIRECTION}s to and from node-${PEER} held ${DELAY_MS} ms; host cut from node-${F1}"
sleep "${FAULT_SECS}"
faulteron "${HOST_ID}" stats > "${LOG_DIR}/rules.txt"
[[ "${D}" != "${HOST_ID}" ]] && faulteron "${D}" stats >> "${LOG_DIR}/rules.txt"
faulteron "${HOST_ID}" detach
[[ "${D}" != "${HOST_ID}" ]] && faulteron "${D}" detach
echo "healed after ${FAULT_SECS}s"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?

probe "${HOST_ID}" follow -Dprobe.clientId=9 > "${LOG_DIR}/fresh-follower.log" 2>&1 &
FRESH_PID=$!
CAUGHT=0
wait_for_log "${LOG_DIR}/fresh-follower.log" "following live" "$(deadline 30)" && CAUGHT=1
kill "${FRESH_PID}" 2> /dev/null

relay_sources > "${LOG_DIR}/sources.txt"
relay_departures > "${LOG_DIR}/departures.txt"
FIRST="$(sed -n 1p "${LOG_DIR}/sources.txt")"
SECOND="$(sed -n 2p "${LOG_DIR}/sources.txt")"
if ((DELAY_MS * CONNECT_EXCHANGES >= ARCHIVE_MESSAGE_TIMEOUT_MS)); then EXPECTED="${LEADER}"; else EXPECTED="${F2}"; fi

echo ""
echo "=== RESULT ==="
echo "rules:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "relay followed               : $(tr '\n' ' ' < "${LOG_DIR}/sources.txt")"
echo "relay left                   :"
sed 's/^/  /' "${LOG_DIR}/departures.txt"
echo "PendingSends producer        : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"
echo "fresh follower caught up     : ${CAUGHT}"

PASS=1
(($(counter dropped < "${LOG_DIR}/rules.txt") > 0)) || {
    echo "FAIL: the host dropped nothing from node-${F1}"
    PASS=0
}
(($(counter delayed < "${LOG_DIR}/rules.txt") > 0)) || {
    echo "FAIL: no archive control message was delayed"
    PASS=0
}
[[ "${FIRST}" == "$(archive_endpoint_of "${F1}")" && "${SECOND}" == "$(archive_endpoint_of "${EXPECTED}")" ]] || {
    echo "FAIL: the relay did not move from node-${F1} to node-${EXPECTED}"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}
((CAUGHT == 1)) || {
    echo "FAIL: a fresh follower on the gateway host did not catch up"
    PASS=0
}
if docker logs "node-${HOST_ID}" 2>&1 | grep -q FATAL; then
    echo "FAIL: the gateway host logged a FATAL"
    PASS=0
fi

echo ""
if ((PASS == 1)); then
    echo "SLOW ARCHIVE CONTROL TEST (${DIRECTION}, ${DELAY_MS} ms): PASS"
    exit 0
fi
echo "SLOW ARCHIVE CONTROL TEST (${DIRECTION}, ${DELAY_MS} ms): FAIL"
exit 1
