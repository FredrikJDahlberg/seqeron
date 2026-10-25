#!/usr/bin/env bash
# Impairs the link between a gateway host and the member its relay reads, chosen by FAULT:
#   loss    (default) LOSS_PCT% of the DATA between them and every NAK, dropped on the host's interface. Unrepaired,
#           the first lost datagram leaves a hole the relay's replay cannot get past, and the 1 Hz heartbeat makes the
#           silence behind it abnormal: the relay must leave the member and resume on the next at the same position.
#   jitter  the member's packets to the host held 20 ms plus up to 30 ms, which reorders them, and 10% sent twice.
#           Aeron's image restores the order and drops the duplicates, so the relay must stay on the member.
#
# The gateway host is node-3, its relay copying group 1's queue and trying a follower F1, then the other follower F2,
# then the leader. A producer on the host submits to and hears from the leader alone, so a rule aimed at F1 touches
# only the relay's traffic.
#
# WHAT IT ASSERTS:
#   1. The fault fired: loss, DATA and NAKs were dropped; jitter, packets were delayed and duplicated.
#   2. The relay's sources: loss, it left F1 and followed F2 next; jitter, it followed F1 throughout.
#   3. A ClusterProbe confirm producer on the host, streaming throughout, sees every frame on the host's queue exactly
#      once, in order.
#   4. After the heal, a fresh ClusterProbe follow on the host cold-starts through the host's Replayer off the relayed
#      recording, and catches up.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
FAULT="${FAULT:-loss}"
LOG_DIR="${REPO_ROOT}/logs/faulteron/relay-link-${FAULT}"

LOSS_PCT="${LOSS_PCT:-5}"
FAULT_SECS="${FAULT_SECS:-10}"

case "${FAULT}" in
    loss | jitter) ;;
    *)
        echo "FAULT is loss or jitter: ${FAULT}"
        exit 1
        ;;
esac

start_cluster
LEADER="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
F1=$(((LEADER + 1) % NODE_COUNT))
F2=$(((LEADER + 2) % NODE_COUNT))
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
if [[ "${FAULT}" == loss ]]; then
    T="${HOST_ID}"
    faulteron "${T}" attach && faulteron "${T}" rule --peer "node-${F1}" --type data --loss "${LOSS_PCT}" &&
        faulteron "${T}" block --peer "node-${F1}" --type nak
else
    T="${F1}"
    faulteron "${T}" attach &&
        faulteron "${T}" rule --peer "node-${HOST_ID}" --delay 20 --jitter 30 --duplicate 10
fi || {
    echo "faulteron could not impair the relay's link"
    exit 1
}
echo "node-${T}: ${FAULT} on the link between node-${F1} and the gateway host"
sleep "${FAULT_SECS}"
faulteron "${T}" stats > "${LOG_DIR}/rules.txt"
faulteron "${T}" detach
echo "healed node-${T} after ${FAULT_SECS}s"

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
FOLLOWED=$(wc -l < "${LOG_DIR}/sources.txt")

echo ""
echo "=== RESULT ==="
echo "rules on node-${T}:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "relay followed               : $(tr '\n' ' ' < "${LOG_DIR}/sources.txt")"
echo "relay left                   :"
sed 's/^/  /' "${LOG_DIR}/departures.txt"
echo "PendingSends producer        : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"
echo "fresh follower caught up     : ${CAUGHT}"

PASS=1
if [[ "${FAULT}" == loss ]]; then
    for type in data nak; do
        (($(counter dropped "${type}" < "${LOG_DIR}/rules.txt") > 0)) || {
            echo "FAIL: no ${type} packet was dropped"
            PASS=0
        }
    done
    # The reason is not asserted: the loss also holes F1's archive control stream, whose failure may come first.
    [[ "${FIRST}" == "$(archive_endpoint_of "${F1}")" && "${SECOND}" == "$(archive_endpoint_of "${F2}")" ]] || {
        echo "FAIL: the relay did not move from node-${F1} to node-${F2}"
        PASS=0
    }
else
    for name in delayed duplicated; do
        (($(counter "${name}" < "${LOG_DIR}/rules.txt") > 0)) || {
            echo "FAIL: no packet was ${name}"
            PASS=0
        }
    done
    ((FOLLOWED == 1)) && [[ "${FIRST}" == "$(archive_endpoint_of "${F1}")" ]] || {
        echo "FAIL: the relay did not stay on node-${F1}"
        PASS=0
    }
fi
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
    echo "RELAY LINK TEST (${FAULT}): PASS"
    exit 0
fi
echo "RELAY LINK TEST (${FAULT}): FAIL"
exit 1
