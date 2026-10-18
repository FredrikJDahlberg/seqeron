#!/usr/bin/env bash
# Starves one follower of log repair while the cluster streams, then heals it.
#
# THE FAULT: LOSS_PCT% of the log DATA sent to the follower's log port is dropped, and so is one half of Aeron's
# repair, chosen by REPAIR:
#   nak         (default) every NAK the follower sends from that port, dropped on the follower's interface;
#   retransmit  every retransmission the leader sends to that port, dropped on the leader's interface.
# Aeron repairs loss only by a NAK answered with a retransmit, so the first lost datagram leaves a hole the
# follower's log image cannot get past: it stops applying the log while still hearing the leader on the consensus
# channel, so no election is called. A process kill or pause never produces a member that is up, connected and
# silently behind.
#
# WHAT IT ASSERTS:
#   1. The fault fired: log DATA and the chosen repair half were dropped.
#   2. The follower fell behind: a ping through its queue, which passed before the fault, times out under it.
#   3. The rest of the cluster kept sequencing: a ClusterProbe confirm producer on the other follower, streaming
#      throughout, sees every frame on its queue exactly once, in order.
#   4. Healed, the follower catches up: a ping through its queue passes again.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/unrepaired-follower"

REPAIR="${REPAIR:-nak}"
LOSS_PCT="${LOSS_PCT:-5}"
FAULT_SECS="${FAULT_SECS:-20}"
CONFIRM_COUNT="${CONFIRM_COUNT:-80000}"

start_cluster
LEADER="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
F=$(((LEADER + 1) % NODE_COUNT))
O=$(((LEADER + 2) % NODE_COUNT))
case "${REPAIR}" in
    nak) T="${F}" ;;
    retransmit) T="${LEADER}" ;;
    *)
        echo "REPAIR is nak or retransmit: ${REPAIR}"
        exit 1
        ;;
esac
echo "leader = member ${LEADER}; starving follower ${F}, producing on follower ${O}"

confirm "${O}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on member ${O}"

BEFORE=0
ping_queue "${F}" > "${LOG_DIR}/ping-before.log" 2>&1 && BEFORE=1
echo "ping through node-${F}'s queue before the fault: ${BEFORE}"

# ── fault ──────────────────────────────────────────────────────────────────────────────────────────
LOG_PORT="$(log_port_of "${F}")"
if ! faulteron "${T}" attach || ! faulteron "${T}" rule --type data --loss "${LOSS_PCT}" "${LOG_PORT}" \
    || ! faulteron "${T}" block --type "${REPAIR}" "${LOG_PORT}"; then
    echo "faulteron could not starve node-${F}"
    exit 1
fi
echo "node-${T}: dropping ${LOSS_PCT}% of log DATA and every ${REPAIR} on port ${LOG_PORT}"
FAULT_START=${SECONDS}
sleep 3

DURING=0
ping_queue "${F}" > "${LOG_DIR}/ping-during.log" 2>&1 && DURING=1
echo "ping through node-${F}'s queue under the fault: ${DURING}"

REMAINING=$((FAULT_SECS - (SECONDS - FAULT_START)))
((REMAINING > 0)) && sleep "${REMAINING}"
faulteron "${T}" stats > "${LOG_DIR}/rules.txt"
faulteron "${T}" detach
HEALED=${SECONDS}
echo "healed node-${T} after $((HEALED - FAULT_START))s"

# A heal takes the retransmit of everything the hole held back, so the first ping may not be the one that passes.
AFTER=0
for ((attempt = 1; attempt <= 5; attempt++)); do
    if ping_queue "${F}" >> "${LOG_DIR}/ping-after.log" 2>&1; then
        AFTER=1
        break
    fi
done
echo "ping through node-${F}'s queue after the heal: ${AFTER} (attempt ${attempt}, $((SECONDS - HEALED))s after the heal)"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?
FINAL="$(await_leader 5)"

echo ""
echo "=== RESULT ==="
echo "rules on node-${T}:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "ping before / during / after : ${BEFORE} / ${DURING} / ${AFTER}"
echo "leader before / after        : ${LEADER} / ${FINAL:-none}"
echo "PendingSends producer        : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"

PASS=1
for type in data "${REPAIR}"; do
    (($(counter dropped "${type}" < "${LOG_DIR}/rules.txt") > 0)) || {
        echo "FAIL: no ${type} packet was dropped"
        PASS=0
    }
done
((BEFORE == 1)) || {
    echo "FAIL: node-${F}'s queue did not answer a ping before the fault"
    PASS=0
}
((DURING == 0)) || {
    echo "FAIL: node-${F}'s queue stayed current under the fault"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}
((AFTER == 1)) || {
    echo "FAIL: node-${F} did not catch up after the heal"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "UNREPAIRED FOLLOWER TEST (${REPAIR}): PASS"
    exit 0
fi
echo "UNREPAIRED FOLLOWER TEST (${REPAIR}): FAIL"
exit 1
