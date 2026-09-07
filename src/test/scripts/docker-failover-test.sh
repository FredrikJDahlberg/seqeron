#!/usr/bin/env bash
# Multi-round containerized failover soak — the docker/compose.yml port of failover-test.sh, run
# long enough and under enough load to be worth believing.
#
# WHY IT IS NOT ONE KILL: a single failover against a heartbeat-only archive proves the replay path
# connects, and nothing else. The frames it replays are the sequencer's own 1 Hz ClusterHeartbeat —
# no application payload ever reaches the log, and one leader tenure never shows whether the second
# one is as good as the first. So this runs ROUNDS failovers, each under continuous ProbeMarker
# load, and restores the killed member between them so every round starts from a full three again.
#
# WHAT IT ASSERTS, in the order they matter:
#   1. Every round is a genuine leadership change — a new member leads, and the killed one rejoins.
#   2. A long-lived observer replica on each surviving node keeps delivering IN ORDER across every
#      failover. ReplayerStreamReceiver enforces gap-freedom: a hole triggers a healing re-walk and
#      logs "re-converged", so the observers' delivered counts and re-convergences are the
#      continuity evidence. This is the assertion a single-kill test never made.
#   3. A cold-start probe, started only at the end, replays the WHOLE history — every tenure — off
#      the final leader's single continuous recording, and reaches "following live". Because load
#      has been running throughout, that history is application payload, not just heartbeats.
#
# Every node records its own node-local tap continuously — the tap publication is never re-created
# on a leadership change — so each node's recording is one run spanning every tenure it lived
# through, which is what makes assertion 3 possible on a member that was a follower for most of them.
#
# Prerequisite: ./gradlew uberJar (docker/Dockerfile copies the jar, it does not build it).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
COMPOSE=(docker compose -f "${REPO_ROOT}/docker/compose.yml")

JAR="${REPO_ROOT}/build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="${REPO_ROOT}/logs/docker-failover"
NODE_COUNT=3
OBSERVER_CLIENT_ID=1
COLD_CLIENT_ID=9

ROUNDS="${ROUNDS:-3}"                       # failovers to perform
SOAK_SECS="${SOAK_SECS:-20}"                # load time per round before the kill
SUBMIT_COUNT="${SUBMIT_COUNT:-5000}"        # ProbeMarkers per submit invocation
SUBMIT_PACING_MICROS="${SUBMIT_PACING_MICROS:-1000}"   # 5000 @ 1ms = ~5s of load per invocation
MIN_DELIVERED="${MIN_DELIVERED:-1000}"      # per-observer in-order frames required
# A SIGKILLed member never unwinds, so its archive and consensus mark files are left "active" and a
# restart fails with "active mark file detected". Wait out the mark-file liveness timeout
# (driverTimeoutMs, 10s) first — chaos-runner.sh calls this SIGKILL_MARKFILE_SETTLE_SECS and uses
# the same 12. Killing the container does not change this: the mark file is in the member's volume,
# and its liveness is judged by a timestamp the kill froze.
SIGKILL_MARKFILE_SETTLE_SECS="${SIGKILL_MARKFILE_SETTLE_SECS:-12}"

# CI runners are slower and noisier than a dev box; every DEADLINE is a multiple of this. Fixed
# settle/soak sleeps are deliberately NOT scaled — the heartbeat is 1 Hz wall clock and the load is
# paced in wall-clock microseconds, so a slow runner accrues the same history in the same seconds.
TIMEOUT_SCALE="${TIMEOUT_SCALE:-1}"
deadline() { echo $(( $1 * TIMEOUT_SCALE )); }

# Locally the image build is part of the run. CI builds it beforehand through buildx so the layer
# cache survives between runs, and sets this to 0 — compose then finds the tag already loaded
# (pull_policy: never, so it never reaches for a registry).
COMPOSE_BUILD="${COMPOSE_BUILD:-1}"

INGRESS_ENDPOINTS="$(ingress_endpoints_string "${NODE_COUNT}" 'node-{id}')"
JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

[[ -f "${JAR}" ]] || { echo "missing ${JAR} — run ./gradlew uberJar"; exit 1; }

rm -rf "${LOG_DIR}"; mkdir -p "${LOG_DIR}"
LOAD_FLAG="${LOG_DIR}/.load-running"

cleanup() {
    rm -f "${LOAD_FLAG}"
    for m in 0 1 2; do "${COMPOSE[@]}" logs --no-color "node-${m}" > "${LOG_DIR}/node-${m}.log" 2>&1; done
    "${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
}
trap cleanup EXIT

# ── cluster ───────────────────────────────────────────────────────────────────────────────────────
"${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
echo "starting ${NODE_COUNT}-node cluster"
# Never a possibly-empty array: macOS ships bash 3.2, where expanding one under `set -u` aborts.
UP_ARGS=(up -d --wait --wait-timeout "$(deadline 120)")
[[ "${COMPOSE_BUILD}" == "1" ]] && UP_ARGS=(up --build -d --wait --wait-timeout "$(deadline 120)")
if ! "${COMPOSE[@]}" "${UP_ARGS[@]}" > "${LOG_DIR}/compose-up.log" 2>&1; then
    echo "cluster did not come up healthy; see ${LOG_DIR}/compose-up.log"; exit 1
fi
echo "cluster up — all ${NODE_COUNT} nodes healthy"

running_nodes() {
    local m
    for m in 0 1 2; do
        [[ "$(docker inspect -f '{{.State.Running}}' "node-${m}" 2>/dev/null)" == "true" ]] && echo "${m}"
    done
}

# The CURRENT leader, not any member that ever led: docker logs accumulate across container
# restarts, so only each node's LAST leadership line describes the cluster as it stands now.
current_leader() {
    local m last
    for m in $(running_nodes); do
        last="$("${COMPOSE[@]}" logs --no-color "node-${m}" 2>/dev/null \
                | grep "SequencerService/${m}\] leadership change" | tail -1)"
        case "${last}" in
            *"memberId=${m} (isLeader=true)") echo "${m}"; return ;;
        esac
    done
}

await_leader() {   # await_leader <deadline-secs> [excluded-member]
    local budget="$1" exclude="${2:-}" waited=0 leader
    while (( waited < budget * 2 )); do
        leader="$(current_leader)"
        if [[ -n "${leader}" && "${leader}" != "${exclude}" ]]; then echo "${leader}"; return 0; fi
        sleep 0.5; waited=$((waited + 1))
    done
    return 1
}

await_healthy() {  # await_healthy <member> <deadline-secs>
    local m="$1" budget="$2" waited=0
    while (( waited < budget * 2 )); do
        [[ "$(docker inspect -f '{{.State.Health.Status}}' "node-${m}" 2>/dev/null)" == "healthy" ]] && return 0
        sleep 0.5; waited=$((waited + 1))
    done
    return 1
}

# ── observers: one long-lived replica per node, the continuity witnesses ───────────────────────────
start_observer() {
    local m="$1"
    docker exec "node-${m}" java "${JAVA_OPTS[@]}" \
        -Dprobe.memberId="${m}" -Dprobe.aeronDir="/dev/shm/aeron-${m}" \
        -Dprobe.clientId="${OBSERVER_CLIENT_ID}" \
        -cp /opt/phixeron/phixeron.jar org.limitless.phixeron.tools.ClusterProbe follow \
        >> "${LOG_DIR}/observer-${m}.log" 2>&1 &
}

for m in 0 1 2; do start_observer "${m}"; done
echo "observers following on all ${NODE_COUNT} nodes"

# ── load: continuous ProbeMarkers, restarted whenever a failover kills the in-flight client ───────
touch "${LOAD_FLAG}"
(
    while [[ -f "${LOAD_FLAG}" ]]; do
        target="$(running_nodes | head -1)"
        if [[ -z "${target}" ]]; then sleep 1; continue; fi
        docker exec "node-${target}" java "${JAVA_OPTS[@]}" \
            -Dprobe.memberId="${target}" -Dprobe.aeronDir="/dev/shm/aeron-${target}" \
            -Dprobe.ingressEndpoints="${INGRESS_ENDPOINTS}" \
            -Dprobe.egressHost="node-${target}" \
            -Dprobe.count="${SUBMIT_COUNT}" -Dprobe.pacingMicros="${SUBMIT_PACING_MICROS}" \
            -cp /opt/phixeron/phixeron.jar org.limitless.phixeron.tools.ClusterProbe submit \
            >> "${LOG_DIR}/load.log" 2>&1
        sleep 0.5
    done
) &
LOAD_PID=$!
echo "load generator started (${SUBMIT_COUNT} markers @ ${SUBMIT_PACING_MICROS}us per invocation)"

# ── rounds ────────────────────────────────────────────────────────────────────────────────────────
LEADER="$(await_leader "$(deadline 120)")" || { echo "no initial leader emerged"; exit 1; }
echo "initial leader = member ${LEADER}"
FAILOVERS=0
TENURES="${LEADER}"

for (( round = 1; round <= ROUNDS; round++ )); do
    echo ""
    echo "--- round ${round}/${ROUNDS}: soaking ${SOAK_SECS}s under load, leader = member ${LEADER} ---"
    sleep "${SOAK_SECS}"

    KILLED="${LEADER}"
    docker kill -s KILL "node-${KILLED}" > /dev/null
    echo "SIGKILLed node-${KILLED} (leader) -> forcing failover"

    LEADER="$(await_leader "$(deadline 120)" "${KILLED}")" \
        || { echo "round ${round}: no new leader emerged after killing member ${KILLED}"; exit 1; }
    FAILOVERS=$((FAILOVERS + 1))
    TENURES="${TENURES} -> ${LEADER}"
    echo "round ${round}: new leader = member ${LEADER}"

    # Restore the member so the next round starts from a full three. It recovers over its own Raft
    # log (the volume persists) and rebuilds its tap recording by full-log replay — no snapshots.
    # The load keeps running throughout the settle, so it rejoins into a moving cluster.
    sleep "${SIGKILL_MARKFILE_SETTLE_SECS}"
    docker start "node-${KILLED}" > /dev/null
    await_healthy "${KILLED}" "$(deadline 120)" \
        || { echo "round ${round}: member ${KILLED} did not rejoin healthy"; exit 1; }
    start_observer "${KILLED}"
    echo "round ${round}: member ${KILLED} rejoined and is following again"
done

echo ""
echo "--- final soak ${SOAK_SECS}s under load ---"
sleep "${SOAK_SECS}"

rm -f "${LOAD_FLAG}"; kill "${LOAD_PID}" 2>/dev/null; wait "${LOAD_PID}" 2>/dev/null
echo "load generator stopped"

# ── assertion 3: a cold client replays the whole history off the final leader ──────────────────────
COLD_LOG="${LOG_DIR}/cold-follower.log"
docker exec "node-${LEADER}" java "${JAVA_OPTS[@]}" \
    -Dprobe.memberId="${LEADER}" -Dprobe.aeronDir="/dev/shm/aeron-${LEADER}" \
    -Dprobe.clientId="${COLD_CLIENT_ID}" \
    -cp /opt/phixeron/phixeron.jar org.limitless.phixeron.tools.ClusterProbe follow \
    > "${COLD_LOG}" 2>&1 &
COLD_PID=$!
echo "started cold probe follower (client ${COLD_CLIENT_ID}) inside node-${LEADER}"

waited=0
until grep -q "following live" "${COLD_LOG}" 2>/dev/null; do
    sleep 0.5; waited=$((waited + 1)); (( waited > $(deadline 180) * 2 )) && break
done
CAUGHT=0; grep -q "following live" "${COLD_LOG}" && CAUGHT=1

# ── collect: SIGTERM every probe so its ShutdownSignalBarrier releases and report() prints ─────────
for m in $(running_nodes); do docker exec "node-${m}" pkill -TERM -f ClusterProbe > /dev/null 2>&1; done
sleep 3
kill "${COLD_PID}" 2>/dev/null; wait "${COLD_PID}" 2>/dev/null

delivered_of() {   # last in-order delivered count reported by node $1's observer
    grep -o "frames delivered in order: [0-9]*" "${LOG_DIR}/observer-${1}.log" 2>/dev/null \
        | tail -1 | grep -o '[0-9]*$'
}

REPLAY_DECISIONS="$("${COMPOSE[@]}" logs --no-color "node-${LEADER}" 2>/dev/null \
                    | grep "replay for client ${COLD_CLIENT_ID}:")"
SEGMENTS=$(printf '%s' "${REPLAY_DECISIONS}" | grep -c . )
REPLAY_BYTES=$(printf '%s' "${REPLAY_DECISIONS}" | tail -1 | grep -o '\[0,[0-9]*)' | grep -o '[0-9]*)' | tr -d ')')
REPLAY_BYTES="${REPLAY_BYTES:-0}"
SUBMITTED=$(grep -c "submitted .* ProbeMarker(s)" "${LOG_DIR}/load.log" 2>/dev/null)
SUBMITTED=$(( SUBMITTED * SUBMIT_COUNT ))

echo ""
echo "=== RESULT ==="
echo "leader tenures                  : ${TENURES}"
echo "failovers completed             : ${FAILOVERS}/${ROUNDS}"
echo "ProbeMarkers submitted (>=)     : ${SUBMITTED}"
echo "cold client caught up           : ${CAUGHT}"
echo "replay segments to client ${COLD_CLIENT_ID}     : ${SEGMENTS}"
echo "replayed history bytes          : ${REPLAY_BYTES}"

PASS=1
[[ "${FAILOVERS}" -eq "${ROUNDS}" ]] || { echo "FAIL: only ${FAILOVERS}/${ROUNDS} failovers"; PASS=0; }
[[ "${CAUGHT}" == "1" ]] || { echo "FAIL: cold client never caught up"; PASS=0; }
[[ "${SEGMENTS}" -ge 1 ]] || { echo "FAIL: no replay segment served to the cold client"; PASS=0; }
[[ "${REPLAY_BYTES}" -gt 100000 ]] \
    || { echo "FAIL: replayed only ${REPLAY_BYTES} bytes — history is too thin to be evidence"; PASS=0; }

for m in 0 1 2; do
    n="$(delivered_of "${m}")"; n="${n:-0}"
    reconverged=$(grep -c "re-converged at globalSeqNo" "${LOG_DIR}/observer-${m}.log" 2>/dev/null)
    echo "observer node-${m}                : ${n} frames in order, ${reconverged} re-convergence(s)"
    [[ "${n}" -ge "${MIN_DELIVERED}" ]] \
        || { echo "FAIL: observer on node-${m} delivered ${n} < ${MIN_DELIVERED} frames"; PASS=0; }
done

echo ""
if [[ "${PASS}" == "1" ]]; then
    echo "DOCKER FAILOVER SOAK: PASS (${ROUNDS} failovers under load)"; exit 0
else
    echo "DOCKER FAILOVER SOAK: FAIL"; exit 1
fi
