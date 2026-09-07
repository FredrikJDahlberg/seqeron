#!/usr/bin/env bash
# Cross-failover cold-start replay test, containerized — the docker/compose.yml port of
# failover-test.sh. Same assertion, different fault primitive: the leader is lost by `docker kill`
# on its container rather than a SIGTERM to a JVM sharing this host, so the member, its archive,
# its media driver and its co-located Replayer all go at once and the survivors lose a peer over
# the network exactly as they would in a deployment.
#
# Forces a leader failover (two leader tenures), then cold-starts a fresh ClusterProbe follower
# inside the surviving new leader's container and verifies it catches up on full history. Since
# every node records its own node-local tap continuously — the tap publication is never re-created
# on a leadership change — the new leader's node holds ONE continuous recording spanning both
# tenures, so the cold-start walk catches up from that single recording. This validates that the
# tap recording on a node that was a follower during tenure 1 still contains tenure-1 history after
# it becomes leader.
#
# PASS iff the fresh client prints "Caught up — following live" AND its co-located ReplayerService
# served it >= 1 replay segment.
#
# Prerequisite: ./gradlew uberJar (docker/Dockerfile copies the jar, it does not build it).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE=(docker compose -f "${REPO_ROOT}/docker/compose.yml")

JAR="${REPO_ROOT}/build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="${REPO_ROOT}/logs/docker-failover"
PROBE_CLIENT_ID=9

# CI runners are slower and noisier than a dev box; every wait is a multiple of this.
TIMEOUT_SCALE="${TIMEOUT_SCALE:-1}"
scaled() { echo $(( $1 * TIMEOUT_SCALE )); }

# Locally the image build is part of the run. CI builds it beforehand through buildx so the layer
# cache survives between runs, and sets this to 0 — compose then finds the tag already loaded
# (pull_policy: never, so it never reaches for a registry).
COMPOSE_BUILD="${COMPOSE_BUILD:-1}"

[[ -f "${JAR}" ]] || { echo "missing ${JAR} — run ./gradlew uberJar"; exit 1; }

rm -rf "${LOG_DIR}"; mkdir -p "${LOG_DIR}"

cleanup() {
    for m in 0 1 2; do "${COMPOSE[@]}" logs --no-color "node-${m}" > "${LOG_DIR}/node-${m}.log" 2>&1; done
    "${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
}
trap cleanup EXIT

# down -v first: a stale volume would carry a previous run's Raft log into this one.
"${COMPOSE[@]}" down -v --remove-orphans > /dev/null 2>&1
echo "starting three-node cluster"
# Never a possibly-empty array: macOS ships bash 3.2, where expanding one under `set -u` aborts.
UP_ARGS=(up -d --wait --wait-timeout "$(scaled 120)")
[[ "${COMPOSE_BUILD}" == "1" ]] && UP_ARGS=(up --build -d --wait --wait-timeout "$(scaled 120)")
if ! "${COMPOSE[@]}" "${UP_ARGS[@]}" > "${LOG_DIR}/compose-up.log" 2>&1; then
    echo "cluster did not come up healthy; see ${LOG_DIR}/compose-up.log"; exit 1
fi
echo "cluster up — all three nodes healthy"

# Which member leads is whatever Raft elected; every member is a legal target.
leader_of() {   # prints the member id currently claiming leadership, or nothing
    local m
    for m in "$@"; do
        "${COMPOSE[@]}" logs --no-color "node-${m}" 2>/dev/null \
            | grep -q "SequencerService/${m}\] leadership change: new leader is memberId=${m} (isLeader=true)" \
            && { echo "${m}"; return; }
    done
}

W=0; L1=""
until [[ -n "${L1}" ]]; do
    L1="$(leader_of 0 1 2)"
    [[ -n "${L1}" ]] && break
    sleep 0.5; W=$((W+1)); (( W > $(scaled 120) )) && { echo "no tenure-1 leader emerged"; exit 1; }
done
echo "tenure-1 leader = member ${L1}"

# Let tenure 1 sequence some history, so the cold-start walk below has something to replay.
sleep "$(scaled 3)"

SURVIVORS=(); for m in 0 1 2; do [[ "${m}" != "${L1}" ]] && SURVIVORS+=("${m}"); done
docker kill -s KILL "node-${L1}" > /dev/null
echo "SIGKILLed node-${L1}'s container -> forcing failover"

W=0; L2=""
until [[ -n "${L2}" ]]; do
    L2="$(leader_of "${SURVIVORS[@]}")"
    [[ -n "${L2}" ]] && break
    sleep 0.5; W=$((W+1)); (( W > $(scaled 240) )) && { echo "no tenure-2 leader emerged"; exit 1; }
done
echo "tenure-2 leader = member ${L2}"

# Let the new leader settle — its tap recording is continuous across the failover.
sleep "$(scaled 3)"

# A fresh cold probe: docker exec rather than a container process, because the point is a client
# that starts knowing nothing, well after the failover. It shares node-${L2}'s aeron directory, so
# it reaches that member's tap and its co-located Replayer over aeron:ipc, as a real app replica does.
PROBE_LOG="${LOG_DIR}/fresh-follower.log"
docker exec "node-${L2}" java \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -Dprobe.memberId="${L2}" -Dprobe.aeronDir="/dev/shm/aeron-${L2}" \
    -Dprobe.clientId="${PROBE_CLIENT_ID}" \
    -cp /opt/phixeron/phixeron.jar org.limitless.phixeron.tools.ClusterProbe follow \
    > "${PROBE_LOG}" 2>&1 &
PROBE_PID=$!
echo "started fresh cold probe follower (client ${PROBE_CLIENT_ID}) inside node-${L2}"

W=0
until grep -q "following live" "${PROBE_LOG}" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); (( W > $(scaled 120) )) && break
done
CAUGHT=0; grep -q "following live" "${PROBE_LOG}" && CAUGHT=1

REPLAYER_DECISIONS="$("${COMPOSE[@]}" logs --no-color "node-${L2}" 2>/dev/null \
                      | grep "replay for client ${PROBE_CLIENT_ID}:")"
SEGMENTS=$(printf '%s' "${REPLAYER_DECISIONS}" | grep -c . )

echo ""
echo "=== RESULT ==="
echo "--- Replayer (member ${L2}) replay decisions for fresh client ${PROBE_CLIENT_ID} ---"
echo "${REPLAYER_DECISIONS:-(none)}"
echo "segments served to client ${PROBE_CLIENT_ID} : ${SEGMENTS}"
echo "fresh client caught up                    : ${CAUGHT}"

kill "${PROBE_PID}" 2>/dev/null; wait "${PROBE_PID}" 2>/dev/null
if [[ "${CAUGHT}" == "1" && "${SEGMENTS}" -ge 1 ]]; then
    echo "DOCKER CROSS-FAILOVER TEST: PASS"; exit 0
else
    echo "DOCKER CROSS-FAILOVER TEST: FAIL"; exit 1
fi
