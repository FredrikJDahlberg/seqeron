#!/usr/bin/env bash
# Launches this container's node: the cluster member, then the ReplayerServer co-located with it.
# Both belong here because the tap is aeron:ipc and the replayer serves history off this member's
# own archive through its embedded media driver — a node is the smallest thing worth losing, so
# docker kill/stop/pause takes the pair down together and a restart brings the pair back.
#
# The member comes from node-env.sh; every endpoint is derived from the distribution's ports.sh, so
# the port formula keeps its single home.
set -euo pipefail

source /opt/seqeron/bin/ports.sh
source /opt/seqeron/bin/seqeron-home.sh
source /opt/seqeron/docker/node-env.sh
seqeron_require_jar

LOG_FILE="${LOG_FILE:-/var/log/seqeron/node.log}"

mkdir -p "$(dirname "${LOG_FILE}")"

# Output goes to both the container log (docker logs, what the harness reads) and a file (what the
# healthcheck greps). Redirecting the shell's own descriptors means both JVMs inherit them.
#
# Truncated, not appended: `docker start` re-runs this entrypoint, and a previous run's "Running"
# left in the file would both start the replayer before this member's archive exists and make the
# healthcheck pass on stale text. docker logs keeps the full history across restarts regardless.
exec > >(tee "${LOG_FILE}") 2>&1

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

# sequencer.host is what the archive control, ingress and replication channels bind to and
# advertise; localhost would be this container's loopback alone and no peer could reach it. The
# consensus/log/transfer endpoints come from clusterMembers, hence the same template twice.
java "${JAVA_OPTS[@]}" \
    -Dsequencer.memberId="${MEMBER_ID}" \
    -Dsequencer.host="${HOST}" \
    -Dsequencer.clusterMembers="$(cluster_members_string "${NODE_COUNT}" "${HOST_TEMPLATE}")" \
    -Dsequencer.baseDir="${BASE_DIR}" \
    -Dsequencer.aeronDir="${AERON_DIR}" \
    -Dsequencer.idleStrategy="${IDLE_STRATEGY:-backoff}" \
    -jar "${SEQERON_JAR}" &
SEQUENCER_PID=$!

# The replayer connects to the member's archive over aeron:ipc, so it cannot start before the
# member's media driver and archive exist.
for _ in $(seq 1 120); do
    grep -q "SequencerServer/${MEMBER_ID}\] Running" "${LOG_FILE}" && break
    kill -0 "${SEQUENCER_PID}" 2>/dev/null || break
    sleep 0.5
done

java "${JAVA_OPTS[@]}" \
    -Dreplayer.memberId="${MEMBER_ID}" \
    -Dreplayer.aeronDir="${AERON_DIR}" \
    -Dreplayer.idleStrategy="${IDLE_STRATEGY:-backoff}" \
    -cp "${SEQERON_JAR}" org.limitless.seqeron.replayer.server.ReplayerServer &
REPLAYER_PID=$!

# docker stop's SIGTERM has to reach both ShutdownSignalBarriers, and whichever process exits first
# takes the container with it, carrying its own status out — a member that stopped because it could
# no longer record its tap exits 70, and that must not be masked by the other process still running.
trap 'kill -TERM "${SEQUENCER_PID}" "${REPLAYER_PID}" 2>/dev/null' TERM INT
wait -n "${SEQUENCER_PID}" "${REPLAYER_PID}"
STATUS=$?
kill -TERM "${SEQUENCER_PID}" "${REPLAYER_PID}" 2>/dev/null || true
exit "${STATUS}"
