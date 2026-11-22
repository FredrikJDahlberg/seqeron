#!/usr/bin/env bash
# Launches this container's node: the cluster member, whose JVM also runs its Replayer, or with
# ROLE=gateway-host a ReplayerServer that relays RELAYED_GROUPS' queues from the members' archives.
#
# The member and its peers' host names come from node-env.sh; SequencerServer derives every endpoint
# from them. A gateway host reads ARCHIVE_ENDPOINTS in the order its relay tries them, else each member's.
set -euo pipefail

source /opt/seqeron/bin/seqeron-home.sh
source /opt/seqeron/docker/node-env.sh
seqeron_require_jar

LOG_FILE="${LOG_FILE:-/var/log/seqeron/node.log}"

mkdir -p "$(dirname "${LOG_FILE}")"

# Output goes to both the container log (docker logs, what the harness reads) and a file (what the
# healthcheck greps). Redirecting the shell's own descriptors means the JVM inherits them.
#
# Truncated, not appended: `docker start` re-runs this entrypoint, and a previous run's "Running"
# left in the file would make the healthcheck pass on stale text. docker logs keeps the full history
# across restarts regardless.
exec > >(tee "${LOG_FILE}") 2>&1

# Docker's DNS knows a container only once it has started, and the media driver's lookup of a peer it does not
# know yet can outlast the driver's 10 s client timeout, failing the node. Wait up to 60 s for every member's name.
deadline=$((SECONDS + 60))
for host in ${SEQERON_HOSTS//,/ }; do
    until getent hosts "${host}" > /dev/null || ((SECONDS >= deadline)); do sleep 0.5; done
    getent hosts "${host}" > /dev/null || echo "[entrypoint] ${host} does not resolve; starting anyway"
done

JAVA_OPTS=("${SEQERON_SERVER_JAVA_OPTS[@]}")

if [[ "${ROLE:-member}" == gateway-host ]]; then
    exec java "${JAVA_OPTS[@]}" \
        -Dreplayer.memberId="${MEMBER_ID}" \
        -Dreplayer.host="$(hostname)" \
        -Dreplayer.baseDir="${BASE_DIR}" \
        -Dreplayer.aeronDir="${AERON_DIR}" \
        -Dreplayer.idleStrategy="${IDLE_STRATEGY:-backoff}" \
        -Dreplayer.groups="${RELAYED_GROUPS:?RELAYED_GROUPS must be set}" \
        ${ARCHIVE_ENDPOINTS:+-Dreplayer.archiveEndpoints="${ARCHIVE_ENDPOINTS}"} \
        -cp "${SEQERON_JAR}" org.limitless.seqeron.replayer.server.ReplayerServer
fi

exec java "${JAVA_OPTS[@]}" \
    -Dsequencer.memberId="${MEMBER_ID}" \
    -Dsequencer.baseDir="${BASE_DIR}" \
    -Dsequencer.aeronDir="${AERON_DIR}" \
    -Dsequencer.idleStrategy="${IDLE_STRATEGY:-backoff}" \
    -jar "${SEQERON_JAR}"
