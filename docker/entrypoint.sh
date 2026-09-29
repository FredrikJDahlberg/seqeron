#!/usr/bin/env bash
# Launches this container's node: the cluster member, whose JVM also runs its Replayer.
#
# The member and its peers' host names come from node-env.sh; SequencerServer derives every endpoint
# from them.
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

JAVA_OPTS=("${SEQERON_JAVA_OPTS[@]}")

exec java "${JAVA_OPTS[@]}" \
    -Dsequencer.memberId="${MEMBER_ID}" \
    -Dsequencer.hosts="${HOSTS}" \
    -Dsequencer.baseDir="${BASE_DIR}" \
    -Dsequencer.aeronDir="${AERON_DIR}" \
    -Dsequencer.idleStrategy="${IDLE_STRATEGY:-backoff}" \
    -jar "${SEQERON_JAR}"
