# node-env.sh — this container's member: its id, its peers' host names and its directories. Sourced
# by entrypoint.sh and by clusterctl, so the tool resolves the same member the node was launched as.
#
# MEMBER_ID is the only thing compose has to supply.

MEMBER_ID="${MEMBER_ID:?MEMBER_ID must be set}"
NODE_COUNT="${NODE_COUNT:-3}"
HOST_TEMPLATE="${HOST_TEMPLATE:-node-{id}}"       # {id} -> member id; the compose service names
HOST="${HOST_TEMPLATE//\{id\}/${MEMBER_ID}}"
BASE_DIR=/var/lib/seqeron
AERON_DIR="/dev/shm/aeron-${MEMBER_ID}"
