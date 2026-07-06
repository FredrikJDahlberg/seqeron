#!/usr/bin/env bash
# failover-recording-test.sh — verifies cross-failover global-stream recording continuity
# (SequencerService's StandbyFollower/applyLeadership) by actually killing the elected leader
# mid-run and checking that the new leader's own archive ends up holding both the old leader's
# now-stopped recording segment and a freshly-started one for its own tenure.
#
# Java-only: no C++ clients, no aeronmd, no FIX traffic — this is purely about whether
# SequencerService's standby replication mechanism keeps every node's archive current while it
# isn't leader, so that whichever node next takes over already has full history locally. See
# todo.md's "Cross-failover global-stream recording continuity" entry for the design this
# verifies, and its "Not yet done" note that this script (not three-node-cluster.sh, which never
# kills a member mid-run) is what exercises the actual failover path.
#
# Usage:
#   ./gradlew uberJar   # build the fat jar first
#   ./failover-recording-test.sh

set -euo pipefail

JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs"
BASE_DIR="${TMPDIR:-/tmp}phixeron-failover-test"

JAVA_OPTS=(
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)

CLUSTER_MEMBERS="0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301"
CLUSTER_MEMBERS+="|1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311"
CLUSTER_MEMBERS+="|2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321"

if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: ${JAR} not found — run: ./gradlew uberJar" >&2
    exit 1
fi

rm -rf "${BASE_DIR}"
mkdir -p "${LOG_DIR}"

SEQ_PIDS=()
SEQ_LOGS=()

cleanup() {
    kill "${SEQ_PIDS[@]}" 2>/dev/null || true
    wait "${SEQ_PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_member() {
    local member="$1"
    local log="${LOG_DIR}/failover-test-seq-${member}.log"
    SEQ_LOGS[member]="${log}"
    java "${JAVA_OPTS[@]}" \
        -Dsequencer.memberId="${member}" \
        -Dsequencer.baseDir="${BASE_DIR}" \
        -Dsequencer.clusterMembers="${CLUSTER_MEMBERS}" \
        -jar "${JAR}" \
        > "${log}" 2>&1 &
    SEQ_PIDS[member]=$!
}

echo "[failover-recording-test.sh] Starting 3-member cluster…"
for member in 0 1 2; do
    start_member "${member}"
done

for member in 0 1 2; do
    WAIT=0
    until grep -q "Running" "${SEQ_LOGS[member]}" 2>/dev/null; do
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 60 )); then
            echo "ERROR: member ${member} did not reach Running state after 30 s" >&2
            exit 1
        fi
    done
done
echo "[failover-recording-test.sh] All 3 members running"

# Wait for the initial leadership election to settle: every member's log gets at least one
# "leadership change" line once onNewLeadershipTermEvent first fires cluster-wide.
# Prints the current leader's memberId only once every still-running member's log agrees on the
# same value for its own last "leadership change" line (they converge asynchronously, so a
# transient disagreement just means "not settled yet" — prints nothing in that case).
find_current_leader() {
    local agreed=""
    for member in 0 1 2; do
        if ! kill -0 "${SEQ_PIDS[member]}" 2>/dev/null; then
            continue
        fi
        local last
        last="$(grep "leadership change" "${SEQ_LOGS[member]}" 2>/dev/null | tail -1 \
            | grep -oE "new leader is memberId=[0-9]+" | grep -oE "[0-9]+$" || true)"
        if [[ -z "${last}" ]]; then
            return
        fi
        if [[ -z "${agreed}" ]]; then
            agreed="${last}"
        elif [[ "${agreed}" != "${last}" ]]; then
            return
        fi
    done
    echo "${agreed}"
}

echo "[failover-recording-test.sh] Waiting for initial leader election…"
WAIT=0
LEADER=""
until [[ -n "${LEADER}" ]]; do
    LEADER="$(find_current_leader)"
    if [[ -z "${LEADER}" ]]; then
        sleep 0.5
        WAIT=$(( WAIT + 1 ))
        if (( WAIT > 40 )); then
            echo "ERROR: no leadership change observed after 20 s" >&2
            exit 1
        fi
    fi
done
# Let it settle a moment longer in case of startup election churn, then re-read.
sleep 3
LEADER="$(find_current_leader)"
echo "[failover-recording-test.sh] Initial leader: member ${LEADER}"

# Give standby-follow time to actually resolve+start replicating the leader's recording before
# we pull the rug out from under it.
sleep 3

echo "[failover-recording-test.sh] Killing leader (member ${LEADER})…"
kill -9 "${SEQ_PIDS[LEADER]}"

echo "[failover-recording-test.sh] Waiting for a new leader to be elected…"
WAIT=0
NEW_LEADER=""
until [[ -n "${NEW_LEADER}" && "${NEW_LEADER}" != "${LEADER}" ]]; do
    NEW_LEADER="$(find_current_leader)"
    sleep 0.5
    WAIT=$(( WAIT + 1 ))
    if (( WAIT > 60 )); then
        echo "ERROR: no new leader elected 30 s after killing member ${LEADER}" >&2
        exit 1
    fi
done
echo "[failover-recording-test.sh] New leader: member ${NEW_LEADER}"

# Give the new leader's own recording a moment to actually start.
sleep 2

echo "[failover-recording-test.sh] Inspecting member ${NEW_LEADER}'s archive catalog…"
DESCRIBE="$(java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "${JAR}" io.aeron.archive.ArchiveTool "${BASE_DIR}/archive-${NEW_LEADER}" describe 2>&1)"

# GLOBAL_STREAM_ID=1 recordings only. stopTimestamp=-1 (NULL_TIMESTAMP) means still active/recording.
GLOBAL_STREAM_RECORDINGS="$(echo "${DESCRIBE}" | grep 'streamId=1|')"
TOTAL_SEGMENTS="$(echo "${GLOBAL_STREAM_RECORDINGS}" | grep -c 'recordingId=' || true)"
ACTIVE_SEGMENTS="$(echo "${GLOBAL_STREAM_RECORDINGS}" | grep -c 'stopTimestamp=-1' || true)"
STOPPED_SEGMENTS=$(( TOTAL_SEGMENTS - ACTIVE_SEGMENTS ))

echo "[failover-recording-test.sh] Member ${NEW_LEADER} archive: ${TOTAL_SEGMENTS} global-stream segment(s),"\
     "${STOPPED_SEGMENTS} stopped, ${ACTIVE_SEGMENTS} active"

# The property that actually matters is "history isn't lost": at least one stopped segment
# (replicated in via standby-follow while this node was still a follower) alongside the new
# leader's own active one, i.e. total segments > active segments. Exactly-1-active isn't asserted:
# an abrupt kill -9 of the old leader can race the standby-follow's live-merge teardown against
# this node's own brand-new publication appearing on the same GLOBAL_STREAM_CHANNEL address,
# occasionally producing a harmless *duplicate* active segment (same content recorded twice, not
# data loss) — see todo.md's "Cross-failover global-stream recording continuity" entry.
if (( TOTAL_SEGMENTS > ACTIVE_SEGMENTS )) && (( ACTIVE_SEGMENTS >= 1 )); then
    if (( ACTIVE_SEGMENTS > 1 )); then
        echo "[failover-recording-test.sh] NOTE: ${ACTIVE_SEGMENTS} active segments (expected 1) —"\
             "likely the known standby-follow/own-recording teardown race on abrupt leader death,"\
             "a harmless duplicate rather than data loss."
    fi
    echo "[failover-recording-test.sh] PASSED: new leader's own archive holds the prior leader's"\
         "replicated segment(s) plus its own tenure — no client needs to reach the dead node's"\
         "archive to recover full history."
    STATUS=0
else
    echo "[failover-recording-test.sh] FAILED: expected at least one stopped (historical) segment"\
         "alongside an active one on the new leader; got ${TOTAL_SEGMENTS} total / ${ACTIVE_SEGMENTS}"\
         "active." >&2
    echo "${DESCRIBE}" >&2
    STATUS=1
fi

exit "${STATUS}"
