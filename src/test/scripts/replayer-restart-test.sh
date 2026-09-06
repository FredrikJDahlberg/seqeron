#!/usr/bin/env bash
# replayer-restart-test.sh — the failover test-coverage gap flagged in review-2.md: nothing exercised an
# app running through its own node's restart, a Replayer restart under a riding client, or a resume/walk
# crossing a genuine recording rotation (where findings 2, 4 and 5 live). failover-test.sh only cold-starts
# a FRESH client after a leader kill against a node whose own tap recording was never rotated; gap-recovery-
# test.sh deliberately keeps its consumer's own member (0) alive throughout. Neither restarts the consumer's
# OWN node or its Replayer. This script does both, against real Aeron/Archive processes — not fabricated
# frames (that precision-testing already lives in ReplayerStreamReceiverTest.cpp's unit suite).
#
# Two phases, one continuous cluster lifetime, both targeting member 0 (started last, after members 1/2
# elect a leader between themselves, so member 0 is never the leader and this stays clear of Raft election
# behaviour — that is failover-test.sh's/chaos-runner.sh's job, not this one's):
#
#   PHASE 1 — Replayer restart under a riding client.
#     A backlog is flooded onto cluster ingress BEFORE the client starts, so its cold-start walk has real
#     work to do. The client (ClusterProbe follow, clientId 9) starts and its walk begins; the instant member
#     0's ReplayerService logs that it has started serving client 9's first segment, member 0's ReplayerServer
#     is killed outright. This is deliberately ordering-based, not timing-based: the server log line is
#     guaranteed to precede whatever the client does with the reply, so the kill always lands no later than
#     "client about to (or already) riding the replay" — whether the client ends up mid-image, waiting on a
#     reply that never arrives, or already finished is immaterial; all three are real instances of "the
#     Replayer this client depends on just went away" and are handled by the same resend/stall machinery
#     (onReplayImageClosed / onReplayStalled / poll()'s resend timer). ReplayerServer is then restarted, and
#     the client must survive (not crash) and eventually converge to "following live".
#
#   PHASE 2 — the client's own node restarts, producing a genuine (not fabricated) multi-recording chain.
#     Member 0's SequencerServer itself is killed. Its co-located ReplayerServer and probe client share its
#     embedded media driver and must fail fast (die within the driver-loss grace window) rather than spin
#     forever against a dead driver — the same invariant chaos-runner.sh's assert_died_on_driver_loss checks,
#     asserted here directly rather than as one random outcome among many. Member 0 is restarted: with no
#     snapshots, recovery is a full-log replay onto a BRAND NEW tap publication/recording (design.md §0),
#     leaving the pre-restart recording as a real, stopped, cold-start-walk segment rather than the current
#     active one. Once member 0's ReplayerServer and the client (same clientId) are restarted fresh, the
#     client's cold-start walk must cross that real 2-recording chain — segment 0 the old recording, segment
#     1 the new one — the live boundary case ReplayerService.serveReplay/ReplayRecordings.stitch and the
#     2026-08-10 recordingId-echo hardening (review-2.md #4) exist for, exercised here for real rather than
#     via hand-fabricated Replaying replies.
#
# Java only — no C++ binary is built or launched (doc/future-arch.md §11 step 5): the backlog is
# ClusterProbe submit and the client is ClusterProbe follow, which attaches to the member's own media
# driver, so this script needs no standalone aeronmd either.
#
# PASS iff: phase 1's client survives the Replayer kill and reaches "following live" once it is back; phase
# 2's ReplayerServer/client both exit within the driver-loss grace window when their node dies, both come back
# after the restart, and the fresh client's cold-start walk is served (at least) two segments naming two
# distinct recordingIds.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../main/scripts/ports.sh"
source "${SCRIPT_DIR}/../../main/scripts/paths.sh"

JAR="build/libs/phixeron-0.1.0-uber.jar"
LOG_DIR="logs/replayer-restart"
FLOOD_FRAMES=1000
CN=0            # client/Replayer restart target — member 0, never the leader (see header)
CLIENT_ID=9     # matches gap-recovery-test.sh/chaos-runner.sh's dedicated test-consumer clientId/port

NODE_START_TIMEOUT_SECS=30
APP_CATCHUP_TIMEOUT_SECS=30
PROC_EXIT_TIMEOUT_SECS=10
DRIVER_LOSS_GRACE_SECS=15   # must exceed Aeron's DEFAULT_MEDIA_DRIVER_TIMEOUT_MS (10s)

rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"

JAVA_OPTS=(
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
)
BASE_DIR="${TMP_DIR}/phixeron-seqfo"
CLUSTER_MEMBERS="$(cluster_members_string 3)"

start_seq() {  # start_seq <memberId> <logfile>
  local m="$1" log="$2"
  java "${JAVA_OPTS[@]}" -Dsequencer.memberId="$m" -Dsequencer.baseDir="$BASE_DIR" \
       -Dsequencer.clusterMembers="$CLUSTER_MEMBERS" -jar "$JAR" > "$log" 2>&1 &
  SEQ_PIDS[$m]=$!
}
start_replayer() {  # start_replayer <memberId> <logfile>
  local m="$1" log="$2"
  java "${JAVA_OPTS[@]}" -Dreplayer.memberId="$m" -cp "$JAR" \
       org.limitless.phixeron.replayer.server.ReplayerServer > "$log" 2>&1 &
  REPLAYER_PIDS[$m]=$!
}
start_client() {  # start_client <logfile>
  local log="$1"
  java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -Dprobe.clientId="$CLIENT_ID" -cp "$JAR" \
       org.limitless.phixeron.tools.ClusterProbe follow > "$log" 2>&1 &
  CLIENT_PID=$!
}
wait_for() {  # wait_for <pattern> <logfile> <timeout_iters (x0.5s)> <description>
  local pattern="$1" log="$2" timeout="$3" desc="$4" W=0
  until grep -q "$pattern" "$log" 2>/dev/null; do
    sleep 0.5; W=$((W+1)); ((W>timeout)) && { echo "TIMEOUT waiting for $desc (see $log)"; return 1; }
  done
  return 0
}
wait_for_exit() {  # wait_for_exit <pid> <timeout_iters (x0.5s)>
  local pid="$1" timeout="$2" W=0
  while kill -0 "$pid" 2>/dev/null; do sleep 0.5; W=$((W+1)); ((W>timeout)) && return 1; done
  return 0
}

pkill -f SequencerServer 2>/dev/null; pkill -f ReplayerServer 2>/dev/null; pkill -f ClusterProbe 2>/dev/null
sleep 1
rm -rf "$BASE_DIR" "${TMP_DIR}/phixeron-seq-aeron-0" "${TMP_DIR}/phixeron-seq-aeron-1" \
       "${TMP_DIR}/phixeron-seq-aeron-2" 2>/dev/null

CLIENT_PID=""
declare -a SEQ_PIDS REPLAYER_PIDS
cleanup() {
  kill "${CLIENT_PID:-0}" "${REPLAYER_PIDS[@]:-}" "${SEQ_PIDS[@]:-}" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── Bring-up: members 1 & 2 first -> leader is one of them; member 0 (our target) joins as a follower ──
start_seq 1 "$LOG_DIR/seq-1.log"; start_seq 2 "$LOG_DIR/seq-2.log"
for m in 1 2; do wait_for "Running" "$LOG_DIR/seq-$m.log" 120 "seq $m up" || exit 1; done
W=0; LEADER=""
until [[ -n "$LEADER" ]]; do
  sleep 0.5; W=$((W+1))
  for m in 1 2; do grep -q "isLeader=true" "$LOG_DIR/seq-$m.log" 2>/dev/null && { LEADER="$m"; break; }; done
  ((W>120)) && { echo "no tenure-1 leader among members 1,2"; exit 1; }
done
start_seq 0 "$LOG_DIR/seq-0.log"
wait_for "Running" "$LOG_DIR/seq-0.log" 120 "seq 0 up" || exit 1
echo "cluster up; tenure-1 leader = member $LEADER ; restart target = member $CN (never the leader)"

for m in 0 1 2; do start_replayer "$m" "$LOG_DIR/replayer-$m.log"; done
for m in 0 1 2; do wait_for "serving replay" "$LOG_DIR/replayer-$m.log" 120 "replayer $m serving" || exit 1; done
echo "replayers serving"
sleep 2

PHASE1_PASS=0
PHASE2_PASS=0

# ═══════════════════════════ PHASE 1 — Replayer restart under a riding client ═══════════════════════════
echo ""
echo "── phase 1: Replayer restart under a riding client ──"
echo "flooding $FLOOD_FRAMES frames onto cluster ingress to give the cold-start walk real work"
java "${JAVA_OPTS[@]}" -Dprobe.memberId="$CN" -Dprobe.count="$FLOOD_FRAMES" -cp "$JAR" \
     org.limitless.phixeron.tools.ClusterProbe submit > "$LOG_DIR/flood.log" 2>&1 || true
sleep 1   # let the flood replicate before the client's cold walk starts racing it

CLIENT_LOG_P1="$LOG_DIR/client-phase1.log"
start_client "$CLIENT_LOG_P1"
echo "started client $CLIENT_ID (cold start) co-located with member $CN"

# The trigger is ORDER, not TIMING: this server-side log line is written before sendReplaying, which is
# strictly before the client can possibly finish riding what it names — see header.
wait_for "replay for client $CLIENT_ID: segment 0" "$LOG_DIR/replayer-$CN.log" 60 \
  "Replayer to start serving client $CLIENT_ID's first segment" || exit 1
kill "${REPLAYER_PIDS[$CN]}" 2>/dev/null
echo "killed member $CN's ReplayerServer the instant it started serving client $CLIENT_ID"

if ! kill -0 "$CLIENT_PID" 2>/dev/null; then
  echo "PHASE 1 FAIL: client $CLIENT_ID crashed when its Replayer went away — should hold and retry"
else
  echo "client $CLIENT_ID survived the Replayer's death (still alive, holding/retrying)"
  start_replayer "$CN" "$LOG_DIR/replayer-$CN-restarted.log"
  if wait_for "serving replay" "$LOG_DIR/replayer-$CN-restarted.log" 60 "restarted replayer $CN serving"; then
    if wait_for "following live" "$CLIENT_LOG_P1" $((APP_CATCHUP_TIMEOUT_SECS * 2)) \
         "client $CLIENT_ID to catch up once the Replayer is back"; then
      echo "client $CLIENT_ID converged once the Replayer came back"
      PHASE1_PASS=1
    else
      echo "PHASE 1 FAIL: client $CLIENT_ID never caught up after the Replayer restarted"
    fi
  else
    echo "PHASE 1 FAIL: restarted Replayer never reached ready"
  fi
fi
echo "phase 1: $([[ $PHASE1_PASS == 1 ]] && echo PASS || echo FAIL)"

# ═══════════════════ PHASE 2 — the client's own node restarts (real recording rotation) ═══════════════════
echo ""
echo "── phase 2: client's own node restarts -> genuine multi-recording chain ──"
echo "killing member $CN's SequencerServer (its co-located ReplayerServer + client must fail fast)"
kill "${SEQ_PIDS[$CN]}" 2>/dev/null
SEQ_DIED=0; REPLAYER_DIED=0; CLIENT_DIED=0
wait_for_exit "${SEQ_PIDS[$CN]}" $((PROC_EXIT_TIMEOUT_SECS * 2)) && SEQ_DIED=1
wait_for_exit "${REPLAYER_PIDS[$CN]}" $((DRIVER_LOSS_GRACE_SECS * 2)) && REPLAYER_DIED=1
wait_for_exit "$CLIENT_PID" $((DRIVER_LOSS_GRACE_SECS * 2)) && CLIENT_DIED=1
echo "  member $CN SequencerServer exited      : $SEQ_DIED"
echo "  co-located ReplayerServer failed fast  : $REPLAYER_DIED"
echo "  co-located client failed fast        : $CLIENT_DIED"

if [[ "$SEQ_DIED" != "1" || "$REPLAYER_DIED" != "1" || "$CLIENT_DIED" != "1" ]]; then
  echo "PHASE 2 FAIL: a co-located process outlived its node's media driver (should fail fast, not spin)"
else
  start_seq "$CN" "$LOG_DIR/seq-$CN-restarted.log"
  if wait_for "Running" "$LOG_DIR/seq-$CN-restarted.log" $((NODE_START_TIMEOUT_SECS * 2)) "member $CN restart"; then
    sleep 2   # let the restored member finish its own full-log replay and settle
    start_replayer "$CN" "$LOG_DIR/replayer-$CN-phase2.log"
    if wait_for "serving replay" "$LOG_DIR/replayer-$CN-phase2.log" 60 "replayer $CN serving post-restart"; then
      CLIENT_LOG_P2="$LOG_DIR/client-phase2.log"
      start_client "$CLIENT_LOG_P2"
      echo "started fresh client $CLIENT_ID (cold start) after member $CN's own restart"
      if wait_for "following live" "$CLIENT_LOG_P2" $((APP_CATCHUP_TIMEOUT_SECS * 2)) \
           "fresh client $CLIENT_ID to catch up across the rotated chain"; then
        SEGMENTS=$(grep -c "replay for client $CLIENT_ID: segment" "$LOG_DIR/replayer-$CN-phase2.log")
        RECORDINGS=$(grep "replay for client $CLIENT_ID: segment" "$LOG_DIR/replayer-$CN-phase2.log" \
          | grep -oE "recording [0-9]+" | sort -u | wc -l | tr -d ' ')
        echo "  segments served to client $CLIENT_ID : $SEGMENTS"
        echo "  distinct recordingIds among them    : $RECORDINGS"
        if [[ "$SEGMENTS" -ge 2 && "$RECORDINGS" -ge 2 ]]; then
          echo "fresh client $CLIENT_ID walked a genuine multi-recording chain and converged"
          PHASE2_PASS=1
        else
          echo "PHASE 2 FAIL: walk did not cross a real 2-recording chain (segments=$SEGMENTS recordings=$RECORDINGS)"
        fi
      else
        echo "PHASE 2 FAIL: fresh client $CLIENT_ID never caught up"
      fi
    else
      echo "PHASE 2 FAIL: restarted replayer $CN never reached ready"
    fi
  else
    echo "PHASE 2 FAIL: member $CN never came back up"
  fi
fi
echo "phase 2: $([[ $PHASE2_PASS == 1 ]] && echo PASS || echo FAIL)"

echo ""
echo "=== RESULT ==="
echo "  phase 1 (Replayer restart under a riding client) : $([[ $PHASE1_PASS == 1 ]] && echo PASS || echo FAIL)"
echo "  phase 2 (own-node restart / recording rotation)  : $([[ $PHASE2_PASS == 1 ]] && echo PASS || echo FAIL)"

if [[ "$PHASE1_PASS" == "1" && "$PHASE2_PASS" == "1" ]]; then
  echo "REPLAYER-RESTART TEST: PASS"; exit 0
else
  echo "REPLAYER-RESTART TEST: FAIL"; exit 1
fi
