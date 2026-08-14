#pragma once

#include <cstdint>

namespace org::limitless::phixeron::fix {

// Pure decision logic behind FixGateway::checkTapStall's second, symmetric case: recovery that never
// converges (doc/review-2026-07-25.md #1). checkTapStall's own silence-while-caught-up fence is gated
// on isCaughtUp(), which leaves the opposite state — continuously !isCaughtUp() — with no bound at all:
// a Replayer that never answers, an onReplayUnavailable refusal, or a "gap in REPLAYED history" this
// node's chain cannot cover all hold isCaughtUp() false indefinitely without ever being fatal on their
// own. For an already-active gateway that is not "still starting up", it is silently wrong: the accept
// gate stays open and the cluster session stays alive (m_ingressSender.keepAlive() runs every duty
// cycle regardless), so the sequencer never sees a lost session and never promotes the standby — the
// gateway keeps accepting FIX orders against a view of the log frozen behind a hole it cannot close.
//
// A cold-starting gateway (never yet caught up) must NOT be fenced by this: its gate legitimately stays
// closed, however long the initial walk takes. The deadline only arms once onCaughtUp() has fired at
// least once. Free of Aeron/logging types so it is unit-testable directly, mirroring how
// TapStallPolicy (Java, SequencerService) is split out from its caller.
//
// PROGRESS, not elapsed recovery (review-3.md #6's follow-up): the deadline used to measure how long
// !isCaughtUp() had held, which fences a re-walk that is legitimately working through the whole chain —
// the very path a recovery takes. A converging recovery always advances the globalSeqNo it has
// dispatched; a non-converging one never does (the re-walk loop re-delivers history it already holds and
// de-dupes all of it). The same predicate RecoveryProgressPolicy alarms on, at a longer deadline,
// because this one fences.
class GatewayRecoveryStallPolicy
{
   public:
    // deadlineMs: how long recovery may run without dispatching a single frame, once this instance has
    // been caught up before, before it is declared unconvergent. Deliberately generous relative to a
    // normal gap-recovery re-walk (seconds, per ReplayerStreamReceiver's own file header) so that is
    // never mistaken for the pathological case this exists to catch.
    explicit GatewayRecoveryStallPolicy(std::int64_t deadlineMs) : m_deadlineMs{deadlineMs} {}

    // Caught up: not (or no longer) recovering. Latches everCaughtUp forever and clears the recovery
    // clock, so re-convergence after a legitimate re-walk re-arms cleanly for the next one.
    void onCaughtUp()
    {
        m_everCaughtUp = true;
        m_recoveryStartMs = 0;
    }

    // Evaluates one !isCaughtUp() observation, given the highest globalSeqNo dispatched so far. The
    // first observation of an episode, and every one that finds that frontier advanced, only re-anchors
    // the clock — the deadline is always measured from the last sign of progress. Returns true the
    // moment recovery has dispatched nothing for >= deadlineMs, for an instance that has been caught up
    // before; always false for a cold start.
    bool onNotCaughtUp(const std::int64_t nowMs, const std::int64_t globalSeqNo)
    {
        if (!m_everCaughtUp)
        {
            return false;
        }
        if (m_recoveryStartMs == 0 || globalSeqNo != m_lastGlobalSeqNo)
        {
            m_lastGlobalSeqNo = globalSeqNo;
            m_recoveryStartMs = nowMs;
            return false;
        }
        return (nowMs - m_recoveryStartMs) >= m_deadlineMs;
    }

   private:
    std::int64_t m_deadlineMs;
    bool m_everCaughtUp = false;
    std::int64_t m_recoveryStartMs = 0;  // 0 = no recovery episode currently timed
    std::int64_t m_lastGlobalSeqNo = 0;  // frontier at the last observation; only meaningful while timing
};

}  // namespace org::limitless::phixeron::fix
