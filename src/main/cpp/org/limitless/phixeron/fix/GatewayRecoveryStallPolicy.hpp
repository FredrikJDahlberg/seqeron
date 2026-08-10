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
class GatewayRecoveryStallPolicy
{
   public:
    // deadlineMs: how long continuous !isCaughtUp() may last, once this instance has been caught up
    // before, before recovery is declared unconvergent. Deliberately generous relative to a normal
    // gap-recovery re-walk (seconds, per ReplayerStreamReceiver's own file header) so that is never
    // mistaken for the pathological case this exists to catch.
    explicit GatewayRecoveryStallPolicy(std::int64_t deadlineMs) : m_deadlineMs{deadlineMs} {}

    // Caught up: not (or no longer) recovering. Latches everCaughtUp forever and clears the recovery
    // clock, so re-convergence after a legitimate re-walk re-arms cleanly for the next one.
    void onCaughtUp()
    {
        m_everCaughtUp = true;
        m_recoveryStartMs = 0;
    }

    // Evaluates one !isCaughtUp() observation. The first observation of a recovery episode only anchors
    // the clock — how long it had already been running before this call is unknown — so the deadline is
    // measured from here. Returns true the moment continuous recovery has lasted >= deadlineMs, for an
    // instance that has been caught up before; always false for a cold start.
    bool onNotCaughtUp(std::int64_t nowMs)
    {
        if (!m_everCaughtUp)
        {
            return false;
        }
        if (m_recoveryStartMs == 0)
        {
            m_recoveryStartMs = nowMs;
            return false;
        }
        return (nowMs - m_recoveryStartMs) >= m_deadlineMs;
    }

   private:
    std::int64_t m_deadlineMs;
    bool m_everCaughtUp = false;
    std::int64_t m_recoveryStartMs = 0;  // 0 = no recovery episode currently timed
};

}  // namespace org::limitless::phixeron::fix
