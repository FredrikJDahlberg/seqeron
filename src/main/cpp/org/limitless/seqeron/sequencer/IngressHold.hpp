#pragma once

#include <cstdint>

namespace org::limitless::seqeron::sequencer {

// What a sender tells, and asks, about a leader change. A send spinning through an election would land in the
// new term ahead of older frames that election lost, so the sender gives it up while the hold is on.
// app::PendingSends is the implementation; the Java twin is sequencer/IngressHold.java.
class IngressHold
{
  public:
    virtual ~IngressHold() = default;

    // Egress named a new leader, for this term.
    virtual void onNewLeader(std::int64_t leadershipTermId) = 0;

    // Whether frames from an earlier term may still need resending ahead of any new one.
    [[nodiscard]] virtual bool isHolding() const = 0;
};

} // namespace org::limitless::seqeron::sequencer
