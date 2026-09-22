#pragma once

#include <cstdint>

namespace org::limitless::seqeron::sequencer::client {

// What publishPayload/publishSystem tell, and ask, about each frame they place, so a producer confirms its
// ingress on the tap without handling a frame itself; and what the sender tells, and asks, about a leader
// change. A send spinning through an election would land in the new term ahead of older frames that election
// lost, so the sender gives it up while the hold is on. PendingSends is the implementation; the Java
// twin is sequencer/client/IngressTracker.java.
class IngressTracker
{
  public:
    virtual ~IngressTracker() = default;

    // Whether another frame can be tracked; one that cannot must not be sent.
    [[nodiscard]] virtual bool isFull() const = 0;

    // A frame just placed, with the sender's clusterSessionId() and leadershipTermId().
    virtual void track(const std::uint8_t* frame, std::uint16_t length, std::int64_t clusterSessionId,
                       std::int64_t leadershipTermId) = 0;

    // Egress named a new leader, for this term.
    virtual void onNewLeader(std::int64_t leadershipTermId) = 0;

    // Whether frames from an earlier term may still need resending ahead of any new one.
    [[nodiscard]] virtual bool isHolding() const = 0;
};

} // namespace org::limitless::seqeron::sequencer::client
