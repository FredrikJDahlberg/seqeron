#pragma once

#include <cstdint>

#include "org/limitless/seqeron/sequencer/IngressHold.hpp"

namespace org::limitless::seqeron::sequencer {

// What publishPayload/publishSystem tell, and ask, about each frame they place, so a producer confirms its
// ingress on the tap without handling a frame itself. app::PendingSends is the implementation; the Java twin
// is sequencer/IngressTracker.java.
class IngressTracker : public IngressHold
{
  public:
    // Whether another frame can be tracked; one that cannot must not be sent.
    [[nodiscard]] virtual bool isFull() const = 0;

    // A frame just placed, with the sender's clusterSessionId() and leadershipTermId().
    virtual void track(const std::uint8_t* frame, std::uint16_t length, std::int64_t clusterSessionId,
                       std::int64_t leadershipTermId) = 0;
};

} // namespace org::limitless::seqeron::sequencer
