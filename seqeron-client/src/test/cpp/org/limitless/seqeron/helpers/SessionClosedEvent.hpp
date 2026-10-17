#pragma once

#include <array>
#include <cstdint>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

#include "org_limitless_seqeron_sbe_frame/SessionClosed.h"

namespace org::limitless::seqeron::helpers {

// The SessionClosed the sequencer synthesizes when a session closes, as a stream client hands it on. The event
// points into this object's own bytes, so it is neither copied nor moved.
struct SessionClosedEvent
{
    explicit SessionClosedEvent(const std::int64_t clusterSessionId)
    {
        sbe::frame::SessionClosed encoder;
        encoder.wrapAndApplyHeader(bytes.data(), 0, bytes.size());
        encoder.header().sourceId(-1).connectionId(-1).sessionId(-1).systemEventType(protocol::SESSION_CLOSED);
        encoder.clusterSessionId(clusterSessionId);
        event.sourceSessionId = -1;
        event.system = true;
        event.systemEventType = protocol::SESSION_CLOSED;
        event.payload = bytes.data() + sbe::frame::MessageHeader::encodedLength();
        event.payloadLength = sbe::frame::SessionClosed::sbeBlockLength();
    }

    SessionClosedEvent(const SessionClosedEvent&) = delete;
    SessionClosedEvent& operator=(const SessionClosedEvent&) = delete;

    std::array<char, 64> bytes{};
    protocol::SequencedEvent event{};
};

} // namespace org::limitless::seqeron::helpers
