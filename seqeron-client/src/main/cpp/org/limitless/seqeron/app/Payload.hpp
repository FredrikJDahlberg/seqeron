#pragma once

#include <cstdint>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"

namespace org::limitless::seqeron::app {

namespace detail {
template<typename Dispatch>
class Session;
} // namespace detail

/**
 * One application payload delivered in order, with the frame layer off it: the envelope is stripped, and
 * body() strips the payload's own messageHeader too, which is where an SBE decoder wraps. A payload that
 * carries no header at all (§13.2, what offerFrame exists for) is addressed by payload() instead, and its
 * templateId, blockLength and version mean nothing.
 *
 * Nothing of seqeron's own vocabulary reaches here — the system family is the façade's business, and a
 * frame of it never arrives as a payload.
 *
 * Dispatch on (payloadId, templateId), never templateId alone: template ids are unique per schema, so two
 * applications' templates can collide.
 *
 * A view: valid only during the handler call; copy anything that must outlive it. The Java twin is
 * app/Payload.java; keep the two in step.
 */
class Payload
{
  public:
    // Cluster-wide monotone sequence number; increments by exactly one per frame.
    [[nodiscard]] std::int64_t globalSeqNo() const noexcept
    {
        return m_event.globalSeqNo;
    }

    // The producer that submitted this frame (spec §5).
    [[nodiscard]] std::int32_t sourceId() const noexcept
    {
        return m_event.sourceId;
    }

    // The producer's connection this frame belongs to, or -1 for a producer-scoped one.
    [[nodiscard]] std::int32_t connectionId() const noexcept
    {
        return m_event.connectionId;
    }

    // The cluster session it was submitted on; a gateway pair shares its sourceId but not this.
    [[nodiscard]] std::int64_t sourceSessionId() const noexcept
    {
        return m_event.sourceSessionId;
    }

    // The Raft consensus timestamp, identical on every node.
    [[nodiscard]] std::int64_t clusterTimestampNs() const noexcept
    {
        return m_event.clusterTimestampNs;
    }

    // When this process read it — a delivery stamp, not the frame's.
    [[nodiscard]] std::int64_t receiveTimeNs() const noexcept
    {
        return m_event.receiveTimeNs;
    }

    // Where this frame's first byte sits in the node's recording — a delivery stamp too, and what a
    // consumer that replays that recording itself (a FIX gateway serving its own resend) anchors on.
    [[nodiscard]] std::int64_t position() const noexcept
    {
        return m_event.position;
    }

    // Which protocol the body speaks (spec §13).
    [[nodiscard]] std::uint16_t payloadId() const noexcept
    {
        return m_event.payloadId;
    }

    // The body's own template within that protocol.
    [[nodiscard]] std::uint16_t templateId() const noexcept
    {
        return m_event.templateId;
    }

    // For the decoder's wrap, from the payload's own messageHeader.
    [[nodiscard]] std::uint16_t blockLength() const noexcept
    {
        return m_event.blockLength;
    }

    // For the decoder's wrap, from the payload's own messageHeader.
    [[nodiscard]] std::uint16_t version() const noexcept
    {
        return m_event.version;
    }

    // The payload's first byte: its own messageHeader included if it has one.
    [[nodiscard]] const char* payload() const noexcept
    {
        return m_event.payload;
    }

    [[nodiscard]] std::uint64_t payloadLength() const noexcept
    {
        return m_event.payloadLength;
    }

    // Where the body starts: past the payload's own messageHeader, which is where a decoder wraps.
    [[nodiscard]] const char* body() const noexcept
    {
        return m_event.payload + sbe::frame::MessageHeader::encodedLength();
    }

    [[nodiscard]] std::uint64_t bodyLength() const noexcept
    {
        return m_event.payloadLength - sbe::frame::MessageHeader::encodedLength();
    }

    // The payload's own decoder, wrapped past its messageHeader with the block length and version that
    // header declares. The Java twin has no equivalent because a Java decoder's wrap() already takes the
    // four accessors above in one call; here they would each have to be passed.
    template<typename Decoder>
    [[nodiscard]] Decoder decode() const
    {
        return protocol::decodeSequenced<Decoder>(m_event.payload, m_event.payloadLength, m_event.blockLength,
                                                  m_event.version);
    }

  private:
    template<typename Dispatch>
    friend class detail::Session;

    explicit Payload(const protocol::SequencedEvent& event) : m_event{ event }
    {}

    const protocol::SequencedEvent& m_event;
};

} // namespace org::limitless::seqeron::app
