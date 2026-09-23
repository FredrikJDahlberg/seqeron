#pragma once

// Encode-and-offer for cluster ingress: the preamble every producer writes before its own fields. Free
// functions over ClusterStreamSender, which stays the session state machine and knows nothing of the
// frame layer.

#include <array>
#include <cstdint>
#include <utility>

#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/sequencer/client/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressTracker.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/Unsequenced.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedSystem.h"

namespace org::limitless::seqeron::sequencer::client {

// Encode buffer for one ingress message: the largest frame the protocol admits (§12).
inline constexpr std::size_t INGRESS_ENCODE_BUFFER_LEN = protocol::MAX_INGRESS_LENGTH;

/**
 * Offers one encoded frame to cluster ingress.
 *
 * @param sender  the cluster session to offer on
 * @param tracker confirms what is placed (spec §16 A-4, A-5): nothing is sent while it holds or is full,
 *                and a placed frame is tracked under the sender's session and term; nullptr tracks nothing
 * @param frame   the frame's first byte, its messageHeader included
 * @param length  the frame's length
 * @return Published, or Declined if the tracker held or the transport did not take it
 */
[[nodiscard]] inline protocol::Publish offerFrame(ClusterStreamSender& sender, IngressTracker* tracker,
                                                  const std::uint8_t* frame, const std::uint16_t length)
{
    if (tracker && (tracker->isHolding() || tracker->isFull()))
    {
        return protocol::Publish::Declined;
    }
    if (!sender.send(frame, length))
    {
        return protocol::Publish::Declined;
    }
    if (tracker)
    {
        tracker->track(frame, length, sender.clusterSessionId(), sender.leadershipTermId());
    }
    return protocol::Publish::Published;
}

/**
 * Wraps one already-encoded payload in an Unsequenced frame and offers it to cluster ingress. The Java twin
 * is the only publish there is on that side, since Java's SBE codecs share no interface; here it is what a
 * caller that encodes into a buffer of its own reaches for.
 *
 * @param sender        the cluster session to offer on
 * @param tracker       confirms what is placed, as offerFrame's does; nullptr tracks nothing
 * @param sourceId      the producer's sourceId; for a reply, the requester's
 * @param connectionId  the connection the payload belongs to, or -1 for none
 * @param payloadId     the payload's protocol; neither 0 nor the retired 1
 * @param payload       the payload's first byte, its own messageHeader included
 * @param payloadLength the payload's length, at most MAX_PAYLOAD_LENGTH
 * @return Published; Refused, permanently, for an illegal sourceId, payloadId or length; Declined otherwise
 */
[[nodiscard]] inline protocol::Publish publishPayload(ClusterStreamSender& sender, IngressTracker* tracker,
                                                      const std::int32_t sourceId, const std::int32_t connectionId,
                                                      const std::uint16_t payloadId, const std::uint8_t* payload,
                                                      const std::uint16_t payloadLength)
{
    // §9.2 conditions 6 and 7, as above.
    if (sourceId == -1 || payloadId == 0 || payloadId == 1 || payloadLength > protocol::MAX_PAYLOAD_LENGTH)
    {
        return protocol::Publish::Refused;
    }
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> buffer{};
    sbe::frame::Unsequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(sourceId)
        .connectionId(connectionId)
        .sessionId(sender.clusterSessionId())
        .payloadId(payloadId);
    frame.putPayload(reinterpret_cast<const char*>(payload), payloadLength);
    const auto length = static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + frame.encodedLength());
    return offerFrame(sender, tracker, buffer.data(), length);
}

/**
 * Encodes one payload inside an Unsequenced frame and offers it to cluster ingress. The payload is encoded
 * into its own buffer and copied in.
 *
 * @tparam Encoder     the payload's SBE encoder, of any schema
 * @param sender       the cluster session to offer on
 * @param tracker      confirms what is placed, as offerFrame's does; nullptr tracks nothing
 * @param sourceId     the producer's sourceId; for a reply, the requester's
 * @param connectionId the connection the payload belongs to, or -1 for none
 * @param payloadId    the payload's protocol; neither 0 nor the retired 1
 * @param fill         called with the encoder, its own messageHeader already applied, to stamp the fields
 * @return Published; Refused, permanently, for an illegal sourceId or payloadId; Declined otherwise
 */
template<typename Encoder, typename Fill>
[[nodiscard]] protocol::Publish publishPayload(ClusterStreamSender& sender, IngressTracker* tracker,
                                               const std::int32_t sourceId, const std::int32_t connectionId,
                                               const std::uint16_t payloadId, Fill&& fill)
{
    // §9.2 conditions 6 and 7: sourceId -1 is the cluster's own (F-4), payloadId 0 names no protocol, and 1
    // is core's retired id.
    if (sourceId == -1 || payloadId == 0 || payloadId == 1)
    {
        return protocol::Publish::Refused;
    }
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> payload{};
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    std::forward<Fill>(fill)(encoder);
    const auto payloadLength =
        static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + encoder.encodedLength());
    return publishPayload(sender, tracker, sourceId, connectionId, payloadId, payload.data(), payloadLength);
}

/**
 * Encodes one payload inside an Unsequenced frame and offers it to cluster ingress, untracked.
 *
 * @tparam Encoder     the payload's SBE encoder, of any schema
 * @param sender       the cluster session to offer on
 * @param sourceId     the producer's sourceId; for a reply, the requester's
 * @param connectionId the connection the payload belongs to, or -1 for none
 * @param payloadId    the payload's protocol; neither 0 nor the retired 1
 * @param fill         called with the encoder, its own messageHeader already applied, to stamp the fields
 * @return Published; Refused, permanently, for an illegal sourceId or payloadId; Declined otherwise
 */
template<typename Encoder, typename Fill>
[[nodiscard]] protocol::Publish publishPayload(ClusterStreamSender& sender, const std::int32_t sourceId,
                                               const std::int32_t connectionId, const std::uint16_t payloadId,
                                               Fill&& fill)
{
    return publishPayload<Encoder>(sender, nullptr, sourceId, connectionId, payloadId, std::forward<Fill>(fill));
}

/**
 * Encodes one of seqeron's own events (spec §7) in an UnsequencedSystem frame and offers it to cluster
 * ingress.
 *
 * @tparam Encoder        the event's encoder from sbe-frame.xml
 * @param sender          the cluster session to offer on
 * @param tracker         confirms what is placed, as offerFrame's does; nullptr tracks nothing
 * @param sourceId        the producer's sourceId
 * @param connectionId    the connection the event concerns, or -1 for none
 * @param systemEventType the event, one a producer may submit
 * @param fill            called with the encoder to stamp the fields; the body has no messageHeader, so the
 *                        encoder was `wrap`ped rather than having one applied (V-3)
 * @return Published; Refused, permanently, for an illegal sourceId or event or a body of the wrong size;
 *         Declined otherwise
 */
template<typename Encoder, typename Fill>
[[nodiscard]] protocol::Publish publishSystem(ClusterStreamSender& sender, IngressTracker* tracker,
                                              const std::int32_t sourceId, const std::int32_t connectionId,
                                              const std::uint16_t systemEventType, Fill&& fill)
{
    // §9.2 conditions 6, 8 and 9.
    const std::int32_t blockLength = protocol::ingressBlockLength(systemEventType);
    if (sourceId == -1 || blockLength == protocol::NOT_INGRESS_LEGAL)
    {
        return protocol::Publish::Refused;
    }
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> body{};
    Encoder encoder;
    encoder.wrapForEncode(reinterpret_cast<char*>(body.data()), 0, body.size());
    std::forward<Fill>(fill)(encoder);
    const auto bodyLength = static_cast<std::uint16_t>(encoder.encodedLength());
    if (bodyLength > protocol::MAX_PAYLOAD_LENGTH || bodyLength < blockLength)
    {
        return protocol::Publish::Refused;
    }

    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> buffer{};
    sbe::frame::UnsequencedSystem frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(sourceId)
        .connectionId(connectionId)
        .sessionId(sender.clusterSessionId())
        .systemEventType(systemEventType);
    frame.putBody(reinterpret_cast<const char*>(body.data()), bodyLength);
    const auto length = static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + frame.encodedLength());
    return offerFrame(sender, tracker, buffer.data(), length);
}

/**
 * Encodes one of seqeron's own events (spec §7) in an UnsequencedSystem frame and offers it, untracked.
 *
 * @tparam Encoder        the event's encoder from sbe-frame.xml
 * @param sender          the cluster session to offer on
 * @param sourceId        the producer's sourceId
 * @param connectionId    the connection the event concerns, or -1 for none
 * @param systemEventType the event, one a producer may submit
 * @param fill            called with the `wrap`ped encoder to stamp the fields
 * @return Published; Refused, permanently, for an illegal sourceId or event or a body of the wrong size;
 *         Declined otherwise
 */
template<typename Encoder, typename Fill>
[[nodiscard]] protocol::Publish publishSystem(ClusterStreamSender& sender, const std::int32_t sourceId,
                                              const std::int32_t connectionId, const std::uint16_t systemEventType,
                                              Fill&& fill)
{
    return publishSystem<Encoder>(sender, nullptr, sourceId, connectionId, systemEventType, std::forward<Fill>(fill));
}

} // namespace org::limitless::seqeron::sequencer::client
