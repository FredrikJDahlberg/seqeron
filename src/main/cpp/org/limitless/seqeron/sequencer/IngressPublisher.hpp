#pragma once

// Encode-and-offer for cluster ingress: the preamble every producer writes before its own fields. Free
// functions over ClusterStreamSender, which stays the session state machine and knows nothing of the
// frame layer.

#include <array>
#include <cstdint>
#include <utility>

#include "org/limitless/seqeron/sequencer/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/sequencer/IngressTracker.hpp"
#include "org/limitless/seqeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/Unsequenced.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedSystem.h"

namespace org::limitless::seqeron::sequencer {

// Encode buffer for one ingress message: the largest frame the protocol admits (§12).
inline constexpr std::size_t INGRESS_ENCODE_BUFFER_LEN = MAX_INGRESS_LENGTH;

// What a publish did. `Refused` is local and permanent (T-3): the body is above MAX_PAYLOAD_LENGTH, or the
// frame breaks §9.2 conditions 6 to 9, and nothing was offered, so retrying cannot succeed. `Declined` —
// transport back-pressure, a lost session, or the tracker holding or full — is the one a caller may retry.
enum class Publish
{
    Published,
    Refused,
    Declined
};

// Offers one encoded frame. Given a tracker (spec §16 A-4, A-5), nothing is sent while it holds or is full,
// and a placed frame is tracked under the sender's session and term.
[[nodiscard]] inline Publish offerFrame(ClusterStreamSender& sender, IngressTracker* tracker, const std::uint8_t* frame,
                                        const std::uint16_t length)
{
    if (tracker && (tracker->isHolding() || tracker->isFull()))
    {
        return Publish::Declined;
    }
    if (!sender.send(frame, length))
    {
        return Publish::Declined;
    }
    if (tracker)
    {
        tracker->track(frame, length, sender.clusterSessionId(), sender.leadershipTermId());
    }
    return Publish::Published;
}

// Encodes one payload inside an Unsequenced frame and offers it to cluster ingress. The session and the
// template are not parameters; sourceId is (for a reply, the requester's). `fill` stamps an encoder of any
// schema whose own messageHeader is applied; the payload is encoded into its own buffer and copied in.
// The overload taking a tracker confirms what it places through it (nullptr tracks nothing).
template<typename Encoder, typename Fill>
[[nodiscard]] Publish publishPayload(ClusterStreamSender& sender, IngressTracker* tracker, const std::int32_t sourceId,
                                     const std::int32_t connectionId, const std::uint16_t payloadId, Fill&& fill)
{
    // §9.2 conditions 6 and 7: sourceId -1 is the cluster's own (F-4), payloadId 0 names no protocol, and 1
    // is core's retired id.
    if (sourceId == -1 || payloadId == 0 || payloadId == 1)
    {
        return Publish::Refused;
    }
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> payload{};
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    std::forward<Fill>(fill)(encoder);
    const auto payloadLength =
        static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + encoder.encodedLength());
    if (payloadLength > MAX_PAYLOAD_LENGTH)
    {
        return Publish::Refused;
    }

    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> buffer{};
    sbe::frame::Unsequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(sourceId)
        .connectionId(connectionId)
        .sessionId(sender.clusterSessionId())
        .payloadId(payloadId);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);
    const auto length = static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + frame.encodedLength());
    return offerFrame(sender, tracker, buffer.data(), length);
}

template<typename Encoder, typename Fill>
[[nodiscard]] Publish publishPayload(ClusterStreamSender& sender, const std::int32_t sourceId,
                                     const std::int32_t connectionId, const std::uint16_t payloadId, Fill&& fill)
{
    return publishPayload<Encoder>(sender, nullptr, sourceId, connectionId, payloadId, std::forward<Fill>(fill));
}

// One of seqeron's own events (spec §7), in an UnsequencedSystem frame. The body has no messageHeader, so
// `fill` sees an encoder that was `wrap`ped (V-3). Tracked as publishPayload is.
template<typename Encoder, typename Fill>
[[nodiscard]] Publish publishSystem(ClusterStreamSender& sender, IngressTracker* tracker, const std::int32_t sourceId,
                                    const std::int32_t connectionId, const std::uint16_t systemEventType, Fill&& fill)
{
    // §9.2 conditions 6, 8 and 9.
    const std::int32_t blockLength = ingressBlockLength(systemEventType);
    if (sourceId == -1 || blockLength == NOT_INGRESS_LEGAL)
    {
        return Publish::Refused;
    }
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> body{};
    Encoder encoder;
    encoder.wrapForEncode(reinterpret_cast<char*>(body.data()), 0, body.size());
    std::forward<Fill>(fill)(encoder);
    const auto bodyLength = static_cast<std::uint16_t>(encoder.encodedLength());
    if (bodyLength > MAX_PAYLOAD_LENGTH || bodyLength < blockLength)
    {
        return Publish::Refused;
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

template<typename Encoder, typename Fill>
[[nodiscard]] Publish publishSystem(ClusterStreamSender& sender, const std::int32_t sourceId,
                                    const std::int32_t connectionId, const std::uint16_t systemEventType, Fill&& fill)
{
    return publishSystem<Encoder>(sender, nullptr, sourceId, connectionId, systemEventType, std::forward<Fill>(fill));
}

} // namespace org::limitless::seqeron::sequencer
