#pragma once

// Encode-and-offer for cluster ingress: the one preamble every producer in this system writes before
// its own fields, factored out of the four places that each spelled it out (BasicDataServer's loader,
// OrderExecServer's PortfolioQueryReply and venue ExecutionReport, FixGateway's GatewayStarted).
//
// Deliberately a free function over ClusterStreamSender rather than a member of it: the sender is the
// cluster *session* state machine and knows only the sbe-cluster.xml wire protocol. Teaching it the
// frame layer would make every includer — the unit suite's in-memory transport fakes included —
// depend on schema 210.

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

// Encode buffer for one ingress message: the largest frame the protocol admits (§12). Sized off the
// constant rather than off the widest message any current producer encodes, so a body at exactly
// MAX_PAYLOAD_LENGTH -- which T-3 below admits -- has somewhere to go.
inline constexpr std::size_t INGRESS_ENCODE_BUFFER_LEN = MAX_INGRESS_LENGTH;

// What a publish did.
//
// T-3 is why this is three-valued rather than a bool. `Refused` is local and permanent: the body is
// above MAX_PAYLOAD_LENGTH, or the frame breaks one of §9.2's conditions 6 to 9 -- what a producer can check
// without the sequencer's state, so the sequencer never drops (S-7) what was sent -- nothing was offered to
// any transport, and a caller
// that retries is retrying something that can never succeed. `Declined` is the transport's answer --
// back-pressure, or a session that is gone -- or the tracker's, while it holds or is full, and is the one a
// caller may retry. What a producer does with a Refused message is its own business (P-0): chunk it, drop it,
// or fail the session; not retry.
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

// Encodes one payload inside an Unsequenced frame and offers it to cluster ingress.
//
// Two header fields are invariant across every producer and so are not parameters: the cluster session
// this process is sending on, and the frame's own template. What varies is sourceId (for a reply, the
// requester's, copied so the gateway can route the answer back — not necessarily this process's own),
// connectionId, payloadId, and the body. The payload carries no header of its own — the frame's is the
// only one — so `fill` sees an encoder with nothing applied but its own framing header, and stamps
// only fields.
//
// The payload is encoded into its own buffer and copied in rather than written in place. It is a memcpy
// of a few dozen bytes on a path that already crosses a cluster, and it keeps the var-data length prefix
// the generated encoder's business rather than this function's.
//
// Encoder may come from any schema: every SBE messageHeader is the same eight bytes, and the frame layer
// neither knows nor decodes what payloadId names (S-2).
//
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

// One of seqeron's own events (doc/seqeron-protocol-spec.md §7), in an UnsequencedSystem frame.
//
// The body carries no messageHeader of its own — header.systemEventType is what names it — so `fill`
// sees an encoder that has been `wrap`ped rather than `wrapAndApplyHeader`ed, and the six bytes the
// system family saves over the application one are exactly that absence. V-3 licenses it: no seqeron
// decoder ever meets bytes another build encoded. Tracked as publishPayload is.
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
