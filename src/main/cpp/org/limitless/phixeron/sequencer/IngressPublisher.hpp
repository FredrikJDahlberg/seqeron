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

#include "org/limitless/phixeron/sequencer/ClusterStreamSender.hpp"
#include "org/limitless/phixeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/Unsequenced.h"

namespace org::limitless::phixeron::sequencer {

// Encode buffer for one ingress message. 512 bytes is what the largest hand-rolled buffer this
// replaced already used (OrderExecServer's ExecutionReport, the widest message any of these producers
// encodes) and comfortably clears ClusterStreamSender::send's MAX_PAYLOAD_LEN check.
inline constexpr std::size_t INGRESS_ENCODE_BUFFER_LEN = 512;

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
template<typename Encoder, typename Fill>
[[nodiscard]] bool publishPayload(ClusterStreamSender& sender, const std::int32_t sourceId,
                                  const std::int32_t connectionId, const std::uint16_t payloadId, Fill&& fill)
{
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> payload{};
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    std::forward<Fill>(fill)(encoder);
    const auto payloadLength =
        static_cast<std::uint16_t>(sbe::frame::MessageHeader::encodedLength() + encoder.encodedLength());

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
    return sender.send(buffer.data(), length);
}

// A core payload (payloadId 1) — the cluster tier's own, and the only payloadId it decodes.
template<typename Encoder, typename Fill>
[[nodiscard]] bool publishCore(ClusterStreamSender& sender, const std::int32_t sourceId,
                               const std::int32_t connectionId, Fill&& fill)
{
    return publishPayload<Encoder>(sender, sourceId, connectionId, CORE_PAYLOAD_ID, std::forward<Fill>(fill));
}

} // namespace org::limitless::phixeron::sequencer
