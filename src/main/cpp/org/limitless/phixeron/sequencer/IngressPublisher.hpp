#pragma once

// Encode-and-offer for cluster ingress: the one preamble every producer in this system writes before
// its own fields, factored out of the four places that each spelled it out (BasicDataServer's loader,
// OrderExecServer's PortfolioQueryReply and venue ExecutionReport, FixGateway's GatewayStarted).
//
// Deliberately a free function over ClusterStreamSender rather than a member of it: the sender is the
// cluster *session* state machine and knows only the sbe-cluster.xml wire protocol. Teaching it the
// sbe-unsequenced.xml application schema would make every includer — the unit suite's in-memory
// transport fakes included — depend on schema 200.

#include <array>
#include <cstdint>
#include <utility>

#include "org/limitless/phixeron/sequencer/ClusterStreamSender.hpp"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/Origin.h"

namespace org::limitless::phixeron::sequencer {

// Encode buffer for one ingress message. 512 bytes is what the largest hand-rolled buffer this
// replaced already used (OrderExecServer's ExecutionReport, the widest message any of these producers
// encodes) and comfortably clears ClusterStreamSender::send's MAX_PAYLOAD_LEN check.
inline constexpr std::size_t INGRESS_ENCODE_BUFFER_LEN = 512;

// Encodes one sbe-unsequenced (schema 200) message and offers it to cluster ingress.
//
// Two header fields are invariant across every producer and so are not parameters: the cluster session
// this process is sending on, and Origin::Application — marking a frame this process originated, as
// opposed to one a FIX gateway relayed from a client. What varies is sourceId (for a reply, the
// requester's, copied so the gateway can route the answer back — not necessarily this process's own),
// connectionId, and the body. `fill` receives the encoder with the header already applied.
//
// Returns what ClusterStreamSender::send returns: false only when there is no cluster session. A caller
// that reserved a FIX MsgSeqNum must act on that; one that did not may discard it (doc/todo.md 0e).
template<typename Encoder, typename Fill>
[[nodiscard]] bool publishUnsequenced(ClusterStreamSender& sender, const std::int32_t sourceId,
                                      const std::int32_t connectionId, Fill&& fill)
{
    alignas(16) std::array<std::uint8_t, INGRESS_ENCODE_BUFFER_LEN> buffer{};
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.header()
        .sourceId(sourceId)
        .connectionId(connectionId)
        .sessionId(sender.clusterSessionId())
        .origin(sbe::unsequenced::Origin::Value::Application);
    std::forward<Fill>(fill)(encoder);
    const auto length =
        static_cast<std::uint16_t>(sbe::unsequenced::MessageHeader::encodedLength() + encoder.encodedLength());
    return sender.send(buffer.data(), length);
}

} // namespace org::limitless::phixeron::sequencer
