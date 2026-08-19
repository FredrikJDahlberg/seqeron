#pragma once

#include <chrono>
#include <cstdint>

#include "Aeron.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"

// Generated SBE C++ codecs from sbe-sequenced.xml (via GenerateSequencedSbeCodecs)
#include "org_limitless_phixeron_sbe_sequenced/Header.h"
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"

// The sequenced stream's wire contract: what a frame on it is, how to decode one, and where it
// sits. Everything here is shared by both stream clients — ClusterStreamClient (archive replay,
// ClusterStreamClient.hpp) and ReplayerStreamReceiver (the live tap) — and by every consumer of
// either. Kept apart from ClusterStreamClient.hpp so reading a frame does not drag in the archive
// client: most consumers follow the tap and never replay an archive themselves.

namespace org::limitless::phixeron::sequencer {

// ── Constants matching SequencerService / SequencerServer ──────────────────────

// Stream id of the recorded sequenced stream. Every node records its node-local aeron:ipc tap
// (SequencerService.FEEDER_CHANNEL / FEEDER_STREAM_ID) into its own archive; that recording is the
// authoritative history clients replay here, matched by stream id alone in the archive catalog
// (listRecordingsForUri). The old UDP multi-destination-cast global stream (stream 1) is retired,
// so there is no live network subscription — clients follow the active recording's growth via an
// open-ended archive replay instead (see start()/poll()).
inline constexpr std::int32_t FEEDER_STREAM_ID = 205;

// ClientConnected/ClientDisconnected aren't FIX messages, so sbe-sequenced.xml
// (like sbe-unsequenced.xml) gives them small, non-ASCII-derived template ids,
// clear of the FIX-MsgType-derived range used by every other message. They mark a FIX
// client's TCP connection to the gateway opening and closing, and the gateway that owns
// that socket publishes them; see LifecycleEvent below.
inline constexpr std::uint16_t CLIENT_CONNECTED_TEMPLATE_ID = 1;
inline constexpr std::uint16_t CLIENT_DISCONNECTED_TEMPLATE_ID = 2;

/**
 * Carries one sbe-sequenced.xml message from the cluster stream.
 *
 * Every raw fragment on the wire *is* a complete sbe-sequenced.xml message
 * (schemaId=202) — no envelope to strip. Every message in that schema
 * declares `header` (sourceId, connectionId, sessionId, globalSeqNo,
 * timestamp) as its first field, at the same fixed offset regardless of
 * templateId, so this client decodes it generically and exposes the fields
 * here — callers don't need to re-decode it themselves before dispatching
 * on templateId.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback; payload addresses the start of the
 * full message (its own 8-byte messageHeader included). Copy the data before
 * returning if it must survive.
 */
struct SequencedEvent
{
    std::int64_t globalSeqNo;
    std::int32_t sourceId;         ///< Fixed constant identifying the submitting gateway process (header.sourceId)
    std::int32_t connectionId;     ///< TCP connection id at that gateway; routes the reply (header.connectionId)
    std::int64_t sourceSessionId;  ///< Aeron Cluster client session id (header.sessionId)
    std::int64_t clusterTimestamp; ///< cluster consensus time (ms) when message was committed
    std::int64_t receiveTimeNs;    ///< wall-clock ns at receipt by this client
    /// Which producer role emitted the frame (header.origin). Present on every message, so a consumer
    /// can tell FIX session traffic from an application frame that merely shares the connectionId,
    /// without knowing the template. Carried through the sequencer unchanged from the publisher.
    sbe::sequenced::Origin::Value origin;
    std::uint16_t templateId;    ///< outer messageHeader templateId; picks the specific decode
    std::uint16_t blockLength;   ///< outer messageHeader blockLength; pass straight to wrapForDecode
    std::uint16_t version;       ///< outer messageHeader version; pass straight to wrapForDecode
    const char* payload;         ///< raw sbe-sequenced.xml message bytes (see struct comment)
    std::uint64_t payloadLength; ///< total byte count
    std::int64_t position;       ///< recording/stream position of this frame's first byte;
                                 ///< pass to ReplayParams::position() to replay from here
};

// The same decode for a frame held as loose bytes rather than a live event — the gateway buffers
// gateway-origin admin frames behind an in-flight resend and replays them once it drains, and its
// outbound resend path re-decodes bytes it cached.
template<typename Decoder>
Decoder
decodeSequenced(const char* payload, const std::uint64_t payloadLength, const std::uint16_t blockLength,
                const std::uint16_t version)
{
    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(payload), sbe::sequenced::MessageHeader::encodedLength(), blockLength,
                          version, payloadLength);
    return decoder;
}

// Wraps an event's payload in the sbe-sequenced decoder the caller has already matched its templateId
// against. Every consumer otherwise repeats this same wrapForDecode preamble once per message type,
// const_cast included — the generated codecs decode through a mutable char*, while the event carries a
// const pointer into the fragment buffer. Decoding does not write to it.
//
// The returned decoder points into that fragment buffer, so it is valid only for the duration of the
// callback, exactly as SequencedEvent::payload is.
template<typename Decoder>
Decoder
decodeSequenced(const SequencedEvent& event)
{
    return decodeSequenced<Decoder>(event.payload, event.payloadLength, event.blockLength, event.version);
}

// Stream position of the first byte of the frame `header` describes — what SequencedEvent::position
// carries, for both clients that populate one, and what the resend path hands to
// ReplayParams::position().
inline std::int64_t frameStartPosition(const aeron::Header& header)
{
    return aeron::concurrent::logbuffer::LogBufferDescriptor::computePosition(
        header.termId(), header.termOffset(), header.positionBitsToShift(), header.initialTermId());
}

/**
 * A FIX client's TCP connection to a gateway opening (ClientConnected) or closing
 * (ClientDisconnected). Published by the gateway that owns the socket — these are external
 * events it observes, forwarded on ingress like any other message, not something the
 * sequencer synthesizes.
 *
 * sourceId/connectionId name the connection the event refers to, and both are needed:
 * connectionId is unique only within the publishing gateway process, so a consumer serving
 * one gateway must match sourceId before acting on a connectionId (see FixGateway).
 */
struct LifecycleEvent
{
    std::int64_t globalSeqNo;
    std::int32_t sourceId;         ///< publishing gateway process (header.sourceId)
    std::int32_t connectionId;     ///< TCP connection at that gateway (header.connectionId)
    std::int64_t sourceSessionId;  ///< Aeron Cluster session the event was submitted on
    std::int64_t clusterTimestamp; ///< cluster consensus time (ms) when committed
    std::int64_t receiveTimeNs;    ///< wall-clock ns at receipt by this client
};

// The wall-clock stamp SequencedEvent/LifecycleEvent carry as receiveTimeNs. Free rather than a
// member of either stream client, so both stamp their events off the same clock.
inline std::int64_t
nowNs()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace org::limitless::phixeron::sequencer
