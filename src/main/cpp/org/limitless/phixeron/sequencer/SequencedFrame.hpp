#pragma once

#include <chrono>
#include <cstdint>

#include "Aeron.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"

// Generated SBE C++ codecs from sbe-frame.xml (via GenerateFrameSbeCodecs)
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/SequencedHeader.h"

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

// ClientConnected/ClientDisconnected aren't FIX messages, so sbe-frame.xml gives
// them small, non-ASCII-derived template ids, clear of the FIX-MsgType-derived
// range the session family uses. They mark a FIX
// client's TCP connection to the gateway opening and closing, and the gateway that owns
// that socket publishes them; see LifecycleEvent below.
inline constexpr std::uint16_t CLIENT_CONNECTED_TEMPLATE_ID = 1;
inline constexpr std::uint16_t CLIENT_DISCONNECTED_TEMPLATE_ID = 2;

// The one payloadId the cluster tier owns and decodes: seqeron's own core payloads
// (doc/seqeron-protocol-spec.md §6.1). Frames carrying anything else are followed and forwarded
// without being opened.
inline constexpr std::uint16_t CORE_PAYLOAD_ID = 1;

/**
 * Carries one message from the cluster stream.
 *
 * Every fragment is a `Sequenced` frame (schemaId=210) whose header carries the identity below and whose
 * body is one opaque payload named by payloadId; the stream client strips the envelope and the fields
 * here describe the *payload*. A consumer dispatches on (payloadId, templateId) — never templateId
 * alone, which is unique per schema only.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback; payload addresses the start of the
 * message (its own 8-byte messageHeader included). Copy the data before
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
    std::uint16_t payloadId;       ///< which protocol templateId belongs to
    std::uint16_t templateId;      ///< the message's messageHeader templateId; picks the specific decode
    std::uint16_t blockLength;     ///< outer messageHeader blockLength; pass straight to wrapForDecode
    std::uint16_t version;         ///< outer messageHeader version; pass straight to wrapForDecode
    const char* payload;           ///< the payload's own bytes, its 8-byte messageHeader included
    std::uint64_t payloadLength;   ///< total byte count
    std::int64_t position;         ///< recording/stream position of this frame's first byte;
                                   ///< pass to ReplayParams::position() to replay from here
};

/**
 * One frame off the tap, unwrapped: everything a consumer dispatches on, with the envelope stripped.
 *
 * The two stream clients both build one of these and then differ only in what they do with it, which is
 * the point: the envelope is stripped here, once, rather than at every consumer. The identity is the
 * frame header's and the message is the payload.
 */
struct FrameView
{
    bool valid; ///< false for a fragment that is not a frame, or too short to read
    std::uint16_t payloadId;
    std::int32_t sourceId;
    std::int32_t connectionId;
    std::int64_t sessionId;
    std::int64_t globalSeqNo;
    std::int64_t timestamp;
    std::uint16_t templateId; ///< the message's own, never the envelope's
    std::uint16_t blockLength;
    std::uint16_t version;
    const char* payload; ///< the message, its 8-byte messageHeader included
    std::uint64_t payloadLength;
};

/**
 * Reads one fragment into a FrameView, stripping the envelope.
 *
 * Bounds are checked because a short fragment would otherwise be read past its end. It should not
 * happen — the sequencer validates every frame on ingress and the recording is what it wrote — so
 * `valid == false` here means the recording itself is damaged, and the caller logs and drops.
 */
inline FrameView unwrapFrame(const char* const frame, const std::uint64_t length)
{
    FrameView view{};
    char* const bytes = const_cast<char*>(frame);
    if (length < sbe::frame::MessageHeader::encodedLength())
    {
        return view;
    }

    sbe::frame::MessageHeader hdr;
    hdr.wrap(bytes, 0U, 0U, length);
    if (hdr.schemaId() != sbe::frame::Sequenced::sbeSchemaId() ||
        hdr.templateId() != sbe::frame::Sequenced::sbeTemplateId() ||
        length < sbe::frame::Sequenced::sbeBlockAndHeaderLength() + sbe::frame::Sequenced::payloadHeaderLength())
    {
        return view;
    }
    sbe::frame::Sequenced sequenced;
    sequenced.wrapForDecode(bytes, sbe::frame::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                            length);
    sbe::frame::SequencedHeader& header = sequenced.header();
    view.payloadId = header.payloadId();
    view.sourceId = header.sourceId();
    view.connectionId = header.connectionId();
    view.sessionId = header.sessionId();
    view.globalSeqNo = header.globalSeqNo();
    view.timestamp = header.timestamp();

    const std::uint64_t payloadLength = sequenced.payloadLength();
    const char* const payload = sequenced.payload();
    if (payloadLength < sbe::frame::MessageHeader::encodedLength())
    {
        return view; // an empty or truncated payload names no message to dispatch on
    }
    sbe::frame::MessageHeader payloadHdr;
    payloadHdr.wrap(const_cast<char*>(payload), 0U, 0U, payloadLength);
    view.templateId = payloadHdr.templateId();
    view.blockLength = payloadHdr.blockLength();
    view.version = payloadHdr.version();
    view.payload = payload;
    view.payloadLength = payloadLength;
    view.valid = true;
    return view;
}

// The same decode for a frame held as loose bytes rather than a live event — the gateway buffers
// gateway-produced admin frames behind an in-flight resend and replays them once it drains, and its
// outbound resend path re-decodes bytes it cached.
template<typename Decoder>
Decoder decodeSequenced(const char* payload, const std::uint64_t payloadLength, const std::uint16_t blockLength,
                        const std::uint16_t version)
{
    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(payload), sbe::frame::MessageHeader::encodedLength(), blockLength, version,
                          payloadLength);
    return decoder;
}

// Wraps an event's payload in the decoder the caller has already matched its (payloadId, templateId)
// against. Every consumer otherwise repeats this same wrapForDecode preamble once per message type,
// const_cast included — the generated codecs decode through a mutable char*, while the event carries a
// const pointer into the fragment buffer. Decoding does not write to it.
//
// The returned decoder points into that fragment buffer, so it is valid only for the duration of the
// callback, exactly as SequencedEvent::payload is.
template<typename Decoder>
Decoder decodeSequenced(const SequencedEvent& event)
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
inline std::int64_t nowNs()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace org::limitless::phixeron::sequencer
