#pragma once

#include <chrono>
#include <cstdint>

#include "Aeron.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"

// Generated SBE C++ codecs from sbe-frame.xml (via GenerateFrameSbeCodecs)
#include "org_limitless_phixeron_sbe_frame/ApplicationRegistered.h"
#include "org_limitless_phixeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_phixeron_sbe_frame/ClusterStarted.h"
#include "org_limitless_phixeron_sbe_frame/ClusterStopped.h"
#include "org_limitless_phixeron_sbe_frame/ConnectionClosed.h"
#include "org_limitless_phixeron_sbe_frame/ConnectionOpened.h"
#include "org_limitless_phixeron_sbe_frame/GatewayActivationRequested.h"
#include "org_limitless_phixeron_sbe_frame/GatewayActive.h"
#include "org_limitless_phixeron_sbe_frame/GatewayRegistered.h"
#include "org_limitless_phixeron_sbe_frame/GatewayStarted.h"
#include "org_limitless_phixeron_sbe_frame/LeadershipChanged.h"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/PayloadIdRegistered.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/SequencedHeader.h"
#include "org_limitless_phixeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_phixeron_sbe_frame/SequencedSystemHeader.h"

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

// ── Limits ────────────────────────────────────────────────────────────────────
//
// These belong to the seqeron protocol, not to this file or to any participant in it: they are defined
// by doc/seqeron-protocol-spec.md §12, and what follows is this language's compiled-in mirror of that
// table. The Java mirror is FrameLayer. A change starts in the spec and lands in both, and is a wire
// change (V-3) whichever way round it is made.
//
// Compiled in, and never read from a transport per frame: S-3 forbids checking against a node's own MTU,
// which would let a node provisioned smaller than its peers fork globalSeqNo. Whether this node's MTUs
// can carry the constant is a start-up question (T-2), not a per-frame one.

// The pinned 1408-byte MTU less the 92-byte ingress header stack.
inline constexpr std::uint16_t MAX_PAYLOAD_LENGTH = 1316;

// Smallest ingress frame of either family: the framing header, the 18-byte header composite and the
// body's own 2-byte length prefix. One constant for both templates -- both composites are 18 bytes (F-3).
inline constexpr std::uint16_t MIN_INGRESS_LENGTH = 28;

// Largest ingress frame: a full payload behind that framing -- 1344 bytes. §9.2 condition 1's ceiling.
inline constexpr std::uint16_t MAX_INGRESS_LENGTH = MIN_INGRESS_LENGTH + MAX_PAYLOAD_LENGTH;

// ── The systemEventType table (doc/seqeron-protocol-spec.md §7) ────────────────
//
// The eight submitted events are their own body codec's template id — the numbers they have always
// held, so a recording made by an older build can never read as one of these. The three the
// sequencer synthesizes have no body codec: a top-level template names each of them, and these are
// the values they nonetheless stamp at offset 16 so that field discriminates every frame on the tap.
// The Java twin is SystemFrame; keep the two in step.
inline constexpr std::uint16_t CONNECTION_OPENED = sbe::frame::ConnectionOpened::sbeTemplateId();
inline constexpr std::uint16_t CONNECTION_CLOSED = sbe::frame::ConnectionClosed::sbeTemplateId();
inline constexpr std::uint16_t LEADERSHIP_CHANGED = 5; // synthesis-only
inline constexpr std::uint16_t CLUSTER_STARTED = sbe::frame::ClusterStarted::sbeTemplateId();
inline constexpr std::uint16_t CLUSTER_STOPPED = sbe::frame::ClusterStopped::sbeTemplateId();
inline constexpr std::uint16_t CLUSTER_HEARTBEAT = 16; // synthesis-only
inline constexpr std::uint16_t GATEWAY_REGISTERED = sbe::frame::GatewayRegistered::sbeTemplateId();
inline constexpr std::uint16_t GATEWAY_ACTIVE = 18; // synthesis-only
inline constexpr std::uint16_t GATEWAY_STARTED = sbe::frame::GatewayStarted::sbeTemplateId();
inline constexpr std::uint16_t PAYLOAD_ID_REGISTERED = sbe::frame::PayloadIdRegistered::sbeTemplateId();
inline constexpr std::uint16_t GATEWAY_ACTIVATION_REQUESTED = sbe::frame::GatewayActivationRequested::sbeTemplateId();
inline constexpr std::uint16_t APPLICATION_REGISTERED = sbe::frame::ApplicationRegistered::sbeTemplateId();

/**
 * Carries one message from the cluster stream.
 *
 * Every fragment on the tap is one of five shapes (doc/seqeron-protocol-spec.md §4), and the same
 * 2-byte field at offset 16 discriminates all of them: a `Sequenced` frame carries one opaque
 * application payload named by payloadId, the four system shapes carry seqeron's own vocabulary named
 * by systemEventType. `system` says which. A consumer of an application frame dispatches on
 * (payloadId, templateId) — never templateId alone, which is unique per schema only.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback; payload addresses the start of the
 * message (its own 8-byte messageHeader included). Copy the data before
 * returning if it must survive.
 */
struct SequencedEvent
{
    std::int64_t globalSeqNo;
    std::int32_t sourceId;         ///< Fixed constant identifying the submitting producer process (header.sourceId)
    std::int32_t connectionId;     ///< Connection id at that producer; routes the reply (header.connectionId)
    std::int64_t sourceSessionId;  ///< Aeron Cluster client session id (header.sessionId)
    std::int64_t clusterTimestamp; ///< cluster consensus time (ms) when message was committed
    std::int64_t receiveTimeNs;    ///< wall-clock ns at receipt by this client
    bool system;                   ///< true: a system frame, named by systemEventType, and payloadId means nothing
    std::uint16_t payloadId;       ///< which protocol templateId belongs to; 0 on a system frame
    std::uint16_t systemEventType; ///< which of §7's eleven events; 0 on an application frame
    std::uint16_t templateId;      ///< the message's messageHeader templateId; picks the specific decode
    std::uint16_t blockLength;     ///< payload messageHeader blockLength; 0 on a system frame (see decodeSystem)
    std::uint16_t version;         ///< payload messageHeader version; 0 on a system frame
    const char* payload;           ///< the payload's own bytes, its 8-byte messageHeader included; on a
                                   ///< system frame, the message with no framing at all
    std::uint64_t payloadLength;   ///< total byte count
    std::int64_t position;         ///< recording/stream position of this frame's first byte;
                                   ///< pass to ReplayParams::position() to replay from here
};

/**
 * One frame off the tap, unwrapped: everything a consumer dispatches on, with the envelope stripped.
 *
 * The two stream clients both build one of these and then differ only in what they do with it, which is
 * the point: the envelope is stripped here, once, rather than at every consumer. The identity is the
 * frame header's; what the message is depends on the family (see `payload` below).
 */
struct FrameView
{
    bool valid;  ///< false for a fragment that is not a frame, or too short to read
    bool system; ///< true: one of the four system shapes; payloadId/templateId mean nothing
    std::uint16_t payloadId;
    std::uint16_t systemEventType;
    std::int32_t sourceId;
    std::int32_t connectionId;
    std::int64_t sessionId;
    std::int64_t globalSeqNo;
    std::int64_t timestamp;
    std::uint16_t templateId; ///< application family: the payload's own, never the envelope's
    /// The payload's own, to wrap its decoder with; both 0 on any system frame, whose message carries
    /// no declaration at all — its decoder's compiled constants are the only ones there are (V-3).
    std::uint16_t blockLength;
    std::uint16_t version;
    /// What a consumer decodes: the payload, its 8-byte messageHeader included, on an application
    /// frame; the body on a submitted system frame; the frame's own block on one of the synthesized
    /// three.
    const char* payload;
    std::uint64_t payloadLength;
};

/// Copies the identity every system shape carries; the two composites are the same 34 bytes.
inline void readSystemHeader(FrameView& view, sbe::frame::SequencedSystemHeader& header)
{
    view.system = true;
    view.systemEventType = header.systemEventType();
    view.sourceId = header.sourceId();
    view.connectionId = header.connectionId();
    view.sessionId = header.sessionId();
    view.globalSeqNo = header.globalSeqNo();
    view.timestamp = header.timestamp();
}

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
    if (hdr.schemaId() != sbe::frame::Sequenced::sbeSchemaId())
    {
        return view;
    }
    const std::uint64_t blockOffset = sbe::frame::MessageHeader::encodedLength();
    const std::uint16_t frameTemplateId = hdr.templateId();

    if (frameTemplateId == sbe::frame::Sequenced::sbeTemplateId())
    {
        if (length < sbe::frame::Sequenced::sbeBlockAndHeaderLength() + sbe::frame::Sequenced::payloadHeaderLength())
        {
            return view;
        }
        sbe::frame::Sequenced sequenced;
        sequenced.wrapForDecode(bytes, blockOffset, hdr.blockLength(), hdr.version(), length);
        sbe::frame::SequencedHeader& header = sequenced.header();
        view.payloadId = header.payloadId();
        view.sourceId = header.sourceId();
        view.connectionId = header.connectionId();
        view.sessionId = header.sessionId();
        view.globalSeqNo = header.globalSeqNo();
        view.timestamp = header.timestamp();

        const std::uint64_t payloadLength = sequenced.payloadLength();
        const char* const payload = sequenced.payload();
        view.payload = payload;
        view.payloadLength = payloadLength;
        view.valid = true;
        // A payload too short to carry a messageHeader is still a frame. §5 admits an empty payload and
        // §13.2 admits a payload that is not SBE at all, so there is not always an inner header to read --
        // and P-3 requires the frame to reach the consumer regardless, or a globalSeqNo goes missing from
        // the continuity read. Leaving templateId/blockLength/version at 0, as the system branches do, is
        // what says "no inner declaration": no (payloadId, templateId) dispatch can match one (P-1).
        if (payloadLength < sbe::frame::MessageHeader::encodedLength())
        {
            return view;
        }
        sbe::frame::MessageHeader payloadHdr;
        payloadHdr.wrap(const_cast<char*>(payload), 0U, 0U, payloadLength);
        view.templateId = payloadHdr.templateId();
        view.blockLength = payloadHdr.blockLength();
        view.version = payloadHdr.version();
        return view;
    }

    if (frameTemplateId == sbe::frame::SequencedSystem::sbeTemplateId())
    {
        if (length <
            sbe::frame::SequencedSystem::sbeBlockAndHeaderLength() + sbe::frame::SequencedSystem::bodyHeaderLength())
        {
            return view;
        }
        sbe::frame::SequencedSystem sequenced;
        sequenced.wrapForDecode(bytes, blockOffset, hdr.blockLength(), hdr.version(), length);
        readSystemHeader(view, sequenced.header());
        // The body carries no messageHeader — systemEventType named it — so blockLength and version
        // stay 0 and a consumer supplies its own decoder's compiled constants (§7, V-3).
        view.payloadLength = sequenced.bodyLength();
        view.payload = sequenced.body();
        view.valid = true;
        return view;
    }

    if (frameTemplateId == sbe::frame::ClusterHeartbeat::sbeTemplateId() ||
        frameTemplateId == sbe::frame::LeadershipChanged::sbeTemplateId() ||
        frameTemplateId == sbe::frame::GatewayActive::sbeTemplateId())
    {
        if (length < blockOffset + sbe::frame::SequencedSystemHeader::encodedLength())
        {
            return view;
        }
        sbe::frame::SequencedSystemHeader header;
        header.wrap(bytes, blockOffset, hdr.version(), length);
        readSystemHeader(view, header);
        // No body: the fields are inline in the frame's own block, so that block is what a consumer
        // wraps its decoder over — with its own compiled constants, exactly as for a submitted body.
        view.payload = frame + blockOffset;
        view.payloadLength = length - blockOffset;
        view.valid = true;
    }
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

// The same for a system frame's message, which carries no framing of its own: header.systemEventType
// named it, so the block length and version come from this build's own decoder rather than from the
// wire (§7, V-3). One helper for both system shapes — a submitted body and a synthesized template's
// inline block are alike in carrying no declaration, and `payload` addresses each of them.
template<typename Decoder>
Decoder decodeSystem(const char* message, const std::uint64_t messageLength)
{
    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(message), 0, Decoder::sbeBlockLength(), Decoder::sbeSchemaVersion(),
                          messageLength);
    return decoder;
}

template<typename Decoder>
Decoder decodeSystem(const SequencedEvent& event)
{
    return decodeSystem<Decoder>(event.payload, event.payloadLength);
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
 * A connection at a producer opening (ConnectionOpened) or closing
 * (ConnectionClosed). Published by the producer that owns it — these are external
 * events it observes, forwarded on ingress like any other message, not something the
 * sequencer synthesizes.
 *
 * sourceId/connectionId name the connection the event refers to, and both are needed:
 * connectionId is unique only within the publishing producer process, so a consumer serving
 * one producer must match sourceId before acting on a connectionId (see FixGateway).
 */
struct LifecycleEvent
{
    std::int64_t globalSeqNo;
    std::int32_t sourceId;         ///< publishing producer process (header.sourceId)
    std::int32_t connectionId;     ///< connection at that producer (header.connectionId)
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
