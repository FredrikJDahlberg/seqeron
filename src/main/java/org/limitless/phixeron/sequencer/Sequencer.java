package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.frame.ClientConnectedDecoder;
import org.limitless.phixeron.sbe.frame.ClientDisconnectedDecoder;
import org.limitless.phixeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveEncoder;
import org.limitless.phixeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.phixeron.sbe.frame.GatewayStartedDecoder;
import org.limitless.phixeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.frame.LeadershipChangedEncoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.frame.SequencedEncoder;
import org.limitless.phixeron.sbe.frame.SequencedHeaderEncoder;
import org.limitless.phixeron.sbe.frame.UnsequencedDecoder;
import org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.phixeron.util.Logger;

/**
 * The sequencer's replicated state machine, free of every Aeron type.
 *
 * <p>Owns the entire replicated state ({@code globalSeqNo}, plus the leader id kept only to
 * de-duplicate leadership events) and all frame encoding. Each {@code sequence*}/event method
 * assigns the next {@code globalSeqNo}, encodes one {@code Sequenced} frame ({@code sbe-frame.xml},
 * schema 210) into {@link #buffer()} starting at offset 0, and returns its length — or {@code 0} when
 * the event produces no frame. The caller publishes {@code buffer()[0, length)} and does nothing else:
 * every decision that must be identical on every node lives here.
 *
 * <p>That split is what makes the state machine testable without a cluster, a media driver, or any
 * Aeron mock — {@link SequencerService} is the thin adapter that owns the tap publication, the
 * archive, and timer scheduling, and it is the only part that needs a live cluster to exercise.
 *
 * <p><b>The copy-through.</b> Every ingress message is an {@code Unsequenced} frame whose body is one
 * opaque length-prefixed payload, named by {@code header.payloadId} and owned by whoever that number
 * names. Sequencing is a copy of the 18-byte {@code unsequencedHeader} with the 16-byte stamp appended
 * ({@code globalSeqNo} and the consensus {@code timestamp}), and the payload copied through
 * byte-identical, never re-encoded (<b>E-1</b>). {@link #sequenceMessage} therefore needs to know
 * nothing about any application's message types — it opens {@code payloadId} 1, the core payloads that
 * are seqeron's own, and nothing else (<b>S-2</b>).
 *
 * <p><b>Determinism.</b> Every method is a pure function of its arguments and the current state —
 * no clock reads, no randomness, no I/O — so replaying the same call sequence on any node produces
 * byte-identical frames and the same final state. The consensus {@code timestamp} is always passed
 * in by the caller, never read here.
 *
 * <p>Single-threaded by contract: driven only from the cluster's conductor thread, so the shared
 * encoders and buffer need no synchronisation.
 */
public final class Sequencer {
    /**
     * header.sourceId/connectionId for events synthesized by the sequencer itself (ClusterHeartbeat /
     * LeadershipChanged): a clock heartbeat or an election has no gateway-process or TCP-level
     * connection id to carry, unlike the ingress messages it forwards.
     *
     * <p>ClientConnected/ClientDisconnected are deliberately not in that list. They denote a FIX
     * client's TCP session opening and closing — external events the gateway observes and publishes
     * on ingress like any other message, carrying the real sourceId/connectionId of the connection
     * they describe. The sequencer used to synthesize them for Aeron <em>cluster</em> sessions
     * instead, which named a different thing entirely and left the actual TCP lifecycle absent from
     * the log; nothing consumed the cluster-session form, so it was removed rather than renamed.
     */
    public static final int NO_SOURCE_ID = -1;

    /** {@link #leadershipChanged} and friends return this when the event produces no frame. */
    public static final int NO_FRAME = 0;

    /**
     * {@link #sessionClosed} returns this instead of {@link #NO_FRAME} when the closing session <em>was</em>
     * an active gateway's, but {@link #promotionTarget} found no standby to hand over to (fail closed
     * rather than name a nonexistent instance). Distinct from {@code NO_FRAME} so the caller can tell "this
     * session was never a gateway's" from "a gateway just went away and nothing replaced it" — the latter
     * leaves the cluster with no active instance of that logical gateway and is worth alerting on.
     */
    public static final int NO_PROMOTION_TARGET = -1;

    /**
     * No gateway instance: {@link #promotionTarget} found no sibling, {@link #rowFor} no row for an
     * instance, {@link #takeOverdueActivation} nothing overdue.
     */
    private static final int NO_GATEWAY_ID = -1;

    /**
     * Period of the internal cluster clock ({@link #clusterHeartbeat}): the leader fires this timer once per
     * second and every node emits a header-only {@code ClusterHeartbeat} carrying the consensus timestamp. It exists
     * so every consumer has a cluster-driven clock that keeps advancing even while an individual FIX
     * session is silent — which is exactly when the gateway's keepalive watchdog must probe/disconnect
     * (the sequenced-header timestamp is the only clock the watchdog is allowed to trust, since only the
     * leader assigns real time). 1 Hz gives ±1 s resolution, ample for the watchdog's tens-of-seconds
     * thresholds. Trade-off: every heartbeat appends a timer event + a heartbeat frame to the replicated
     * log/recording, so full-log-replay recovery grows with uptime; this constant is the single knob to
     * trade watchdog resolution against that cost. (A tighter win — gating clock emission on active FIX
     * sessions — is noted in doc/gap.md; 1 Hz is the low-risk interim.)
     *
     * <p>It lives here rather than in the adapter because the state machine's own deadlines are evaluated
     * in cluster time, on heartbeat timestamps, so this is the resolution every one of them is quantised to.
     * Public because it is a contract rather than an internal: consumers size their own tap watchdogs in
     * heartbeat periods (the C++ edge duplicates it as
     * {@code FixGateway::CLUSTER_HEARTBEAT_INTERVAL_MS} for want of a way to
     * share it), and a watchdog tighter than the clock it watches fires on a healthy stream.
     */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_MS = 1000;

    /**
     * How long a designated instance has, in cluster time, to answer a {@code GatewayActive} with a
     * {@code GatewayStarted} before {@link #pendingGatewayActivationTimeout} hands the role to a sibling.
     *
     * <p>Five heartbeats, matching the order of the cluster's own {@code sessionTimeoutNs} — this is a failover
     * deadline, and a gateway tier with no active instance is down. What it bounds is small: observe a
     * frame on the co-located tap and publish one back. A caught-up instance does that in a duty cycle.
     *
     * <p>It deliberately does <em>not</em> clear a cold start, which has no useful bound (there are no
     * snapshots, so a late-in-the-day start replays the whole log). A pair that is still replaying
     * therefore trades the role every five heartbeats until one of them catches up, and that is cheap: a
     * superseded instance keeps replaying ({@code ExchangeGateway.standDown} is a no-op before the socket
     * exists), whoever finishes first answers the next activation naming it, and the churn is one frame
     * per period against a log already taking 60 heartbeats a minute. An instance that <em>has</em> answered is
     * never swapped out — {@link #takeOverdueActivation} drops its deadline instead — so steady state
     * costs nothing at all.
     *
     * <p>Package-private so {@code SequencerTest} drives exactly this deadline rather than hardcoding it
     * a second time.
     */
    static final long GATEWAY_ACTIVATION_TIMEOUT_MS = 5 * CLUSTER_HEARTBEAT_INTERVAL_MS;

    /**
     * The one {@code payloadId} the sequencer decodes: seqeron's own core payloads
     * (doc/seqeron-protocol-spec.md §6.1). Every other value is copied through opaque.
     */
    public static final int CORE_PAYLOAD_ID = CoreFrame.PAYLOAD_ID;

    /**
     * Smallest {@code Unsequenced} frame: the framing header, the 18-byte {@code unsequencedHeader} and
     * the payload's own 2-byte length prefix. A frame this size carries an empty payload, which is legal.
     */
    static final int MIN_FRAME_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH + UnsequencedHeaderDecoder.ENCODED_LENGTH +
                                        UnsequencedDecoder.payloadHeaderLength();

    /**
     * {@code varDataEncoding}'s {@code nullValue}. A prefix of 65535 is "absent", not a 65535-byte payload,
     * and admitting it would read the frame a length short of what it claims.
     */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    /** Offset of the payload's length prefix in every frame this class encodes. */
    private static final int TAP_PAYLOAD_PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;

    /** Offset of the payload itself, one length prefix past that. */
    private static final int TAP_PAYLOAD_OFFSET = TAP_PAYLOAD_PREFIX_OFFSET + UnsequencedDecoder.payloadHeaderLength();

    /**
     * Largest value the framing header's uint16 {@code blockLength} can carry — 65535 is SBE's null
     * value for the type. {@code MessageHeaderEncoder.blockLength(int)} casts to {@code short} without
     * complaint, so anything above this has to be refused before it is written.
     */
    private static final int MAX_BLOCK_LENGTH = 65534;

    // Frame decode (schema 210, sbe-frame.xml).
    private final MessageHeaderDecoder msgHeaderDecoder = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeaderDecoder = new UnsequencedHeaderDecoder();

    // Core payload decode (payloadId 1). Body fields are read from exactly two of them — GatewayRegistered
    // and GatewayStarted, whose scalars feed the derived topology below; the rest are matched on
    // templateId alone, off identity the frame header already carries.
    private final MessageHeaderDecoder payloadHeaderDecoder = new MessageHeaderDecoder();
    private final GatewayRegisteredDecoder gatewayRegisteredDecoder = new GatewayRegisteredDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();

    // Frame encode (schema 210). headerEncoder writes the outer framing header and, on a synthesized
    // frame, the payload's own — at two different offsets, the outer one already written by then.
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SequencedHeaderEncoder tapHeaderEncoder = new SequencedHeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final ClusterHeartbeatEncoder clusterHeartbeatEncoder = new ClusterHeartbeatEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();

    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    // Topology
    /** One Gateway row: an instance ({@code gatewayId}) of a logical gateway ({@code gatewaySourceId}). */
    private record GatewayRow(int gatewayId, int gatewaySourceId, short preferenceRank) { }

    /**
     * Every roster row seen, in log order, de-duplicated on {@code gatewayId} so a re-published roster
     * (an operator re-running {@code load-topology}) re-asserts rather than duplicates. Read only by {@link #promotionTarget},
     * and only ever by index, so the iteration order is the log's and every node agrees.
     */
    private final java.util.List<GatewayRow> gatewayRows = new java.util.ArrayList<>();

    /**
     * The {@code gatewayId}s the bootstrap still has to activate — one per logical gateway, the rank-0
     * row of each {@code gatewaySourceId}, filled from {@link #gatewayRows} behind the first complete
     * roster and drained one frame per call by {@link #pendingGatewayBootstrapActivation}.
     *
     * <p>One per <em>logical</em> gateway, because the deployment has more than one: the client-facing
     * pair and the exchange-facing pair elect independently and neither may activate the other's
     * instances. A single designated primary here (which is what this was) let whichever rank-0 row
     * loaded last silently take the other pair's bootstrap. Empty when no row designated a primary —
     * nothing is activated, which is the fail-closed answer.
     */
    private final java.util.ArrayDeque<Integer> bootstrapActivations = new java.util.ArrayDeque<>();

    // Replicated state (advanced identically on every node; not snapshotted)

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * memberId of whichever node last reported itself the leader; -1 until the first leadership
     * event. Kept only to de-duplicate leadership changes and to stamp {@code newLeaderMemberId}
     * onto the synthesized {@code LeadershipChanged}.
     */
    private int currentLeaderMemberId = -1;

    /**
     * The cluster session each <em>active</em> FIX gateway instance is attached on, mapped to that
     * instance's {@code gatewayId}, so {@link #sessionClosed} knows which instance it just lost and can
     * promote a sibling. Added in {@link #sequenceMessage} on a {@code GatewayStarted}, removed in
     * {@link #sessionClosed}. Replicated state: built identically on every node, so every node promotes
     * at the same close.
     *
     * <p><b>{@code GatewayStarted} is the only thing that puts a session in here, and that is load-bearing.</b>
     * This used to key off {@code header.sourceId} landing in the set of known gateway sourceIds, which
     * is not an assertion the publisher makes about itself: that field is the <em>routing</em> id of the
     * gateway a message is travelling to or from, and other clients legitimately echo it — the
     * OrderExecServer stamps the originating gateway's sourceId onto every ExecutionReport and
     * PortfolioQueryReply it submits. Its cluster session was therefore recorded as a gateway's, and an
     * ordinary OrderExecServer restart promoted the standby out from under a perfectly healthy primary.
     * {@code GatewayStarted} is published by a gateway about itself, on activation and nowhere else, so
     * it is the one frame that means what this map needs it to mean.
     */
    private final java.util.Map<Long, Integer> activeGatewaySession = new java.util.HashMap<>();

    /**
     * The connections currently open at each gateway: {@code header.sourceId} to the set of {@code
     * header.connectionId}s that have had a {@code ClientConnected} and no {@code ClientDisconnected}
     * yet, maintained from those frames as they pass through {@link #sequenceMessage} — the same
     * pattern as {@link #gatewayRows}. Replicated state: every node sees the same frames in the same
     * order and holds the same set. Only ever {@code add}/{@code remove}/{@code size}-d, never
     * iterated, so its hash order cannot reach a frame.
     */
    private final java.util.Map<Integer, java.util.Set<Integer>> openConnections = new java.util.HashMap<>();

    /**
     * Count of TCP clients currently connected across every gateway: {@link #openConnections}'s total
     * size, maintained incrementally rather than summed.
     *
     * <p>A live gauge, not a running tally. It moves only when a connection actually enters or leaves
     * that map, so an unmatched {@code ClientDisconnected} cannot take it negative and a repeated
     * {@code ClientConnected} cannot double-count — and a gateway that dies without disconnecting its
     * clients has its still-open connections released by the {@code GatewayStarted} its successor
     * publishes (see {@link #releaseStaleConnections}), which is what keeps this from drifting upward
     * over a day of gateway restarts.
     */
    private int connectedClientCount = 0;

    /** True once the bootstrap {@code GatewayActive} has been synthesized (on the first complete roster). */
    private boolean bootstrapActivationEmitted = false;

    /** An outstanding {@code GatewayActive}: which instance was named, and when it stops being excused. */
    private record PendingActivation(int gatewaySourceId, int gatewayId, long deadline) { }

    /**
     * Every {@code GatewayActive} synthesized but not yet answered by a {@code GatewayStarted}, at most
     * one per logical gateway — armed by {@link #gatewayActive}, resolved by {@link
     * #pendingGatewayActivationTimeout} (see there for why an activation needs a deadline at all).
     *
     * <p>Keyed on {@code gatewaySourceId} rather than held as a single outstanding activation, because
     * the two logical gateways bootstrap back to back: the second activation used to overwrite the
     * first's deadline, so a designated instance that never arrived was never handed over. A list in
     * arm order, replaced in place, so the iteration {@link #pendingGatewayActivationTimeout} walks is
     * the log's on every node — the same reason {@link #gatewayRows} is not a map.
     */
    private final java.util.List<PendingActivation> pendingActivations = new java.util.ArrayList<>();

    /**
     * This node's cluster memberId, for the diagnostic slot in {@link #reject}'s log line. Node-local
     * and <em>not</em> replicated state — no state transition reads it, so it cannot reach a frame —
     * which is also why it is set rather than constructed: {@code cluster.memberId()} is still
     * {@code NULL_VALUE} while this class is being built (see {@code SequencerService.ensureCounters}).
     * Null until then, which the logger renders as no member context rather than a wrong one.
     */
    private Integer memberId;

    /** Topology is derived from the sequenced Gateway rows (see {@link #gatewayRows}), not configured. */
    public Sequencer() {
    }

    /** The buffer every encode writes into, from offset 0. Valid up to the length just returned. */
    public MutableDirectBuffer buffer() {
        return encodeBuffer;
    }

    /** The last assigned sequence number; 0 before anything has been sequenced. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** memberId of the last observed leader; -1 until the first {@link #leadershipChanged}. */
    public int currentLeaderMemberId() {
        return currentLeaderMemberId;
    }

    /** Count of TCP clients currently connected across every gateway; 0 before any {@code ClientConnected}. */
    public int connectedClientCount() {
        return connectedClientCount;
    }

    /**
     * Names this node in the rejection log line; see {@link #memberId}.
     * @param memberId this node's cluster memberId
     */
    public void memberId(final int memberId) {
        this.memberId = memberId;
    }

    /**
     * Re-stamps one {@code Unsequenced} ingress frame as a {@code Sequenced} one, copying its payload
     * through verbatim — see the class Javadoc.
     *
     * @param buffer    holding the ingress message
     * @param offset    of the ingress message's outer {@code MessageHeader}
     * @param length    of the whole ingress message, framing header included
     * @param sessionId Aeron Cluster session the message arrived on
     * @param timestamp cluster consensus time to stamp
     * @return length of the encoded frame in {@link #buffer()}, or {@link #NO_FRAME} if the ingress
     *     message was malformed and skipped
     */
    public int sequenceMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                               final long timestamp) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return reject("length " + length + " is below the " + MessageHeaderDecoder.ENCODED_LENGTH +
                          "-byte framing header");
        }
        msgHeaderDecoder.wrap(buffer, offset);
        if (msgHeaderDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return reject("schemaId " + msgHeaderDecoder.schemaId() + " is not " + MessageHeaderDecoder.SCHEMA_ID);
        }
        return sequenceFrame(buffer, offset, length, sessionId, timestamp);
    }

    /**
     * Sequences an {@code Unsequenced} frame: the envelope's copy-18/append-16, plus the core state the
     * sequencer derives when the payload is its own.
     *
     * <p>The payload is copied verbatim, its length prefix included, and is never re-encoded (<b>E-1</b>).
     * Decoding it at all happens only for {@code payloadId} 1, and only after the bounds each read needs
     * have been established — reading a {@code templateId} out of an unverified payload is reading a
     * foreign schema's numbering as core's (<b>P-4</b>).
     */
    private int sequenceFrame(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                              final long timestamp) {
        if (length < MIN_FRAME_LENGTH) {
            return reject("length " + length + " is below the " + MIN_FRAME_LENGTH + "-byte minimum framing");
        }
        if (msgHeaderDecoder.version() != MessageHeaderDecoder.SCHEMA_VERSION) {
            return reject("version " + msgHeaderDecoder.version() + " is not " +
                          MessageHeaderDecoder.SCHEMA_VERSION);
        }
        if (msgHeaderDecoder.templateId() != UnsequencedDecoder.TEMPLATE_ID) {
            return reject("templateId " + msgHeaderDecoder.templateId() + " is not Unsequenced (" +
                          UnsequencedDecoder.TEMPLATE_ID + ")");
        }
        // An equality, not a floor, and against the composite's own constant: a short blockLength puts the
        // var-data prefix inside the header composite, a long one silently drops bytes off the end.
        if (msgHeaderDecoder.blockLength() != UnsequencedHeaderDecoder.ENCODED_LENGTH) {
            return reject("blockLength " + msgHeaderDecoder.blockLength() + " is not " +
                          UnsequencedHeaderDecoder.ENCODED_LENGTH);
        }

        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int prefixOffset = bodyOffset + UnsequencedHeaderDecoder.ENCODED_LENGTH;
        final int payloadLength = buffer.getShort(prefixOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        if (payloadLength == NULL_PAYLOAD_LENGTH || MIN_FRAME_LENGTH + payloadLength != length) {
            return reject("payload length " + payloadLength + " does not fit a " + length + "-byte frame");
        }

        frameHeaderDecoder.wrap(buffer, bodyOffset);
        final int payloadId = frameHeaderDecoder.payloadId();
        if (payloadId == 0) {
            return reject("payloadId 0 is not a protocol");
        }
        final int sourceId = frameHeaderDecoder.sourceId();
        final int connectionId = frameHeaderDecoder.connectionId();

        if (payloadId == CORE_PAYLOAD_ID &&
            !applyCore(buffer, prefixOffset + UnsequencedDecoder.payloadHeaderLength(), payloadLength, sourceId,
                       connectionId, sessionId)) {
            return NO_FRAME;
        }

        final long globalSeq = ++globalSeqNo;
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(SequencedEncoder.BLOCK_LENGTH)
            .templateId(SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        tapHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sourceId(sourceId)
            .connectionId(connectionId)
            .sessionId(sessionId)
            .payloadId(payloadId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        encodeBuffer.putBytes(TAP_PAYLOAD_PREFIX_OFFSET, buffer, prefixOffset,
                              UnsequencedDecoder.payloadHeaderLength() + payloadLength);
        return TAP_PAYLOAD_OFFSET + payloadLength;
    }

    /**
     * The core state the sequencer derives from a {@code payloadId} 1 payload, and the only place it
     * decodes one. Returns {@link #NO_FRAME} to admit the frame, or a rejection.
     *
     * <p>Each condition establishes what the next may read: fitting the frame exactly says nothing about
     * being long enough for a body field, and the floor a read needs is the decoder's compiled block
     * length, never the wire's — an SBE decoder reads a fixed-width field at its fixed offset whatever
     * acting block length it was wrapped with.
     */
    private boolean applyCore(final DirectBuffer buffer, final int payloadOffset, final int payloadLength,
                              final int sourceId, final int connectionId, final long sessionId) {
        if (payloadLength < MessageHeaderDecoder.ENCODED_LENGTH) {
            return rejectPayload("core payload of " + payloadLength + " bytes is shorter than its framing header");
        }
        payloadHeaderDecoder.wrap(buffer, payloadOffset);
        if (payloadHeaderDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return rejectPayload("core payload declares schemaId " + payloadHeaderDecoder.schemaId() + ", not " +
                          MessageHeaderDecoder.SCHEMA_ID);
        }
        final int templateId = payloadHeaderDecoder.templateId();
        if (templateId == ClusterHeartbeatDecoder.TEMPLATE_ID || templateId == LeadershipChangedDecoder.TEMPLATE_ID) {
            return rejectPayload("templateId " + templateId + " is synthesized by the cluster and illegal on ingress");
        }

        final int bodyOffset = payloadOffset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int bodyLength = payloadLength - MessageHeaderDecoder.ENCODED_LENGTH;
        final int version = payloadHeaderDecoder.version();

        if (templateId == GatewayRegisteredDecoder.TEMPLATE_ID) {
            if (bodyLength < GatewayRegisteredDecoder.BLOCK_LENGTH) {
                return rejectPayload("GatewayRegistered body of " + bodyLength + " bytes is short of " +
                              GatewayRegisteredDecoder.BLOCK_LENGTH);
            }
            gatewayRegisteredDecoder.wrap(buffer, bodyOffset, GatewayRegisteredDecoder.BLOCK_LENGTH, version);
            addGatewayRow(gatewayRegisteredDecoder.gatewayId(), gatewayRegisteredDecoder.gatewaySourceId(),
                          gatewayRegisteredDecoder.preferenceRank());
            // remaining == 0 is the roster's last row, and the whole completeness edge: the publisher
            // counts the rows it read, so the cluster never has to infer "have I seen everyone?".
            if (gatewayRegisteredDecoder.remaining() == 0 && !bootstrapActivationEmitted) {
                bootstrapActivationEmitted = true;
                for (final GatewayRow row : gatewayRows) {
                    if (row.preferenceRank() == 0) {
                        bootstrapActivations.add(row.gatewayId());
                    }
                }
            }
        } else if (templateId == GatewayStartedDecoder.TEMPLATE_ID) {
            if (bodyLength < GatewayStartedDecoder.BLOCK_LENGTH) {
                return rejectPayload("GatewayStarted body of " + bodyLength + " bytes is short of " +
                              GatewayStartedDecoder.BLOCK_LENGTH);
            }
            gatewayStartedDecoder.wrap(buffer, bodyOffset, GatewayStartedDecoder.BLOCK_LENGTH, version);
            activeGatewaySession.put(sessionId, gatewayStartedDecoder.gatewayId());
            releaseStaleConnections(sourceId);
        } else if (templateId == ClientConnectedDecoder.TEMPLATE_ID) {
            if (openConnections.computeIfAbsent(sourceId, source -> new java.util.HashSet<>()).add(connectionId)) {
                connectedClientCount++;
            }
        } else if (templateId == ClientDisconnectedDecoder.TEMPLATE_ID) {
            final java.util.Set<Integer> open = openConnections.get(sourceId);
            if (open != null && open.remove(connectionId)) {
                connectedClientCount--;
            }
        }
        return true;
    }

    /**
     * Skips a malformed ingress message
     * @param reason rejection description
     */
    private boolean rejectPayload(final String reason) {
        reject(reason);
        return false;
    }

    /**
     * Skips a malformed ingress message
     * @param reason rejection description
     */
    private int reject(final String reason) {
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.MalformedIngressMessage, memberId,
                     "skipping malformed ingress message: %s (globalSeqNo stays %d)", reason, globalSeqNo);
        return NO_FRAME;
    }

    /**
     * Encodes one internal clock frame carrying the consensus timestamp. Consumers (the FIX gateway)
     * read {@code header.timestamp} off it to keep their session clock moving while a counterparty is silent.
     * @param timestamp now
     */
    public int clusterHeartbeat(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        beginSynthesized(globalSeq, timestamp);
        clusterHeartbeatEncoder.wrapAndApplyHeader(encodeBuffer, TAP_PAYLOAD_OFFSET, headerEncoder);
        return endSynthesized(MessageHeaderEncoder.ENCODED_LENGTH + clusterHeartbeatEncoder.encodedLength());
    }

    /**
     * Encodes a {@code LeadershipChanged} event, or {@link #NO_FRAME} if this leader is already the
     * one on record. De-duplicating here (rather than at the caller) keeps the {@code globalSeqNo}
     * advance and the suppression decision in one place, so every node consumes the same number of
     * sequence numbers for the same log.
     * @param leaderMemberId leader member identity
     * @param timestamp now
     */
    public int leadershipChanged(final int leaderMemberId, final long timestamp) {
        if (leaderMemberId == currentLeaderMemberId) {
            return NO_FRAME;
        }
        currentLeaderMemberId = leaderMemberId;
        final long globalSeq = ++globalSeqNo;
        beginSynthesized(globalSeq, timestamp);
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, TAP_PAYLOAD_OFFSET, headerEncoder);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        return endSynthesized(MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength());
    }

    /**
     * The bootstrap activations, synthesized once behind the roster's last row: the cluster designates
     * the primary of each logical gateway by naming its {@code gatewayId} in a {@code GatewayActive}, so
     * exactly one instance of each pair opens its accept gate at cold start and its standby waits.
     *
     * <p><b>One frame per call.</b> There is one activation per logical gateway and each takes its own
     * {@code globalSeqNo}, so the adapter calls this in a loop until {@link #NO_FRAME} — emitting what
     * comes back before asking again, since every call re-encodes into the same {@link #buffer()}. The
     * loop runs right after the {@link #sequenceMessage} that sequenced the roster's last row, so the
     * frames take the next {@code globalSeqNo}s in {@link #gatewayRows} order — identically on every node
     * and on replay.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, or {@link #NO_FRAME} when none is left pending
     */
    public int pendingGatewayBootstrapActivation(final long timestamp) {
        final Integer gatewayId = bootstrapActivations.poll();
        // Empty when no roster row designated a primary — nothing to activate (fail closed).
        return gatewayId == null ? NO_FRAME : gatewayActive(gatewayId, timestamp);
    }

    /**
     * A cluster session closed. If it was the session an active FIX gateway declared itself on (via
     * {@code GatewayStarted}), promote a standby of the same logical gateway; otherwise no frame.
     * @param sessionId session identity
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, {@link #NO_FRAME} if this wasn't a gateway session, or
     *     {@link #NO_PROMOTION_TARGET} if it was one but no standby could be found
     */
    public int sessionClosed(final long sessionId, final long timestamp) {
        final Integer closedGatewayId = activeGatewaySession.remove(sessionId);
        if (closedGatewayId == null) {
            return NO_FRAME;
        }
        final int promoted = promotionTarget(closedGatewayId);
        if (promoted == NO_GATEWAY_ID) {
            Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d's session closed with no standby to promote — this logical "
                             + "gateway has no active instance until one starts (globalSeqNo stays %d)",
                         closedGatewayId, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        return gatewayActive(promoted, timestamp);
    }

    /**
     * The other way a logical gateway loses its active instance: the one just designated never declares
     * itself started. Every activation — bootstrap and promotion alike — is answered by a {@code
     * GatewayStarted} the instance publishes when it opens its accept gate, and that is the only frame
     * that registers it in {@link #activeGatewaySession}. An instance that dies, or wedges, before ever
     * getting there was therefore never a registered instance, so nothing about its session closing (or
     * never closing) can promote a sibling — the cluster simply has no gateway, and no path back.
     *
     * <p>This is that path: {@link #GATEWAY_ACTIVATION_TIMEOUT_MS} after a {@code GatewayActive}, an
     * instance that has not declared itself started hands the role to its next-ranked sibling, exactly as
     * {@link #sessionClosed} does. The promotion arms the same deadline on the instance it names, so a
     * whole gateway tier that is down converges the moment any instance comes up rather than depending on
     * which one the cluster happened to designate first.
     *
     * <p>Deterministic off the cluster clock: driven from the 1 Hz {@code ClusterHeartbeat}'s consensus timestamp, so
     * every node evaluates the same deadline against the same time and synthesizes the same frame — like
     * {@link #pendingGatewayBootstrapActivation}, the caller invokes it right after the {@link #clusterHeartbeat} it
     * belongs to and simply publishes what comes back.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, {@link #NO_FRAME} if nothing was overdue, or {@link
     *     #NO_PROMOTION_TARGET} if an activation went unanswered and there was no sibling to hand it to
     */
    public int pendingGatewayActivationTimeout(final long timestamp) {
        final int designated = takeOverdueActivation(timestamp);
        if (designated == NO_GATEWAY_ID) {
            return NO_FRAME;
        }
        final int promoted = promotionTarget(designated);
        if (promoted == NO_GATEWAY_ID) {
            Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d never declared itself started within %dms and has no sibling to "
                             + "promote — this logical gateway has no active instance until one starts "
                             + "(globalSeqNo stays %d)",
                         designated, GATEWAY_ACTIVATION_TIMEOUT_MS, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayActivationTimeout, memberId,
                     "gateway instance %d never declared itself started within %dms of being designated — "
                         + "handing the role to instance %d",
                     designated, GATEWAY_ACTIVATION_TIMEOUT_MS, promoted);
        return gatewayActive(promoted, timestamp);
    }

    /**
     * Removes and returns the first activation whose deadline has passed and that no {@code
     * GatewayStarted} answered, or {@link #NO_GATEWAY_ID} if none is overdue. An overdue activation the
     * instance did answer is dropped too — it is simply resolved, and leaving it would have it looked at
     * on every heartbeat from here on.
     *
     * <p>The pending entry is taken before the caller decides anything, so the {@code gatewayActive} that
     * a hand-over ends in cannot re-enter {@link #pendingActivations} mid-iteration.
     * @param timestamp now
     */
    private int takeOverdueActivation(final long timestamp) {
        for (final java.util.Iterator<PendingActivation> it = pendingActivations.iterator(); it.hasNext(); ) {
            final PendingActivation pending = it.next();
            if (timestamp < pending.deadline()) {
                continue;
            }
            it.remove();
            if (!activeGatewaySession.containsValue(pending.gatewayId())) {
                return pending.gatewayId();
            }
        }
        return NO_GATEWAY_ID;
    }

    /**
     * The {@code gatewayId} to hand over to when instance {@code closedGatewayId} goes away: the
     * lowest-{@code preferenceRank} other instance of the same logical gateway, ties broken by log
     * order, or {@link #NO_GATEWAY_ID} if that instance has no known row or no sibling.
     *
     * <p>A <b>{@code gatewayId}</b>, never the {@code gatewaySourceId} this used to promote with. Those
     * are separate id spaces, and every instance of a pair shares the sourceId, so a {@code
     * GatewayActive} carrying one designated <em>both</em> instances at once — which the consumer could
     * only survive by latching the first match it ever saw, and that in turn made a restarting instance
     * re-activate itself off a superseded frame during cold-start replay.
     * @param closedGatewayId the instance whose session just closed
     */
    private int promotionTarget(final int closedGatewayId) {
        final GatewayRow closed = rowFor(closedGatewayId);
        if (closed == null) {
            return NO_GATEWAY_ID;
        }
        GatewayRow best = null;
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewaySourceId() == closed.gatewaySourceId() && row.gatewayId() != closedGatewayId &&
                (best == null || row.preferenceRank() < best.preferenceRank())) {
                best = row;
            }
        }
        return best == null ? NO_GATEWAY_ID : best.gatewayId();
    }

    /**
     * Drops every connection still open under {@code gatewaySourceId}. A {@code GatewayStarted} is a new
     * instance declaring it has taken that logical gateway over, so anything still open under it belongs
     * to the instance that went away, whose sockets died with it — a crash cannot publish the {@code
     * ClientDisconnected}s that would have closed them out, which is the whole reason that frame exists
     * (it carries the {@code firstConnectionId} the new instance resumes allocating from for the same
     * reason). Without this, every gateway crash leaves its clients counted forever.
     *
     * <p>It cannot drop a live connection: a gateway publishes {@code GatewayStarted} before it opens its
     * accept gate ({@code FixGateway.cpp}), so none of its own {@code ClientConnected}s can precede it.
     * @param gatewaySourceId the logical gateway whose epoch just rolled
     */
    private void releaseStaleConnections(final int gatewaySourceId) {
        final java.util.Set<Integer> stale = openConnections.remove(gatewaySourceId);
        if (stale != null) {
            connectedClientCount -= stale.size();
        }
    }

    /** Records a Gateway row, replacing any earlier row for the same {@code gatewayId} in place. */
    private void addGatewayRow(final int gatewayId, final int gatewaySourceId, final short preferenceRank) {
        final GatewayRow row = new GatewayRow(gatewayId, gatewaySourceId, preferenceRank);
        for (int i = 0; i < gatewayRows.size(); i++) {
            if (gatewayRows.get(i).gatewayId() == gatewayId) {
                gatewayRows.set(i, row);
                return;
            }
        }
        gatewayRows.add(row);
    }

    /**
     * Encodes one {@code GatewayActive} naming {@code gatewayId}; advances {@code globalSeqNo}. Arms the
     * activation deadline on the instance it names — every activation is a claim the instance still has
     * to answer (see {@link #pendingGatewayActivationTimeout}), so arming here is what keeps bootstrap
     * and promotion from needing to remember to.
     * @param gatewayId gateway identity
     * @param timestamp now
     */
    private int gatewayActive(final int gatewayId, final long timestamp) {
        armActivationDeadline(gatewayId, timestamp);
        final long globalSeq = ++globalSeqNo;
        beginSynthesized(globalSeq, timestamp);
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, TAP_PAYLOAD_OFFSET, headerEncoder);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return endSynthesized(MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength());
    }

    /**
     * Opens a frame the cluster synthesized on its own initiative, leaving {@link #encodeBuffer} ready for
     * the caller to encode its core payload at {@link #TAP_PAYLOAD_OFFSET}.
     *
     * <p>These are the frames with no producer, so <b>F-4</b>'s {@code -1} stands in for the identity an
     * ingress frame carries, and <b>E-1</b>'s exception applies: every node encodes its own copy rather
     * than copying one through, which is why the encode must be a pure function of its arguments.
     */
    private void beginSynthesized(final long globalSeq, final long timestamp) {
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(SequencedEncoder.BLOCK_LENGTH)
            .templateId(SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        tapHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .payloadId(CORE_PAYLOAD_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
    }

    /**
     * Closes a synthesized frame by writing the payload's length prefix, which is only known once the
     * payload is encoded.
     * @param payloadLength bytes the caller wrote at {@link #TAP_PAYLOAD_OFFSET}
     * @return length of the whole frame
     */
    private int endSynthesized(final int payloadLength) {
        encodeBuffer.putShort(TAP_PAYLOAD_PREFIX_OFFSET, (short) payloadLength, java.nio.ByteOrder.LITTLE_ENDIAN);
        return TAP_PAYLOAD_OFFSET + payloadLength;
    }

    /**
     * Puts {@code gatewayId} on the clock as its logical gateway's outstanding activation, replacing any
     * earlier one for that {@code gatewaySourceId} in place — a fresh activation supersedes the claim the
     * previous one made, exactly as it does for the consumers. An instance with no Gateway row is not
     * armed: nothing could be promoted in its place anyway, since {@link #promotionTarget} finds siblings
     * through that row.
     * @param gatewayId the instance just designated
     * @param timestamp now
     */
    private void armActivationDeadline(final int gatewayId, final long timestamp) {
        final GatewayRow row = rowFor(gatewayId);
        if (row == null) {
            return;
        }
        final PendingActivation armed =
            new PendingActivation(row.gatewaySourceId(), gatewayId, timestamp + GATEWAY_ACTIVATION_TIMEOUT_MS);
        for (int i = 0; i < pendingActivations.size(); i++) {
            if (pendingActivations.get(i).gatewaySourceId() == row.gatewaySourceId()) {
                pendingActivations.set(i, armed);
                return;
            }
        }
        pendingActivations.add(armed);
    }

    /** The Gateway row for {@code gatewayId}, or null if no load has named that instance. */
    private GatewayRow rowFor(final int gatewayId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewayId() == gatewayId) {
                return row;
            }
        }
        return null;
    }
}
