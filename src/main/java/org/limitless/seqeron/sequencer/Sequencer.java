package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStoppedDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionClosedDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedDecoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderDecoder;
import org.limitless.seqeron.util.Logger;

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
 * <p><b>The copy-through.</b> Every ingress message is one of two shapes: an {@code Unsequenced} frame
 * whose body is one opaque length-prefixed payload named by {@code header.payloadId} and owned by
 * whoever that number names, or an {@code UnsequencedSystem} frame whose body is one of seqeron's own
 * events, named by {@code header.systemEventType} (§7). Sequencing is the same for both — a copy of the
 * 18-byte header with the 16-byte stamp appended ({@code globalSeqNo} and the consensus
 * {@code timestamp}), and the body copied through byte-identical, never re-encoded (<b>E-1</b>) — which
 * is what the two composites being byte-for-byte identical apart from that one field's name buys.
 * {@link #sequenceMessage} therefore needs to know nothing about any application's message types: it
 * decodes <b>no {@code payloadId} at all</b>, and opens only the system bodies it derives state from
 * (<b>S-2</b>).
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
     * header.sourceId/connectionId for events synthesized by the sequencer itself (ClusterHeartbeat,
     * LeadershipChanged, GatewayActive): a clock heartbeat or an election has no gateway-process or
     * TCP-level connection id to carry, unlike the ingress messages it forwards.
     *
     * <p>ConnectionOpened/ConnectionClosed are deliberately not in that list. They denote a FIX
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
     * sessions — is possible; 1 Hz is the low-risk interim.)
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
     * Core's retired {@code payloadId} (doc/seqeron-protocol-spec.md §15 step 10). Core is not an
     * application and no longer rides a payload, so 1 is refused on ingress rather than reserved and
     * decoded — a producer still on the old build fails loudly instead of having core bytes copied
     * through as an application payload.
     */
    private static final int RETIRED_CORE_ID = 1;

    /**
     * {@code varDataEncoding}'s {@code nullValue}. A prefix of 65535 is "absent", not a 65535-byte payload,
     * and admitting it would read the frame a length short of what it claims.
     */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    /** {@link #ingressBlockLength}'s answer for a {@code systemEventType} that may not be submitted. */
    private static final int NOT_INGRESS_LEGAL = -1;

    /**
     * Offset of the body's length prefix in every forwarded frame this class encodes — the same in both
     * families, because both sequenced header composites are 34 bytes (<b>F-3</b>).
     */
    private static final int TAP_BODY_PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;

    /** Offset of the body itself, one length prefix past that. */
    private static final int TAP_BODY_OFFSET = TAP_BODY_PREFIX_OFFSET + UnsequencedDecoder.payloadHeaderLength();

    // Frame decode (schema 210, sbe-frame.xml). The two ingress header composites are byte-identical
    // apart from the name of the uint16 at offset 16, so the first covers every frame-layer field of
    // both families and the second is wrapped only to read that one field under its own name.
    private final MessageHeaderDecoder msgHeaderDecoder = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeaderDecoder = new UnsequencedHeaderDecoder();
    private final UnsequencedSystemHeaderDecoder systemHeaderDecoder = new UnsequencedSystemHeaderDecoder();

    // System body decode. Fields are read from exactly three of them — GatewayRegistered, GatewayStarted
    // and GatewayActivationRequested, whose scalars feed the derived topology below; the rest are matched
    // on systemEventType alone, off identity the frame header already carries.
    private final GatewayRegisteredDecoder gatewayRegisteredDecoder = new GatewayRegisteredDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();
    private final GatewayActivationRequestedDecoder activationRequestedDecoder =
        new GatewayActivationRequestedDecoder();

    // Frame encode (schema 210). headerEncoder writes the outer framing header; tapHeaderEncoder writes
    // the three stamp fields, whose offsets are common to both sequenced header composites.
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
     * Every list row seen, in log order, de-duplicated on {@code gatewayId} so a re-published list
     * (an operator re-running {@code load-topology}) re-asserts rather than duplicates. Read only by {@link #promotionTarget},
     * and only ever by index, so the iteration order is the log's and every node agrees.
     */
    private final java.util.List<GatewayRow> gatewayRows = new java.util.ArrayList<>();

    /**
     * The {@code gatewayId}s still to be designated: the rank-0 row of each {@code gatewaySourceId} behind
     * the first complete list, and whatever a {@code GatewayActivationRequested} has since named.
     * Filled by {@link #applySystem} and drained one frame per call by {@link #pendingGatewayActivation}.
     *
     * <p>One bootstrap entry per <em>logical</em> gateway, because the deployment has more than one: the
     * client-facing pair and the exchange-facing pair elect independently and neither may activate the
     * other's instances. A single designated primary here (which is what this was) let whichever rank-0
     * row loaded last silently take the other pair's bootstrap. Empty when no row designated a primary —
     * nothing is activated, which is the fail-closed answer.
     */
    private final java.util.ArrayDeque<QueuedActivation> activationQueue = new java.util.ArrayDeque<>();

    /**
     * A designation waiting for its {@code globalSeqNo}, and which of the two paths queued it. The
     * provenance is carried because the operator's path and the cold-start path are indistinguishable
     * once drained, and {@link #bootstrapActivationEmitted} must not answer yes to the wrong one.
     */
    private record QueuedActivation(int gatewayId, boolean bootstrap) { }

    // Replicated state (advanced identically on every node; not snapshotted)

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * Ingress frames refused by §9.2, since this node started (<b>S-7</b>). Node-local and <em>not</em>
     * replicated state — nothing reads it, so it cannot reach a frame — but every node rejects the same
     * frames (<b>S-3</b>), so nodes that have applied the same log prefix must agree on it: a divergence is
     * a forked tap. {@code SequencerService} mirrors it onto the operator counter of the same name; here it
     * is what lets the conformance suite assert a rejection cost exactly one, with no Aeron in the test.
     */
    private long rejectedFrameCount = 0;

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
     * header.connectionId}s that have had a {@code ConnectionOpened} and no {@code ConnectionClosed}
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
     * that map, so an unmatched {@code ConnectionClosed} cannot take it negative and a repeated
     * {@code ConnectionOpened} cannot double-count — and a gateway that dies without disconnecting its
     * clients has its still-open connections released by the {@code GatewayStarted} its successor
     * publishes (see {@link #releaseStaleConnections}), which is what keeps this from drifting upward
     * over a day of gateway restarts.
     */
    private int connectedClientCount = 0;

    /** True once the first complete list has queued its bootstrap run, so a re-published list re-asserts
     * the rows without re-designating anybody. */
    private boolean bootstrapActivationQueued = false;

    /**
     * True once a <em>bootstrap</em> {@code GatewayActive} has actually left {@link
     * #pendingGatewayActivation} — the trading day is open. Replicated state in the same sense as {@link
     * #rejectedFrameCount}: derived identically on every node and on replay, and read by nothing that can
     * reach a frame. {@code SequencerService} mirrors it onto the operator gauge of the same name.
     *
     * <p>Separate from {@link #bootstrapActivationQueued} on both edges. A list whose rank-0 rows are all
     * missing queues nothing, so the day never opened even though the list completed; and an operator's
     * {@code GatewayActivationRequested} drains through the same queue, so counting drains alone let a
     * manual activation report a bootstrap that never happened — on a truncated list, which is exactly
     * when an operator reaches for the manual path and exactly when the gauge must still read 0.
     */
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

    /** Count of TCP clients currently connected across every gateway; 0 before any {@code ConnectionOpened}. */
    public int connectedClientCount() {
        return connectedClientCount;
    }

    /** Ingress frames refused by §9.2 since this node started; see {@link #rejectedFrameCount}. */
    public long rejectedFrameCount() {
        return rejectedFrameCount;
    }

    /**
     * Names this node in the rejection log line; see {@link #memberId}.
     * @param memberId this node's cluster memberId
     */
    public void memberId(final int memberId) {
        this.memberId = memberId;
    }

    /**
     * Re-stamps one ingress frame as its sequenced counterpart — {@code Unsequenced} as {@code Sequenced},
     * {@code UnsequencedSystem} as {@code SequencedSystem} — copying its body through verbatim; see the
     * class Javadoc.
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
     * Sequences one ingress frame of either family: the envelope's copy-18/append-16, plus the state the
     * sequencer derives when the frame is a system one.
     *
     * <p>The body is copied verbatim, its length prefix included, and is never re-encoded (<b>E-1</b>).
     * The conditions are §9.2's, in §9.2's order — each establishes what the next may read.
     */
    private int sequenceFrame(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                              final long timestamp) {
        // Condition 1, both halves. The ceiling is the backstop of T-3: a conforming producer's encode
        // method refuses an oversized body on its own stack, so what reaches here is a producer that is not one.
        if (length < FrameLayer.MIN_INGRESS_LENGTH) {
            return reject("length " + length + " is below the " + FrameLayer.MIN_INGRESS_LENGTH +
                          "-byte minimum framing");
        }
        if (length > FrameLayer.MAX_INGRESS_LENGTH) {
            return reject("length " + length + " is above the " + FrameLayer.MAX_INGRESS_LENGTH +
                          "-byte maximum framing");
        }
        // Condition 2's second half; the schemaId is checked in sequenceMessage.
        if (msgHeaderDecoder.version() != MessageHeaderDecoder.SCHEMA_VERSION) {
            return reject("version " + msgHeaderDecoder.version() + " is not " +
                          MessageHeaderDecoder.SCHEMA_VERSION);
        }
        // Condition 3. Two ingress templates now, and admitting exactly those is also what refuses the
        // three synthesis-only ones structurally, without opening a body.
        final int templateId = msgHeaderDecoder.templateId();
        final boolean system = templateId == UnsequencedSystemDecoder.TEMPLATE_ID;
        if (!system && templateId != UnsequencedDecoder.TEMPLATE_ID) {
            return reject("templateId " + templateId + " is neither Unsequenced (" + UnsequencedDecoder.TEMPLATE_ID +
                          ") nor UnsequencedSystem (" + UnsequencedSystemDecoder.TEMPLATE_ID + ")");
        }
        // Condition 4. An equality, not a floor, and against the composite's own constant: a short
        // blockLength puts the var-data prefix inside the header composite, a long one silently drops
        // bytes off the end. One equality for both families — both composites are 18 bytes.
        if (msgHeaderDecoder.blockLength() != UnsequencedHeaderDecoder.ENCODED_LENGTH) {
            return reject("blockLength " + msgHeaderDecoder.blockLength() + " is not " +
                          UnsequencedHeaderDecoder.ENCODED_LENGTH);
        }

        // Condition 5.
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int prefixOffset = headerOffset + UnsequencedHeaderDecoder.ENCODED_LENGTH;
        final int bodyLength = buffer.getShort(prefixOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        if (bodyLength == NULL_PAYLOAD_LENGTH || FrameLayer.MIN_INGRESS_LENGTH + bodyLength != length) {
            return reject("body length " + bodyLength + " does not fit a " + length + "-byte frame");
        }

        // Condition 6. An int32 compare, so nothing here can throw: -1 marks the frames the cluster
        // synthesizes (F-4), and admitting one on ingress would let a producer forge that class.
        frameHeaderDecoder.wrap(buffer, headerOffset);
        final int sourceId = frameHeaderDecoder.sourceId();
        if (sourceId == NO_SOURCE_ID) {
            return reject("sourceId " + NO_SOURCE_ID + " is reserved for the cluster's own frames");
        }
        final int connectionId = frameHeaderDecoder.connectionId();

        if (system) {
            systemHeaderDecoder.wrap(buffer, headerOffset);
            final int systemEventType = systemHeaderDecoder.systemEventType();
            // Condition 8.
            final int blockLength = ingressBlockLength(systemEventType);
            if (blockLength == NOT_INGRESS_LEGAL) {
                return reject("systemEventType " + systemEventType + " is not an allocated, ingress-legal event");
            }
            // Condition 9. The floor is the decoder's compiled constant and never the wire's declared
            // length: an SBE decoder reads a fixed-width field at its fixed offset whatever acting block
            // length it was wrapped with, so a producer-declared length bounds nothing.
            if (bodyLength < blockLength) {
                return reject("systemEventType " + systemEventType + " body of " + bodyLength +
                              " bytes is short of " + blockLength);
            }
            // Condition 10.
            if (!applySystem(buffer, prefixOffset + UnsequencedDecoder.payloadHeaderLength(), systemEventType,
                             sourceId, connectionId, sessionId)) {
                return NO_FRAME;
            }
        } else {
            // Condition 7. Nothing else about an application payload is read, ever (S-2).
            final int payloadId = frameHeaderDecoder.payloadId();
            if (payloadId == 0) {
                return reject("payloadId 0 is not a protocol");
            }
            if (payloadId == RETIRED_CORE_ID) {
                return reject("payloadId " + RETIRED_CORE_ID +
                              " is core's retired id and carries no application protocol");
            }
        }

        final long globalSeq = ++globalSeqNo;
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(system ? SequencedSystemEncoder.BLOCK_LENGTH : SequencedEncoder.BLOCK_LENGTH)
            .templateId(system ? SequencedSystemEncoder.TEMPLATE_ID : SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        // Copy-18, append-16 (§9.5). The 18 bytes go across verbatim, which is what keeps this one path
        // for both families — the field at offset 16 is a payloadId or a systemEventType and neither is
        // read here. The three fields written back are at offsets common to both sequenced composites.
        encodeBuffer.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, buffer, headerOffset,
                              UnsequencedHeaderDecoder.ENCODED_LENGTH);
        tapHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sessionId(sessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        encodeBuffer.putBytes(TAP_BODY_PREFIX_OFFSET, buffer, prefixOffset,
                              UnsequencedDecoder.payloadHeaderLength() + bodyLength);
        return TAP_BODY_OFFSET + bodyLength;
    }

    /**
     * The compiled {@code BLOCK_LENGTH} of the event {@code systemEventType} names, or
     * {@link #NOT_INGRESS_LEGAL} if that value is unallocated or has no ingress form — §9.2 conditions 8
     * and 9 in one lookup, since the second's floor is only defined once the first has passed.
     *
     * <p>The three synthesis-only events are absent here rather than listed and refused: they have no
     * ingress form to be short of, and the templates that carry them are already refused by condition 3.
     */
    private static int ingressBlockLength(final int systemEventType) {
        return switch (systemEventType) {
            case SystemFrame.CONNECTION_OPENED -> ConnectionOpenedDecoder.BLOCK_LENGTH;
            case SystemFrame.CONNECTION_CLOSED -> ConnectionClosedDecoder.BLOCK_LENGTH;
            case SystemFrame.CLUSTER_STARTED -> ClusterStartedDecoder.BLOCK_LENGTH;
            case SystemFrame.CLUSTER_STOPPED -> ClusterStoppedDecoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_REGISTERED -> GatewayRegisteredDecoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_STARTED -> GatewayStartedDecoder.BLOCK_LENGTH;
            case SystemFrame.PAYLOAD_ID_REGISTERED -> PayloadIdRegisteredDecoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> GatewayActivationRequestedDecoder.BLOCK_LENGTH;
            case SystemFrame.APPLICATION_REGISTERED -> ApplicationRegisteredDecoder.BLOCK_LENGTH;
            default -> NOT_INGRESS_LEGAL;
        };
    }

    /**
     * The state the sequencer derives from a system body, and the only place it decodes one. Returns
     * whether the frame is admitted.
     *
     * <p>The body carries no {@code MessageHeader} of its own — {@code header.systemEventType} is what
     * names it — so every decode here supplies {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} from the
     * decoder's own compiled constants (<b>V-3</b>). Condition 9 has already established that the body
     * is long enough for the block each read below sits in.
     */
    private boolean applySystem(final DirectBuffer buffer, final int bodyOffset, final int systemEventType,
                                final int sourceId, final int connectionId, final long sessionId) {
        // S-6 case 2. A system frame claiming a sourceId the list names must arrive on a session a
        // GatewayStarted already bound to that sourceId, so one process cannot speak for another's logical
        // gateway. GatewayStarted is exempt because case 1 below is what creates the binding, and every
        // unlisted sourceId is unchecked (case 3) — clusterctl's markers and the node-local publishers.
        if (systemEventType != SystemFrame.GATEWAY_STARTED && listClaims(sourceId) &&
            !boundToGateway(sessionId, sourceId)) {
            return rejectSystem("systemEventType " + systemEventType + " claims listed sourceId " + sourceId +
                                 " on a session no GatewayStarted bound");
        }

        final int version = MessageHeaderDecoder.SCHEMA_VERSION;
        switch (systemEventType) {
            case SystemFrame.GATEWAY_REGISTERED -> {
                gatewayRegisteredDecoder.wrap(buffer, bodyOffset, GatewayRegisteredDecoder.BLOCK_LENGTH, version);
                addGatewayRow(gatewayRegisteredDecoder.gatewayId(), gatewayRegisteredDecoder.gatewaySourceId(),
                              gatewayRegisteredDecoder.preferenceRank());
                // remaining == 0 is the list's last row, and the whole completeness edge: the publisher
                // counts the rows it read, so the cluster never has to infer "have I seen everyone?".
                if (gatewayRegisteredDecoder.remaining() == 0 && !bootstrapActivationQueued) {
                    bootstrapActivationQueued = true;
                    for (final GatewayRow row : gatewayRows) {
                        if (row.preferenceRank() == 0) {
                            activationQueue.add(new QueuedActivation(row.gatewayId(), true));
                        }
                    }
                }
            }
            case SystemFrame.GATEWAY_STARTED -> {
                gatewayStartedDecoder.wrap(buffer, bodyOffset, GatewayStartedDecoder.BLOCK_LENGTH, version);
                // S-6 case 1, both halves. Total, so a GatewayStarted ahead of load-topology is rejected
                // against an empty list — the start-up order as a wire rule.
                final int gatewayId = gatewayStartedDecoder.gatewayId();
                final GatewayRow row = rowFor(gatewayId);
                if (row == null) {
                    return rejectSystem("GatewayStarted names gatewayId " + gatewayId + ", which no list row does");
                }
                if (row.gatewaySourceId() != sourceId) {
                    return rejectSystem("GatewayStarted for gatewayId " + gatewayId + " carries sourceId " +
                                         sourceId + ", not its row's " + row.gatewaySourceId());
                }
                // One session per instance. An instance that restarts and reconnects before the cluster
                // times its old session out declares itself on the new one while the dead one is still
                // bound, and the late close of that dead session promoted a sibling out from under the
                // instance that had just started. Its epoch is over either way — releaseStaleConnections
                // below already says so — so the superseded binding goes with it.
                activeGatewaySession.values().removeIf(bound -> bound == gatewayId);
                activeGatewaySession.put(sessionId, gatewayId);
                releaseStaleConnections(sourceId);
            }
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> {
                activationRequestedDecoder.wrap(buffer, bodyOffset, GatewayActivationRequestedDecoder.BLOCK_LENGTH,
                                                version);
                // The operator's act is the fact, and the designation is still the cluster's: the frame is
                // forwarded and the GatewayActive answering it is synthesized behind it, through the path
                // bootstrap and both promotions take. Which is what gives the manual path the list
                // validation the other three get from iterating the list in the first place.
                final int gatewayId = activationRequestedDecoder.gatewayId();
                if (rowFor(gatewayId) == null) {
                    return rejectSystem("GatewayActivationRequested names gatewayId " + gatewayId +
                                         ", which no list row does");
                }
                activationQueue.add(new QueuedActivation(gatewayId, false));
            }
            case SystemFrame.CONNECTION_OPENED -> {
                if (openConnections.computeIfAbsent(sourceId, source -> new java.util.HashSet<>())
                        .add(connectionId)) {
                    connectedClientCount++;
                }
            }
            case SystemFrame.CONNECTION_CLOSED -> {
                final java.util.Set<Integer> open = openConnections.get(sourceId);
                if (open != null && open.remove(connectionId)) {
                    connectedClientCount--;
                }
            }
            default -> {
                // Allocated, ingress-legal and derived from by nothing: forwarded and not opened (S-2).
            }
        }
        return true;
    }

    /**
     * Skips a malformed ingress message
     * @param reason rejection description
     */
    private boolean rejectSystem(final String reason) {
        reject(reason);
        return false;
    }

    /**
     * Skips a malformed ingress message
     * @param reason rejection description
     */
    private int reject(final String reason) {
        rejectedFrameCount++;
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
        clusterHeartbeatEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(clusterHeartbeatEncoder.header(), SystemFrame.CLUSTER_HEARTBEAT, globalSeq, timestamp);
        return MessageHeaderEncoder.ENCODED_LENGTH + clusterHeartbeatEncoder.encodedLength();
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
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(leadershipChangedEncoder.header(), SystemFrame.LEADERSHIP_CHANGED, globalSeq, timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        return MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength();
    }

    /**
     * The activations owed to the frame just sequenced: the bootstrap run behind the list's last row —
     * the cluster designating the primary of each logical gateway by naming its {@code gatewayId} in a
     * {@code GatewayActive}, so exactly one instance of each pair opens its accept gate at cold start and
     * its standby waits — and the one a {@code GatewayActivationRequested} asks for.
     *
     * <p><b>One frame per call.</b> Each activation takes its own {@code globalSeqNo}, so the adapter
     * calls this in a loop until {@link #NO_FRAME} — emitting what comes back before asking again, since
     * every call re-encodes into the same {@link #buffer()}. The loop runs right after the
     * {@link #sequenceMessage} that queued them, so the frames take the next {@code globalSeqNo}s in
     * {@link #gatewayRows} order — identically on every node and on replay.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, or {@link #NO_FRAME} when none is left pending
     */
    public int pendingGatewayActivation(final long timestamp) {
        final QueuedActivation queued = activationQueue.poll();
        // Empty when no list row designated a primary — nothing to activate (fail closed).
        if (queued == null) {
            return NO_FRAME;
        }
        bootstrapActivationEmitted |= queued.bootstrap();
        return gatewayActive(queued.gatewayId(), timestamp);
    }

    /**
     * Whether the cold-start designation has been made — the gauge {@code SequencerService} publishes as
     * {@code seqeron_sequencer_bootstrap_activated}.
     * @return true once a bootstrap {@code GatewayActive} has been handed back by {@link
     *     #pendingGatewayActivation}
     */
    public boolean bootstrapActivationEmitted() {
        return bootstrapActivationEmitted;
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
     * {@link #pendingGatewayActivation}, the caller invokes it right after the {@link #clusterHeartbeat} it
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
     * ConnectionClosed}s that would have closed them out, which is the whole reason that frame exists
     * (it carries the {@code firstConnectionId} the new instance resumes allocating from for the same
     * reason). Without this, every gateway crash leaves its clients counted forever.
     *
     * <p>It cannot drop a live connection: a gateway publishes {@code GatewayStarted} before it opens its
     * accept gate ({@code FixGateway.cpp}), so none of its own {@code ConnectionOpened}s can precede it.
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
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(gatewayActiveEncoder.header(), SystemFrame.GATEWAY_ACTIVE, globalSeq, timestamp);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }

    /**
     * Stamps the header of a frame the cluster synthesized on its own initiative. Each of the three has a
     * template of its own and carries its fields inline, so there is no body, no length prefix and no
     * scratch buffer — the whole frame is one flat encode.
     *
     * <p>These are the frames with no producer, so <b>F-4</b>'s {@code -1} stands in for the identity an
     * ingress frame carries, and <b>E-1</b>'s exception applies: every node encodes its own copy rather
     * than copying one through, which is why the encode must be a pure function of its arguments.
     * {@code systemEventType} is redundant against the template id and written anyway, so that offset 16
     * discriminates every frame on the tap.
     */
    private static void stampSynthesized(final SequencedSystemHeaderEncoder header, final int systemEventType,
                                         final long globalSeq, final long timestamp) {
        header.sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .systemEventType(systemEventType)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
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

    /** Whether any list row names {@code sourceId} as its logical gateway (<b>S-6</b> cases 2 and 3). */
    private boolean listClaims(final int sourceId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewaySourceId() == sourceId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code sessionId} was bound by a {@code GatewayStarted} naming an instance of
     * {@code sourceId} (<b>S-6</b> case 2). Bound to a different logical gateway does not count.
     */
    private boolean boundToGateway(final long sessionId, final int sourceId) {
        final Integer boundGatewayId = activeGatewaySession.get(sessionId);
        if (boundGatewayId == null) {
            return false;
        }
        final GatewayRow row = rowFor(boundGatewayId);
        return row != null && row.gatewaySourceId() == sourceId;
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
