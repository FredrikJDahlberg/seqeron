package org.limitless.phixeron.sequencer;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.limitless.phixeron.sbe.sequenced.ClientConnectedEncoder;
import org.limitless.phixeron.sbe.sequenced.ClientDisconnectedEncoder;
import org.limitless.phixeron.sbe.sequenced.HeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.unsequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;

/**
 * Aeron Cluster service that imposes a total order on messages arriving from multiple clients.
 *
 * <p>For every committed {@link #onSessionMessage} the service assigns a
 * <b>globalSeqNo</b> — a cluster-wide monotone counter shared across all sources and
 * lifecycle events (connect / disconnect) — and stamps it, together with the cluster
 * consensus timestamp, into the message's {@code header} composite before republishing it.
 *
 * <p>Ingress messages arrive already SBE-encoded as {@code sbe-unsequenced.xml} (schema
 * ID 200) — the FIX gateway encodes every admin and application FIX message that way and
 * offers it directly to the cluster, with {@code header.sourceId} identifying the submitting
 * gateway <em>process</em> (a fixed constant, stable across restarts and unique across every
 * gateway instance sharing this cluster), {@code header.connectionId} identifying the specific
 * TCP connection at that gateway, and {@code header.sessionId} the Aeron Cluster session. This
 * service does not need to know about individual FIX message types to re-stamp them: {@code
 * sbe-sequenced.xml} (schema ID 202) is deliberately kept byte-identical to {@code
 * sbe-unsequenced.xml} past the {@code header} composite (same field order/types/ids, same
 * var-data layout), so {@link #onSessionMessage} decodes only the outer {@code
 * MessageHeader} and the {@code header} composite (both always at a fixed offset,
 * regardless of {@code templateId}), then copies every remaining byte — the rest of the
 * fixed block plus all var-data — verbatim into a new {@code sbe-sequenced.xml} message
 * whose {@code header} composite carries the original {@code sourceId}/{@code connectionId}/
 * {@code sessionId} plus the new {@code globalSeqNo}/{@code timestamp}.
 *
 * <p>The decorated message is published on the <em>global stream</em>
 * ({@link #GLOBAL_STREAM_CHANNEL} / {@link #GLOBAL_STREAM_ID}), which the current leader's
 * co-located Aeron Archive records so C++ clients can replay history on startup.
 *
 * <p><b>Leader-only publishing:</b> all cluster nodes maintain identical sequencing state
 * (updated on every callback), but only the leader writes to the global stream. The
 * {@link ExclusivePublication} itself (and its recording) is created lazily by whichever node
 * is currently leader — see {@link #applyLeadership} — and closed when that node loses
 * leadership, rather than existing on every node from startup.
 *
 * <p><b>Cross-failover recording continuity:</b> every node that is <em>not</em> currently
 * leader continuously replicates the current leader's global-stream recording into its own
 * local archive as a live-following standby copy (via {@link AeronArchive#replicate}, using
 * {@link #archiveEndpointsByMemberId} to reach the leader's archive control port). When
 * leadership changes, the new leader stops standby-following (it is now the source) and starts
 * a new recording of its own for its own tenure; the node that just lost leadership stops its
 * own recording and starts standby-following the new leader instead. Because every node has
 * been shadowing the leader the whole time it was a follower, by the time any node becomes
 * leader its own local archive catalog already holds every earlier segment (each leader's tenure
 * is a separate recording, since Aeron's recording continuation across independent publication
 * instances would require exact term/session alignment — see {@code todo.md}'s "Cross-failover
 * global-stream recording continuity" entry for why segment-based stitching was chosen over
 * that). Clients therefore only ever need to reach the <em>current</em> leader's archive to
 * replay full history, walking recording segments for {@link #GLOBAL_STREAM_ID} in order.
 *
 * <p><b>Snapshot format</b> (little-endian binary, single fragment):
 * <pre>
 *   int64  globalSeqNo
 * </pre>
 */
public final class SequencerService implements ClusteredService {
    /**
     * Multi-destination-cast (dynamic control mode) channel for the global sequenced stream.
     * This is the publisher/archive-recording channel: the current leader's {@link
     * ExclusivePublication} and {@code startRecording} both use it (see {@link
     * #applyLeadership}) — the same fixed address regardless of which node is leader, so exactly
     * one node ever binds it at a time. Subscribers connect with {@link
     * #GLOBAL_STREAM_SUBSCRIBER_CHANNEL} instead, which points at the same control address but
     * carries its own (ephemeral) data endpoint.
     */
    public static final String GLOBAL_STREAM_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:9200";

    /**
     * Subscriber-side channel for the global stream's MDC dynamic control mode: same control
     * address as {@link #GLOBAL_STREAM_CHANNEL}, plus an ephemeral local data endpoint that the
     * publisher discovers and adds as a destination automatically.
     *
     * <p>The C++ global-stream clients ({@code GlobalStreamClient.hpp}) are the only subscribers;
     * this constant is kept byte-identical to the C++ one so the two stay in lock-step (Java itself
     * only publishes/records the global stream, never subscribes to it).
     *
     * <p><b>{@code tether=false} — audit S4 fix.</b> An untethered subscriber that falls behind the
     * publisher's window is moved to a resting state instead of back-pressuring the publisher, so a
     * slow or stalled global-stream consumer can never wedge this service's single conductor thread
     * in {@link #offerToGlobalStream}. A rested subscriber loses the messages it fell behind on and
     * rejoins live past them; the C++ client detects that hole from the gap-free {@code globalSeqNo}
     * run and re-bootstraps the missing range from the archive — the intended "fall behind, recover
     * via archive replay" posture rather than "back-pressure the sequencer".
     *
     * <p><b>Live-cluster caveat:</b> if a smoke test shows the publisher stalling (or {@code offer()}
     * returning {@code NOT_CONNECTED}) once the <em>only</em> consumer rests, add {@code |ssc=true}
     * (spies-simulate-connection) to {@link #GLOBAL_STREAM_CHANNEL} so the co-located archive spy
     * keeps the publication connected and its limit advancing on its own.
     */
    public static final String GLOBAL_STREAM_SUBSCRIBER_CHANNEL
        = "aeron:udp?control-mode=dynamic|control=localhost:9200|endpoint=localhost:0|tether=false";

    public static final int GLOBAL_STREAM_ID = 1;

    /**
     * Archive control stream id shared by every member — must match {@code SequencerNode}'s
     * {@code Archive.Context.controlStreamId(100)} so replication requests can reach a peer's
     * archive.
     */
    private static final int ARCHIVE_CONTROL_STREAM_ID = 100;

    /** Local, ephemeral endpoint the standby replication's live-merge subscription listens on. */
    private static final String STANDBY_LIVE_DESTINATION = "aeron:udp?endpoint=localhost:0";

    /**
     * Maximum consecutive back-pressure spins on the global stream before printing an alert.
     * At ~10 ns/spin this is ~10 ms per alert period.
     */
    private static final int MAX_BACK_PRESSURE_SPINS = 1_000_000;

    private static final int SNAPSHOT_POLL_BATCH = 10;

    /**
     * header.sourceId/connectionId for lifecycle events synthesized by this service
     * (ClientConnected / ClientDisconnected): an Aeron Cluster session opening/closing has no
     * gateway-process or TCP-level connection id to carry, unlike the ingress messages it
     * forwards.
     */
    private static final int NO_SOURCE_ID = -1;

    // ── SBE codecs — single conductor thread; no synchronisation needed ───────

    // Ingress decode (schema 200, sbe-unsequenced.xml). Only the outer framing
    // header and the generic `header` composite are ever decoded — body
    // fields are copied through as opaque bytes, see onSessionMessage.
    private final MessageHeaderDecoder ingressMsgHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder ingressHeaderDecoder = new HeaderDecoder();

    // Egress encode (schema 202, sbe-sequenced.xml).
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final HeaderEncoder egressHeaderEncoder = new HeaderEncoder();
    private final ClientConnectedEncoder clientConnEncoder = new ClientConnectedEncoder();
    private final ClientDisconnectedEncoder clientDiscEncoder = new ClientDisconnectedEncoder();
    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    // ── Sequencing state (snapshotted; updated on every node for determinism) ─

    /** Cluster-wide sequence counter; incremented for messages and lifecycle events. */
    private long globalSeqNo = 0;

    // ── Aeron runtime (not snapshotted) ──────────────────────────────────────

    private Cluster cluster;
    private boolean isLeader;
    private ExclusivePublication globalStreamPub;
    private AeronArchive aeronArchive;

    /** This node's own memberId → archive control endpoint ("host:port"), for every member. */
    private final Map<Integer, String> archiveEndpointsByMemberId;

    /**
     * memberId of whichever node last reported itself the leader; NULL_VALUE-as-int (-1)
     * until the first {@link #onNewLeadershipTermEvent}.
     */
    private int currentLeaderMemberId = -1;

    /**
     * Runs standby-follow entirely off the ClusteredService's single conductor thread — see
     * its class Javadoc for why.
     */
    private StandbyFollower standbyFollower;
    private Thread standbyFollowerThread;

    public SequencerService(final Map<Integer, String> archiveEndpointsByMemberId) {
        this.archiveEndpointsByMemberId = archiveEndpointsByMemberId;
    }

    // ── ClusteredService lifecycle ────────────────────────────────────────────

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;

        // Connect to the co-located archive via IPC — NoOpLock is safe on the single conductor thread.
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                                                .aeron(cluster.context().aeron())
                                                .controlRequestChannel("aeron:ipc")
                                                .controlRequestStreamId(100)
                                                .controlResponseChannel("aeron:ipc")
                                                .controlResponseStreamId(101)
                                                .lock(NoOpLock.INSTANCE));

        standbyFollower = new StandbyFollower(cluster.context().aeron().context().aeronDirectoryName(),
                                              cluster.memberId(), archiveEndpointsByMemberId);
        standbyFollowerThread = new Thread(standbyFollower, "standby-follower-" + cluster.memberId());
        standbyFollowerThread.setDaemon(true);
        standbyFollowerThread.start();

        // The global-stream ExclusivePublication (and its recording) is created lazily, only by
        // whichever node actually becomes leader — see applyLeadership(). Every node used to
        // create this publication unconditionally right here, which is what broke multi-member
        // same-host deployment: control-mode=dynamic binds a real local UDP socket at
        // GLOBAL_STREAM_CHANNEL's fixed "control=localhost:9200" address as soon as the
        // publication is created, so with all 3 members on one host, only the first to start
        // would win that port and the other two would abort with "Address already in use"
        // (see todo.md's "Global-stream control port collision" entry). Deferring creation to
        // leadership acquisition means at most one process ever holds that port at a time,
        // exactly like only one process ever calls offer() on it.

        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }
        clientConnEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientConnEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + clientConnEncoder.encodedLength());
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }
        clientDiscEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientDiscEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + clientDiscEncoder.encodedLength());
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        final long sourceSessionId = session.id();
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }

        // Decode just enough of the ingress (schema 200) message to re-stamp
        // it: the outer framing header (for templateId/blockLength) and the
        // `header` composite (for sourceId/connectionId) — both at fixed
        // offsets, independent of message type.
        ingressMsgHeaderDecoder.wrap(buffer, offset);
        final int templateId = ingressMsgHeaderDecoder.templateId();
        final int ingressBlockLen = ingressMsgHeaderDecoder.blockLength();

        final int ingressBodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        ingressHeaderDecoder.wrap(buffer, ingressBodyOffset);
        final int sourceId = ingressHeaderDecoder.sourceId();
        final int connectionId = ingressHeaderDecoder.connectionId();

        // sbe-sequenced.xml's header composite is sbe-unsequenced.xml's plus
        // two int64 fields (globalSeqNo, timestamp); every other field is
        // byte-identical, so the egress blockLength is simply the ingress
        // blockLength with the header composite's growth added on.
        final int egressBlockLen = HeaderEncoder.ENCODED_LENGTH + (ingressBlockLen - HeaderDecoder.ENCODED_LENGTH);

        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(egressBlockLen)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);

        final int egressBodyOffset = MessageHeaderEncoder.ENCODED_LENGTH;
        egressHeaderEncoder.wrap(encodeBuffer, egressBodyOffset)
            .sourceId(sourceId)
            .connectionId(connectionId)
            .sessionId(sourceSessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);

        // Copy every byte after the ingress header composite — the rest of
        // the fixed block plus all var-data — verbatim; see class Javadoc.
        final int copyFromOffset = ingressBodyOffset + HeaderDecoder.ENCODED_LENGTH;
        final int copyLength = length - MessageHeaderDecoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH;
        encodeBuffer.putBytes(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH, buffer, copyFromOffset, copyLength);

        offerToGlobalStream(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH + copyLength);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        encodeBuffer.putLong(0, globalSeqNo);
        long offerResult;
        do {
            offerResult = snapshotPublication.offer(encodeBuffer, 0, Long.BYTES);
            if (offerResult == ExclusivePublication.CLOSED
                || offerResult == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] Snapshot publication failed: " + offerResult);
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    private void loadSnapshot(final Image snapshotImage) {
        final FragmentAssembler handler = new FragmentAssembler((buf, off, len, hdr) -> globalSeqNo = buf.getLong(off));
        while (!snapshotImage.isClosed()) {
            cluster.idleStrategy().idle(snapshotImage.poll(handler, SNAPSHOT_POLL_BATCH));
        }
    }

    // ── Leadership ────────────────────────────────────────────────────────────

    @Override
    public void onNewLeadershipTermEvent(final long logPosition, final long leadershipTermId, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        applyLeadership(leaderMemberId);
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // Deliberately a no-op: onNewLeadershipTermEvent already fires on every node (leader and
        // followers alike) with an explicit leaderMemberId, which is what applyLeadership needs
        // to decide both "am I leader" and, if not, "whose archive should I be standby-following."
        // Driving the same transition from two independent callbacks previously required an
        // idempotency guard against double-firing; keying everything off one authoritative event
        // removes that class of bug entirely.
    }

    // Called on every node whenever a new leadership term begins (including this node's own
    // promotion). Creates/releases the global-stream ExclusivePublication and its recording when
    // this node itself becomes/stops being leader (see onStart()'s comment for why creation can't
    // happen unconditionally at startup) — both fast, local (IPC) archive calls, safe to leave on
    // the conductor thread like the rest of this method. Standby-follow of whichever node the
    // leader now is happens entirely on the StandbyFollower background thread instead: it needs a
    // remote network round trip to resolve the leader's recording id before it can even start
    // replicating, and doing that synchronously here previously stalled onSessionMessage (and
    // therefore every client's Logon) for as long as that round trip took — see the class
    // Javadoc's "Cross-failover recording continuity" section and StandbyFollower's own Javadoc.
    private void applyLeadership(final int leaderMemberId) {
        if (leaderMemberId == currentLeaderMemberId) {
            return;
        }
        currentLeaderMemberId = leaderMemberId;
        final boolean leader = leaderMemberId == cluster.memberId();
        System.out.printf("[SequencerService/%d] leadership change: new leader is memberId=%d (isLeader=%b)%n",
                          cluster.memberId(), leaderMemberId, leader);

        if (isLeader && globalStreamPub != null) {
            globalStreamPub.close();
            globalStreamPub = null;
        }
        isLeader = leader;

        if (isLeader) {
            globalStreamPub
                = cluster.context().aeron().addExclusivePublication(GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID);
            // Idempotent: safe to call again if this node regains leadership later.
            aeronArchive.startRecording(GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID, SourceLocation.LOCAL);
        }
        standbyFollower.onLeadershipChange(leaderMemberId);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        if (standbyFollower != null) {
            standbyFollower.shutdown();
            try {
                standbyFollowerThread.join(1000);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        if (aeronArchive != null) {
            aeronArchive.close();
        }
        if (globalStreamPub != null) {
            globalStreamPub.close();
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    // Spins on the single conductor thread until the offer lands. This no longer couples the
    // sequencer's liveness to a slow global-stream consumer (audit S4): the subscribers connect
    // untethered (see GLOBAL_STREAM_SUBSCRIBER_CHANNEL), so one that falls behind is moved to
    // resting and never back-pressures this publication — the only back-pressure left is the
    // co-located archive recording, which is fast and local, so the spin is bounded in practice.
    private void offerToGlobalStream(final int length) {
        int idleSpins = 0;
        long result;
        while ((result = globalStreamPub.offer(encodeBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] Global stream publication failed: " + result);
            }
            if (++idleSpins >= MAX_BACK_PRESSURE_SPINS) {
                System.err.printf("[SequencerService] ALERT: global stream back-pressure at globalSeqNo=%d%n",
                                  globalSeqNo);
                idleSpins = 0;
            }
            cluster.idleStrategy().idle();
        }
    }

    /**
     * Runs this node's standby-follow of whatever node is currently leader entirely on its own
     * thread, with its own independent {@link Aeron} client and local {@link AeronArchive}
     * connection — deliberately separate from {@link SequencerService}'s own {@code aeronArchive}
     * and from the cluster's {@code Aeron} instance, both of which are only safe to touch from
     * the single ClusteredService conductor thread (that's why they use {@link NoOpLock}).
     *
     * <p>Resolving the leader's active recording id requires a control-session round trip to a
     * <em>remote</em> archive, which can legitimately take an unbounded amount of time (the peer
     * may be mid-election, slow, or partitioned) — doing that on the conductor thread previously
     * stalled every {@code onSessionMessage} call (and therefore every client's Logon) for as long
     * as the round trip took. Leadership-change notifications arrive via a coalescing queue
     * ({@link #onLeadershipChange}) so this thread always acts on the most recent leader, never a
     * backlog of stale ones.
     */
    private static final class StandbyFollower implements Runnable {
        private static final int LOCAL_ARCHIVE_RESPONSE_STREAM_ID = 101;

        private final Aeron aeron;
        private final AeronArchive localArchive;
        private final int selfMemberId;
        private final Map<Integer, String> archiveEndpointsByMemberId;
        private final LinkedBlockingDeque<Integer> events = new LinkedBlockingDeque<>();
        private volatile boolean running = true;
        private long activeReplicationId = NULL_VALUE;

        StandbyFollower(final String aeronDirectoryName, final int selfMemberId,
                        final Map<Integer, String> archiveEndpointsByMemberId) {
            this.selfMemberId = selfMemberId;
            this.archiveEndpointsByMemberId = archiveEndpointsByMemberId;
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
            localArchive = AeronArchive.connect(new AeronArchive.Context()
                                                    .aeron(aeron)
                                                    .ownsAeronClient(false)
                                                    .controlRequestChannel("aeron:ipc")
                                                    .controlRequestStreamId(ARCHIVE_CONTROL_STREAM_ID)
                                                    .controlResponseChannel("aeron:ipc")
                                                    .controlResponseStreamId(LOCAL_ARCHIVE_RESPONSE_STREAM_ID));
        }

        /** Non-blocking: just enqueues, safe to call from the conductor thread. */
        void onLeadershipChange(final int leaderMemberId) {
            events.addLast(leaderMemberId);
        }

        void shutdown() {
            running = false;
            events.addLast(NULL_VALUE);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    int leaderMemberId = events.takeFirst();
                    Integer queued;
                    while ((queued = events.pollFirst()) != null) {
                        leaderMemberId = queued;
                    }
                    if (running) {
                        applyFollow(leaderMemberId);
                    }
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (final RuntimeException ex) {
                    System.err.printf("[StandbyFollower] %s%n", ex);
                }
            }
            stopActiveReplication();
            localArchive.close();
            aeron.close();
        }

        private void applyFollow(final int leaderMemberId) {
            stopActiveReplication();
            if (leaderMemberId == selfMemberId || leaderMemberId == NULL_VALUE) {
                return;
            }

            final String leaderEndpoint = archiveEndpointsByMemberId.get(leaderMemberId);
            if (leaderEndpoint == null) {
                System.err.printf(
                    "[StandbyFollower] No archive endpoint known for leaderMemberId=%d; cannot standby-follow%n",
                    leaderMemberId);
                return;
            }
            final String leaderArchiveChannel = "aeron:udp?endpoint=" + leaderEndpoint;

            final long srcRecordingId = resolveActiveRecordingId(leaderArchiveChannel);
            if (srcRecordingId == NULL_VALUE) {
                System.err.printf(
                    "[StandbyFollower] No active global-stream recording found on leaderMemberId=%d (%s); "
                        + "cannot standby-follow yet%n",
                    leaderMemberId, leaderEndpoint);
                return;
            }

            activeReplicationId = localArchive.replicate(srcRecordingId, NULL_VALUE, ARCHIVE_CONTROL_STREAM_ID,
                                                         leaderArchiveChannel, STANDBY_LIVE_DESTINATION);
        }

        // Opens a short-lived control session directly to a peer archive purely to find the
        // recording id of its currently-active (stopTimestamp unset) GLOBAL_STREAM_ID recording,
        // i.e. the one the leader is writing to right now. Returns NULL_VALUE if that peer has no
        // active recording (e.g. it just became leader and hasn't started recording yet) or is
        // unreachable. This blocking remote round trip is the entire reason this class exists on
        // its own thread instead of running inline in SequencerService.applyLeadership.
        private long resolveActiveRecordingId(final String archiveChannel) {
            final long[] recordingId = { NULL_VALUE };
            try (AeronArchive remote
                 = AeronArchive.connect(new AeronArchive.Context()
                                            .aeron(aeron)
                                            .ownsAeronClient(false)
                                            .controlRequestChannel(archiveChannel)
                                            .controlRequestStreamId(ARCHIVE_CONTROL_STREAM_ID)
                                            .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
                remote.listRecordingsForUri(0, Integer.MAX_VALUE, "", GLOBAL_STREAM_ID,
                                            (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp,
                                             startPosition, stopPosition, initialTermId, segmentFileLength,
                                             termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                                             originalChannel, sourceIdentity) -> {
                                                if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                                                    recordingId[0] = recId;
                                                }
                                            });
            } catch (final RuntimeException ex) {
                System.err.printf("[StandbyFollower] Failed to reach archive at %s: %s%n", archiveChannel, ex);
            }
            return recordingId[0];
        }

        private void stopActiveReplication() {
            if (activeReplicationId == NULL_VALUE) {
                return;
            }
            localArchive.tryStopReplication(activeReplicationId);
            activeReplicationId = NULL_VALUE;
        }
    }
}
