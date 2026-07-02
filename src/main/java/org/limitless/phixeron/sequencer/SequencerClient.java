package org.limitless.phixeron.sequencer;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.sequencer.*;

/**
 * Abstract base class for Sequencer clients.
 *
 * <p><b>Startup sequence</b> (driven by {@link #start()}):
 * <ol>
 *   <li>Launches an embedded {@link MediaDriver} and connects an {@link Aeron} instance.</li>
 *   <li>Connects to the Aeron Archive on the sequencer node to locate the global stream recording.</li>
 *   <li>Starts an archive replay from {@link #replayStartPosition()} using
 *       {@link AeronArchive#NULL_POSITION} as length so the image follows the live recording
 *       seamlessly — no gap and no separate subscription is needed.</li>
 *   <li>Connects to the Aeron Cluster ingress so the application can call {@link #send}.</li>
 * </ol>
 *
 * <p><b>Duty cycle</b> — call {@link #poll()} on a tight loop:
 * <pre>{@code
 *   client.start();
 *   while (running) {
 *       idleStrategy.idle(client.poll());
 *   }
 *   client.close();
 * }</pre>
 *
 * <p><b>Callbacks</b> — subclasses implement:
 * <ul>
 *   <li>{@link #onSequencedMessage} — every message (replay history + live).</li>
 *   <li>{@link #onSourceConnected} / {@link #onSourceDisconnected} — optional lifecycle events.</li>
 *   <li>{@link #onReplayComplete} — called once when the client has processed all history that
 *       existed at startup time and is now receiving live messages.</li>
 * </ul>
 *
 * <p><b>State persistence</b> — override {@link #replayStartPosition()} to return the archive
 * byte position of the last processed message so restarts skip already-applied history.
 * The image position is available via {@link Header#position()} inside each fragment callback.
 *
 * <p><b>How live follow-through works</b> — the archive replay uses {@code length=NULL_POSITION}
 * which tells the archive to continue delivering data as new messages are recorded.  Once the
 * replay image catches up to the recording's stop position at startup time,
 * {@link #onReplayComplete} fires and subsequent messages arrive with live latency through
 * the same image.  If the recording stops (sequencer node shuts down), the image closes and
 * the client falls back to a direct multicast subscription so it can reconnect to a new leader.
 *
 * <p><b>Multi-node clusters</b> — the archive control address should point to the current leader.
 * Detect a leader change via a jump in {@code globalSeqNo} and reconnect.
 */
public abstract class SequencerClient implements AutoCloseable {

    // ── Defaults matching SequencerNode single-node layout ───────────────────

    /** Default archive control channel (SequencerNode member-0, archive port 9301). */
    public static final String DEFAULT_ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9301";
    public static final int    DEFAULT_ARCHIVE_CONTROL_STREAM  = 100;

    /** Default cluster ingress for single-node development. */
    public static final String DEFAULT_INGRESS_ENDPOINTS = "0=localhost:9302";

    // Private unicast channel for the archive to deliver replay data to this client.
    private static final String REPLAY_CHANNEL   = "aeron:udp?endpoint=localhost:0";
    private static final int    REPLAY_STREAM_ID = 110;

    private static final int FRAGMENT_LIMIT = 10;

    // ── Configuration ─────────────────────────────────────────────────────────

    private final String archiveControlChannel;
    private final int    archiveControlStreamId;
    private final String ingressEndpoints;
    private final String aeronDir;

    // ── Runtime ───────────────────────────────────────────────────────────────

    private MediaDriver  mediaDriver;
    private Aeron        aeron;
    private AeronArchive aeronArchive;
    private AeronCluster cluster;

    // Replay subscription (follows live recording via NULL_POSITION length)
    private Subscription replaySub;
    private long         replaySessionId  = -1;
    private Image        replayImage;

    // Fallback live subscription (used only if the replay image closes unexpectedly)
    private Subscription liveSub;

    // Catch-up tracking: the recording stop position observed at startup
    private long    catchUpPosition     = 0;
    private boolean catchUpNotified;

    // ── SBE decoders — single-threaded; reused per message ───────────────────

    private final MessageHeaderDecoder      headerDecoder  = new MessageHeaderDecoder();
    private final SequencedMessageDecoder   seqMsgDecoder  = new SequencedMessageDecoder();
    private final SourceConnectedDecoder    srcConnDecoder = new SourceConnectedDecoder();
    private final SourceDisconnectedDecoder srcDiscDecoder = new SourceDisconnectedDecoder();

    // ── SBE encoder for outbound AppMessage ──────────────────────────────────

    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final AppMessageEncoder    appMsgEncoder    = new AppMessageEncoder();
    private final MutableDirectBuffer  sendBuffer       = new ExpandableDirectByteBuffer(4096);

    // ── Constructors ──────────────────────────────────────────────────────────

    /** Single-node development defaults. */
    protected SequencerClient() {
        this(DEFAULT_ARCHIVE_CONTROL_CHANNEL, DEFAULT_ARCHIVE_CONTROL_STREAM,
             DEFAULT_INGRESS_ENDPOINTS,
             System.getProperty("java.io.tmpdir") + "/phixeron-seq-client");
    }

    protected SequencerClient(final String archiveControlChannel,
                               final int    archiveControlStreamId,
                               final String ingressEndpoints,
                               final String aeronDir) {
        this.archiveControlChannel  = archiveControlChannel;
        this.archiveControlStreamId = archiveControlStreamId;
        this.ingressEndpoints       = ingressEndpoints;
        this.aeronDir               = aeronDir;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Starts the embedded driver, archive connection, replay, and cluster connection.
     * Must be called before {@link #poll()}.
     */
    public final void start() {
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .dirDeleteOnStart(true));

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(archiveControlChannel)
            .controlRequestStreamId(archiveControlStreamId)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")
            .controlResponseStreamId(0));

        final long recordingId = findGlobalStreamRecording();

        // Start replay; NULL_POSITION length causes the archive to follow the live recording.
        replaySessionId = aeronArchive.startReplay(
            recordingId, replayStartPosition(), AeronArchive.NULL_POSITION,
            REPLAY_CHANNEL, REPLAY_STREAM_ID);

        replaySub = aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);

        // Connect to cluster for sending AppMessages.
        cluster = AeronCluster.connect(new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(ingressEndpoints)
            .egressListener((sessionId, correlationId, buf, off, len, hdr) -> {}));
    }

    /**
     * Drives the replay subscription (and fallback live subscription if needed).
     * Also polls cluster egress for keepalive.
     *
     * @return the number of work items done (useful for idle-strategy back-off)
     */
    public final int poll() {
        int workCount = cluster.pollEgress();

        if (replayImage == null && replaySub != null) {
            replayImage = replaySub.imageBySessionId((int) replaySessionId);
        }

        if (replayImage != null) {
            if (!replayImage.isClosed()) {
                workCount += replayImage.poll(this::onFragment, FRAGMENT_LIMIT);
                if (!catchUpNotified && replayImage.position() >= catchUpPosition) {
                    catchUpNotified = true;
                    onReplayComplete();
                }
            } else {
                // Archive recording stopped (e.g., leader failover): fall back to live.
                if (!catchUpNotified) {
                    catchUpNotified = true;
                    onReplayComplete();
                }
                closeReplay();
                liveSub = aeron.addSubscription(
                    SequencerService.GLOBAL_STREAM_CHANNEL, SequencerService.GLOBAL_STREAM_ID);
            }
        } else if (liveSub != null) {
            final Image live = liveSub.imageAtIndex(0);
            if (live != null) {
                workCount += live.poll(this::onFragment, FRAGMENT_LIMIT);
            }
        }

        return workCount;
    }

    /**
     * Wraps {@code payload} in an {@code AppMessage} SBE envelope and offers it to the
     * sequencer cluster via Aeron Cluster ingress.
     *
     * @return the Aeron offer result — negative values indicate back-pressure or disconnect
     */
    public final long send(final DirectBuffer payload, final int offset, final int length) {
        appMsgEncoder.wrapAndApplyHeader(sendBuffer, 0, outHeaderEncoder)
            .putPayload(payload, offset, length);
        return cluster.offer(sendBuffer, 0,
            MessageHeaderEncoder.ENCODED_LENGTH + appMsgEncoder.encodedLength());
    }

    @Override
    public void close() {
        closeReplay();
        if (liveSub    != null) { liveSub.close();    liveSub    = null; }
        if (cluster    != null) { cluster.close();    cluster    = null; }
        if (aeronArchive != null) { aeronArchive.close(); aeronArchive = null; }
        if (aeron      != null) { aeron.close();      aeron      = null; }
        if (mediaDriver != null) { mediaDriver.close(); mediaDriver = null; }
    }

    // ── Callbacks for subclasses ──────────────────────────────────────────────

    /**
     * Called for every {@code SequencedMessage} on the global stream (replay + live).
     *
     * <p>The {@code decoder} is only valid for the duration of this call — copy the payload
     * out before returning if it must survive:
     * <pre>{@code
     *   final byte[] bytes = new byte[decoder.payloadLength()];
     *   decoder.getPayload(bytes, 0, bytes.length);
     * }</pre>
     *
     * @param globalSeqNo     cluster-wide monotone sequence number
     * @param sourceSessionId Aeron Cluster session ID of the originating client
     * @param appSeqNo        per-source monotone sequence number
     * @param timestamp       cluster-consensus time in milliseconds
     * @param decoder         positioned at the payload; use {@code payloadLength()} / {@code getPayload(...)}
     */
    protected abstract void onSequencedMessage(long globalSeqNo,
                                               long sourceSessionId,
                                               long appSeqNo,
                                               long timestamp,
                                               SequencedMessageDecoder decoder);

    /** Called when a source client opened a session on the sequencer (replay + live). */
    protected void onSourceConnected(final long globalSeqNo,
                                      final long sourceSessionId,
                                      final long timestamp) {}

    /** Called when a source client closed its session on the sequencer (replay + live). */
    protected void onSourceDisconnected(final long globalSeqNo,
                                         final long sourceSessionId,
                                         final long timestamp) {}

    /**
     * Called once after the client has processed all history that existed at startup and is
     * now receiving live data.  Override to begin accepting business traffic after recovery.
     */
    protected void onReplayComplete() {}

    /**
     * Returns the archive stream byte position from which to start replay.
     * Default is {@code 0} (replay from the beginning of the recording).
     * Override to return the saved position from the last processed message to skip history.
     */
    protected long replayStartPosition() {
        return 0L;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void onFragment(final DirectBuffer buffer, final int offset,
                             final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId) {
            case SequencedMessageDecoder.TEMPLATE_ID -> {
                seqMsgDecoder.wrap(buffer, bodyOffset,
                    headerDecoder.blockLength(), headerDecoder.version());
                onSequencedMessage(seqMsgDecoder.globalSeqNo(), seqMsgDecoder.sourceSessionId(),
                    seqMsgDecoder.appSeqNo(), seqMsgDecoder.clusterTimestamp(), seqMsgDecoder);
            }
            case SourceConnectedDecoder.TEMPLATE_ID -> {
                srcConnDecoder.wrap(buffer, bodyOffset,
                    headerDecoder.blockLength(), headerDecoder.version());
                onSourceConnected(srcConnDecoder.globalSeqNo(),
                    srcConnDecoder.sourceSessionId(), srcConnDecoder.clusterTimestamp());
            }
            case SourceDisconnectedDecoder.TEMPLATE_ID -> {
                srcDiscDecoder.wrap(buffer, bodyOffset,
                    headerDecoder.blockLength(), headerDecoder.version());
                onSourceDisconnected(srcDiscDecoder.globalSeqNo(),
                    srcDiscDecoder.sourceSessionId(), srcDiscDecoder.clusterTimestamp());
            }
            default -> System.err.printf(
                "[SequencerClient] Unknown SBE templateId=%d; ignored%n", templateId);
        }
    }

    private long findGlobalStreamRecording() {
        long bestRecordingId  = -1L;
        long bestStopPosition = Long.MIN_VALUE;

        // Temporary holder for the lambda to populate.
        final long[] result = {-1L, Long.MIN_VALUE};

        aeronArchive.listRecordingsForUri(
            0, Integer.MAX_VALUE,
            SequencerService.GLOBAL_STREAM_CHANNEL,
            SequencerService.GLOBAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId,
             startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength,
             sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                // After a leader change there may be multiple recordings; prefer the one
                // with the largest stop position (holds the most committed data).
                if (stopPosition > result[1]) {
                    result[0] = recordingId;
                    result[1] = stopPosition;
                }
            });

        if (result[0] < 0) {
            throw new IllegalStateException(
                "No global stream recording found on channel=" + SequencerService.GLOBAL_STREAM_CHANNEL
                + " streamId=" + SequencerService.GLOBAL_STREAM_ID
                + ". Ensure SequencerNode has started and accepted at least one message.");
        }

        long stopPos = result[1];
        if (stopPos == AeronArchive.NULL_POSITION) {
            // Recording is active; use the current write position as the catch-up target.
            stopPos = aeronArchive.getRecordingPosition(result[0]);
        }
        catchUpPosition = stopPos;
        if (catchUpPosition <= replayStartPosition()) {
            // Already caught up (empty, new, or fully-replayed recording).
            catchUpNotified = true;
            onReplayComplete();
        }

        return result[0];
    }

    private void closeReplay() {
        replayImage = null;
        if (replaySub != null) {
            replaySub.close();
            replaySub = null;
        }
    }
}
