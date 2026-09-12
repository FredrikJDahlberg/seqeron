package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.limitless.seqeron.metrics.SeqeronCounters;
import org.limitless.seqeron.sequencer.SequencerService;

/**
 * The production {@link Replayer}: {@link ReplayerService}'s calls forwarded to the
 * co-located node's {@code Aeron} client and {@code AeronArchive} control session. Every method here
 * is a forward — any decision belongs above the seam, in {@code ReplayerService}, where it is
 * testable.
 *
 * <p>Not thread-safe, the control publication is exclusive and every call is from the duty-cycle thread
 * (see {@code ReplayerServer}'s {@code NoOpLock} note).
 */
public final class AeronReplayer implements Replayer {
    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final ExclusivePublication controlPub;
    private final Subscription requestSub;

    public AeronReplayer(final Aeron aeron, final AeronArchive archive, final int memberId) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
        this.controlPub = aeron.addExclusivePublication(ReplayerService.IPC_CHANNEL, ReplayerService.CONTROL_STREAM_ID);
        this.requestSub = aeron.addSubscription(ReplayerService.IPC_CHANNEL, ReplayerService.REQUEST_STREAM_ID);
    }

    @Override
    public List<ReplayRecordings.RecordingSpan> listTapRecordings() {
        final List<ReplayRecordings.RecordingSpan> spans = new ArrayList<>();
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", SequencerService.FEEDER_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity)
                -> spans.add(new ReplayRecordings.RecordingSpan(recordingId, startPosition,
                                                                stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
        return spans;
    }

    @Override
    public long recordingPosition(final long recordingId) {
        return archive.getRecordingPosition(recordingId);
    }

    @Override
    public long stopPosition(final long recordingId) {
        return archive.getStopPosition(recordingId);
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        return archive.startReplay(recordingId, position, length, ReplayerService.IPC_CHANNEL, streamId);
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        archive.stopReplay(replaySessionId);
    }

    @Override
    public int pollRequests(final FragmentHandler handler, final int fragmentLimit) {
        return requestSub.poll(handler, fragmentLimit);
    }

    @Override
    public long offerControl(final DirectBuffer buffer, final int offset, final int length) {
        return controlPub.offer(buffer, offset, length);
    }

    @Override
    public SelfCheckStream openSelfCheckStream(final long replaySessionId) {
        final String channel = ReplayerService.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
        final Subscription subscription = aeron.addSubscription(channel, ReplayerService.SELF_CHECK_STREAM_ID);
        return new SelfCheckStream() {
            @Override
            public int poll(final FragmentHandler handler, final int fragmentLimit) {
                return subscription.poll(handler, fragmentLimit);
            }

            @Override
            public void close() {
                subscription.close();
            }
        };
    }

    @Override
    public AtomicCounter newCounter(final int typeId, final String label) {
        return SeqeronCounters.addCounter(aeron, typeId, label, memberId);
    }

    @Override
    public long epochMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
