package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.status.StreamCounter;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import java.util.ArrayList;
import java.util.List;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.util.Clocks;

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
        this.controlPub = aeron.addExclusivePublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.CONTROL_STREAM_ID);
        this.requestSub = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
    }

    @Override
    public List<Replayer.RecordingSpan> listTapRecordings() {
        final List<Replayer.RecordingSpan> spans = new ArrayList<>();
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", FrameLayer.FEEDER_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity)
                -> spans.add(new Replayer.RecordingSpan(
                    recordingId, startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP, termBufferLength)));
        return spans;
    }

    @Override
    public Replayer.TapBacklog tapBacklog(final Replayer.RecordingSpan span) {
        final CountersReader counters = aeron.countersReader();
        final DirectBuffer metaData = counters.metaDataBuffer();
        for (int counterId = 0, maxId = counters.maxCounterId(); counterId <= maxId; counterId++) {
            if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED ||
                counters.getCounterTypeId(counterId) != AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID) {
                continue;
            }
            final int key = CountersReader.metaDataOffset(counterId) + CountersReader.KEY_OFFSET;
            if (metaData.getInt(key + StreamCounter.STREAM_ID_OFFSET) != FrameLayer.FEEDER_STREAM_ID ||
                !FrameLayer.FEEDER_CHANNEL.equals(metaData.getStringAscii(key + StreamCounter.CHANNEL_OFFSET))) {
                continue;
            }
            final int recordingCounterId = RecordingPos.findCounterIdBySession(
                counters, metaData.getInt(key + StreamCounter.SESSION_ID_OFFSET), archive.archiveId());
            if (recordingCounterId != CountersReader.NULL_COUNTER_ID &&
                RecordingPos.getRecordingId(counters, recordingCounterId) == span.recordingId()) {
                return new Replayer.TapBacklog(
                    counters.getCounterValue(counterId) - counters.getCounterValue(recordingCounterId),
                    Configuration.producerWindowLength(span.termBufferLength(),
                                                       Configuration.ipcPublicationTermWindowLength()));
            }
        }
        return null;
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
        return archive.startReplay(recordingId, position, length, ReplayProtocol.IPC_CHANNEL, streamId);
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        archive.stopReplay(replaySessionId);
    }

    @Override
    public String pollArchive() {
        return archive.pollForErrorResponse();
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
        final String channel = ReplayProtocol.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
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
    public IndexStream openIndexStream(final long recordingId, final long position) {
        final long replaySessionId = archive.startReplay(recordingId, position, AeronArchive.REPLAY_ALL_AND_FOLLOW,
                                                         ReplayProtocol.IPC_CHANNEL, ReplayerService.INDEX_STREAM_ID);
        final String channel = ReplayProtocol.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
        final Subscription subscription = aeron.addSubscription(channel, ReplayerService.INDEX_STREAM_ID);
        return new IndexStream() {
            private boolean connected;
            private IndexFrameHandler handler;
            private final FragmentHandler fragments = (buffer, offset, length, header) -> {
                // Only snapshot frames are indexed, and they are small enough never to be fragmented; a
                // fragment of a larger frame is skipped rather than read as a frame.
                if ((header.flags() & FrameDescriptor.UNFRAGMENTED) != FrameDescriptor.UNFRAGMENTED) {
                    return;
                }
                handler.onFrame(buffer, offset, length,
                                header.position() - BitUtil.align(length + DataHeaderFlyweight.HEADER_LENGTH,
                                                                  FrameDescriptor.FRAME_ALIGNMENT));
            };

            @Override
            public int poll(final IndexFrameHandler frameHandler, final int fragmentLimit) {
                handler = frameHandler;
                connected |= subscription.imageCount() > 0;
                return subscription.poll(fragments, fragmentLimit);
            }

            @Override
            public boolean isEnded() {
                return connected && subscription.imageCount() == 0;
            }

            @Override
            public void close() {
                try {
                    archive.stopReplay(replaySessionId);
                } catch (final RuntimeException ex) {
                    // Already gone with its recording or the archive; nothing left to stop.
                }
                subscription.close();
            }
        };
    }

    @Override
    public AtomicCounter newSourceCounter(final int typeId, final String label, final int sourceId) {
        return SeqeronCounters.addSourceCounter(aeron, typeId, label, memberId, sourceId);
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
    public long nowMs() {
        return Clocks.monotonicMs();
    }
}
