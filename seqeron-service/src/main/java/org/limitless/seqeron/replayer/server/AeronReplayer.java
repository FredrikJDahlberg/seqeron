package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.status.StreamCounter;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
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
    /** How long the replay of a cut search may take to connect. */
    private static final long SCAN_CONNECT_TIMEOUT_MS = 5_000;

    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final ExclusivePublication controlPub;
    private final Subscription requestSub;

    /** The counter {@link #recordingPosition} last found, while it still counts that recording. */
    private int recordingCounterId = CountersReader.NULL_COUNTER_ID;

    /** The publisher-limit counter of each replay {@link #isReplayLive} has found, by replay session id. */
    private final Long2LongHashMap replayCounterIds = new Long2LongHashMap(CountersReader.NULL_COUNTER_ID);

    public AeronReplayer(final Aeron aeron, final AeronArchive archive, final int memberId) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
        this.controlPub = aeron.addExclusivePublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.CONTROL_STREAM_ID);
        this.requestSub = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
    }

    @Override
    public List<Replayer.RecordingSpan> listQueueRecordings(final int groupId) {
        final List<Replayer.RecordingSpan> spans = new ArrayList<>();
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity)
                -> spans.add(new Replayer.RecordingSpan(
                    recordingId, startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP, termBufferLength,
                    segmentFileLength)));
        return spans;
    }

    @Override
    public Replayer.QueueBacklog queueBacklog(final int groupId, final Replayer.RecordingSpan span) {
        final CountersReader counters = aeron.countersReader();
        final DirectBuffer metaData = counters.metaDataBuffer();
        for (int counterId = 0, maxId = counters.maxCounterId(); counterId <= maxId; counterId++) {
            if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED ||
                counters.getCounterTypeId(counterId) != AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID) {
                continue;
            }
            final int key = CountersReader.metaDataOffset(counterId) + CountersReader.KEY_OFFSET;
            if (metaData.getInt(key + StreamCounter.STREAM_ID_OFFSET) != FrameLayer.queueStreamId(groupId) ||
                !FrameLayer.QUEUE_CHANNEL.equals(metaData.getStringAscii(key + StreamCounter.CHANNEL_OFFSET))) {
                continue;
            }
            final int recordingCounterId = RecordingPos.findCounterIdBySession(
                counters, metaData.getInt(key + StreamCounter.SESSION_ID_OFFSET), archive.archiveId());
            if (recordingCounterId != CountersReader.NULL_COUNTER_ID &&
                RecordingPos.getRecordingId(counters, recordingCounterId) == span.recordingId()) {
                return new Replayer.QueueBacklog(
                    counters.getCounterValue(counterId) - counters.getCounterValue(recordingCounterId),
                    Configuration.producerWindowLength(span.termBufferLength(),
                                                       Configuration.ipcPublicationTermWindowLength()));
            }
        }
        return null;
    }

    @Override
    public ScanEnd scanQueueRecording(final long recordingId, final long fromPosition, final long endPosition,
                                      final FrameVisitor visitor) {
        final long replaySessionId =
            archive.startReplay(recordingId, fromPosition, endPosition - fromPosition, ReplayProtocol.IPC_CHANNEL,
                                ReplayProtocol.REPLAY_STREAM_ID);
        final String channel = ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replaySessionId;
        try (Subscription subscription = aeron.addSubscription(channel, ReplayProtocol.REPLAY_STREAM_ID)) {
            return scan(subscription, (int)replaySessionId, endPosition, visitor);
        } finally {
            stopReplay(replaySessionId);
        }
    }

    /** Reads the replay one frame at a time until the visitor stops, the bound is reached or a fragment is no frame. */
    private ScanEnd scan(final Subscription subscription, final int replaySessionId, final long endPosition,
                         final FrameVisitor visitor) {
        final long deadlineMs = System.currentTimeMillis() + SCAN_CONNECT_TIMEOUT_MS;
        Image image;
        while ((image = subscription.imageBySessionId(replaySessionId)) == null) {
            if (System.currentTimeMillis() > deadlineMs) {
                throw new IllegalStateException("the replay of a cut search did not connect");
            }
            Thread.yield();
        }
        final ScanEnd[] ended = {null};
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final FragmentHandler handler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (ended[0] != null) {
                return;
            }
            if (!frame.wrap(buffer, offset, length)) {
                ended[0] = ScanEnd.UNREADABLE;
            } else if (!visitor.onFrame(LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                                            header.positionBitsToShift(),
                                                                            header.initialTermId()),
                                        frame.clusterSeqNo())) {
                ended[0] = ScanEnd.STOPPED;
            }
        });
        while (ended[0] == null) {
            if (image.poll(handler, 1) == 0 && (image.isClosed() || image.position() >= endPosition)) {
                return ScanEnd.TIP;
            }
        }
        return ended[0];
    }

    @Override
    public long recordingPosition(final long recordingId) {
        final CountersReader counters = aeron.countersReader();
        if (recordingCounterId == CountersReader.NULL_COUNTER_ID ||
            !RecordingPos.isActive(counters, recordingCounterId, recordingId)) {
            recordingCounterId = RecordingPos.findCounterIdByRecording(counters, recordingId, archive.archiveId());
            if (recordingCounterId == CountersReader.NULL_COUNTER_ID) {
                return AeronArchive.NULL_POSITION;
            }
        }
        return counters.getCounterValue(recordingCounterId);
    }

    @Override
    public long stopPosition(final long recordingId) {
        return archive.getStopPosition(recordingId);
    }

    @Override
    public boolean purgeSegments(final long recordingId, final long newStartPosition) {
        try {
            archive.purgeSegments(recordingId, newStartPosition);
            return true;
        } catch (final ArchiveException refused) {
            return false;
        }
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        return archive.startReplay(recordingId, position, length, ReplayProtocol.IPC_CHANNEL, streamId);
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        replayCounterIds.remove(replaySessionId);
        archive.stopReplay(replaySessionId);
    }

    @Override
    public boolean isReplayLive(final long replaySessionId) {
        final CountersReader counters = aeron.countersReader();
        final int cached = (int)replayCounterIds.get(replaySessionId);
        if (cached != CountersReader.NULL_COUNTER_ID && isReplayCounter(counters, cached, replaySessionId)) {
            return true;
        }
        replayCounterIds.remove(replaySessionId);
        for (int counterId = 0, maxId = counters.maxCounterId(); counterId <= maxId; counterId++) {
            if (isReplayCounter(counters, counterId, replaySessionId)) {
                replayCounterIds.put(replaySessionId, counterId);
                return true;
            }
        }
        return false;
    }

    private static boolean isReplayCounter(final CountersReader counters, final int counterId,
                                           final long replaySessionId) {
        if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED ||
            counters.getCounterTypeId(counterId) != AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID) {
            return false;
        }
        final int key = CountersReader.metaDataOffset(counterId) + CountersReader.KEY_OFFSET;
        final DirectBuffer metaData = counters.metaDataBuffer();
        return metaData.getInt(key + StreamCounter.STREAM_ID_OFFSET) == ReplayProtocol.REPLAY_STREAM_ID &&
            metaData.getInt(key + StreamCounter.SESSION_ID_OFFSET) == (int)replaySessionId;
    }

    @Override
    public String pollArchive() {
        final String error = archive.pollForErrorResponse();
        return AeronArchive.NOT_CONNECTED_MSG.equals(error) ? ARCHIVE_SESSION_CLOSED : error;
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
    public AtomicCounter newCounter(final int typeId, final String label) {
        return SeqeronCounters.addCounter(aeron, typeId, label, memberId);
    }

    @Override
    public long epochMillis() {
        return System.currentTimeMillis();
    }
}
