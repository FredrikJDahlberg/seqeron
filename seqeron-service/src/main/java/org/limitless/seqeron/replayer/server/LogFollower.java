package org.limitless.seqeron.replayer.server;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntArrayList;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder;
import org.limitless.seqeron.sequencer.FramePredictor;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;

/**
 * The derivation {@link DerivingReplayer} follows the log with, from the member's start to its commit position: it
 * keeps the checkpoints and the runs a replay continues from, and decides what a purge releases and what thinning
 * drops, which is what the trim may delete below. Reads the log's segment files and is otherwise free of Aeron.
 *
 * <p>Not thread-safe.
 */
final class LogFollower implements Sequencer.FrameSink {
    /** What a run is counted as holding besides its frames. */
    private static final int RUN_BYTES = 512;
    private static final int ENTRY_LIMIT = 256;

    /**
     * Where a derivation continues from.
     *
     * @param logPosition    of the log entry to apply next
     * @param state          the deriver's before that entry
     * @param reemitMarker   whether the state is a cluster snapshot's, whose marker starts each queue
     * @param queuePositions where each queue stood, slot {@code groupId - 1}
     */
    record Checkpoint(long logPosition, LogDeriver.State state, boolean reemitMarker, long[] queuePositions) { }

    /** A frame as the sequencer published it. */
    record Frame(int groupId, byte[] bytes) { }

    /**
     * A stretch of log a {@link FramePredictor} derives as the sequencer did, behind the frames of the entry before
     * it, which it does not: the derivation's start, an entry the two differed on, or no entry where a checkpoint
     * was due.
     *
     * @param logPosition     of that entry, or {@code entriesFrom}
     * @param entriesFrom     where the predicted entries start
     * @param lead            that entry's frames, or the ones the derivation's start published
     * @param queuePositions  where each queue stood before {@code lead}, slot {@code groupId - 1}
     * @param clusterSeqNo    the predictor's state after {@code lead}, with the next two
     * @param activeGroupMask the groups a frame has reached
     * @param groupSeqNos     of the last frame on each queue, slot {@code groupId - 1}
     * @param state           null, or the deriver's after {@code lead}: the runs were past their bound, so this one
     *                        keeps no run for the entries the predictor does not derive up to the next checkpoint, and
     *                        a {@link LogDeriver} derives it
     */
    record Run(long logPosition, long entriesFrom, List<Frame> lead, long[] queuePositions, long clusterSeqNo,
               int activeGroupMask, long[] groupSeqNos, LogDeriver.State state) {
        long bytes() {
            long bytes = RUN_BYTES;
            for (final Frame frame : lead) {
                bytes += frame.bytes().length;
            }
            return bytes;
        }
    }

    private final File archiveDir;
    private final ArchiveRecording logRecording;
    private final LongSupplier commitPosition;
    private final int maxCheckpoints;
    private final long maxRunBytes;
    private final int termLength;
    private final int mtu;
    private final List<Checkpoint> checkpoints = new ArrayList<>();
    private final long[] queuePositions;
    /** Where each queue's history starts: where the service created it, or where it was purged to. */
    private final long[] queueStarts;
    private final RecordingReader log;
    private final LogDeriver deriver;
    /** The runs kept, and how many a purge has released before them: a run's number counts those. */
    private final List<Run> runs = new ArrayList<>();
    private long runsReleased;
    private long runBytes;
    /** Whether the last run is one a {@link LogDeriver} derives, which no run is kept behind. */
    private boolean sequencedRun;
    private final FramePredictor predictor = new FramePredictor();
    private final SequencedHeaderDecoder queueHeader = new SequencedHeaderDecoder();
    /** Of the last frame on each queue, and the groups a frame has reached. */
    private final long[] groupSeqNos = new long[FrameLayer.MAX_GROUPS];
    private int activeGroupMask;
    /** The frames of the entry being applied, and where each queue stood before it. */
    private final ExpandableArrayBuffer entryFrames = new ExpandableArrayBuffer();
    private final IntArrayList entryFrameGroups = new IntArrayList();
    private final IntArrayList entryFrameEnds = new IntArrayList();
    private final long[] entryQueuePositions = new long[FrameLayer.MAX_GROUPS];
    /** How many of the entry's frames the predictor has published, and whether one differed. */
    private int predicted;
    private boolean differed;
    private final Sequencer.FrameSink comparison = new Sequencer.FrameSink() {
        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            final int i = predicted++;
            if (i >= entryFrameGroups.size() || entryFrameGroups.getInt(i) != groupId) {
                differed = true;
                return;
            }
            final int from = i == 0 ? 0 : entryFrameEnds.getInt(i - 1);
            differed |=
                !Arrays.equals(entryFrames.byteArray(), from, entryFrameEnds.getInt(i), buffer.byteArray(), 0, length);
        }
    };
    /** Where the last entry applied ends in the log. */
    private long entryEnd;
    private final RecordingReader.MessageHandler handler = this::onLogEntry;
    /** The commit position when this started: its queue positions are no tip before it has derived that far. */
    private final long caughtUpAt;
    private boolean caughtUp;
    /** The log between two checkpoints, which doubles each time they are thinned. */
    private long checkpointSpacing;

    /**
     * @param memberId           the member whose log this reads, for the log lines only
     * @param start              where the member's service started
     * @param archiveDir         where the member's archive keeps the log's segment files
     * @param logRecording       the log's recording, which holds {@code start}
     * @param commitPosition     the log's commit position, which the recording has reached
     * @param checkpointLogBytes the log between two checkpoints at first
     * @param maxCheckpoints     the most kept
     * @param maxRunBytes        the most the runs may hold
     */
    LogFollower(final int memberId, final DerivationStart start, final File archiveDir,
                final ArchiveRecording logRecording, final LongSupplier commitPosition, final long checkpointLogBytes,
                final int maxCheckpoints, final long maxRunBytes) {
        this.archiveDir = archiveDir;
        this.logRecording = logRecording;
        this.commitPosition = commitPosition;
        this.checkpointSpacing = checkpointLogBytes;
        this.maxCheckpoints = maxCheckpoints;
        this.maxRunBytes = maxRunBytes;
        this.termLength = start.termLength();
        this.mtu = start.mtu();
        this.deriver = new LogDeriver(memberId, this);
        this.queuePositions = start.queueStarts().clone();
        this.queueStarts = start.queueStarts().clone();
        this.caughtUpAt = commitPosition.getAsLong();
        this.caughtUp = caughtUpAt <= start.logPosition();
        checkpoints.add(
            new Checkpoint(start.logPosition(), start.state(), start.restored(), start.queueStarts().clone()));
        entryEnd = start.logPosition();
        log = logReader(entryEnd);
        startEntry();
        deriver.restore(start.state(), start.restored());
        addRun(entryEnd, entryEnd);
    }

    /** The term length the queues are published with. */
    int termLength() {
        return termLength;
    }

    /** The MTU the queues are published with. */
    int mtu() {
        return mtu;
    }

    /** Where the last entry applied ends in the log: how far a replay may read it. */
    long entryEnd() {
        return entryEnd;
    }

    /** Where group {@code groupId}'s queue's history starts. */
    long queueStart(final int groupId) {
        return queueStarts[groupId - 1];
    }

    /** Where the derivation has brought group {@code groupId}'s queue. */
    long queuePosition(final int groupId) {
        return queuePositions[groupId - 1];
    }

    /** The checkpoints kept, in log order: the first is where replays need the log from. */
    List<Checkpoint> checkpoints() {
        return Collections.unmodifiableList(checkpoints);
    }

    /** The runs kept, the derivation's start among them. */
    int runCount() {
        return runs.size();
    }

    /** How many of the runs kept a {@link LogDeriver} derives, the runs having been past their bound. */
    int sequencedRunCount() {
        return (int)runs.stream().filter(run -> run.state() != null).count();
    }

    private void startEntry() {
        entryFrameGroups.clear();
        entryFrameEnds.clear();
        System.arraycopy(queuePositions, 0, entryQueuePositions, 0, queuePositions.length);
    }

    /** Keeps a run behind the frames collected since {@link #startEntry}, from where the sequencer stands. */
    private void addRun(final long logPosition, final long entriesFrom) {
        final List<Frame> lead = new ArrayList<>(entryFrameGroups.size());
        for (int i = 0, from = 0; i < entryFrameGroups.size(); from = entryFrameEnds.getInt(i++)) {
            final byte[] bytes = new byte[entryFrameEnds.getInt(i) - from];
            entryFrames.getBytes(from, bytes);
            lead.add(new Frame(entryFrameGroups.getInt(i), bytes));
        }
        sequencedRun = entriesFrom > logPosition && runBytes > maxRunBytes;
        runs.add(new Run(logPosition, entriesFrom, lead, entryQueuePositions.clone(), deriver.clusterSeqNo(),
                         activeGroupMask, groupSeqNos.clone(), sequencedRun ? deriver.state() : null));
        runBytes += runs.getLast().bytes();
        predictor.restore(deriver.clusterSeqNo(), activeGroupMask, groupSeqNos);
    }

    /** The run of a number, or null if it is released or not kept yet. */
    Run run(final long number) {
        return number < runsReleased || number >= runsReleased + runs.size() ? null
                                                                             : runs.get((int)(number - runsReleased));
    }

    /**
     * The number of the last run before which group {@code groupId}'s queue stood at or before {@code position}, or
     * -1 if there is none.
     */
    long runAt(final int groupId, final long position) {
        for (int i = runs.size() - 1; i >= 0; i--) {
            if (runs.get(i).queuePositions()[groupId - 1] <= position) {
                return runsReleased + i;
            }
        }
        return -1;
    }

    /** A reader of the log's recording from an entry's start. */
    RecordingReader logReader(final long position) {
        return new RecordingReader(archiveDir, logRecording.recordingId(), logRecording.startPosition(),
                                   logRecording.termLength(), logRecording.segmentLength(), position);
    }

    /** Whether the derivation has caught up with the log, so that where it has brought a queue is its tip. */
    boolean isValid() {
        return caughtUp;
    }

    /** Moves a queue's start, and releases the checkpoints no replay can continue from any more. */
    void purge(final int groupId, final long newStart) {
        queueStarts[groupId - 1] = newStart;
        while (checkpoints.size() > 1 && isBelowStarts(checkpoints.get(1).queuePositions())) {
            checkpoints.removeFirst();
        }
        int released = 0;
        while (released + 1 < runs.size() && isBelowStarts(runs.get(released + 1).queuePositions())) {
            runBytes -= runs.get(released++).bytes();
        }
        runs.subList(0, released).clear();
        runsReleased += released;
    }

    private boolean isBelowStarts(final long[] positions) {
        for (int i = 0; i < queueStarts.length; i++) {
            if (positions[i] > queueStarts[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean checkpointDue() {
        return entryEnd - checkpoints.getLast().logPosition() >= checkpointSpacing;
    }

    /** Releases every other checkpoint, keeping the first, which a queue's start may be continued from alone. */
    private void thin() {
        int kept = 0;
        for (int i = 0; i < checkpoints.size(); i += 2) {
            checkpoints.set(kept++, checkpoints.get(i));
        }
        checkpoints.subList(kept, checkpoints.size()).clear();
        checkpointSpacing *= 2;
    }

    /**
     * Applies the committed entries up to the next checkpoint due, at most {@link #ENTRY_LIMIT} of them.
     *
     * @return the entries applied
     */
    int doWork() {
        final int entries = log.poll(handler, commitPosition.getAsLong(), ENTRY_LIMIT);
        if (checkpointDue()) {
            checkpoints.add(new Checkpoint(entryEnd, deriver.state(), false, queuePositions.clone()));
            startEntry();
            addRun(entryEnd, entryEnd); // so that a replay's first frame is no further off than from a checkpoint
            if (checkpoints.size() > maxCheckpoints) {
                thin();
            }
        }
        caughtUp |= entryEnd >= caughtUpAt;
        return entries;
    }

    /** Ends a poll at the entry a checkpoint is due after. */
    private boolean onLogEntry(final DirectBuffer buffer, final int offset, final int length, final long endPosition) {
        startEntry();
        deriver.onLogEntry(buffer, offset, length);
        if (!sequencedRun) {
            predicted = 0;
            differed = false;
            predictor.onLogEntry(buffer, offset, length, -1, comparison);
            if (differed || predicted != entryFrameGroups.size()) {
                addRun(entryEnd, endPosition);
            }
        }
        entryEnd = endPosition;
        return !checkpointDue();
    }

    @Override
    public void activate(final int[] groupIds) {
    }

    @Override
    public void publish(final int groupId, final DirectBuffer buffer, final int length) {
        final long start = QueuePositions.frameStart(queuePositions[groupId - 1], length, termLength, mtu);
        queuePositions[groupId - 1] = start + QueuePositions.framedLength(length, mtu);
        groupSeqNos[groupId - 1] = queueHeader.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH).groupSeqNo();
        activeGroupMask |= 1 << (groupId - 1);
        final int from = entryFrameEnds.isEmpty() ? 0 : entryFrameEnds.getInt(entryFrameEnds.size() - 1);
        entryFrames.putBytes(from, buffer, 0, length);
        entryFrameGroups.addInt(groupId);
        entryFrameEnds.addInt(from + length);
    }
}
