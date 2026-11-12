package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.RecordingLog;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * Trims a member's Raft log and snapshot recordings below its two newest restart-safe snapshots (doc/snapshot.md
 * §10.7), from the member's Replayer thread: once each time a snapshot becomes restart-safe, and when {@code clusterctl
 * trim-log} asks. A request is a correlation id and whether to print only; the reply is each line of the run, then
 * its count of refused purges.
 */
public final class LogTrimmer {
    /** Where {@code clusterctl} asks for a trim, on {@link ReplayProtocol#IPC_CHANNEL}. */
    public static final int REQUEST_STREAM_ID = 208;
    /** Where the trimmer answers. */
    public static final int REPLY_STREAM_ID = 209;
    /** A request: the correlation id, then one byte, non-zero for a dry run. */
    public static final int REQUEST_LENGTH = Long.BYTES + 1;
    /** A reply's {@code failures} field when it carries a line of the run and not its end. */
    public static final int LINE = -1;
    /** A reply: the correlation id, {@code failures}, then a line's ASCII text. */
    public static final int REPLY_HEADER_LENGTH = Long.BYTES + Integer.BYTES;

    /** {@link #floorCap}'s answer while the queues' history is not known yet, which holds every trim of the log. */
    public static final long HOLD = -1;

    private static final long REPLY_TIMEOUT_MS = 1_000;

    private final AeronArchive archive;
    private final int memberId;
    private final File clusterDir;
    private final RestartSafeLog safeLog;
    private final LongSupplier floorCap;
    private final Subscription requests;
    private final ExclusivePublication replies;
    private final UnsafeBuffer reply = new UnsafeBuffer(new byte[REPLY_HEADER_LENGTH + 256]);
    private int trimmedAtSafeCount;

    /**
     * @param safeLog  the member's {@code restart-safe.log}, in its cluster directory
     * @param floorCap the lowest log position anything but a restart needs, {@link Long#MAX_VALUE} for none, or
     *                 {@link #HOLD}
     */
    LogTrimmer(final Aeron aeron, final AeronArchive archive, final int memberId, final RestartSafeLog safeLog,
               final LongSupplier floorCap) {
        this.archive = archive;
        this.memberId = memberId;
        this.clusterDir = safeLog.directory().toFile();
        this.safeLog = safeLog;
        this.floorCap = floorCap;
        this.trimmedAtSafeCount = safeLog.safeCount();
        this.requests = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL, REQUEST_STREAM_ID);
        this.replies = aeron.addExclusivePublication(ReplayProtocol.IPC_CHANNEL, REPLY_STREAM_ID);
    }

    int doWork() {
        int work = requests.poll((buffer, offset, length, header) -> {
            if (length >= REQUEST_LENGTH) {
                final long correlationId = buffer.getLong(offset);
                final int failures =
                    trim(buffer.getByte(offset + Long.BYTES) != 0, line -> offerReply(correlationId, LINE, line));
                offerReply(correlationId, failures, "");
            }
        }, 1);
        final int safeCount = safeLog.safeCount();
        if (safeCount != trimmedAtSafeCount) {
            trimmedAtSafeCount = safeCount;
            trim(false, line -> Logger.info(Logger.CoreComponent.ReplayerService, memberId, "trim-log: %s", line));
            ++work;
        }
        return work;
    }

    private void offerReply(final long correlationId, final int failures, final String line) {
        final byte[] text = line.getBytes(StandardCharsets.US_ASCII);
        final int length = Math.min(text.length, reply.capacity() - REPLY_HEADER_LENGTH);
        reply.putLong(0, correlationId);
        reply.putInt(Long.BYTES, failures);
        reply.putBytes(REPLY_HEADER_LENGTH, text, 0, length);
        final long deadlineMs = Clocks.monotonicMs() + REPLY_TIMEOUT_MS;
        while (replies.offer(reply, 0, REPLY_HEADER_LENGTH + length) < 0 && Clocks.monotonicMs() < deadlineMs) {
            Thread.onSpinWait();
        }
    }

    /**
     * Purges what the plan names, or with {@code dryRun} reports it. Safe to repeat: a recording already gone counts as
     * done, and a purge the archive refuses leaves more than is needed.
     * @return how many purges the archive refused
     */
    private int trim(final boolean dryRun, final Consumer<String> report) {
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            report.accept("no recording log in " + clusterDir);
            return 1;
        }
        final LogTrimPlan plan;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            plan = LogTrimPlan.of(recordingLog.entries(), safeLog.state());
        }
        if (plan == LogTrimPlan.NOTHING) {
            report.accept("fewer than two restart-safe snapshots — nothing to trim");
            return 0;
        }
        int failures = 0;
        try {
            for (final long recordingId : plan.snapshotRecordings()) {
                failures += purgeSnapshot(recordingId, dryRun, report);
            }
            final long cap = floorCap.getAsLong();
            if (cap == HOLD) {
                report.accept("the log is kept whole until the queues are derived from it");
                return failures;
            }
            for (final long recordingId : plan.logRecordings()) {
                failures += trimLogRecording(recordingId, Math.min(plan.floor(), cap), dryRun, report);
            }
        } catch (final RuntimeException ex) {
            report.accept("the member's archive did not answer (" + ex.getMessage() + ")");
            return failures + 1;
        }
        return failures;
    }

    private int purgeSnapshot(final long recordingId, final boolean dryRun, final Consumer<String> report) {
        try {
            if (dryRun) {
                archive.getStartPosition(recordingId);
                report.accept("would purge snapshot recording " + recordingId);
            } else {
                archive.purgeRecording(recordingId);
                report.accept("purged snapshot recording " + recordingId);
            }
        } catch (final ArchiveException ex) {
            if (ex.errorCode() != ArchiveException.UNKNOWN_RECORDING) { // else a run before this one purged it
                report.accept("snapshot recording " + recordingId + ": " + ex.getMessage());
                return 1;
            }
        }
        return 0;
    }

    /** Purges, or with {@code dryRun} reports, a log recording's segments below {@code floor}; 1 if refused. */
    private int trimLogRecording(final long recordingId, final long floor, final boolean dryRun,
                                 final Consumer<String> report) {
        final long[] summary = new long[4]; // start, stop, term length, segment length
        if (archive.listRecording(recordingId,
                                  (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition,
                                   stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                                   sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                                      summary[0] = startPosition;
                                      summary[1] = stopPosition;
                                      summary[2] = termBufferLength;
                                      summary[3] = segmentFileLength;
                                  }) == 0) {
            return 0; // already gone
        }
        final long end =
            summary[1] != AeronArchive.NULL_POSITION ? summary[1] : archive.getRecordingPosition(recordingId);
        final long target = SegmentPurge.target(summary[0], (int)summary[2], (int)summary[3], Math.min(floor, end));
        if (target == SegmentPurge.NOTHING) {
            return 0;
        }
        if (dryRun) {
            report.accept("would purge log recording " + recordingId + " below position " + target);
            return 0;
        }
        try {
            archive.purgeSegments(recordingId, target);
            report.accept("purged log recording " + recordingId + " below position " + target);
            return 0;
        } catch (final ArchiveException ex) {
            report.accept("log recording " + recordingId + " below position " + target + ": " + ex.getMessage());
            return 1;
        }
    }
}
