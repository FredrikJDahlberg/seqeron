package org.limitless.seqeron.replayer.server;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import java.util.ArrayList;
import java.util.List;

/**
 * A recording as an archive's catalog describes it.
 *
 * @param stopPosition  {@link AeronArchive#NULL_POSITION} while it is active
 * @param active        whether it is still recording: the catalog holds no stop timestamp for it
 * @param termLength    the recorded stream's
 * @param segmentLength the recording's segment file length
 */
record ArchiveRecording(long recordingId, long startPosition, long stopPosition, boolean active, int termLength,
                        int segmentLength) {
    /** Every recording of a stream, on any channel, in the catalog's order. */
    static List<ArchiveRecording> ofStream(final AeronArchive archive, final int streamId) {
        final List<ArchiveRecording> recordings = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", streamId, collector(recordings));
        return recordings;
    }

    /** The newest active recording of a stream, or null. */
    static ArchiveRecording activeOf(final AeronArchive archive, final int streamId) {
        ArchiveRecording newest = null;
        for (final ArchiveRecording recording : ofStream(archive, streamId)) {
            if (recording.active() && (newest == null || recording.recordingId() > newest.recordingId())) {
                newest = recording;
            }
        }
        return newest;
    }

    /** A recording, or null if the archive holds none by that id. */
    static ArchiveRecording of(final AeronArchive archive, final long recordingId) {
        final List<ArchiveRecording> recordings = new ArrayList<>(1);
        archive.listRecording(recordingId, collector(recordings));
        return recordings.isEmpty() ? null : recordings.getFirst();
    }

    private static RecordingDescriptorConsumer collector(final List<ArchiveRecording> recordings) {
        return (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity)
                   -> recordings.add(new ArchiveRecording(recordingId, startPosition, stopPosition,
                                                          stopTimestamp == AeronArchive.NULL_TIMESTAMP,
                                                          termBufferLength, segmentFileLength));
    }
}
