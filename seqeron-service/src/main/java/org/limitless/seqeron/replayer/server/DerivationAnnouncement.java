package org.limitless.seqeron.replayer.server;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.sequencer.LogDeriver;

/**
 * What a member tells a gateway host, which cannot read its driver: where a derivation of its queues may start and
 * which counter bounds a replay of its log. The member publishes one when it starts
 * and one after each purge, on a stream its archive records, and the recording is active for as long as the counter
 * id holds.
 *
 * @param start           where the member's service started, or the cut its queues were purged to
 * @param commitCounterId the id of the commit position counter on the member's driver
 */
public record DerivationAnnouncement(DerivationStart start, int commitCounterId) {
    static final String CHANNEL = "aeron:ipc?alias=seqeron-derivation-start|term-length=1m";
    public static final int STREAM_ID = 207;

    private static final int VERSION = 1;
    private static final int HEADER_LENGTH = 5 * Integer.BYTES + Long.BYTES + 1 + FrameLayer.MAX_GROUPS * Long.BYTES;

    /** The messages to publish in order: a header, then each record of the sequencer's state. */
    List<byte[]> encode() {
        final long[] sessions = start.state().openSessions();
        final UnsafeBuffer header =
            new UnsafeBuffer(new byte[HEADER_LENGTH + Integer.BYTES + sessions.length * Long.BYTES]);
        int offset = 0;
        header.putInt(offset, VERSION);
        header.putInt(offset += Integer.BYTES, commitCounterId);
        header.putInt(offset += Integer.BYTES, start.termLength());
        header.putInt(offset += Integer.BYTES, start.mtu());
        header.putInt(offset += Integer.BYTES, start.state().sequencer().size());
        header.putLong(offset += Integer.BYTES, start.logPosition());
        header.putByte(offset += Long.BYTES, (byte)(start.restored() ? 1 : 0));
        offset += 1;
        for (final long queueStart : start.queueStarts()) {
            header.putLong(offset, queueStart);
            offset += Long.BYTES;
        }
        header.putInt(offset, sessions.length);
        offset += Integer.BYTES;
        for (final long session : sessions) {
            header.putLong(offset, session);
            offset += Long.BYTES;
        }
        final List<byte[]> messages = new ArrayList<>();
        messages.add(header.byteArray());
        messages.addAll(start.state().sequencer());
        return messages;
    }

    /** @return the last announcement {@code messages} holds all of, or null if this version reads none */
    public static DerivationAnnouncement decode(final List<byte[]> messages) {
        DerivationAnnouncement last = null;
        for (int from = 0; from < messages.size();) {
            final DerivationAnnouncement next = decodeFirst(messages.subList(from, messages.size()));
            if (next == null) {
                break;
            }
            last = next;
            from += 1 + next.start().state().sequencer().size();
        }
        return last;
    }

    private static DerivationAnnouncement decodeFirst(final List<byte[]> messages) {
        if (messages.getFirst().length < HEADER_LENGTH + Integer.BYTES) {
            return null;
        }
        final UnsafeBuffer header = new UnsafeBuffer(messages.getFirst());
        int offset = 0;
        if (header.getInt(offset) != VERSION) {
            return null;
        }
        final int commitCounterId = header.getInt(offset += Integer.BYTES);
        final int termLength = header.getInt(offset += Integer.BYTES);
        final int mtu = header.getInt(offset += Integer.BYTES);
        final int records = header.getInt(offset += Integer.BYTES);
        final long logPosition = header.getLong(offset += Integer.BYTES);
        final boolean restored = header.getByte(offset += Long.BYTES) != 0;
        offset += 1;
        if (messages.size() < 1 + records) {
            return null;
        }
        final long[] queueStarts = new long[FrameLayer.MAX_GROUPS];
        for (int i = 0; i < queueStarts.length; i++) {
            queueStarts[i] = header.getLong(offset);
            offset += Long.BYTES;
        }
        final long[] sessions = new long[header.getInt(offset)];
        offset += Integer.BYTES;
        for (int i = 0; i < sessions.length; i++) {
            sessions[i] = header.getLong(offset);
            offset += Long.BYTES;
        }
        final LogDeriver.State state = new LogDeriver.State(List.copyOf(messages.subList(1, 1 + records)), sessions);
        return new DerivationAnnouncement(
            new DerivationStart(logPosition, state, restored, queueStarts, termLength, mtu), commitCounterId);
    }
}
