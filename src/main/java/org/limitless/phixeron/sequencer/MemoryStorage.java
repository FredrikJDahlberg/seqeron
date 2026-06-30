package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Circular buffer of the last {@link #capacity} outbound SBE-encoded messages.
 * <p>
 * Indexed by outboundSeqNum. When the buffer wraps, the oldest slot is evicted and
 * the caller is notified so it can populate outboundArchiveIndex. This is the
 * fast-path store for servicing buy-side ResendRequests without Archive replay.
 * <p>
 * Session messages (templateId in {48,49,50,51,52,53,65}) are retransmitted as
 * GAP_FILL commands. Application messages (everything else) are retransmitted as
 * RESEND commands with PossDupFlag implied.
 * <p>
 * Not thread-safe. All access is from the single Aeron Cluster conductor thread.
 */
public final class MemoryStorage {
    // SBE template IDs that represent FIX session-layer messages (not resent individually).
    // Values equal ASCII codes of FIX MsgType: '0'=48 '1'=49 '2'=50 '3'=51 '4'=52 '5'=53 'A'=65.
    private static final int[] SESSION_TEMPLATE_IDS = {48, 49, 50, 51, 52, 53, 65};

    private final int capacity;

    // Parallel arrays forming the ring.
    private final long[] seqNums;
    private final long[] clusterPositions;
    private final int[]  templateIds;
    private final byte[][] payloads;
    private final int[]  payloadLens;

    private int headIndex = 0;  // index of oldest entry
    private int tailIndex = -1; // index of newest entry; -1 when empty
    private int count     = 0;

    public MemoryStorage(final int capacity) {
        this.capacity       = capacity;
        this.seqNums        = new long[capacity];
        this.clusterPositions = new long[capacity];
        this.templateIds    = new int[capacity];
        this.payloads       = new byte[capacity][];
        this.payloadLens    = new int[capacity];
    }

    /**
     * Stores an outbound message. If the buffer is full the oldest entry is evicted
     * and returned so the caller can archive it; otherwise returns null.
     */
    public EvictedEntry store(final long seqNum,
                              final long clusterPosition,
                              final int templateId,
                              final DirectBuffer payload,
                              final int length) {
        EvictedEntry evicted = null;
        final int nextIndex;

        if (count == capacity) {
            // Evict the oldest slot (headIndex) before overwriting it.
            evicted = new EvictedEntry(
                seqNums[headIndex],
                clusterPositions[headIndex]);
            nextIndex = headIndex;
            headIndex = (headIndex + 1) % capacity;
        } else {
            nextIndex = (tailIndex + 1) % capacity;
            count++;
        }

        tailIndex = nextIndex;
        seqNums[tailIndex] = seqNum;
        clusterPositions[tailIndex] = clusterPosition;
        templateIds[tailIndex] = templateId;
        payloadLens[tailIndex] = length;

        if (payloads[tailIndex] == null || payloads[tailIndex].length < length) {
            payloads[tailIndex] = new byte[Math.max(length, 256)];
        }
        payload.getBytes(0, payloads[tailIndex], 0, length);
        return evicted;
    }

    /**
     * Returns true if every sequence number in [beginSeqNo, resolvedEndSeqNo] is present.
     * endSeqNo == 0 means open-ended up to the highest stored sequence number.
     */
    public boolean covers(final long beginSeqNo, final long endSeqNo)
    {
        if (count == 0) {
            return false;
        }
        final long head = seqNums[headIndex];
        final long tail = seqNums[tailIndex];
        final long resolvedEnd = (endSeqNo == 0) ? tail : endSeqNo;
        return beginSeqNo >= head && resolvedEnd <= tail;
    }

    /**
     * Builds RESEND / GAP_FILL commands for the range [beginSeqNo, resolvedEndSeqNo].
     * Consecutive session messages are collapsed into a single GapFillCommand.
     * Must only be called after {@link #covers} returns true for the range.
     */
    public Command[] buildResendCommands(final long sessionId,
                                         final long beginSeqNo,
                                         final long endSeqNo) {
        final long resolvedEnd = (endSeqNo == 0) ? seqNums[tailIndex] : endSeqNo;
        final List<Command> cmds = new ArrayList<>();
        long gapFillStart = -1;
        for (long seq = beginSeqNo; seq <= resolvedEnd; seq++) {
            final int slotIndex = slotIndexFor(seq);
            if (slotIndex < 0) {
                break;
            }

            final int templateId = templateIds[slotIndex];
            if (isApplicationMessage(templateId)) {
                if (gapFillStart >= 0) {
                    cmds.add(new Command.GapFillCommand(sessionId, gapFillStart, seq));
                    gapFillStart = -1;
                }

                final byte[] payload = payloads[slotIndex];
                final int    len     = payloadLens[slotIndex];
                cmds.add(new Command.ResendCommand(
                    sessionId, seq, templateId,
                    new UnsafeBuffer(payload, 0, len), len));
            } else if (gapFillStart < 0) {
                gapFillStart = seq;
            }
        }
        if (gapFillStart >= 0)
        {
            cmds.add(new Command.GapFillCommand(sessionId, gapFillStart, resolvedEnd + 1));
        }
        return cmds.toArray(Command[]::new);
    }

    // ── Snapshot serialisation ────────────────────────────────────────────────

    /**
     * Writes the ring into buf starting at offset.
     * Format: count(int32) then for each slot in order from oldest to newest:
     *   seqNum(int64), clusterPosition(int64), templateId(int32),
     *   payloadLen(int32), payload(bytes)
     * <p>
     * Returns the number of bytes written.
     */
    public int encodeTo(final MutableDirectBuffer buf, int offset) {
        final int start = offset;
        buf.putInt(offset, count);
        offset += Integer.BYTES;

        for (int i = 0; i < count; i++) {
            final int idx = (headIndex + i) % capacity;
            buf.putLong(offset, seqNums[idx]);
            offset += Long.BYTES;
            buf.putLong(offset, clusterPositions[idx]);
            offset += Long.BYTES;
            buf.putInt(offset, templateIds[idx]);
            offset += Integer.BYTES;
            buf.putInt(offset, payloadLens[idx]);
            offset += Integer.BYTES;
            buf.putBytes(offset, payloads[idx], 0, payloadLens[idx]);
            offset += payloadLens[idx];
        }
        return offset - start;
    }

    /**
     * Restores the ring from buf starting at offset.
     * Returns the number of bytes consumed.
     */
    public int decodeFrom(final DirectBuffer buf, int offset) {
        final int start = offset;
        final int stored = buf.getInt(offset);
        offset += Integer.BYTES;

        headIndex = 0;
        count     = 0;
        tailIndex = -1;
        for (int i = 0; i < stored; i++) {
            final long seqNum          = buf.getLong(offset);     offset += Long.BYTES;
            final long clusterPosition = buf.getLong(offset);     offset += Long.BYTES;
            final int  templateId      = buf.getInt(offset);      offset += Integer.BYTES;
            final int  length          = buf.getInt(offset);      offset += Integer.BYTES;
            tailIndex = (tailIndex + 1) % capacity;
            seqNums[tailIndex]         = seqNum;
            clusterPositions[tailIndex] = clusterPosition;
            templateIds[tailIndex]     = templateId;
            payloadLens[tailIndex]     = length;

            if (payloads[tailIndex] == null || payloads[tailIndex].length < length)
            {
                payloads[tailIndex] = new byte[Math.max(length, 256)];
            }
            buf.getBytes(offset, payloads[tailIndex], 0, length);
            offset += length;
            count++;
        }
        return offset - start;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private int slotIndexFor(final long seqNum) {
        if (count == 0) return -1;
        final long head = seqNums[headIndex];
        final long tail = seqNums[tailIndex];
        if (seqNum < head || seqNum > tail) {
            return -1;
        }
        return (int) ((headIndex + (seqNum - head)) % capacity);
    }

    private static boolean isApplicationMessage(final int templateId) {
        for (final int sessionId : SESSION_TEMPLATE_IDS)
        {
            if (templateId == sessionId) return false;
        }
        return true;
    }

    /** Carries the sequence number and cluster position of an evicted slot. */
    public record EvictedEntry(long seqNum, long clusterPosition) {
    }
}