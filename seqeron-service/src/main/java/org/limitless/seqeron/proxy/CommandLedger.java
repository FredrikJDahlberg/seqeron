package org.limitless.seqeron.proxy;

import org.limitless.seqeron.protocol.Publish;

/**
 * The accounting of a client's commands in a ClientProxy's queue (doc/hosted-facade-draft.md §17.4): H-5's window,
 * counted in the encoded length of the windowed commands not yet placed, and H-3's epoch gate. Holds no Aeron
 * object; the proxy queues the bytes and publishes {@link #retired()} to its counter.
 *
 * <p>Single-threaded: every method belongs to the proxy's duty cycle.
 */
public final class CommandLedger {
    private final long window;
    private long epoch;
    private boolean leading;
    private long queuedBytes;
    private long peakQueuedBytes;
    private long retired;
    private long placed;
    private long epochDrops;
    private long refusedDrops;
    private long headDeclines;

    public CommandLedger(final long window) {
        this.window = window;
    }

    /** A new attachment starts from epoch 0, not leading, with nothing queued. */
    public void reset() {
        epoch = 0;
        leading = false;
        queuedBytes = 0;
        peakQueuedBytes = 0;
        retired = 0;
        placed = 0;
        epochDrops = 0;
        refusedDrops = 0;
        headDeclines = 0;
    }

    /** The gate's epoch: it rises at every edge, and a command is placed only in the epoch it was decided in. */
    public long epoch() {
        return epoch;
    }

    public boolean leading() {
        return leading;
    }

    /** An edge of the façade's gate: leadership gained or lost, an edge opened or closed (H-3). */
    public void edge(final boolean nowLeading) {
        leading = nowLeading;
        epoch++;
    }

    /** Whether queueing this message would put the client past its window; a close is outside it (H-5). */
    public boolean exceedsWindow(final int length, final boolean windowed) {
        return queuedBytes + (windowed ? length : 0) > window;
    }

    /** The message is in the queue. */
    public void enqueued(final int length, final boolean windowed) {
        if (windowed) {
            queuedBytes += length;
            peakQueuedBytes = Math.max(peakQueuedBytes, queuedBytes);
        }
    }

    /** Whether the head was decided in an epoch that has passed, or while the façade is not leading. */
    public boolean stale(final long commandEpoch) {
        return commandEpoch != epoch || !leading;
    }

    /** The head is dropped for its epoch. */
    public void dropStale(final int length, final boolean windowed) {
        epochDrops++;
        retire(length, windowed);
    }

    /**
     * The façade's answer for the head: Declined keeps it where it is, anything else removes it.
     *
     * @return whether the head has left the queue
     */
    public boolean settle(final Publish result, final int length, final boolean windowed) {
        if (result == Publish.Declined) {
            headDeclines++;
            return false;
        }
        if (result == Publish.Published) {
            placed++;
        } else {
            refusedDrops++;
        }
        retire(length, windowed);
        return true;
    }

    /** The queue was emptied without placing anything: the attachment ended. */
    public void discarded() {
        queuedBytes = 0;
    }

    private void retire(final int length, final boolean windowed) {
        if (windowed) {
            queuedBytes -= length;
            retired += length;
        }
    }

    /** Total length of the windowed commands that have left the queue, placed or dropped; the retired counter. */
    public long retired() {
        return retired;
    }

    public long queuedBytes() {
        return queuedBytes;
    }

    public long peakQueuedBytes() {
        return peakQueuedBytes;
    }

    public long window() {
        return window;
    }

    public long placed() {
        return placed;
    }

    public long epochDrops() {
        return epochDrops;
    }

    public long refusedDrops() {
        return refusedDrops;
    }

    public long headDeclines() {
        return headDeclines;
    }
}
