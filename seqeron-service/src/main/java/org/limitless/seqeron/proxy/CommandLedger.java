package org.limitless.seqeron.proxy;

import org.limitless.seqeron.protocol.Publish;

/**
 * The accounting of a client's commands in a ClientProxy's queue (doc/seqeron-protocol-spec.md §17.4): H-5's window,
 * counted in the encoded length of the windowed commands not yet placed, and H-3's epoch gate. Holds no Aeron
 * object; the proxy queues the bytes and publishes {@link #retired()} to its counter.
 *
 * <p>It also decides the fate of each {@code Publish} (H-17): a command with a {@code commandSeq} that is not placed
 * is reported to the {@link DropListener} in runs of consecutive sequence numbers, and {@link #resolved()} never
 * passes one it has not reported.
 *
 * <p>Single-threaded: every method belongs to the proxy's duty cycle.
 */
public final class CommandLedger {
    /** Why a {@code Publish} was not placed. */
    public enum Reason {
        EPOCH, REFUSED
    }

    /** Told of the {@code Publish} commands firstSeq to lastSeq that were not placed; it MUST NOT throw. */
    public interface DropListener {
        void dropped(long firstSeq, long lastSeq, Reason reason);
    }

    /** A command that is not an Application's {@code Publish} carries no sequence number. */
    public static final long NO_SEQ = 0;

    private final long window;
    private final DropListener listener;
    private long epoch;
    private boolean leading;
    private long queuedBytes;
    private long peakQueuedBytes;
    private long retired;
    private long placed;
    private long epochDrops;
    private long refusedDrops;
    private long headDeclines;
    private long resolved;
    private long runFirst;
    private long runLast;
    private Reason runReason;

    public CommandLedger(final long window, final DropListener listener) {
        this.window = window;
        this.listener = listener;
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
        resolved = 0;
        runFirst = 0;
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
    public void dropStale(final int length, final boolean windowed, final long commandSeq) {
        epochDrops++;
        retire(length, windowed);
        notPlaced(commandSeq, Reason.EPOCH);
    }

    /**
     * The façade's answer for the head: Declined keeps it where it is, anything else removes it.
     *
     * @return whether the head has left the queue
     */
    public boolean settle(final Publish result, final int length, final boolean windowed, final long commandSeq) {
        if (result == Publish.Declined) {
            headDeclines++;
            return false;
        }
        if (result == Publish.Published) {
            placed++;
            if (commandSeq != NO_SEQ) {
                closeRun();
                resolved = commandSeq;
            }
        } else {
            refusedDrops++;
            notPlaced(commandSeq, Reason.REFUSED);
        }
        retire(length, windowed);
        return true;
    }

    /** Reports the run in progress, if any: the proxy calls it once it has drained the queue for a cycle. */
    public void flush() {
        closeRun();
    }

    private void notPlaced(final long commandSeq, final Reason reason) {
        if (commandSeq == NO_SEQ) {
            return;
        }
        if (runFirst != 0 && commandSeq == runLast + 1 && reason == runReason) {
            runLast = commandSeq;
        } else {
            closeRun();
            runFirst = commandSeq;
            runLast = commandSeq;
            runReason = reason;
        }
        resolved = commandSeq;
    }

    private void closeRun() {
        if (runFirst != 0) {
            final long first = runFirst;
            runFirst = 0;
            listener.dropped(first, runLast, runReason);
        }
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

    /** The highest {@code commandSeq} whose fate is decided and, if it was not placed, reported (H-17); what {@code Resolved} carries. */
    public long resolved() {
        return runFirst != 0 ? runFirst - 1 : resolved;
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
