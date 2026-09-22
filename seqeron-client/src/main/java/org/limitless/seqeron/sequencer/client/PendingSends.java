package org.limitless.seqeron.sequencer.client;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;

/**
 * A producer's ingress frames that its own tap has not yet shown, which of them a leader change lost, and
 * their resend ahead of anything new.
 *
 * <p><b>Why a count is enough.</b> A leader drops ingress stamped with any term but its own, and every
 * election discards the ingress the old leader had not read. So a frame stamped term T is either on the tap
 * before the first {@code LeadershipChanged} with a term above T, or lost, and the lost ones are the newest
 * frames stamped T. Once that boundary has passed, every frame still pending with a term below it is
 * {@link #missing()}. The same holds when the same member wins the new term.
 *
 * <p><b>Resent in order, never twice.</b> A committed frame is never missing, so nothing is duplicated. From a
 * newer term's first sign — the sender's {@code NewLeader} or the tap's {@code LeadershipChanged}, whichever
 * comes first — until every older frame is seen or resent, {@link #isHolding()}: send nothing new, and give
 * this to the sender with {@code setIngressHold}, so a send already spinning through the election is given
 * up rather than landing ahead of the resend. {@link #resendMissing} does the resend. Given to an
 * {@code IngressPublisher}, this is tracked and gated on without the producer handling a frame.
 *
 * <p><b>Own frames are matched by session, not {@code sourceId}</b>: a gateway pair shares its
 * {@code sourceId}, and cluster session ids are never reused. Each frame records the session it went out
 * on, so a session replaced mid-term (the Java sender's reconnect) still matches its earlier frames. An own
 * frame must equal the oldest pending copy; anything else latches {@link #isFaulted()}, because the order
 * the count relies on no longer holds — most often the sequencer rejected a frame (S-7), so check §9.2
 * before sending. Tracking into a full ring latches it too: an untracked send breaks the count.
 *
 * <p>Limits: it lasts only as long as the process, so a promoted standby starts with nothing pending; it
 * needs the producer to follow its own tap; a lost session ({@code isSessionLost()}) stays terminal; loss
 * with no leader change has no boundary and is not detected. The C++ twin is {@code sequencer/client/PendingSends.hpp}; keep
 * the two in step.
 */
public final class PendingSends implements IngressTracker {
    private static final int SLOT_LENGTH = FrameLayer.MAX_INGRESS_LENGTH;
    private static final int NO_TERM = -1;

    private final int capacity;
    private final UnsafeBuffer frames;
    private final int[] lengths;
    private final long[] sessionIds;
    private final long[] termIds;
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeader = new UnsequencedHeaderDecoder();
    private final UnsafeBuffer resendView = new UnsafeBuffer(0, 0);
    private int head;
    private int size;
    private long closedTermId = NO_TERM;
    private long newLeaderTermId = NO_TERM;
    private boolean faulted;

    /**
     * A tracker holding nothing, for one producer's own sends.
     *
     * @param capacity frames that may be pending at once; each holds one {@code MAX_INGRESS_LENGTH} copy
     */
    public PendingSends(final int capacity) {
        this.capacity = capacity;
        frames = new UnsafeBuffer(new byte[capacity * SLOT_LENGTH]);
        lengths = new int[capacity];
        sessionIds = new long[capacity];
        termIds = new long[capacity];
    }

    /** Check before sending: a frame that cannot be tracked must not be sent. */
    @Override
    public boolean isFull() {
        return size == capacity;
    }

    /**
     * A frame the sender placed, read straight after its {@code send} returned true.
     * @param clusterSessionId the sender's {@code clusterSessionId()}
     * @param leadershipTermId the sender's {@code leadershipTermId()}
     */
    @Override
    public void track(final DirectBuffer frame, final int length, final long clusterSessionId,
                      final long leadershipTermId) {
        if (isFull()) {
            faulted = true;
            return;
        }
        final int slot = slot(size);
        frames.putBytes(slot * SLOT_LENGTH, frame, 0, length);
        lengths[slot] = length;
        sessionIds[slot] = clusterSessionId;
        termIds[slot] = leadershipTermId;
        size++;
    }

    /** From the stream client's leadership callback: closes the count on every earlier term. */
    public void onLeadershipChanged(final long leadershipTermId) {
        closedTermId = Math.max(closedTermId, leadershipTermId);
    }

    /** From the sender, which calls it on every {@code NewLeader}. */
    @Override
    public void onNewLeader(final long leadershipTermId) {
        newLeaderTermId = Math.max(newLeaderTermId, leadershipTermId);
    }

    /** Send nothing new while this holds: an older term's frames are still unseen or unresent. */
    @Override
    public boolean isHolding() {
        return size > 0 && termIds[head] < Math.max(newLeaderTermId, closedTermId);
    }

    /**
     * Resends the missing frames oldest first, each going back to the end of the ring as pending under the
     * term it now carries. Stops at the first send that fails, so a later call picks up where this left off.
     * @return how many were resent
     */
    public int resendMissing(final IngressSender sender) {
        if (sender.leadershipTermId() < closedTermId) {
            return 0; // the sender has no NewLeader yet, and the leader would drop them again
        }
        int resent = 0;
        while (size > 0 && termIds[head] < closedTermId) {
            resendView.wrap(frames, head * SLOT_LENGTH, lengths[head]);
            if (!sender.send(resendView, lengths[head])) {
                break;
            }
            moveHeadToTail(sender.clusterSessionId(), sender.leadershipTermId());
            resent++;
        }
        return resent;
    }

    /** Every frame off the tap, in order. Only this producer's own frames change anything. */
    public void onSequenced(final SequencedEvent event) {
        final int index = missing();
        if (index == size) {
            return;
        }
        final int slot = slot(index);
        if (event.sourceSessionId() != sessionIds[slot]) {
            return;
        }
        if (!matches(slot, event)) {
            faulted = true;
            return;
        }
        remove(index);
    }

    /** Pending frames a leader change has lost, oldest first at the front. */
    public int missing() {
        int count = 0;
        while (count < size && termIds[slot(count)] < closedTermId) {
            count++;
        }
        return count;
    }

    /** Frames tracked and not yet seen on the tap, missing ones included. */
    public int size() {
        return size;
    }

    /** Latched: the count can no longer be trusted, so fence the producer. */
    public boolean isFaulted() {
        return faulted;
    }

    private int slot(final int index) {
        return (head + index) % capacity;
    }

    /** The two families' headers share their layout (F-3), so one decoder reads either one's id at offset 16. */
    private boolean matches(final int slot, final SequencedEvent event) {
        final int base = slot * SLOT_LENGTH;
        final boolean system = messageHeader.wrap(frames, base).templateId() == UnsequencedSystemDecoder.TEMPLATE_ID;
        final int id = frameHeader.wrap(frames, base + MessageHeaderDecoder.ENCODED_LENGTH).payloadId();
        final int bodyLength = lengths[slot] - FrameLayer.MIN_INGRESS_LENGTH;
        if (event.isSystem() != system || (system ? event.systemEventType() : event.payloadId()) != id ||
            event.payloadLength() != bodyLength) {
            return false;
        }
        final DirectBuffer body = event.buffer();
        final int bodyBase = base + FrameLayer.MIN_INGRESS_LENGTH;
        for (int i = 0; i < bodyLength; i++) {
            if (frames.getByte(bodyBase + i) != body.getByte(event.payloadOffset() + i)) {
                return false;
            }
        }
        return true;
    }

    private void moveHeadToTail(final long clusterSessionId, final long leadershipTermId) {
        final int from = head;
        final int to = slot(size);
        if (to != from) {
            frames.putBytes(to * SLOT_LENGTH, frames, from * SLOT_LENGTH, lengths[from]);
            lengths[to] = lengths[from];
        }
        sessionIds[to] = clusterSessionId;
        termIds[to] = leadershipTermId;
        head = slot(1);
    }

    /** Drops the entry at {@code index}, moving the missing ones ahead of it up one slot to keep them in order. */
    private void remove(final int index) {
        for (int i = index; i > 0; i--) {
            final int to = slot(i);
            final int from = slot(i - 1);
            frames.putBytes(to * SLOT_LENGTH, frames, from * SLOT_LENGTH, lengths[from]);
            lengths[to] = lengths[from];
            sessionIds[to] = sessionIds[from];
            termIds[to] = termIds[from];
        }
        head = slot(1);
        size--;
    }
}
