package org.limitless.seqeron.app;

import org.agrona.DirectBuffer;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.QueueResolvedDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;

/**
 * Which queue this instance reads (spec R-6): a {@code QueueRequest} to the node's Replayer, resent until it
 * answers with a {@code QueueResolved}. Until then the Replayer answers {@code ReplayPending}, which holds the
 * resend off as it does a replay request's. Holds no Aeron runtime; {@link Session} is its transport. The C++
 * twin is {@code app/detail/QueueResolution.hpp}; keep the two in step.
 */
final class QueueResolution {
    /** {@link #groupId()} before the Replayer has answered. */
    static final int UNRESOLVED = 0;

    static final long RESEND_INTERVAL_MS = 500;

    /** What this cannot do itself: the send, the id replies are addressed to, and the clock. */
    interface Actions {
        /** Offers one {@code QueueRequest}; best-effort, since the resend timer retries. */
        void sendQueueRequest(long requestId);

        /** The id the Replayer addresses this instance's replies to (R-4). */
        long clientId();

        /** Monotonic millis. */
        long nowMs();
    }

    private final Actions actions;
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final QueueResolvedDecoder resolved = new QueueResolvedDecoder();
    private final ReplayPendingDecoder pending = new ReplayPendingDecoder();

    private boolean started;
    private long requestId;
    private long lastRequestMs;
    private int groupId = UNRESOLVED;

    QueueResolution(final Actions actions) {
        this.actions = actions;
    }

    /** Sends the first request. */
    void start() {
        started = true;
        send();
    }

    /**
     * The resend, once per duty cycle.
     * @param publicationPending the request publication has not connected yet: retried every cycle rather than
     *                           waiting out a resend interval for a race much shorter than one
     */
    void doTimers(final boolean publicationPending) {
        if (started && groupId == UNRESOLVED &&
            (publicationPending || actions.nowMs() - lastRequestMs > RESEND_INTERVAL_MS)) {
            send();
        }
    }

    /** Takes one control message; only a reply to the current request counts (R-1, R-4). */
    void onControl(final DirectBuffer buffer, final int offset, final int length) {
        if (groupId != UNRESOLVED || length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        if (header.templateId() == QueueResolvedDecoder.TEMPLATE_ID) {
            resolved.wrap(buffer, bodyOffset, header.blockLength(), header.version());
            if (resolved.clientId() == actions.clientId() && resolved.requestId() == requestId) {
                groupId = resolved.queueId();
            }
        } else if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
            pending.wrap(buffer, bodyOffset, header.blockLength(), header.version());
            if (pending.clientId() == actions.clientId() && pending.requestId() == requestId) {
                lastRequestMs = actions.nowMs();
            }
        }
    }

    /** The group whose queue this instance reads, or {@link #UNRESOLVED}. */
    int groupId() {
        return groupId;
    }

    /** The id the next reply must carry to count. */
    long requestId() {
        return requestId;
    }

    private void send() {
        lastRequestMs = actions.nowMs();
        actions.sendQueueRequest(++requestId);
    }
}
