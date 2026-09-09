package org.limitless.seqeron.replayer.server;

import java.util.HashMap;
import java.util.Map;

/**
 * Detects two co-located apps that were launched with the same {@code SEQERON_REPLAYER_CLIENT_ID}.
 */
public final class ReplayClientIdCollisions {
    /**
     * Backwards {@code requestId} steps within {@link #windowMs} that mean "two processes", not "one
     * restarted". A restart contributes exactly one, ever; two live clients each resending on their own
     * ~500ms timer contribute several per second.
     */
    private static final int SUSPECT_THRESHOLD = 3;

    private static final class Sequence {
        long lastRequestId;
        long windowStartMs;
        int backwardsSteps;
        boolean reported;
    }

    private final long windowMs;
    private final Map<Integer, Sequence> byClientId = new HashMap<>();

    public ReplayClientIdCollisions(final long windowMs) {
        this.windowMs = windowMs;
    }

    /**
     * Records one {@code ReplayRequest} and says whether it is the one that first makes a collision
     * suspect — true at most once per client id, so the caller logs a fault once rather than at the
     * clients' combined resend rate.
     * @param clientId the requesting client
     * @param requestId that request's id (strictly increasing for a single live client)
     * @param nowMs current time
     * @return true on the transition into "suspected", false otherwise
     */
    public boolean onRequest(final int clientId, final long requestId, final long nowMs) {
        final Sequence sequence = byClientId.get(clientId);
        if (sequence == null) {
            // First request from this id: it seeds the baseline and is evidence of nothing. Comparing it
            // against a baseline seeded from itself counted every client's first request as a backwards
            // step, which put the threshold one real regression lower than it reads.
            final Sequence created = new Sequence();
            created.lastRequestId = requestId;
            created.windowStartMs = nowMs;
            byClientId.put(clientId, created);
            return false;
        }

        final boolean backwards = requestId <= sequence.lastRequestId;
        sequence.lastRequestId = requestId;
        if (!backwards) {
            return false;
        }

        if ((nowMs - sequence.windowStartMs) > windowMs) {
            sequence.windowStartMs = nowMs;
            sequence.backwardsSteps = 0;
        }
        ++sequence.backwardsSteps;
        if (sequence.backwardsSteps < SUSPECT_THRESHOLD || sequence.reported) {
            return false;
        }
        sequence.reported = true;
        return true;
    }
}
