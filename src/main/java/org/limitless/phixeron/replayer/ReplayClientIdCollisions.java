package org.limitless.phixeron.replayer;

import java.util.HashMap;
import java.util.Map;

/**
 * Detects two co-located apps that were launched with the same {@code PHIXERON_REPLAYER_CLIENT_ID}.
 * Pure — no Aeron, no archive, no clock of its own — so it is unit-testable directly, mirroring {@link
 * ReplaySlotAllocator}'s split from {@link ReplayerService}.
 *
 * <p><b>Why this needs detecting at all.</b> {@link ReplayerService} keys everything on {@code
 * clientId}: {@code onRequest} supersedes (and stops) that client's in-flight replay on every request it
 * sees. Two processes sharing an id therefore stop each other's replay on each request — each sees its
 * image close short of its bound, each re-requests, and each re-request kills the other's. Neither ever
 * completes a segment, neither ever catches up, and nothing says why: the logs show only ordinary
 * "closed short of catchUpPosition" lines on both sides. It is a livelock produced by a one-line
 * configuration mistake, and it is invisible.
 *
 * <p><b>What separates the two cases.</b> A single client's {@code requestId} advances on every send and
 * is never reset in-process, and the request stream is an ordered IPC publication — so for one live
 * client the ids seen here are strictly increasing. A <em>restart</em> shows up as one backwards step
 * (the new process starts its counter again) followed by monotone ids. Two live processes interleave
 * their two counters, so they produce backwards steps repeatedly, at the rate they resend. Counting
 * backwards steps within a window separates them: a restart contributes one, a collision contributes a
 * stream of them.
 *
 * <p><b>Reports, does not refuse.</b> Answering {@code ReplayUnavailable} to a suspected collision would
 * be fail-closed, but it converts one livelock into another — the apps hold and never dispatch either
 * way — while giving a false positive the power to stop a healthy replica from ever recovering. The
 * livelock being <em>silent</em> is the defect; a fault line plus a counter is what fixes that, and the
 * operator fixes the configuration.
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
        final Sequence sequence = byClientId.computeIfAbsent(clientId, id -> {
            final Sequence created = new Sequence();
            created.lastRequestId = requestId;
            created.windowStartMs = nowMs;
            return created;
        });

        final boolean backwards = requestId <= sequence.lastRequestId;
        sequence.lastRequestId = requestId;
        if (!backwards) {
            return false;
        }

        if ((nowMs - sequence.windowStartMs) > windowMs) {
            // The previous steps are too old to be evidence of anything: an app restarting once an hour
            // must not accumulate its way to a collision report.
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
