package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;

/**
 * A {@link ReplayerRecovery} and its transport, for a test outside this package, which can name neither:
 * {@code ReplaySeamTest} drives it against the Replayer through this. A subclass is the transport.
 */
public abstract class ReplayerRecoveryBridge implements ReplayerRecoveryActions {
    private final ReplayerRecovery recovery;

    protected ReplayerRecoveryBridge(final SequencedHandler onSequenced) {
        recovery = new ReplayerRecovery(this, onSequenced, null, null);
    }

    public void start() {
        recovery.start();
    }

    public void close() {
        recovery.close();
    }

    public void onFrame(final DirectBuffer buffer, final int offset, final int length, final long framePosition,
                        final long receiveNs, final boolean fromReplay) {
        recovery.onFrame(buffer, offset, length, framePosition, receiveNs, fromReplay);
    }

    public void onControl(final DirectBuffer buffer, final int offset, final int length) {
        recovery.onControl(buffer, offset, length);
    }

    public void onReplayPosition(final long position) {
        recovery.onReplayPosition(position);
    }

    public int doTimers(final boolean requestPublicationPending) {
        return recovery.doTimers(requestPublicationPending);
    }

    public String fault() {
        return recovery.fault();
    }

    public boolean isCaughtUp() {
        return recovery.isCaughtUp();
    }

    public boolean isAwaitingReplay() {
        return recovery.isAwaitingReplay();
    }

    public long replaySessionId() {
        return recovery.replaySessionId();
    }
}
