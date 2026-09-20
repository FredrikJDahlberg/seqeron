package org.limitless.seqeron.replayer.client;

/** Receives every in-order frame that is not intercepted as a leadership change. */
@FunctionalInterface
public interface SequencedHandler {
    /**
     * Called once per frame, in {@code globalSeqNo} order.
     * @param event a flyweight, valid only for this call; copy anything that must outlive it
     */
    void onSequenced(SequencedEvent event);
}
