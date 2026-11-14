package org.limitless.seqeron.replayer.server;

/** A run of occurrences of one fault, ended by a quiet period; each run is reported once. */
final class Episode {
    private final long quietMs;
    private long lastMs = 0;

    /**
     * @param quietMs how long without an occurrence ends the episode
     */
    Episode(final long quietMs) {
        this.quietMs = quietMs;
    }

    /**
     * Notes an occurrence.
     * @param nowMs the clock
     * @return whether it begins an episode
     */
    boolean begins(final long nowMs) {
        final boolean begins = lastMs == 0 || nowMs - lastMs >= quietMs;
        lastMs = nowMs;
        return begins;
    }
}
