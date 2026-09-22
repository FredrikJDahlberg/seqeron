package org.limitless.seqeron.app;

/**
 * Observes how far behind the leader a co-located tap is. A tap-silence watchdog bounds a tap that goes
 * silent and {@link RecoveryStallFence} one that never goes contiguous; neither sees a tap that is contiguous
 * and punctual but old — a member applying the committed log behind the leader publishes a gap-free tap at a
 * healthy 1 Hz while the view it serves sits arbitrarily far in the past.
 *
 * <p><b>Observation only; it raises no fence.</b> Raw lag cannot tell "my node is behind" from "the whole
 * cluster is behind", and fencing every gateway on the second turns a slow cluster into an outage.
 *
 * <p><b>The sample is arrival lateness</b> — {@code receiveTimeNs - clusterTimestampNs} of each
 * {@code ClusterHeartbeat} — not its age at check time. Frames retained during a re-walk are handed over as
 * live once the hole closes; they arrived promptly and were merely held, and "now minus timestamp" would
 * read the whole re-walk as lag. The two stamps come from different hosts' clocks, so the sample includes
 * their offset (see {@code doc/ops.md}, "The consensus clock").
 *
 * <p>The C++ twin is {@code app/TapLagMonitor.hpp}; keep the two in step.
 */
public final class TapLagMonitor {
    /** An edge crossed by one sample. */
    public enum TapLag {
        /** No edge crossed — the common case, so the caller logs nothing. */
        NONE,
        /** Lag reached the threshold, having been under it. */
        BECAME_STALE,
        /** Lag fell back under the threshold, having been over it. */
        BECAME_FRESH,
        /** A heartbeat arrived a threshold or more before its own timestamp; reported once, then latched. */
        SKEW_SUSPECTED
    }

    private final long thresholdNs;

    private long lastLagNs;
    private long peakLagNs;
    private long sampleCount;
    private boolean stale;
    private boolean skewReported;

    /**
     * A monitor that has seen no sample yet.
     *
     * @param thresholdNs the lag at which the tap is called stale. No tighter than the caller's tap-silence
     *                    timeout: the first heartbeat after a tolerated silence is that late.
     */
    public TapLagMonitor(final long thresholdNs) {
        this.thresholdNs = thresholdNs;
    }

    /**
     * Evaluates one live {@code ClusterHeartbeat}. The caller gates on {@code isCaughtUp()}: a replayed
     * heartbeat's lateness is the age of history, not lag.
     * @param clusterTimestampNs the heartbeat's consensus timestamp
     * @param receiveTimeNs      when this client received it
     * @return only edges, so a caller logs per transition rather than at the sample rate
     */
    public TapLag onClusterHeartbeat(final long clusterTimestampNs, final long receiveTimeNs) {
        lastLagNs = receiveTimeNs - clusterTimestampNs;
        ++sampleCount;
        if (lastLagNs > peakLagNs) {
            peakLagNs = lastLagNs;
        }
        // Impossible on one clock: this host is behind the leader by at least the threshold, so a real lag of
        // that size would read as fresh. Latched, because it is a deployment fault, not an event.
        if (lastLagNs <= -thresholdNs) {
            if (skewReported) {
                return TapLag.NONE;
            }
            skewReported = true;
            return TapLag.SKEW_SUSPECTED;
        }
        if (lastLagNs >= thresholdNs) {
            if (stale) {
                return TapLag.NONE;
            }
            stale = true;
            return TapLag.BECAME_STALE;
        }
        if (stale) {
            stale = false;
            return TapLag.BECAME_FRESH;
        }
        return TapLag.NONE;
    }

    /** Lag of the most recent sample. */
    public long lastLagNs() {
        return lastLagNs;
    }

    /**
     * Worst lag seen since start. Report it with {@link #sampleCount()}: a client that never caught up also
     * peaks at 0, and that must not read as "lag was perfect".
     */
    public long peakLagNs() {
        return peakLagNs;
    }

    /** Samples taken since start; 0 means {@link #peakLagNs()} says nothing. */
    public long sampleCount() {
        return sampleCount;
    }

    /** Whether the last sample was beyond the threshold. */
    public boolean isStale() {
        return stale;
    }

    /**
     * Latched once a heartbeat arrives a threshold or more before its own timestamp: this host's clock
     * trails the leader's, so {@link #isStale()} cannot be trusted until the clocks are synchronised.
     */
    public boolean isSkewSuspected() {
        return skewReported;
    }
}
