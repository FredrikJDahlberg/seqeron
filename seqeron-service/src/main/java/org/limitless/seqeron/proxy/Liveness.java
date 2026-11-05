package org.limitless.seqeron.proxy;

import org.agrona.concurrent.status.CountersReader;

/**
 * Stale when the counter's value has not changed for the timeout on the reader's own clock, or it is no longer the
 * counter it was given (§17.2.1).
 */
public final class Liveness {
    private final CountersReader counters;
    private final long timeoutNs;
    private int counterId;
    private long registrationId;
    private long lastValue;
    private long lastChangeNs;

    public Liveness(final CountersReader counters, final long timeoutNs) {
        this.counters = counters;
        this.timeoutNs = timeoutNs;
    }

    public void watch(final int counterId, final long registrationId, final long nowNs) {
        this.counterId = counterId;
        this.registrationId = registrationId;
        lastValue = counters.getCounterValue(counterId);
        lastChangeNs = nowNs;
    }

    public boolean stale(final long nowNs) {
        if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED ||
            counters.getCounterRegistrationId(counterId) != registrationId) {
            return true;
        }
        final long value = counters.getCounterValue(counterId);
        if (value != lastValue) {
            lastValue = value;
            lastChangeNs = nowNs;
        }
        return nowNs - lastChangeNs > timeoutNs;
    }

    public long silentMs(final long nowNs) {
        return (nowNs - lastChangeNs) / 1_000_000;
    }
}
