package org.limitless.seqeron.tools;

import io.aeron.Counter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.sbe.hosted.MessageHeaderDecoder;

/**
 * Transport of the hosted-façade prototype (doc/hosted-facade-draft.md §17.2): schema 218 on four {@code aeron:ipc}
 * streams, and the four counters of §17.2.1. Windowed lengths (H-5) are the encoded message, {@code MessageHeader}
 * included, which is the fragment length both ends see.
 */
final class HostedWire {
    static final String HOST_NAME = "hosted";
    static final int PROTOCOL_VERSION = 1;

    static final String ATTACH_REQUEST_CHANNEL = "aeron:ipc?term-length=64k";
    static final String ATTACH_REPLY_CHANNEL = "aeron:ipc?term-length=64k";
    static final String EVENTS_CHANNEL = "aeron:ipc";
    static final String COMMANDS_CHANNEL = "aeron:ipc?term-length=1m";
    static final int ATTACH_REQUEST_STREAM = 301;
    static final int ATTACH_REPLY_STREAM = 302;
    static final int EVENTS_STREAM = 303;
    static final int COMMANDS_STREAM = 304;
    static final int NO_ATTACH = -1;

    /** Proposed counter type ids (§17.2.1). */
    static final int CLIENT_HEARTBEAT_TYPE_ID = 5300;
    static final int CLIENT_APPLIED_TYPE_ID = 5301;
    static final int HOST_HEARTBEAT_TYPE_ID = 5302;
    static final int HOST_RETIRED_TYPE_ID = 5303;

    static final int HEADER = MessageHeaderDecoder.ENCODED_LENGTH;

    /** A reader's subscription to the other end's per-attachment stream (H-15). */
    static String sessionChannel(final int sessionId) {
        return "aeron:ipc?session-id=" + sessionId;
    }

    /** A heartbeat value: this process's monotonic clock in ms (§17.2.1). */
    static long heartbeatNow() {
        return System.nanoTime() / 1_000_000;
    }

    static void beat(final Counter counter) {
        counter.setOrdered(heartbeatNow());
    }

    /**
     * Stale when the counter's value has not changed for the timeout on the reader's own clock, or it is no longer the
     * counter it was given (§17.2.1).
     */
    static final class Liveness {
        private final CountersReader counters;
        private final long timeoutNs;
        private int counterId;
        private long registrationId;
        private long lastValue;
        private long lastChangeNs;

        Liveness(final CountersReader counters, final long timeoutNs) {
            this.counters = counters;
            this.timeoutNs = timeoutNs;
        }

        void watch(final int counterId, final long registrationId, final long nowNs) {
            this.counterId = counterId;
            this.registrationId = registrationId;
            lastValue = counters.getCounterValue(counterId);
            lastChangeNs = nowNs;
        }

        boolean stale(final long nowNs) {
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

        long silentMs(final long nowNs) {
            return (nowNs - lastChangeNs) / 1_000_000;
        }
    }

    private HostedWire() {
    }
}
