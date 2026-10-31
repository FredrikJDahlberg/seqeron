package org.limitless.seqeron.protocol;

import io.aeron.Counter;
import org.limitless.seqeron.sbe.hosted.MessageHeaderDecoder;

/**
 * What a ClientProxy and its client share (doc/hosted-facade-draft.md §17.2, §17.7): schema 218 on four {@code
 * aeron:ipc} streams, and the heartbeat both write. The counters' type ids are in {@link SeqeronCounters}. The C++
 * twin is {@code protocol/ProxyProtocol.hpp}, which leaves the channels to each end.
 */
public final class ProxyProtocol {
    /** The {@code hostName} a client names in its {@code Attach}. */
    public static final String HOST_NAME = "hosted";

    /** {@code Attach.protocolVersion}. */
    public static final int PROTOCOL_VERSION = 1;

    /** The schema id of the {@code MessageHeader} on all four streams. */
    public static final int SCHEMA_ID = 218;

    /** Client → proxy: {@code Attach} and {@code Detach}, shared by every client. */
    public static final String ATTACH_REQUEST_CHANNEL = "aeron:ipc?term-length=64k";
    /** Proxy → client: {@code Attached}, shared by every client. */
    public static final String ATTACH_REPLY_CHANNEL = "aeron:ipc?term-length=64k";
    /** Proxy → client: one exclusive publication per attachment. */
    public static final String EVENTS_CHANNEL = "aeron:ipc";
    /** Client → proxy: one exclusive publication per attachment. */
    public static final String COMMANDS_CHANNEL = "aeron:ipc?term-length=1m";

    public static final int ATTACH_REQUEST_STREAM = 301;
    public static final int ATTACH_REPLY_STREAM = 302;
    public static final int EVENTS_STREAM = 303;
    public static final int COMMANDS_STREAM = 304;

    /** {@code Attached.eventSessionId} of a refused attach. */
    public static final int NO_ATTACH = -1;

    /** The length of the {@code MessageHeader} that starts every message. */
    public static final int HEADER = MessageHeaderDecoder.ENCODED_LENGTH;

    /** A reader's subscription to the other end's per-attachment stream (H-15). */
    public static String sessionChannel(final int sessionId) {
        return "aeron:ipc?session-id=" + sessionId;
    }

    /** A heartbeat value: this process's monotonic clock in ms (§17.2.1). */
    public static long heartbeatNow() {
        return System.nanoTime() / 1_000_000;
    }

    /** Writes {@link #heartbeatNow()} to a heartbeat counter. */
    public static void beat(final Counter counter) {
        counter.setOrdered(heartbeatNow());
    }

    private ProxyProtocol() {
    }
}
