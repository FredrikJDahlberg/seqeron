package org.limitless.seqeron.tools;

import org.agrona.MutableDirectBuffer;

/**
 * Wire of the hosted-façade prototype (doc/hosted-facade-draft.md), bench-only and not schema 218: a 16-byte header
 * of {@code type:1, pad:7, attachId:8}, then the fields at the offsets the two mains name. Stream 301 carries the
 * host's messages, stream 302 the client's, both {@code aeron:ipc} on the member's driver.
 */
final class HostedWire {
    static final String IPC = "aeron:ipc";
    static final int EVENTS_STREAM = 301;
    static final int COMMANDS_STREAM = 302;
    static final int HEADER = 16;

    // Host to client.
    /** nonce:8 at 16, echoing the Attach's; retiredCounterId:4 at 24, commandWindow:8 at 28. */
    static final byte ATTACHED = 1;
    /** clusterSeqNo:8 at 16, sourceId:4 at 24, payloadId:4 at 28, templateId:4 at 32, length:4 at 36, bytes at 40. */
    static final byte PAYLOAD = 2;
    /** gateEpoch:8 at 16, leading:1 at 24, canPublish:1 at 25. */
    static final byte GATE = 3;
    /** clusterSeqNo:8 at 16. */
    static final byte CAUGHT_UP = 4;
    /** recordIndex:4 at 16, formatVersion:4 at 20, length:4 at 24, bytes at 28. */
    static final byte RESTORE_RECORD = 5;
    /** cutNo:8 at 16. */
    static final byte SNAPSHOT_CUT = 6;
    /** hostNs:8 at 16. */
    static final byte HEARTBEAT = 7;
    /** length:4 at 16, text at 20. */
    static final byte FENCED = 8;

    // Client to host.
    /** nonce:8 at 16, clientId:4 at 24, formatVersion:4 at 28; attachId 0. */
    static final byte ATTACH = 21;
    /** correlationId:8 at 16, gateEpoch:8 at 24, payloadId:4 at 32, length:4 at 36, bytes at 40. */
    static final byte PUBLISH = 22;
    /** cutNo:8 at 16, recordIndex:4 at 24, length:4 at 28, bytes at 32. */
    static final byte SNAPSHOT_RECORD = 23;
    /** cutNo:8 at 16, recordCount:4 at 24. */
    static final byte SNAPSHOT_END = 24;
    /** clientNs:8 at 16, appliedClusterSeqNo:8 at 24. */
    static final byte CLIENT_HEARTBEAT = 25;
    static final byte DETACH = 26;

    static void header(final MutableDirectBuffer buffer, final byte type, final long attachId) {
        buffer.putByte(0, type);
        buffer.putLong(8, attachId);
    }

    private HostedWire() {
    }
}
