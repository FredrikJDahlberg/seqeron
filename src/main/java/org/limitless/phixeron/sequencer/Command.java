package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Commands returned by FixSessionStateMachine and dispatched by FixAeronHandler.
 * <p>
 * Stream 2 command envelope (all publishable commands):
 *   byte  0:     uint8   discriminator
 *   bytes 1–8:   uint64  clusterSessionPosition of the originating log entry
 *   bytes 9+:    SBE payload (command-specific)
 * <p>
 * ScheduleTimerCommand, StartArchiveReplayCommand, and CancelPendingResendCommand
 * are handled locally by FixAeronHandler and are never published on stream 2.
 */
public sealed interface Command
    permits Command.SendCommand,
            Command.ForwardAppCommand,
            Command.ResendCommand,
            Command.GapFillCommand,
            Command.DisconnectCommand,
            Command.ScheduleTimerCommand,
            Command.StartArchiveReplayCommand,
            Command.CancelPendingResendCommand {
    Command[] NONE = new Command[0];

    // ── Stream 2 command discriminators ──────────────────────────────────────
    int CMD_SEND        = 0x10;
    int CMD_FORWARD_APP = 0x11;
    int CMD_RESEND      = 0x12;
    int CMD_GAP_FILL    = 0x13;
    int CMD_DISCONNECT  = 0x14;

    // ── Envelope layout ───────────────────────────────────────────────────────
    int ENVELOPE_DISCRIMINATOR_OFFSET = 0;
    int ENVELOPE_CLUSTER_POS_OFFSET   = 1;
    int ENVELOPE_PAYLOAD_OFFSET       = 9;

    // ── Publishable command API ───────────────────────────────────────────────

    default void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " is not publishable");
    }

    default int encodedLength() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " is not publishable");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Publishable commands (emitted on Aeron IPC stream 2 to AppWorker)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Instructs the AppWorker / FIX Session Proxy to send an outbound session message
     * (Logon-Ack, Heartbeat, TestRequest, Logout, ResendRequest).
     * <p>
     * payload contains a complete SBE-encoded FIX session message.
     * seqNum is the outboundSeqNum assigned to this message.
     * templateId identifies the SBE message type for MemoryStorage classification.
     */
    record SendCommand(
        long         sessionId,
        long         seqNum,
        int          templateId,
        DirectBuffer payload,
        int          length)
        implements Command {
        @Override
        public void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition)
        {
            buf.putByte(offset + ENVELOPE_DISCRIMINATOR_OFFSET, (byte) CMD_SEND);
            buf.putLong(offset + ENVELOPE_CLUSTER_POS_OFFSET, clusterPosition);
            buf.putBytes(offset + ENVELOPE_PAYLOAD_OFFSET, payload, 0, length);
        }

        @Override
        public int encodedLength()
        {
            return ENVELOPE_PAYLOAD_OFFSET + length;
        }
    }

    /**
     * Forwards an inbound application-layer FIX message (D, F, G, …) to the AppWorker
     * for risk-check and routing.
     * <p>
     * payload is the inbound SBE frame verbatim.
     */
    record ForwardAppCommand(
        long         sessionId,
        DirectBuffer payload,
        int          length)
        implements Command {
        @Override
        public void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition)
        {
            buf.putByte(offset + ENVELOPE_DISCRIMINATOR_OFFSET, (byte) CMD_FORWARD_APP);
            buf.putLong(offset + ENVELOPE_CLUSTER_POS_OFFSET, clusterPosition);
            buf.putLong(offset + ENVELOPE_PAYLOAD_OFFSET, sessionId);
            buf.putBytes(offset + ENVELOPE_PAYLOAD_OFFSET + Long.BYTES, payload, 0, length);
        }

        @Override
        public int encodedLength() {
            return ENVELOPE_PAYLOAD_OFFSET + Long.BYTES + length;
        }
    }

    /**
     * Retransmits an outbound SBE session message with PossDupFlag implied
     * (the FIX proxy adds tag 43=Y on encoding).
     * <p>
     * payload is the original outbound SBE payload from MemoryStorage.
     */
    record ResendCommand(
        long         sessionId,
        long         seqNum,
        int          templateId,
        DirectBuffer payload,
        int          length)
        implements Command
    {
        @Override
        public void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition) {
            buf.putByte(offset + ENVELOPE_DISCRIMINATOR_OFFSET, (byte) CMD_RESEND);
            buf.putLong(offset + ENVELOPE_CLUSTER_POS_OFFSET, clusterPosition);
            buf.putBytes(offset + ENVELOPE_PAYLOAD_OFFSET, payload, 0, length);
        }

        @Override
        public int encodedLength()
        {
            return ENVELOPE_PAYLOAD_OFFSET + length;
        }
    }

    /**
     * Instructs the FIX proxy to send a SequenceReset-GapFill covering
     * [fromSeqNum, newSeqNo) (i.e., skipping those outbound sequence numbers).
     * <p>
     * Stream 2 payload (9-byte envelope + 24 bytes):
     *   sessionId:   int64
     *   fromSeqNum:  int64  (MsgSeqNum of the SequenceReset; first skipped seq)
     *   newSeqNo:    int64  (NewSeqNo field; first expected after the gap)
     */
    record GapFillCommand(
        long sessionId,
        long fromSeqNum,
        long newSeqNo)
        implements Command {
        private static final int PAYLOAD_LEN = Long.BYTES * 3; // sessionId + fromSeqNum + newSeqNo

        @Override
        public void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition) {
            buf.putByte(offset + ENVELOPE_DISCRIMINATOR_OFFSET, (byte) CMD_GAP_FILL);
            buf.putLong(offset + ENVELOPE_CLUSTER_POS_OFFSET, clusterPosition);
            buf.putLong(offset + ENVELOPE_PAYLOAD_OFFSET, sessionId);
            buf.putLong(offset + ENVELOPE_PAYLOAD_OFFSET + Long.BYTES, fromSeqNum);
            buf.putLong(offset + ENVELOPE_PAYLOAD_OFFSET + Long.BYTES * 2, newSeqNo);
        }

        @Override
        public int encodedLength()
        {
            return ENVELOPE_PAYLOAD_OFFSET + PAYLOAD_LEN;
        }
    }

    /**
     * Instructs the FIX proxy to close the TCP fd associated with sessionId
     * (cluster-initiated disconnect; §6.2.5).
     * <p>
     * The FIX proxy writes MSG_TX_CLOSE to the TX ring → Core 1 closes the fd.
     * Core 1 does NOT publish MSG_RX_DISCONNECT in response (the session is already
     * known to the cluster as DISCONNECTED).
     * <p>
     * Stream 2 payload (9-byte envelope + 8 bytes sessionId).
     */
    record DisconnectCommand(long sessionId) implements Command {
        private static final int PAYLOAD_LEN = Long.BYTES;

        @Override
        public void encodeInto(final MutableDirectBuffer buf, final int offset, final long clusterPosition) {
            buf.putByte(offset + ENVELOPE_DISCRIMINATOR_OFFSET, (byte) CMD_DISCONNECT);
            buf.putLong(offset + ENVELOPE_CLUSTER_POS_OFFSET, clusterPosition);
            buf.putLong(offset + ENVELOPE_PAYLOAD_OFFSET, sessionId);
        }

        @Override
        public int encodedLength() {
            return ENVELOPE_PAYLOAD_OFFSET + PAYLOAD_LEN;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal commands (handled by FixAeronHandler, never published on stream 2)
    // ─────────────────────────────────────────────────────────────────────────

    /** Registers a heartbeat or test-request timer with the Aeron Cluster conductor. */
    record ScheduleTimerCommand(long correlationId, long deadlineMs) implements Command {
    }

    /**
     * Starts an asynchronous Aeron Archive replay to service a slow-path ResendRequest
     * (requested range extends beyond MemoryStorage's 2500-message window).
     * endSeqNo is the last requested sequence number (0 = open-ended up to outboundSeqNum).
     */
    record StartArchiveReplayCommand(long recordingId, long startPosition, long endSeqNo) implements Command {}

    /**
     * Cancels any in-progress Archive replay for sessionId.
     * Emitted by onSessionDisconnect to prevent ghost-resend after disconnect.
     */
    record CancelPendingResendCommand(long sessionId) implements Command {
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    static Command[] concat(final Command[] a, final Command[] b)
    {
        if (a.length == 0) {
            return b;
        }
        if (b.length == 0) {
            return a;
        }
        final Command[] result = new Command[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}