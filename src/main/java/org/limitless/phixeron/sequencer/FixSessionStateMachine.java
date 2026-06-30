package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.LinkedHashMap;

import static org.limitless.phixeron.sequencer.Command.*;

/**
 * Per-session FIX Session State Machine.
 *
 * Contains all FIX session logic: phase transitions, sequence number validation,
 * heartbeat and test-request timer management, and outbound message building.
 * Has no direct dependency on Aeron Cluster APIs — receives decoded events and
 * emits typed Command arrays consumed by FixAeronHandler.
 *
 * All access is from the single Aeron Cluster conductor thread.
 *
 * SBE message layout (little-endian, sbe-session.xml):
 *   SBE header (8 bytes):  blockLength(u16) templateId(u16) schemaId(u16) version(u16)
 *   Standard block (28 bytes): sender(8) target(8) seqNum(u32) sendingTimeMs(i64)
 *   Message-specific body starts at SBE_HEADER_LEN + STANDARD_BLOCK_LEN = 36.
 *
 *   Logon (template 65):  encryptMethod(u8@36) heartbeatInterval(u32@37) xmlData_len(u16@41)
 *   Heartbeat (template 48): testReqID(32 chars @36)
 *   TestRequest (template 49): testReqID(32 chars @36)
 *   ResendRequest (template 50): beginSeqNo(u32@36) endSeqNo(u32@40)
 *   SequenceReset (template 52): gapFillFlag(u8@36) newSeqNo(u32@37)
 *   Logout (template 53): text_len(u16@36)
 */
public final class FixSessionStateMachine
{
    // ── SBE layout constants ──────────────────────────────────────────────────
    static final int SBE_HEADER_LEN          = 8;
    static final int SBE_TEMPLATE_ID_OFFSET  = 2;   // within SBE header
    static final int STANDARD_BLOCK_LEN      = 28;
    static final int BLOCK_START             = SBE_HEADER_LEN;  // 8
    static final int BLOCK_SENDER_OFFSET     = BLOCK_START + 0;
    static final int BLOCK_TARGET_OFFSET     = BLOCK_START + 8;
    static final int BLOCK_SEQ_NUM_OFFSET    = BLOCK_START + 16;
    static final int BLOCK_SENDING_TIME_OFFSET = BLOCK_START + 20;
    static final int BODY_START              = BLOCK_START + STANDARD_BLOCK_LEN; // 36

    // Logon body offsets from message start
    static final int LOGON_ENCRYPT_METHOD_OFFSET     = BODY_START;      // u8
    static final int LOGON_HEARTBEAT_INTERVAL_OFFSET = BODY_START + 1;  // u32
    static final int LOGON_XML_DATA_LEN_OFFSET       = BODY_START + 5;  // u16

    // Heartbeat / TestRequest body
    static final int HEARTBEAT_TEST_REQ_ID_OFFSET    = BODY_START;      // 32 chars

    // ResendRequest body
    static final int RESEND_BEGIN_SEQ_NO_OFFSET      = BODY_START;      // u32
    static final int RESEND_END_SEQ_NO_OFFSET        = BODY_START + 4;  // u32

    // SequenceReset body
    static final int SEQ_RESET_GAP_FILL_FLAG_OFFSET  = BODY_START;      // u8
    static final int SEQ_RESET_NEW_SEQ_NO_OFFSET     = BODY_START + 1;  // u32

    // Logout body
    static final int LOGOUT_TEXT_LEN_OFFSET          = BODY_START;      // u16

    // Session-connect payload (no SBE header; raw fixed fields after envelope)
    static final int SESSION_CONNECT_PEER_COMP_ID_OFFSET  = 0;   // 8 bytes
    static final int SESSION_CONNECT_LOCAL_COMP_ID_OFFSET = 8;   // 8 bytes

    // SBE template IDs (= ASCII value of FIX MsgType)
    static final int TEMPLATE_HEARTBEAT      = 48;  // '0'
    static final int TEMPLATE_TEST_REQUEST   = 49;  // '1'
    static final int TEMPLATE_RESEND_REQUEST = 50;  // '2'
    static final int TEMPLATE_SEQUENCE_RESET = 52;  // '4'
    static final int TEMPLATE_LOGOUT         = 53;  // '5'
    static final int TEMPLATE_LOGON          = 65;  // 'A'

    // SBE schema constants for outbound encoding
    static final int SCHEMA_ID      = 100;
    static final int SCHEMA_VERSION = 0;

    // Inbound event discriminators (envelope byte 0; FIX MsgType as ASCII value for FIX messages)
    static final int DISC_SESSION_CONNECT    = 0x01;
    static final int DISC_SESSION_DISCONNECT = 0x02;
    static final int DISC_HEARTBEAT          = TEMPLATE_HEARTBEAT;
    static final int DISC_TEST_REQUEST       = TEMPLATE_TEST_REQUEST;
    static final int DISC_RESEND_REQUEST     = TEMPLATE_RESEND_REQUEST;
    static final int DISC_SEQUENCE_RESET     = TEMPLATE_SEQUENCE_RESET;
    static final int DISC_LOGOUT             = TEMPLATE_LOGOUT;
    static final int DISC_LOGON              = TEMPLATE_LOGON;

    // GapFillFlag sentinel values (uint8; 0 = absent, 89 = 'Y', 78 = 'N')
    static final int GAP_FILL_FLAG_Y = 89;

    // Timing defaults; may be overridden by configuration
    static final long LOGON_TIMEOUT_MS    = 30_000L;
    static final long TEST_REQ_TIMEOUT_MS = 30_000L;
    static final long LOGOUT_TIMEOUT_MS   = 10_000L;

    // CompID field length in SBE (chars, space-padded to 8)
    static final int COMP_ID_LEN     = 8;
    static final int TEST_REQ_ID_LEN = 32;

    // ── Identity ──────────────────────────────────────────────────────────────
    private final long        sessionId;
    private final SessionRole sessionRole;

    // ── Snapshotted state ─────────────────────────────────────────────────────
    private SessionPhase sessionPhase     = SessionPhase.DISCONNECTED;
    private long         inboundSeqNum    = 0;
    private long         outboundSeqNum   = 0;
    private String       pendingTestReqId = null;
    private long         heartbeatTimer   = 0;
    private long         heartbeatTimerDeadline = 0;
    private long         testReqTimer     = 0;
    private long         testReqTimerDeadline   = 0;
    private long         nextTimerId      = 1;
    private long         heartbeatIntervalMs = 30_000L;

    // CompIDs extracted from the first Logon; space-padded to COMP_ID_LEN bytes
    private final byte[] localCompId = new byte[COMP_ID_LEN]; // gateway side
    private final byte[] peerCompId  = new byte[COMP_ID_LEN]; // counterparty

    // Outbound message storage
    final MemoryStorage memoryStorage;
    final LinkedHashMap<Long, ArchivePosition> outboundArchiveIndex = new LinkedHashMap<>();

    // ── Runtime-only state (not snapshotted) ──────────────────────────────────

    /**
     * Set to true by FixAeronHandler after onLoadSnapshot when pendingTestReqId != null.
     * On the first committed message, clear stale probe state and reschedule a fresh
     * heartbeat timer (§4.5.1 post-failover TestRequest invariant).
     */
    boolean postFailoverPendingTestReqReset = false;

    // Shared encoding buffer for building outbound SBE messages.
    // Sized to accommodate the largest session message (Heartbeat = 68 bytes).
    private final MutableDirectBuffer outboundBuf = new UnsafeBuffer(new byte[512]);

    // ── Construction ──────────────────────────────────────────────────────────

    public FixSessionStateMachine(final long sessionId, final SessionRole role, final int memoryStorageCapacity)
    {
        this.sessionId    = sessionId;
        this.sessionRole  = role;
        this.memoryStorage = new MemoryStorage(memoryStorageCapacity);
        Arrays.fill(localCompId, (byte) ' ');
        Arrays.fill(peerCompId,  (byte) ' ');
    }

    /** Convenience constructor for buy-side acceptor sessions with default capacity. */
    FixSessionStateMachine(final long sessionId)
    {
        this(sessionId, SessionRole.ACCEPTOR, 2500);
    }

    public long sessionId()     { return sessionId; }
    public SessionRole role()   { return sessionRole; }

    // Accessor for snapshot re-registration of timers
    public long heartbeatTimer()          { return heartbeatTimer; }
    public long heartbeatTimerDeadline()  { return heartbeatTimerDeadline; }
    public long testReqTimer()            { return testReqTimer; }
    public long testReqTimerDeadline()    { return testReqTimerDeadline; }
    public String pendingTestReqId()      { return pendingTestReqId; }
    public long outboundSeqNum()          { return outboundSeqNum; }

    // ── Event dispatch ────────────────────────────────────────────────────────

    public Command[] onEvent(
        final int          discriminator,
        final DirectBuffer buffer,
        final int          offset,
        final int          length,
        final long         nowMs)
    {
        final Command[] result = switch (discriminator)
        {
            case DISC_SESSION_CONNECT    -> onSessionConnect(buffer, offset, nowMs);
            case DISC_SESSION_DISCONNECT -> onSessionDisconnect();
            case DISC_LOGON              -> onInboundLogon(buffer, offset, nowMs);
            case DISC_LOGOUT             -> onInboundLogout(buffer, offset, nowMs);
            case DISC_HEARTBEAT          -> onInboundHeartbeat(buffer, offset, nowMs);
            case DISC_TEST_REQUEST       -> onInboundTestRequest(buffer, offset);
            case DISC_RESEND_REQUEST     -> onInboundResendRequest(buffer, offset);
            case DISC_SEQUENCE_RESET     -> onInboundSequenceReset(buffer, offset);
            default                      -> onInboundApplicationMessage(discriminator, buffer, offset, length);
        };

        // Post-failover: on the first inbound committed message (not a lifecycle event),
        // clear any stale TestRequest probe state and reschedule a fresh heartbeat timer
        // so the session doesn't spuriously Logout (§4.5.1).
        if (postFailoverPendingTestReqReset
            && discriminator != DISC_SESSION_CONNECT
            && discriminator != DISC_SESSION_DISCONNECT)
        {
            postFailoverPendingTestReqReset = false;
            pendingTestReqId = null;
            testReqTimer     = 0;
            heartbeatTimer   = nextTimerId++;
            heartbeatTimerDeadline = nowMs + heartbeatIntervalMs;
            final Command[] reschedule = new Command[]{
                new ScheduleTimerCommand(heartbeatTimer, heartbeatTimerDeadline) };
            return Command.concat(result, reschedule);
        }

        return result;
    }

    public Command[] onTimer(final long correlationId, final long nowMs)
    {
        if (correlationId == heartbeatTimer && heartbeatTimer != 0) return onHeartbeatTimerExpiry(nowMs);
        if (correlationId == testReqTimer   && testReqTimer   != 0) return onTestReqTimerExpiry(nowMs);
        return Command.NONE;
    }

    // ── Phase transitions ─────────────────────────────────────────────────────

    private Command[] onSessionConnect(
        final DirectBuffer buffer, final int offset, final long nowMs)
    {
        if (sessionPhase != SessionPhase.DISCONNECTED) return Command.NONE;

        // Extract CompIDs from the SESSION_CONNECT payload (raw; no SBE header).
        if (buffer != null && offset >= 0)
        {
            buffer.getBytes(offset + SESSION_CONNECT_PEER_COMP_ID_OFFSET,  peerCompId);
            buffer.getBytes(offset + SESSION_CONNECT_LOCAL_COMP_ID_OFFSET, localCompId);
        }

        sessionPhase   = SessionPhase.LOGON_PENDING;
        heartbeatTimer = nextTimerId++;
        heartbeatTimerDeadline = nowMs + LOGON_TIMEOUT_MS;

        return new Command[]{ new ScheduleTimerCommand(heartbeatTimer, heartbeatTimerDeadline) };
    }

    private Command[] onSessionDisconnect()
    {
        sessionPhase     = SessionPhase.DISCONNECTED;
        heartbeatTimer   = 0;
        heartbeatTimerDeadline = 0;
        testReqTimer     = 0;
        testReqTimerDeadline = 0;
        pendingTestReqId = null;
        // Cancel any in-progress Archive replay so the handler doesn't continue
        // emitting RESEND commands for a session that no longer has a TCP connection.
        return new Command[]{ new CancelPendingResendCommand(sessionId) };
    }

    private Command[] onInboundLogon(
        final DirectBuffer buffer, final int offset, final long nowMs)
    {
        if (sessionPhase != SessionPhase.LOGON_PENDING) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        // Extract CompIDs and negotiated heartbeat interval from the Logon payload.
        buffer.getBytes(offset + BLOCK_SENDER_OFFSET, peerCompId);
        buffer.getBytes(offset + BLOCK_TARGET_OFFSET, localCompId);
        final long hbInterval = Integer.toUnsignedLong(buffer.getInt(offset + LOGON_HEARTBEAT_INTERVAL_OFFSET));
        if (hbInterval > 0) heartbeatIntervalMs = hbInterval;

        sessionPhase   = SessionPhase.ACTIVE;
        heartbeatTimer = nextTimerId++;
        heartbeatTimerDeadline = nowMs + heartbeatIntervalMs;

        return new Command[]{
            buildLogonAck(nowMs),
            new ScheduleTimerCommand(heartbeatTimer, heartbeatTimerDeadline)
        };
    }

    private Command[] onInboundHeartbeat(
        final DirectBuffer buffer, final int offset, final long nowMs)
    {
        if (sessionPhase != SessionPhase.ACTIVE) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        // Any Heartbeat satisfies an outstanding TestRequest probe.
        // Also satisfies if TestReqID echoed back (we trust the sequence number path).
        pendingTestReqId = null;
        testReqTimer     = 0;
        testReqTimerDeadline = 0;

        heartbeatTimer = nextTimerId++;
        heartbeatTimerDeadline = nowMs + heartbeatIntervalMs;
        return new Command[]{ new ScheduleTimerCommand(heartbeatTimer, heartbeatTimerDeadline) };
    }

    private Command[] onInboundTestRequest(
        final DirectBuffer buffer, final int offset)
    {
        if (sessionPhase != SessionPhase.ACTIVE) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        // Echo the TestReqID back in a Heartbeat.
        final String echoId = extractTestReqId(buffer, offset);
        return new Command[]{ buildHeartbeat(echoId, 0L) };
    }

    private Command[] onInboundResendRequest(
        final DirectBuffer buffer, final int offset)
    {
        if (sessionPhase != SessionPhase.ACTIVE) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        final long beginSeqNo = Integer.toUnsignedLong(buffer.getInt(offset + RESEND_BEGIN_SEQ_NO_OFFSET));
        final long endSeqNo   = Integer.toUnsignedLong(buffer.getInt(offset + RESEND_END_SEQ_NO_OFFSET));

        if (memoryStorage.covers(beginSeqNo, endSeqNo))
        {
            return memoryStorage.buildResendCommands(sessionId, beginSeqNo, endSeqNo);
        }

        // Slow path: MemoryStorage does not cover the full range; fall back to Archive replay.
        // Guard against beginSeqNo predating the first outbound message.
        final ArchivePosition pos = outboundArchiveIndex.get(beginSeqNo);
        if (pos == null)
        {
            final long resolvedEnd = (endSeqNo == 0) ? outboundSeqNum : endSeqNo;
            return new Command[]{ new GapFillCommand(sessionId, beginSeqNo, resolvedEnd + 1) };
        }

        return new Command[]{ new StartArchiveReplayCommand(pos.recordingId(), pos.startPosition(), endSeqNo) };
    }

    private Command[] onInboundLogout(
        final DirectBuffer buffer, final int offset, final long nowMs)
    {
        if (sessionPhase == SessionPhase.DISCONNECTED) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        if (sessionPhase == SessionPhase.LOGOUT_PENDING)
        {
            // Our Logout was confirmed by the peer — clean close.
            sessionPhase = SessionPhase.DISCONNECTED;
            return new Command[]{ new DisconnectCommand(sessionId) };
        }

        // Peer-initiated Logout → echo Logout-Ack and wait for the TCP close.
        sessionPhase = SessionPhase.LOGOUT_PENDING;
        return new Command[]{ buildLogout(nowMs) };
    }

    private Command[] onInboundSequenceReset(
        final DirectBuffer buffer, final int offset)
    {
        final int gapFillFlagRaw = buffer.getByte(offset + SEQ_RESET_GAP_FILL_FLAG_OFFSET) & 0xFF;
        final long newSeqNo = Integer.toUnsignedLong(buffer.getInt(offset + SEQ_RESET_NEW_SEQ_NO_OFFSET));

        if (gapFillFlagRaw == GAP_FILL_FLAG_Y)
        {
            // GapFill: only advance inboundSeqNum; never rewind.
            if (newSeqNo > inboundSeqNum + 1) inboundSeqNum = newSeqNo - 1;
            return Command.NONE;
        }

        // Hard SequenceReset (GapFillFlag absent or 'N'): NewSeqNo must be greater than
        // the current counter. A reset that rewinds the counter is a fatal protocol violation.
        if (newSeqNo <= inboundSeqNum)
        {
            sessionPhase = SessionPhase.LOGOUT_PENDING;
            return new Command[]{
                buildLogout(0L),
                new CancelPendingResendCommand(sessionId)
            };
        }

        inboundSeqNum = newSeqNo - 1;
        return Command.NONE;
    }

    private Command[] onInboundApplicationMessage(
        final int discriminator, final DirectBuffer buffer, final int offset, final int length)
    {
        if (sessionPhase != SessionPhase.ACTIVE) return Command.NONE;

        final Command[] seqCheck = validateAndAdvanceSeqNum(buffer, offset);
        if (seqCheck != null) return seqCheck;

        return new Command[]{ new ForwardAppCommand(sessionId, buffer, length) };
    }

    // ── Timer expiry ──────────────────────────────────────────────────────────

    private Command[] onHeartbeatTimerExpiry(final long nowMs)
    {
        return switch (sessionPhase)
        {
            case LOGON_PENDING ->
            {
                // Logon wait timeout: client never sent a Logon within the allowed window.
                sessionPhase = SessionPhase.DISCONNECTED;
                heartbeatTimer = 0;
                yield new Command[]{ new DisconnectCommand(sessionId) };
            }
            case ACTIVE ->
            {
                // No Heartbeat received within the interval: send a TestRequest probe.
                pendingTestReqId = Long.toString(nowMs);   // unique within cluster.timeMs()
                testReqTimer     = nextTimerId++;
                testReqTimerDeadline = nowMs + TEST_REQ_TIMEOUT_MS;
                yield new Command[]{
                    buildTestRequest(pendingTestReqId, nowMs),
                    new ScheduleTimerCommand(testReqTimer, testReqTimerDeadline)
                };
            }
            case LOGOUT_PENDING ->
            {
                // Logout timeout: peer did not echo our Logout. Force close.
                sessionPhase = SessionPhase.DISCONNECTED;
                heartbeatTimer = 0;
                yield new Command[]{ new DisconnectCommand(sessionId) };
            }
            default -> Command.NONE;
        };
    }

    private Command[] onTestReqTimerExpiry(final long nowMs)
    {
        if (sessionPhase != SessionPhase.ACTIVE) return Command.NONE;

        // Peer did not respond to our TestRequest probe: initiate Logout.
        sessionPhase     = SessionPhase.LOGOUT_PENDING;
        pendingTestReqId = null;
        testReqTimer     = 0;
        testReqTimerDeadline = 0;

        // Reuse heartbeatTimer slot as the logout timeout correlationId.
        heartbeatTimer = nextTimerId++;
        heartbeatTimerDeadline = nowMs + LOGOUT_TIMEOUT_MS;

        return new Command[]{
            buildLogout(nowMs),
            new ScheduleTimerCommand(heartbeatTimer, heartbeatTimerDeadline)
        };
    }

    // ── Sequence number validation ────────────────────────────────────────────

    /**
     * Extracts MsgSeqNum (tag 34) and PossDupFlag (tag 43) from the SBE block,
     * compares against the expected next inbound sequence number, and advances
     * the counter on match.
     *
     * Returns null on success (call proceeds normally).
     * Returns a non-null Command[] if a gap, duplicate, or fatal error is detected.
     */
    private Command[] validateAndAdvanceSeqNum(final DirectBuffer buffer, final int offset)
    {
        final long msgSeqNum = Integer.toUnsignedLong(buffer.getInt(offset + BLOCK_SEQ_NUM_OFFSET));
        // PossDupFlag (tag 43) is not in the SBE StandardHeader block; it is carried as a
        // separate field in the SBE payload when the FIX proxy re-encodes a retransmission.
        // For simplicity, treat any low seqNum without a PossDupFlag field as fatal.
        final long expected  = inboundSeqNum + 1;

        if (msgSeqNum == expected)
        {
            inboundSeqNum = msgSeqNum;
            return null;
        }
        else if (msgSeqNum > expected)
        {
            // Gap detected: request retransmission.
            return new Command[]{ buildResendRequest(expected, msgSeqNum - 1, 0L) };
        }
        else
        {
            // Too-low seqNum without PossDupFlag: fatal sequence error → Logout.
            sessionPhase = SessionPhase.LOGOUT_PENDING;
            return new Command[]{ buildLogout(0L) };
        }
    }

    // ── Archive replay helpers (called by FixAeronHandler) ───────────────────

    /** Classifies one replayed fragment and returns RESEND or GAP_FILL. */
    public Command[] onResendFragment(
        final DirectBuffer buffer, final int offset, final int length, final long nowMs)
    {
        final int templateId = buffer.getShort(offset + SBE_TEMPLATE_ID_OFFSET) & 0xFFFF;
        final long seqNum    = Integer.toUnsignedLong(buffer.getInt(offset + BLOCK_SEQ_NUM_OFFSET));

        if (isApplicationTemplate(templateId))
        {
            return new Command[]{ new ResendCommand(
                sessionId, seqNum, templateId,
                new UnsafeBuffer(buffer, offset, length), length) };
        }
        // Session message: collapse to GAP_FILL spanning just this single slot.
        return new Command[]{ new GapFillCommand(sessionId, seqNum, seqNum + 1) };
    }

    /** Emit RESEND/GAP_FILL commands for messages in MemoryStorage that were
     *  beyond the Archive window (tail of the resend range). */
    public Command[] flushMemoryStorageTail(final long fromSeqNum, final long endSeqNo)
    {
        if (!memoryStorage.covers(fromSeqNum, endSeqNo)) return Command.NONE;
        return memoryStorage.buildResendCommands(sessionId, fromSeqNum, endSeqNo);
    }

    // ── Outbound message builders ─────────────────────────────────────────────
    // Each builder encodes an SBE message into outboundBuf, increments outboundSeqNum,
    // and returns a SendCommand. FixAeronHandler handles MemoryStorage storage.

    private SendCommand buildLogonAck(final long nowMs)
    {
        outboundSeqNum++;
        int pos = 0;
        pos = writeSbeHeader(outboundBuf, pos, TEMPLATE_LOGON, 33);
        pos = writeStandardBlock(outboundBuf, pos, outboundSeqNum, nowMs);
        outboundBuf.putByte(pos, (byte) 0);      // encryptMethod = None
        pos += Byte.BYTES;
        outboundBuf.putInt(pos, (int) (heartbeatIntervalMs & 0xFFFFFFFFL));
        pos += Integer.BYTES;
        outboundBuf.putShort(pos, (short) 0);    // xmlData length = 0 (absent)
        pos += Short.BYTES;

        return new SendCommand(sessionId, outboundSeqNum, TEMPLATE_LOGON,
            snapshot(outboundBuf, pos), pos);
    }

    public SendCommand buildHeartbeat(final String testReqId, final long nowMs)
    {
        outboundSeqNum++;
        int pos = 0;
        pos = writeSbeHeader(outboundBuf, pos, TEMPLATE_HEARTBEAT, 60);
        pos = writeStandardBlock(outboundBuf, pos, outboundSeqNum, nowMs);
        pos = writeTestReqId(outboundBuf, pos, testReqId);

        return new SendCommand(sessionId, outboundSeqNum, TEMPLATE_HEARTBEAT,
            snapshot(outboundBuf, pos), pos);
    }

    private SendCommand buildTestRequest(final String testReqId, final long nowMs)
    {
        outboundSeqNum++;
        int pos = 0;
        pos = writeSbeHeader(outboundBuf, pos, TEMPLATE_TEST_REQUEST, 60);
        pos = writeStandardBlock(outboundBuf, pos, outboundSeqNum, nowMs);
        pos = writeTestReqId(outboundBuf, pos, testReqId);

        return new SendCommand(sessionId, outboundSeqNum, TEMPLATE_TEST_REQUEST,
            snapshot(outboundBuf, pos), pos);
    }

    private SendCommand buildLogout(final long nowMs)
    {
        outboundSeqNum++;
        int pos = 0;
        pos = writeSbeHeader(outboundBuf, pos, TEMPLATE_LOGOUT, 28);
        pos = writeStandardBlock(outboundBuf, pos, outboundSeqNum, nowMs);
        outboundBuf.putShort(pos, (short) 0);   // text varData length = 0 (absent)
        pos += Short.BYTES;

        return new SendCommand(sessionId, outboundSeqNum, TEMPLATE_LOGOUT,
            snapshot(outboundBuf, pos), pos);
    }

    private SendCommand buildResendRequest(final long beginSeqNo, final long endSeqNo, final long nowMs)
    {
        outboundSeqNum++;
        int pos = 0;
        pos = writeSbeHeader(outboundBuf, pos, TEMPLATE_RESEND_REQUEST, 36);
        pos = writeStandardBlock(outboundBuf, pos, outboundSeqNum, nowMs);
        outboundBuf.putInt(pos, (int) (beginSeqNo & 0xFFFFFFFFL));  pos += Integer.BYTES;
        outboundBuf.putInt(pos, (int) (endSeqNo   & 0xFFFFFFFFL));  pos += Integer.BYTES;

        return new SendCommand(sessionId, outboundSeqNum, TEMPLATE_RESEND_REQUEST,
            snapshot(outboundBuf, pos), pos);
    }

    // ── SBE encoding helpers ──────────────────────────────────────────────────

    private int writeSbeHeader(
        final MutableDirectBuffer buf, final int offset,
        final int templateId, final int blockLength)
    {
        buf.putShort(offset,     (short) (blockLength & 0xFFFF));  // blockLength
        buf.putShort(offset + 2, (short) (templateId  & 0xFFFF));  // templateId
        buf.putShort(offset + 4, (short) (SCHEMA_ID   & 0xFFFF));  // schemaId
        buf.putShort(offset + 6, (short) (SCHEMA_VERSION & 0xFFFF)); // version
        return offset + SBE_HEADER_LEN;
    }

    private int writeStandardBlock(
        final MutableDirectBuffer buf, final int offset,
        final long seqNum, final long sendingTimeMs)
    {
        buf.putBytes(offset,      localCompId, 0, COMP_ID_LEN);  // sender = us
        buf.putBytes(offset + 8,  peerCompId,  0, COMP_ID_LEN);  // target = peer
        buf.putInt(  offset + 16, (int) (seqNum & 0xFFFFFFFFL));
        buf.putLong( offset + 20, sendingTimeMs);
        return offset + STANDARD_BLOCK_LEN;
    }

    private int writeTestReqId(
        final MutableDirectBuffer buf, final int offset, final String testReqId)
    {
        final byte[] id    = (testReqId != null) ? testReqId.getBytes() : new byte[0];
        final int    idLen = Math.min(id.length, TEST_REQ_ID_LEN);
        buf.putBytes(offset, id, 0, idLen);
        // Zero-pad to 32 bytes; byte[0]==0x00 signals absent.
        for (int i = idLen; i < TEST_REQ_ID_LEN; i++) buf.putByte(offset + i, (byte) 0);
        return offset + TEST_REQ_ID_LEN;
    }

    private static DirectBuffer snapshot(final MutableDirectBuffer buf, final int length)
    {
        final byte[] copy = new byte[length];
        buf.getBytes(0, copy, 0, length);
        return new UnsafeBuffer(copy);
    }

    // ── Field extraction helpers ──────────────────────────────────────────────

    private static String extractTestReqId(final DirectBuffer buffer, final int offset)
    {
        final int start = offset + HEARTBEAT_TEST_REQ_ID_OFFSET;
        if ((buffer.getByte(start) & 0xFF) == 0) return null;   // absent
        final byte[] raw = new byte[TEST_REQ_ID_LEN];
        buffer.getBytes(start, raw);
        int len = TEST_REQ_ID_LEN;
        while (len > 0 && raw[len - 1] == 0) len--;
        return new String(raw, 0, len);
    }

    private static boolean isApplicationTemplate(final int templateId)
    {
        return templateId != TEMPLATE_HEARTBEAT
            && templateId != TEMPLATE_TEST_REQUEST
            && templateId != TEMPLATE_RESEND_REQUEST
            && templateId != TEMPLATE_SEQUENCE_RESET
            && templateId != TEMPLATE_LOGOUT
            && templateId != TEMPLATE_LOGON;
    }

    // ── Snapshot serialisation ────────────────────────────────────────────────

    /**
     * Serialises all snapshotted state into buf starting at offset.
     * Returns the number of bytes written.
     *
     * Format (little-endian, matches §6.1.10):
     *   sessionId(i64) sessionRole(u8) sessionPhase(u8)
     *   inboundSeqNum(i64) outboundSeqNum(i64)
     *   pendingTestReqIdLen(u16) pendingTestReqId(chars)
     *   heartbeatTimer(i64) heartbeatTimerDeadline(i64)
     *   testReqTimer(i64)   testReqTimerDeadline(i64)
     *   nextTimerId(i64) heartbeatIntervalMs(i64)
     *   localCompId(8 bytes) peerCompId(8 bytes)
     *   memoryStorage(variable)
     *   outboundArchiveIndex: count(u32) then count × (seqNum i64 recordingId i64 startPos i64 endPos i64)
     */
    public int encodeTo(final MutableDirectBuffer buf, int offset)
    {
        final int start = offset;

        buf.putLong(offset, sessionId);             offset += Long.BYTES;
        buf.putByte(offset, sessionRole.encode());  offset += Byte.BYTES;
        buf.putByte(offset, sessionPhase.encode()); offset += Byte.BYTES;
        buf.putLong(offset, inboundSeqNum);         offset += Long.BYTES;
        buf.putLong(offset, outboundSeqNum);        offset += Long.BYTES;

        final byte[] pendingBytes = (pendingTestReqId != null)
            ? pendingTestReqId.getBytes()
            : new byte[0];
        buf.putShort(offset, (short) pendingBytes.length);  offset += Short.BYTES;
        if (pendingBytes.length > 0)
        {
            buf.putBytes(offset, pendingBytes);  offset += pendingBytes.length;
        }

        buf.putLong(offset, heartbeatTimer);            offset += Long.BYTES;
        buf.putLong(offset, heartbeatTimerDeadline);    offset += Long.BYTES;
        buf.putLong(offset, testReqTimer);              offset += Long.BYTES;
        buf.putLong(offset, testReqTimerDeadline);      offset += Long.BYTES;
        buf.putLong(offset, nextTimerId);               offset += Long.BYTES;
        buf.putLong(offset, heartbeatIntervalMs);       offset += Long.BYTES;

        buf.putBytes(offset, localCompId);  offset += COMP_ID_LEN;
        buf.putBytes(offset, peerCompId);   offset += COMP_ID_LEN;

        offset += memoryStorage.encodeTo(buf, offset);

        // outboundArchiveIndex
        buf.putInt(offset, outboundArchiveIndex.size());  offset += Integer.BYTES;
        for (final var entry : outboundArchiveIndex.entrySet())
        {
            buf.putLong(offset, entry.getKey());                           offset += Long.BYTES;
            buf.putLong(offset, entry.getValue().recordingId());           offset += Long.BYTES;
            buf.putLong(offset, entry.getValue().startPosition());         offset += Long.BYTES;
            buf.putLong(offset, entry.getValue().endPosition());           offset += Long.BYTES;
        }

        return offset - start;
    }

    /** Deserialises a FixSessionStateMachine from buf at offset. */
    public static FixSessionStateMachine decodeFrom(final DirectBuffer buf, int offset)
    {
        final long sessionId   = buf.getLong(offset);            offset += Long.BYTES;
        final SessionRole role = SessionRole.decode(buf.getByte(offset)); offset += Byte.BYTES;
        final SessionPhase ph  = SessionPhase.decode(buf.getByte(offset)); offset += Byte.BYTES;

        final FixSessionStateMachine fsm = new FixSessionStateMachine(sessionId, role, 2500);
        fsm.sessionPhase = ph;

        fsm.inboundSeqNum  = buf.getLong(offset);   offset += Long.BYTES;
        fsm.outboundSeqNum = buf.getLong(offset);   offset += Long.BYTES;

        final int pendingLen = buf.getShort(offset) & 0xFFFF;  offset += Short.BYTES;
        if (pendingLen > 0)
        {
            final byte[] raw = new byte[pendingLen];
            buf.getBytes(offset, raw);
            fsm.pendingTestReqId = new String(raw);
            offset += pendingLen;
        }

        fsm.heartbeatTimer         = buf.getLong(offset);  offset += Long.BYTES;
        fsm.heartbeatTimerDeadline = buf.getLong(offset);  offset += Long.BYTES;
        fsm.testReqTimer           = buf.getLong(offset);  offset += Long.BYTES;
        fsm.testReqTimerDeadline   = buf.getLong(offset);  offset += Long.BYTES;
        fsm.nextTimerId            = buf.getLong(offset);  offset += Long.BYTES;
        fsm.heartbeatIntervalMs    = buf.getLong(offset);  offset += Long.BYTES;

        buf.getBytes(offset, fsm.localCompId);  offset += COMP_ID_LEN;
        buf.getBytes(offset, fsm.peerCompId);   offset += COMP_ID_LEN;

        offset += fsm.memoryStorage.decodeFrom(buf, offset);

        final int archiveCount = buf.getInt(offset);  offset += Integer.BYTES;
        for (int i = 0; i < archiveCount; i++)
        {
            final long seqNum      = buf.getLong(offset);  offset += Long.BYTES;
            final long recordingId = buf.getLong(offset);  offset += Long.BYTES;
            final long startPos    = buf.getLong(offset);  offset += Long.BYTES;
            final long endPos      = buf.getLong(offset);  offset += Long.BYTES;
            fsm.outboundArchiveIndex.put(seqNum, new ArchivePosition(recordingId, startPos, endPos));
        }

        return fsm;
    }
}