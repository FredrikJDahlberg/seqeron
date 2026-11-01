package org.limitless.seqeron.tools;

import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.ProxyProtocol;
import org.limitless.seqeron.proxy.Liveness;
import io.aeron.Aeron;
import io.aeron.AeronVersion;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.sbe.hosted.AttachEncoder;
import org.limitless.seqeron.sbe.hosted.AttachedDecoder;
import org.limitless.seqeron.sbe.hosted.CaughtUpDecoder;
import org.limitless.seqeron.sbe.hosted.FencedDecoder;
import org.limitless.seqeron.sbe.hosted.GateDecoder;
import org.limitless.seqeron.sbe.hosted.Kind;
import org.limitless.seqeron.sbe.hosted.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.hosted.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.hosted.PayloadDecoder;
import org.limitless.seqeron.sbe.hosted.PublishEncoder;
import org.limitless.seqeron.sbe.hosted.Refusal;
import org.limitless.seqeron.sbe.hosted.RestoreRecordDecoder;
import org.limitless.seqeron.sbe.hosted.SnapshotCutDecoder;
import org.limitless.seqeron.sbe.hosted.SnapshotEndEncoder;
import org.limitless.seqeron.sbe.hosted.SnapshotRecordEncoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;

/**
 * The client of the hosted-façade prototype (doc/hosted-facade-draft.md): {@code TestApplication}'s state and snapshot
 * records, byte for byte, behind {@link ProxyProtocol}'s streams instead of an {@code Application}. Its state is the count
 * of markers sequenced under its {@code sourceId} and a digest of their {@code clusterSeqNo}s, so a direct replica
 * comparing a round against the one this client wrote fences if the two differ (spec A-7).
 *
 * <p>Properties on top of {@link ClusterProbe}'s {@code probe.memberId} / {@code probe.aeronDir}:
 * {@code probe.sourceId} (16), {@code probe.intervalMs} (100), {@code client.stallCutNo} (0: stall in that cut, after
 * printing {@code IN CUT}, until killed), {@code client.slowCutMs} (0: spend that long in every cut, heartbeating,
 * before the record), {@code client.staleEpoch} (once leading, send one marker under the previous gate epoch first),
 * {@code client.ghost} (send one marker from a second publication on stream 304, which the host does not read: H-15), {@code client.corruptRecord} (write a digest one off in every snapshot record), {@code
 * client.hostTimeoutMs} (3000), {@code client.flood} (publish as fast as the window allows), {@code client.maxMarkers} (0: no limit; else
 * offer that many and go on counting), {@code
 * client.ignoreWindow} (a control: do not keep H-5's window, so the host must close the attachment).
 */
public final class HostedTestApplicationClient {
    private static final int EXIT_FENCED = 70;
    private static final long STALE_MARKER = 0x5A1E5A1EL;
    /** Set in the {@code seqNo} of every marker this client publishes, to tell them from the direct replicas'. */
    private static final long OWN_TAG = 1L << 40;
    private static final long GHOST_MARKER = 0x6805_7A57L;
    private static final long CUT_BEAT_NS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long ATTACH_RETRY_NS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long ATTACH_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(90);

    private final int sourceId = Integer.getInteger("probe.sourceId", 16);
    private final long intervalNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("probe.intervalMs", 100));
    private final long stallCutNo = Long.getLong("client.stallCutNo", 0);
    private final long slowCutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("client.slowCutMs", 0));
    private final boolean staleEpoch = Boolean.getBoolean("client.staleEpoch");
    private final boolean ghost = Boolean.getBoolean("client.ghost");
    private final long maxMarkers = Long.getLong("client.maxMarkers", 0);
    private final boolean flood = Boolean.getBoolean("client.flood");
    private final boolean ignoreWindow = Boolean.getBoolean("client.ignoreWindow");
    private final boolean corruptRecord = Boolean.getBoolean("client.corruptRecord");
    private final long hostTimeoutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("client.hostTimeoutMs", 3000));
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[64 * 1024]);
    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[128 * 1024]);
    private final FragmentHandler onEvent = new FragmentAssembler(this::onEvent);
    private final FragmentHandler onReply = this::onReply;
    private final MessageHeaderDecoder headerIn = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerOut = new MessageHeaderEncoder();
    private final MessageHeaderDecoder bodyHeader = new MessageHeaderDecoder();
    private final AttachedDecoder attachedIn = new AttachedDecoder();
    private final PayloadDecoder payloadIn = new PayloadDecoder();
    private final GateDecoder gateIn = new GateDecoder();
    private final CaughtUpDecoder caughtUpIn = new CaughtUpDecoder();
    private final RestoreRecordDecoder restoreIn = new RestoreRecordDecoder();
    private final SnapshotCutDecoder cutIn = new SnapshotCutDecoder();
    private final FencedDecoder fencedIn = new FencedDecoder();
    private final ProbeMarkerDecoder probeIn = new ProbeMarkerDecoder();
    private final AttachEncoder attachOut = new AttachEncoder();
    private final PublishEncoder publishOut = new PublishEncoder();
    private final SnapshotRecordEncoder recordOut = new SnapshotRecordEncoder();
    private final SnapshotEndEncoder endOut = new SnapshotEndEncoder();

    private ExclusivePublication commands;
    private Counter heartbeat;
    private Counter applied;
    private Liveness hostLiveness;
    private long clientId;
    private long requestId;
    private int eventSessionId = ProxyProtocol.NO_ATTACH;
    private int attachExit;
    private CountersReader counters;
    private int retiredCounterId;
    private long commandWindow;
    private long offeredBytes;
    private long markers;
    private long digest;
    private long lastAppliedSeqNo;
    private long epoch;
    private boolean leading;
    private boolean canPublish;
    private boolean fenced;
    private boolean staleSent;
    private long markerSeqNo;
    private long ownSequenced;
    private long lastOwn;
    private long cuts;
    private long declined;
    private long staleDropped;

    public static void main(final String[] args) {
        System.exit(new HostedTestApplicationClient().run());
    }

    private int run() {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ClusterProbe.aeronDir()))) {
            counters = aeron.countersReader();
            heartbeat = aeron.addCounter(SeqeronCounters.PROXY_CLIENT_HEARTBEAT_TYPE_ID, "seqeron.client.heartbeat " + sourceId);
            applied = aeron.addCounter(SeqeronCounters.PROXY_CLIENT_APPLIED_TYPE_ID, "seqeron.client.applied " + sourceId);
            hostLiveness = new Liveness(counters, hostTimeoutNs);
            commands = aeron.addExclusivePublication(ProxyProtocol.COMMANDS_CHANNEL, ProxyProtocol.COMMANDS_STREAM);
            final ExclusivePublication ghostCommands =
                ghost ? aeron.addExclusivePublication(ProxyProtocol.COMMANDS_CHANNEL, ProxyProtocol.COMMANDS_STREAM) : null;
            if (!attach(aeron)) {
                return attachExit;
            }
            final Subscription events = aeron.addSubscription(ProxyProtocol.sessionChannel(eventSessionId),
                                                              ProxyProtocol.EVENTS_STREAM);
            log("attached, clientId=%d", clientId);
            if (ghost) {
                publishOut.wrapAndApplyHeader(out, 0, headerOut).epoch(epoch).connectionId(0).groupMask(0)
                    .payloadId(ClusterProbe.PROBE_PAYLOAD_ID);
                final int length = marker.encodePayload(GHOST_MARKER, ClusterProbe.NO_FILLER);
                publishOut.putBody(marker.payload(), 0, length);
                send(ghostCommands, ProxyProtocol.HEADER + publishOut.encodedLength());
                log("sent one command from a publication the host does not read");
            }
            final IdleStrategy idle = new BackoffIdleStrategy();
            long nextMarkerNs = 0;
            long nextStateNs = 0;
            while (!fenced) {
                ProxyProtocol.beat(heartbeat);
                int work = events.poll(onEvent, 16);
                final long now = System.nanoTime();
                if (canPublish && staleEpoch && !staleSent && epoch > 0) {
                    publishStaleMarker();
                }
                if (canPublish && (maxMarkers == 0 || markerSeqNo < maxMarkers) && (flood || now >= nextMarkerNs) &&
                    publishMarker()) {
                    nextMarkerNs = now + intervalNs;
                    work++;
                }
                if (now >= nextStateNs) {
                    log("state: %d marker(s), digest %016x, published %d, declined %d, own %d", markers, digest, markerSeqNo,
                        declined, ownSequenced);
                    nextStateNs = now + TimeUnit.SECONDS.toNanos(2);
                }
                if (hostLiveness.stale(now)) {
                    log("HOST_LOST: heartbeat counter silent for %dms", hostLiveness.silentMs(now));
                    return EXIT_FENCED;
                }
                idle.idle(work);
            }
            return EXIT_FENCED;
        }
    }

    /** H-8: resend the request until the host answers it; a refusal for another client or request is ignored (H-15). */
    private boolean attach(final Aeron aeron) {
        clientId = aeron.nextCorrelationId();
        requestId = 1;
        final Publication requests = aeron.addPublication(ProxyProtocol.ATTACH_REQUEST_CHANNEL, ProxyProtocol.ATTACH_REQUEST_STREAM);
        final Subscription replies = aeron.addSubscription("aeron:ipc?tether=false", ProxyProtocol.ATTACH_REPLY_STREAM);
        final long deadline = System.nanoTime() + ATTACH_TIMEOUT_NS;
        long nextAttachNs = 0;
        while (eventSessionId == ProxyProtocol.NO_ATTACH && attachExit == 0 && System.nanoTime() < deadline) {
            ProxyProtocol.beat(heartbeat);
            replies.poll(onReply, 16);
            final long now = System.nanoTime();
            if (now >= nextAttachNs) {
                attachOut.wrapAndApplyHeader(out, 0, headerOut)
                    .clientId(clientId)
                    .requestId(requestId)
                    .kind(Kind.APPLICATION)
                    .protocolVersion(ProxyProtocol.PROTOCOL_VERSION)
                    .commandSessionId(commands.sessionId())
                    .heartbeatCounterId(heartbeat.id())
                    .heartbeatRegistrationId(heartbeat.registrationId())
                    .appliedCounterId(applied.id())
                    .appliedRegistrationId(applied.registrationId())
                    .formatVersion(1)
                    .supportedFormatsCount(1)
                    .next()
                    .formatVersion(1);
                final byte[] hostName = System.getProperty("client.hostName", ProxyProtocol.DEFAULT_HOST_NAME).getBytes(StandardCharsets.UTF_8);
                final byte[] aeronVersion = AeronVersion.VERSION.getBytes(StandardCharsets.UTF_8);
                attachOut.putHostName(hostName, 0, hostName.length).putAeronVersion(aeronVersion, 0, aeronVersion.length);
                send(requests, ProxyProtocol.HEADER + attachOut.encodedLength());
                nextAttachNs = now + ATTACH_RETRY_NS;
            }
            Thread.onSpinWait();
        }
        if (eventSessionId == ProxyProtocol.NO_ATTACH && attachExit == 0) {
            log("no ATTACHED within %ds", TimeUnit.NANOSECONDS.toSeconds(ATTACH_TIMEOUT_NS));
            attachExit = 3;
        }
        replies.close();
        requests.close();
        return attachExit == 0;
    }

    private void onReply(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerIn.wrap(buffer, offset);
        if (headerIn.schemaId() != 218 || headerIn.templateId() != AttachedDecoder.TEMPLATE_ID) {
            return;
        }
        attachedIn.wrap(buffer, offset + ProxyProtocol.HEADER, headerIn.blockLength(), headerIn.version());
        if (attachedIn.clientId() != clientId || attachedIn.requestId() != requestId) {
            return;
        }
        final Refusal refusal = attachedIn.refusal();
        if (refusal == Refusal.BUSY) {
            return;
        }
        if (refusal != Refusal.NONE) {
            log("attach refused: %s", refusal);
            attachExit = 78;
            return;
        }
        retiredCounterId = attachedIn.retiredCounterId();
        commandWindow = attachedIn.commandWindow();
        hostLiveness.watch(attachedIn.heartbeatCounterId(), attachedIn.heartbeatRegistrationId(), System.nanoTime());
        eventSessionId = attachedIn.eventSessionId();
    }

    private void onEvent(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerIn.wrap(buffer, offset);
        final int body = offset + ProxyProtocol.HEADER;
        switch (headerIn.templateId()) {
        case PayloadDecoder.TEMPLATE_ID -> {
            payloadIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            final long clusterSeqNo = payloadIn.clusterSeqNo();
            final long position = payloadIn.position();
            if (payloadIn.sourceId() == sourceId && payloadIn.payloadId() == ClusterProbe.PROBE_PAYLOAD_ID) {
                final int payloadLength = payloadIn.bodyLength();
                payloadIn.getBody(scratch, 0, payloadLength);
                bodyHeader.wrap(scratch, 0);
                if (bodyHeader.templateId() == ProbeMarkerDecoder.TEMPLATE_ID) {
                    probeIn.wrap(scratch, ProxyProtocol.HEADER, bodyHeader.blockLength(), bodyHeader.version());
                    onMarker(clusterSeqNo, probeIn.seqNo());
                }
            }
            lastAppliedSeqNo = clusterSeqNo;
            applied.setOrdered(position);
        }
        case GateDecoder.TEMPLATE_ID -> {
            gateIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            epoch = gateIn.epoch();
            final boolean nowLeading = gateIn.leading() != 0;
            canPublish = gateIn.canPublish() != 0;
            log("gate epoch %d leading %b canPublish %b", epoch, nowLeading, canPublish);
            if (nowLeading != leading) {
                leading = nowLeading;
                log(leading ? "now leading — submitting markers" : "not leading — silent");
            }
        }
        case CaughtUpDecoder.TEMPLATE_ID -> {
            caughtUpIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            log("Caught up — following live at clusterSeqNo %d, %d marker(s) sequenced, digest %016x",
                caughtUpIn.clusterSeqNo(), markers, digest);
        }
        case RestoreRecordDecoder.TEMPLATE_ID -> {
            restoreIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            final int recordIndex = restoreIn.recordIndex();
            final int formatVersion = restoreIn.formatVersion();
            final int dataLength = restoreIn.dataLength();
            restoreIn.getData(scratch, 0, dataLength);
            if (recordIndex == 0) {
                markers = scratch.getLong(0);
                digest = scratch.getLong(Long.BYTES);
            }
            log("restored record %d (format %d): %d marker(s), digest %016x", recordIndex, formatVersion, markers, digest);
        }
        case SnapshotCutDecoder.TEMPLATE_ID -> {
            cutIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            onCut(cutIn.roundId());
        }
        case FencedDecoder.TEMPLATE_ID -> {
            fencedIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            final byte[] text = new byte[fencedIn.detailLength()];
            fencedIn.getDetail(text, 0, text.length);
            log("FENCED: %s", new String(text, StandardCharsets.UTF_8));
            fenced = true;
        }
        default -> {
        }
        }
    }

    private void onMarker(final long clusterSeqNo, final long seqNo) {
        if (seqNo == STALE_MARKER) {
            log("STALE MARKER SEQUENCED at clusterSeqNo %d", clusterSeqNo);
        } else if (seqNo == GHOST_MARKER) {
            log("GHOST MARKER SEQUENCED at clusterSeqNo %d", clusterSeqNo);
        } else if ((seqNo & OWN_TAG) != 0) {
            final long own = seqNo & (OWN_TAG - 1);
            if (own <= lastOwn) {
                log("OWN MARKER OUT OF ORDER: %d after %d", own, lastOwn);
            }
            lastOwn = own;
            ownSequenced++;
        }
        markers++;
        digest = digest * 31 + clusterSeqNo;
    }

    /** One record: the marker count, then the digest. The state is as of the last payload applied. */
    private void onCut(final long cutNo) {
        cuts++;
        if (cuts == stallCutNo) {
            log("IN CUT round %d", cutNo);
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        final long slowEndNs = System.nanoTime() + slowCutNs;
        if (slowCutNs > 0) {
            log("IN SLOW CUT round %d", cutNo);
        }
        while (System.nanoTime() < slowEndNs) {
            ProxyProtocol.beat(heartbeat);
            LockSupport.parkNanos(CUT_BEAT_NS);
        }
        scratch.putLong(0, markers);
        scratch.putLong(Long.BYTES, corruptRecord ? digest + 1 : digest);
        recordOut.wrapAndApplyHeader(out, 0, headerOut).roundId(cutNo).recordIndex(0).putData(scratch, 0, 2 * Long.BYTES);
        send(commands, ProxyProtocol.HEADER + recordOut.encodedLength());
        endOut.wrapAndApplyHeader(out, 0, headerOut).roundId(cutNo).recordCount(1);
        send(commands, ProxyProtocol.HEADER + endOut.encodedLength());
    }

    /** A marker under the epoch before the current one: the host must decline it, and nothing may sequence it. */
    private void publishStaleMarker() {
        staleSent = true;
        final int length = marker.encodePayload(STALE_MARKER, ClusterProbe.NO_FILLER);
        publishOut.wrapAndApplyHeader(out, 0, headerOut).epoch(epoch - 1).connectionId(0).groupMask(0)
            .payloadId(ClusterProbe.PROBE_PAYLOAD_ID).putBody(marker.payload(), 0, length);
        send(commands, ProxyProtocol.HEADER + publishOut.encodedLength());
        log("sent one marker under stale gate epoch %d", epoch - 1);
    }

    /**
     * Offers one marker if H-5's window has room for it and the stream takes it; false is a local decline, retried.
     * It counts as published only once offered, since the host answers nothing for a command it drops (H-4).
     */
    private boolean publishMarker() {
        final int length = marker.encodePayload(OWN_TAG | (markerSeqNo + 1), ClusterProbe.NO_FILLER);
        final int messageLength = ProxyProtocol.HEADER + PublishEncoder.BLOCK_LENGTH + Integer.BYTES + length;
        if (!ignoreWindow && offeredBytes - counters.getCounterValue(retiredCounterId) + messageLength > commandWindow) {
            declined++;
            return false;
        }
        publishOut.wrapAndApplyHeader(out, 0, headerOut).epoch(epoch).connectionId(0).groupMask(0)
            .payloadId(ClusterProbe.PROBE_PAYLOAD_ID).putBody(marker.payload(), 0, length);
        if (commands.offer(out, 0, messageLength) < 0) {
            declined++;
            return false;
        }
        markerSeqNo++;
        offeredBytes += messageLength;
        return true;
    }

    /** Offers {@code out}; with no host subscribed there is nothing to wait for, and the host's silence says so. */
    private void send(final Publication publication, final int length) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (true) {
            final long result = publication.offer(out, 0, length);
            if (result >= 0 || result == Publication.NOT_CONNECTED || result == Publication.CLOSED ||
                System.nanoTime() > deadline) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    private static void log(final String format, final Object... args) {
        System.out.printf("[client %tT.%<tL] %s%n", System.currentTimeMillis(), String.format(format, args));
        System.out.flush();
    }
}
