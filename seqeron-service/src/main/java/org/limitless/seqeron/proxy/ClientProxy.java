package org.limitless.seqeron.proxy;

import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.ProxyProtocol;
import org.limitless.seqeron.proxy.Liveness;
import io.aeron.Aeron;
import io.aeron.AeronVersion;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Consumer;
import org.limitless.seqeron.app.Gateway;
import org.limitless.seqeron.app.GroupDirectory;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.app.RequestRef;
import org.limitless.seqeron.app.SnapshotListener;
import org.limitless.seqeron.app.Tuning;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.proxy.AttachRules;
import org.limitless.seqeron.proxy.CommandLedger;
import org.limitless.seqeron.sbe.hosted.ActivatedDecoder;
import org.limitless.seqeron.sbe.hosted.AttachDecoder;
import org.limitless.seqeron.sbe.hosted.AttachedEncoder;
import org.limitless.seqeron.sbe.hosted.CaughtUpEncoder;
import org.limitless.seqeron.sbe.hosted.CloseConnectionDecoder;
import org.limitless.seqeron.sbe.hosted.ConnectionEncoder;
import org.limitless.seqeron.sbe.hosted.DetachDecoder;
import org.limitless.seqeron.sbe.hosted.FencedEncoder;
import org.limitless.seqeron.sbe.hosted.GateClosedDecoder;
import org.limitless.seqeron.sbe.hosted.GateEncoder;
import org.limitless.seqeron.sbe.hosted.GroupsResolvedEncoder;
import org.limitless.seqeron.sbe.hosted.Kind;
import org.limitless.seqeron.sbe.hosted.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.hosted.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.hosted.OpenConnectionDecoder;
import org.limitless.seqeron.sbe.hosted.PayloadEncoder;
import org.limitless.seqeron.sbe.hosted.PublishDecoder;
import org.limitless.seqeron.sbe.hosted.Refusal;
import org.limitless.seqeron.sbe.hosted.ReplyDecoder;
import org.limitless.seqeron.sbe.hosted.RestoreRecordEncoder;
import org.limitless.seqeron.sbe.hosted.RoleEncoder;
import org.limitless.seqeron.sbe.hosted.SnapshotCutEncoder;
import org.limitless.seqeron.sbe.hosted.SnapshotEndDecoder;
import org.limitless.seqeron.sbe.hosted.SnapshotRecordDecoder;

/**
 * The proxy of the hosted façades (doc/hosted-facade-draft.md): the real {@link Application}, {@link Gateway} or
 * {@link Consumer}, started when a client sends {@code Attach} and closed when it is lost, its callbacks relayed in
 * schema 218 over {@link ProxyProtocol}'s streams. Launched beside a member.
 *
 * <p>Properties: {@code probe.memberId} (0), {@code probe.aeronDir}, {@code probe.ingressEndpoints}, {@code probe.egressHost}:
 * {@code probe.sourceId} (16), {@code probe.clientId} (11), {@code probe.snapshotDir}, {@code host.clientTimeoutMs}
 * (2000), {@code host.commandWindow} (65536 bytes: H-5's window, here the encoded length of the {@code Publish}
 * messages not yet placed), {@code host.placePerCycle} (64), {@code host.pendingCapacity} (the façade's tracker, 0: its
 * default), {@code host.spin} (busy-spin the loop rather than back off), {@code host.allocWarmupFrames} (0: off) and {@code host.allocWindowFrames} (20000): once that many frames
 * have crossed the loop, logs the bytes it allocated over the next window, for {@code HostedAllocationTest}.
 */
public final class ClientProxy implements Application.Listener, Gateway.Listener, Consumer.Listener, SnapshotListener {
    private static final int EXIT_FENCED = 70;
    private static final long BACK_PRESSURE_NS = TimeUnit.SECONDS.toNanos(1);

    private final int memberId = Integer.getInteger("probe.memberId", 0);
    private final String aeronDir = System.getProperty("probe.aeronDir", PortLayout.aeronDir(memberId));
    private final String ingressEndpoints = System.getProperty("probe.ingressEndpoints", PortLayout.ingressEndpoints());
    private final String egressChannel = "aeron:udp?endpoint=" + System.getProperty("probe.egressHost",
        memberId < PortLayout.HOSTS.size() ? PortLayout.HOSTS.get(memberId) : PortLayout.DEFAULT_HOST) + ":0";
    private final int sourceId = Integer.getInteger("probe.sourceId", 16);
    private final int clientId = Integer.getInteger("probe.clientId", 11);
    private final long clientTimeoutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("host.clientTimeoutMs", 2000));
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[128 * 1024]);
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[16 * 1024]);
    private final List<byte[]> cutRecords = new ArrayList<>();
    private final FragmentHandler onAttachRequest = this::onAttachRequest;
    private final FragmentAssembler onCommand = new FragmentAssembler(this::onCommand);
    private final MessageHeaderDecoder headerIn = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerOut = new MessageHeaderEncoder();
    private final AttachDecoder attachIn = new AttachDecoder();
    private final DetachDecoder detachIn = new DetachDecoder();
    private final PublishDecoder publishIn = new PublishDecoder();
    private final ReplyDecoder replyIn = new ReplyDecoder();
    private final GroupsResolvedEncoder groupsOut = new GroupsResolvedEncoder();
    private final OpenConnectionDecoder openIn = new OpenConnectionDecoder();
    private final CloseConnectionDecoder closeIn = new CloseConnectionDecoder();
    private final GateClosedDecoder gateClosedIn = new GateClosedDecoder();
    private final ActivatedDecoder activatedIn = new ActivatedDecoder();
    private final ConnectionEncoder connectionOut = new ConnectionEncoder();
    private final RoleEncoder roleOut = new RoleEncoder();
    private final String gatewayName = System.getProperty("probe.gatewayName");
    private final int consumerGroup = Integer.getInteger("host.consumerGroup", 0);
    private final AttachRules attachRules = AttachRules.of(AeronVersion.VERSION, gatewayName != null, consumerGroup != 0);
    private final SnapshotRecordDecoder recordIn = new SnapshotRecordDecoder();
    private final SnapshotEndDecoder endIn = new SnapshotEndDecoder();
    private final AttachedEncoder attachedOut = new AttachedEncoder();
    private final PayloadEncoder payloadOut = new PayloadEncoder();
    private final GateEncoder gateOut = new GateEncoder();
    private final CaughtUpEncoder caughtUpOut = new CaughtUpEncoder();
    private final RestoreRecordEncoder restoreOut = new RestoreRecordEncoder();
    private final SnapshotCutEncoder cutOut = new SnapshotCutEncoder();
    private final FencedEncoder fencedOut = new FencedEncoder();
    private final long commandWindow = Long.getLong("host.commandWindow", 64 * 1024);
    private final CommandLedger ledger = new CommandLedger(commandWindow);
    private final int placePerCycle = Integer.getInteger("host.placePerCycle", 64);
    private final int pendingCapacity = Integer.getInteger("host.pendingCapacity", 0);
    /** Sized once to hold the window with its per-message framing, so it never grows. */
    private final ExpandableRingBuffer queue = new ExpandableRingBuffer(
        BitUtil.findNextPositivePowerOfTwo((int)(2 * commandWindow)),
        BitUtil.findNextPositivePowerOfTwo((int)(2 * commandWindow)), false);
    private final ExpandableRingBuffer.MessageConsumer place = this::place;
    private final ExpandableRingBuffer.MessageConsumer discard = (buffer, offset, length, head) -> true;
    private final long allocWarmupFrames = Long.getLong("host.allocWarmupFrames", 0);
    private final long allocWindowFrames = Long.getLong("host.allocWindowFrames", 20_000);
    private final ThreadMXBean threads = (ThreadMXBean)ManagementFactory.getThreadMXBean();

    private Aeron aeron;
    private Subscription attachRequests;
    private Publication attachReplies;
    private ExclusivePublication events;
    private Subscription commands;
    private Liveness clientLiveness;
    private Counter hostHeartbeat;
    private Path snapshotDir;
    private Application app;
    private Gateway gateway;
    private Consumer consumer;
    private boolean consumerKind;
    private boolean live;
    private int attachedSourceId = sourceId;
    private boolean gatewayKind;
    private boolean activating;
    private String breach;
    private boolean activatedSeen;
    private boolean activatedOpened;
    private long connectionsOpened;
    private boolean attached;
    private long liveClientId;
    private long liveRequestId;
    private long connectDeadlineNs;
    private boolean commandsSeen;
    private long cutNo;
    private long frames;
    private Counter retiredCounter;
    private long loggedDropsAndDeclines;
    private long loggedPlaced;
    private long lastQueueLogNs;
    private long windowFrames;
    private long windowBytes;
    private long windowReadCost;
    private long windowEpoch;
    private boolean windowOpen;
    private boolean windowDone;
    private boolean lastCanPublish;
    private boolean collecting;
    private boolean endSeen;
    private boolean cutLost;
    private boolean stalled;
    private volatile boolean running = true;
    private volatile boolean fenced;

    public static void main(final String[] args) {
        System.exit(new ClientProxy().run());
    }

    private int run() {
        snapshotDir = Path.of(System.getProperty("probe.snapshotDir"));
        final Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                main.join(TimeUnit.SECONDS.toMillis(5));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        attachRequests = aeron.addSubscription(ProxyProtocol.ATTACH_REQUEST_CHANNEL + "|tether=false",
                                               ProxyProtocol.ATTACH_REQUEST_STREAM);
        attachReplies = aeron.addPublication(ProxyProtocol.ATTACH_REPLY_CHANNEL, ProxyProtocol.ATTACH_REPLY_STREAM);
        hostHeartbeat = aeron.addCounter(SeqeronCounters.PROXY_HEARTBEAT_TYPE_ID, "seqeron.host.heartbeat " + sourceId);
        retiredCounter = aeron.addCounter(SeqeronCounters.PROXY_RETIRED_TYPE_ID, "seqeron.host.retired " + sourceId);
        clientLiveness = new Liveness(aeron.countersReader(), clientTimeoutNs);
        log("host ready: member %d, sourceId %d, clientId %d", memberId, sourceId, clientId);
        final IdleStrategy idle = Boolean.getBoolean("host.spin") ? new BusySpinIdleStrategy() : new BackoffIdleStrategy();
        while (running && !fenced) {
            ProxyProtocol.beat(hostHeartbeat);
            int work = attachRequests.poll(onAttachRequest, 16);
            if (live) {
                work += commands != null ? commands.poll(onCommand, 16) : 0;
                if (!attached) {
                    connect();
                }
            }
            if (facade()) {
                work += app != null ? app.doWork() : gateway != null ? gateway.doWork() : consumer.doWork();
                if (!fenced && facade()) {
                    placeCommands();
                    afterDoWork();
                    allocWindow();
                }
            }
            idle.idle(work);
        }
        closeFacade();
        aeron.close();
        return fenced ? EXIT_FENCED : 0;
    }

    /** Counts the allocation of this thread over a window of frames, after a warm-up for the JIT. */
    private void allocWindow() {
        if (allocWarmupFrames == 0 || windowDone) {
            return;
        }
        final long id = Thread.currentThread().threadId();
        if (!windowOpen && frames >= allocWarmupFrames) {
            log("measuring from here");
            final long first = threads.getThreadAllocatedBytes(id);
            windowBytes = threads.getThreadAllocatedBytes(id);
            windowReadCost = windowBytes - first;
            windowFrames = frames;
            windowEpoch = ledger.epoch();
            windowOpen = true;
        } else if (windowOpen && frames - windowFrames >= allocWindowFrames) {
            final long bytes = threads.getThreadAllocatedBytes(id) - windowBytes - windowReadCost;
            windowDone = true;
            log("alloc window: %d bytes over %d frames, gate epoch %d to %d", bytes, frames - windowFrames,
                windowEpoch, ledger.epoch());
        }
    }

    private void afterDoWork() {
        final long now = System.nanoTime();
        final boolean canPublish = app != null ? app.canPublish() : gateway != null && gateway.canAccept();
        if (canPublish != lastCanPublish) {
            lastCanPublish = canPublish;
            sendGate();
        }
        final long dropsAndDeclines = ledger.epochDrops() + ledger.refusedDrops() + ledger.headDeclines();
        if (dropsAndDeclines != 0 && (dropsAndDeclines != loggedDropsAndDeclines || ledger.placed() != loggedPlaced) &&
            now - lastQueueLogNs >= 1_000_000_000L) {
            logQueue();
            loggedDropsAndDeclines = dropsAndDeclines;
            loggedPlaced = ledger.placed();
            lastQueueLogNs = now;
        }
        if (stalled) {
            closeApp("CLIENT_STALLED: events stream back-pressured for " + BACK_PRESSURE_NS / 1_000_000 + "ms");
        } else if (clientLiveness.stale(now)) {
            closeApp("CLIENT_LOST: heartbeat counter silent for " + clientLiveness.silentMs(now) + "ms");
        } else if (commandsSeen && commands != null && !commands.isConnected()) {
            closeApp("CLIENT_LOST: command stream gone");
        }
    }

    // ── Attach, streams 301 and 302 ─────────────────────────────────────────────────────────────────────────

    private void onAttachRequest(final DirectBuffer buffer, final int offset, final int length,
                                 final io.aeron.logbuffer.Header header) {
        headerIn.wrap(buffer, offset);
        if (headerIn.schemaId() != 218) {
            return;
        }
        if (headerIn.templateId() == AttachDecoder.TEMPLATE_ID) {
            attachIn.wrap(buffer, offset + ProxyProtocol.HEADER, headerIn.blockLength(), headerIn.version());
            onAttach();
        } else if (headerIn.templateId() == DetachDecoder.TEMPLATE_ID) {
            detachIn.wrap(buffer, offset + ProxyProtocol.HEADER, headerIn.blockLength(), headerIn.version());
            if (facade() && detachIn.clientId() == liveClientId) {
                if (collecting) {
                    cutLost = true;
                    endSeen = true;
                } else {
                    closeApp("DETACHED");
                }
            }
        }
    }

    /** H-8: a resend of the live request is answered again, any other client is refused as busy. */
    private void onAttach() {
        final long clientId = attachIn.clientId();
        final long requestId = attachIn.requestId();
        final Kind kind = attachIn.kind();
        final int protocolVersion = attachIn.protocolVersion();
        final int commandSessionId = attachIn.commandSessionId();
        final int heartbeatCounterId = attachIn.heartbeatCounterId();
        final long heartbeatRegistrationId = attachIn.heartbeatRegistrationId();
        attachIn.appliedCounterId();
        attachIn.appliedRegistrationId();
        attachIn.formatVersion();
        final AttachDecoder.SupportedFormatsDecoder formats = attachIn.supportedFormats();
        while (formats.hasNext()) {
            formats.next();
        }
        final byte[] hostNameBytes = new byte[attachIn.hostNameLength()];
        attachIn.getHostName(hostNameBytes, 0, hostNameBytes.length);
        final byte[] aeronVersionBytes = new byte[attachIn.aeronVersionLength()];
        attachIn.getAeronVersion(aeronVersionBytes, 0, aeronVersionBytes.length);
        final String hostName = new String(hostNameBytes, StandardCharsets.UTF_8);
        final String aeronVersion = new String(aeronVersionBytes, StandardCharsets.UTF_8);
        switch (attachRules.decide(live, liveClientId, liveRequestId, hostName, clientId, requestId, kind,
                                   protocolVersion, aeronVersion)) {
        case IGNORE -> {
            return;
        }
        case RESEND -> {
            sendAttached(clientId, requestId, Refusal.NONE);
            return;
        }
        case BUSY -> {
            log("refused Attach from another client: attachment of client %d is live", liveClientId);
            sendAttached(clientId, requestId, Refusal.BUSY);
            return;
        }
        case VERSION -> {
            sendAttached(clientId, requestId, Refusal.VERSION);
            return;
        }
        case KIND -> {
            sendAttached(clientId, requestId, Refusal.KIND);
            return;
        }
        case ACCEPT -> {
        }
        }
        liveClientId = clientId;
        gatewayKind = kind == Kind.GATEWAY;
        consumerKind = kind == Kind.CONSUMER;
        attachedSourceId = consumerKind ? -1 : sourceId;
        live = true;
        liveRequestId = requestId;
        ledger.reset();
        retiredCounter.setOrdered(0);
        cutNo = 0;
        lastCanPublish = false;
        cutLost = false;
        stalled = false;
        commandsSeen = false;
        events = aeron.addExclusivePublication(ProxyProtocol.EVENTS_CHANNEL, ProxyProtocol.EVENTS_STREAM);
        commands = consumerKind
                       ? null
                       : aeron.addSubscription(ProxyProtocol.sessionChannel(commandSessionId), ProxyProtocol.COMMANDS_STREAM);
        final long now = System.nanoTime();
        clientLiveness.watch(heartbeatCounterId, heartbeatRegistrationId, now);
        connectDeadlineNs = now + clientTimeoutNs;
        sendAttached(clientId, requestId, Refusal.NONE);
    }

    /** True from the Attached that accepted a client until its attachment ends. */
    private boolean connectionOrLive() {
        return live;
    }

    /**
     * H-8: the façade is built only once the client's subscription has connected to the events publication, so every
     * callback, the restore included, has a reader.
     */
    private void connect() {
        final long now = System.nanoTime();
        if (events.isConnected()) {
            startApp();
        } else if (now > connectDeadlineNs || clientLiveness.stale(now)) {
            closeApp("CLIENT_LOST: subscription did not connect within " + clientTimeoutNs / 1_000_000 + "ms");
        }
    }

    private boolean facade() {
        return app != null || gateway != null || consumer != null;
    }

    private void keepFacadeAlive() {
        if (app != null) {
            app.keepAlive();
        } else {
            gateway.keepAlive();
        }
    }

    private void closeFacade() {
        if (app != null) {
            app.close();
            app = null;
        }
        if (gateway != null) {
            gateway.close();
            gateway = null;
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    private void startApp() {
        if (consumerKind) {
            consumer = Consumer.builder()
                           .clientId(clientId)
                           .memberId(memberId)
                           .groupId(consumerGroup)
                           .listener(this)
                           .build();
            attached = true;
            consumer.start(aeron);
            log("ATTACHED clientId=%d (consumer of group %d)", liveClientId, consumerGroup);
            return;
        }
        if (gatewayKind) {
            gateway = Gateway.builder()
                          .gatewayName(gatewayName)
                          .sourceId(sourceId)
                          .clientId(clientId)
                          .memberId(memberId)
                          .egressChannel(egressChannel)
                          .ingressEndpoints(ingressEndpoints)
                          .listener(this)
                          .tuning(pendingCapacity == 0 ? Tuning.DEFAULT : Tuning.DEFAULT.withPendingCapacity(pendingCapacity))
                          .build();
            attached = true;
            gateway.start(aeron);
            log("ATTACHED clientId=%d (gateway %s)", liveClientId, gatewayName);
            return;
        }
        app = Application.builder()
                  .sourceId(sourceId)
                  .clientId(clientId)
                  .memberId(memberId)
                  .egressChannel(egressChannel)
                  .ingressEndpoints(ingressEndpoints)
                  .listener(this)
                  .tuning(pendingCapacity == 0 ? Tuning.DEFAULT : Tuning.DEFAULT.withPendingCapacity(pendingCapacity))
                  .snapshots(this, snapshotDir)
                  .build();
        attached = true;
        app.start(aeron);
        log("ATTACHED clientId=%d", liveClientId);
    }

    private void sendAttached(final long clientId, final long requestId, final Refusal refusal) {
        final boolean accepted = refusal == Refusal.NONE;
        attachedOut.wrapAndApplyHeader(out, 0, headerOut)
            .clientId(clientId)
            .requestId(requestId)
            .eventSessionId(accepted ? events.sessionId() : ProxyProtocol.NO_ATTACH)
            .refusal(refusal)
            .heartbeatCounterId(hostHeartbeat.id())
            .heartbeatRegistrationId(hostHeartbeat.registrationId())
            .retiredCounterId(retiredCounter.id())
            .retiredRegistrationId(retiredCounter.registrationId())
            .commandWindow(commandWindow)
            .sourceId(attachedSourceId);
        final int length = ProxyProtocol.HEADER + attachedOut.encodedLength();
        final long deadline = System.nanoTime() + BACK_PRESSURE_NS;
        while (attachReplies.offer(out, 0, length) < 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    // ── Commands, stream 304 ─────────────────────────────────────────────────────────────────────────────────

    private void onCommand(final DirectBuffer buffer, final int offset, final int length,
                           final io.aeron.logbuffer.Header header) {
        if (!facade()) {
            return;
        }
        commandsSeen = true;
        headerIn.wrap(buffer, offset);
        final int body = offset + ProxyProtocol.HEADER;
        switch (headerIn.templateId()) {
        case PublishDecoder.TEMPLATE_ID, ReplyDecoder.TEMPLATE_ID, OpenConnectionDecoder.TEMPLATE_ID ->
            onPublish(buffer, offset, length, true);
        case CloseConnectionDecoder.TEMPLATE_ID, GateClosedDecoder.TEMPLATE_ID -> onPublish(buffer, offset, length, false);
        case ActivatedDecoder.TEMPLATE_ID -> {
            if (activating) {
                activatedIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
                activatedOpened = activatedIn.opened() != 0;
                activatedSeen = true;
            }
        }
        case SnapshotRecordDecoder.TEMPLATE_ID -> {
            recordIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            if (collecting && recordIn.roundId() == cutNo) {
                final byte[] record = new byte[recordIn.dataLength()];
                recordIn.getData(record, 0, record.length);
                cutRecords.add(record);
            }
        }
        case SnapshotEndDecoder.TEMPLATE_ID -> {
            endIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            if (collecting && endIn.roundId() == cutNo) {
                endSeen = true;
            }
        }
        default -> {
        }
        }
    }

    /** Queues the message whole; H-5's window decides whether the client may have sent it, and does not count a close. */
    private void onPublish(final DirectBuffer buffer, final int offset, final int length, final boolean windowed) {
        if (ledger.exceedsWindow(length, windowed) || !queue.append(buffer, offset, length)) {
            closeApp("CLIENT_WINDOW: " + ledger.queuedBytes() + " bytes queued, window " + commandWindow);
            return;
        }
        ledger.enqueued(length, windowed);
    }

    private void placeCommands() {
        if (!queue.isEmpty()) {
            queue.consume(place, placePerCycle);
        }
        if (breach != null) {
            final String reason = breach;
            breach = null;
            closeApp(reason);
        }
    }

    /** H-3 and H-5: the head is placed in the epoch it was decided in, dropped otherwise, kept if the façade declines. */
    private boolean place(final MutableDirectBuffer buffer, final int offset, final int length, final int head) {
        headerIn.wrap(buffer, offset);
        final int templateId = headerIn.templateId();
        final boolean windowed =
            templateId != CloseConnectionDecoder.TEMPLATE_ID && templateId != GateClosedDecoder.TEMPLATE_ID;
        final int body = offset + ProxyProtocol.HEADER;
        final long commandEpoch;
        switch (templateId) {
        case ReplyDecoder.TEMPLATE_ID -> {
            replyIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            commandEpoch = replyIn.epoch();
        }
        case OpenConnectionDecoder.TEMPLATE_ID -> {
            openIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            commandEpoch = openIn.epoch();
        }
        case CloseConnectionDecoder.TEMPLATE_ID -> {
            closeIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            commandEpoch = closeIn.epoch();
        }
        case GateClosedDecoder.TEMPLATE_ID -> {
            gateClosedIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            commandEpoch = gateClosedIn.epoch();
        }
        default -> {
            publishIn.wrap(buffer, body, headerIn.blockLength(), headerIn.version());
            commandEpoch = publishIn.epoch();
        }
        }
        if (ledger.stale(commandEpoch)) {
            ledger.dropStale(length, windowed);
            retiredCounter.setOrdered(ledger.retired());
            return true;
        }
        final Publish result;
        switch (templateId) {
        case ReplyDecoder.TEMPLATE_ID -> {
            final int payloadLength = replyIn.bodyLength();
            replyIn.getBody(scratch, 0, payloadLength);
            result = app.reply(new RequestRef(replyIn.sourceId(), (int)replyIn.connectionId(), (int)replyIn.groupMask()),
                replyIn.payloadId(), scratch, payloadLength);
        }
        case OpenConnectionDecoder.TEMPLATE_ID -> {
            final int dataLength = openIn.dataLength();
            openIn.getData(scratch, 0, dataLength);
            final int id = gateway.openConnection(scratch, dataLength);
            if (id == Gateway.NO_CONNECTION) {
                result = Publish.Refused;
            } else if (id != openIn.connectionId()) {
                breach = "CLIENT_PROTOCOL: connection id " + openIn.connectionId() + " but the façade allocated " + id;
                result = Publish.Refused;
            } else {
                connectionsOpened++;
                log("opened connection %d", id);
                result = Publish.Published;
            }
        }
        case CloseConnectionDecoder.TEMPLATE_ID -> {
            gateway.closeConnection((int)closeIn.connectionId());
            result = Publish.Published;
        }
        case GateClosedDecoder.TEMPLATE_ID -> {
            gateway.gateClosed();
            ledger.edge(false);
            lastCanPublish = false;
            log("gate closed by the client — gate epoch %d", ledger.epoch());
            sendGate();
            result = Publish.Published;
        }
        default -> {
            final int payloadLength = publishIn.bodyLength();
            publishIn.getBody(scratch, 0, payloadLength);
            result = gateway != null ? gateway.publish((int)publishIn.connectionId(), (int)publishIn.groupMask(),
                                                       publishIn.payloadId(), scratch, payloadLength)
                                     : app.publish((int)publishIn.groupMask(), publishIn.payloadId(), scratch,
                                                   payloadLength);
        }
        }
        if (!ledger.settle(result, length, windowed)) {
            return false;
        }
        frames++;
        retiredCounter.setOrdered(ledger.retired());
        return true;
    }

    private void logQueue() {
        log("queue: placed %d, epochDrops %d, refused %d, headDeclines %d, queued %d, peakQueued %d, window %d, connections %d",
            ledger.placed(), ledger.epochDrops(), ledger.refusedDrops(), ledger.headDeclines(), ledger.queuedBytes(),
            ledger.peakQueuedBytes(), commandWindow,
            connectionsOpened);
    }

    private void closeApp(final String reason) {
        log("%s", reason);
        logQueue();
        closeFacade();
        attached = false;
        live = false;
        CloseHelper.quietClose(commands);
        CloseHelper.quietClose(events);
        commands = null;
        events = null;
        queue.consume(discard, Integer.MAX_VALUE);
        ledger.discarded();
        collecting = false;
    }

    // ── Application.Listener ─────────────────────────────────────────────────────────────────────────────────

    @Override
    public void onGroupsResolved(final GroupDirectory directory) {
        int count = 0;
        for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
            count += directory.name(groupId) == null ? 0 : 1;
        }
        groupsOut.wrapAndApplyHeader(out, 0, headerOut);
        final GroupsResolvedEncoder.GroupsEncoder groups = groupsOut.groupsCount(count);
        for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
            final String name = directory.name(groupId);
            if (name != null) {
                groups.next().groupId(groupId).mask(directory.mask(name)).putName(name.getBytes(StandardCharsets.UTF_8), 0,
                    name.length());
            }
        }
        send(ProxyProtocol.HEADER + groupsOut.encodedLength());
    }

    @Override
    public void onLeadershipChanged(final boolean nowLeading) {
        ledger.edge(nowLeading);
        log(nowLeading ? "now leading — gate epoch %d, epochDrops %d" : "not leading — gate epoch %d, epochDrops %d",
            ledger.epoch(), ledger.epochDrops());
        sendGate();
    }

    @Override
    public boolean onActivated(final int firstConnectionId) {
        roleOut.wrapAndApplyHeader(out, 0, headerOut).active((short)1).firstConnectionId(firstConnectionId);
        send(ProxyProtocol.HEADER + roleOut.encodedLength());
        activatedSeen = false;
        activatedOpened = false;
        activating = true;
        while (!activatedSeen) {
            commands.poll(onCommand, 16);
            gateway.keepAlive();
            ProxyProtocol.beat(hostHeartbeat);
            if (stalled || clientLiveness.stale(System.nanoTime())) {
                activating = false;
                return false;
            }
            Thread.onSpinWait();
        }
        if (activatedOpened) {
            ledger.edge(true);
            lastCanPublish = true;
            log("now serving — gate epoch %d, first connection id %d", ledger.epoch(), firstConnectionId);
            sendGate();
        }
        activating = false;
        return activatedOpened;
    }

    @Override
    public void onStandby() {
        roleOut.wrapAndApplyHeader(out, 0, headerOut).active((short)0).firstConnectionId(0);
        send(ProxyProtocol.HEADER + roleOut.encodedLength());
        ledger.edge(false);
        lastCanPublish = false;
        log("standby — gate epoch %d", ledger.epoch());
        sendGate();
    }

    @Override
    public void onConnectionOpened(final int connectionId, final DirectBuffer data, final int offset, final int length) {
        connectionOut.wrapAndApplyHeader(out, 0, headerOut).connectionId(connectionId).opened((short)1).putData(data, offset,
            length);
        send(ProxyProtocol.HEADER + connectionOut.encodedLength());
    }

    @Override
    public void onConnectionClosed(final int connectionId) {
        connectionOut.wrapAndApplyHeader(out, 0, headerOut).connectionId(connectionId).opened((short)0).putData(scratch, 0, 0);
        send(ProxyProtocol.HEADER + connectionOut.encodedLength());
    }

    @Override
    public void onSequenced(final Payload payload) {
        frames++;
        payloadOut.wrapAndApplyHeader(out, 0, headerOut)
            .clusterSeqNo(payload.clusterSeqNo())
            .groupSeqNo(payload.groupSeqNo())
            .sourceId(payload.sourceId())
            .connectionId(payload.connectionId())
            .groupMask(payload.groupMask())
            .sourceSessionId(payload.sourceSessionId())
            .clusterTimestampNs(payload.clusterTimestampNs())
            .receiveTimeNs(payload.receiveTimeNs())
            .position(payload.position())
            .payloadId(payload.payloadId())
            .putBody(payload.buffer(), payload.payloadOffset(), payload.payloadLength());
        send(ProxyProtocol.HEADER + payloadOut.encodedLength());
    }

    /** Both listeners default it, so the host must choose; ClusterTime is not sent by the prototype. */
    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
    }

    @Override
    public void onCaughtUp(final long clusterSeqNo) {
        caughtUpOut.wrapAndApplyHeader(out, 0, headerOut).clusterSeqNo(clusterSeqNo);
        send(ProxyProtocol.HEADER + caughtUpOut.encodedLength());
        log("host caught up at clusterSeqNo %d", clusterSeqNo);
    }

    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        log("FENCED: %s — %s", fence, detail);
        final byte[] text = (fence + ": " + detail).getBytes(StandardCharsets.UTF_8);
        fencedOut.wrapAndApplyHeader(out, 0, headerOut).errorCode(fence.ordinal()).putDetail(text, 0, text.length);
        send(ProxyProtocol.HEADER + fencedOut.encodedLength());
        fenced = true;
    }

    // ── SnapshotListener ─────────────────────────────────────────────────────────────────────────────────────

    @Override
    public int formatVersion() {
        return 1;
    }

    /**
     * The first call of a round sends the cut and waits for the client's records and end, then each call serves one.
     * Waiting, the host polls the command stream and keeps the cluster session alive; a client silent for
     * {@code host.clientTimeoutMs} abandons the round.
     */
    @Override
    public int onSnapshot(final MutableDirectBuffer buffer, final int recordIndex) {
        if (recordIndex == 0) {
            cutNo++;
            cutRecords.clear();
            endSeen = false;
            cutLost = false;
            collecting = true;
            cutOut.wrapAndApplyHeader(out, 0, headerOut).roundId(cutNo);
            send(ProxyProtocol.HEADER + cutOut.encodedLength());
            while (!endSeen) {
                commands.poll(onCommand, 16);
                attachRequests.poll(onAttachRequest, 16);
                keepFacadeAlive();
                ProxyProtocol.beat(hostHeartbeat);
                if (clientLiveness.stale(System.nanoTime())) {
                    cutLost = true;
                    break;
                }
                Thread.onSpinWait();
            }
            collecting = false;
            if (cutLost) {
                log("ABANDON round: client lost in cut %d after %d record(s)", cutNo, cutRecords.size());
                return -1;
            }
        }
        if (cutLost) {
            return -1;
        }
        if (recordIndex < cutRecords.size()) {
            final byte[] record = cutRecords.get(recordIndex);
            buffer.putBytes(0, record);
            return record.length;
        }
        return 0;
    }

    @Override
    public void onRestore(final DirectBuffer buffer, final int length, final int recordIndex, final int formatVersion) {
        restoreOut.wrapAndApplyHeader(out, 0, headerOut)
            .recordIndex(recordIndex)
            .formatVersion(formatVersion)
            .putData(buffer, 0, length);
        send(ProxyProtocol.HEADER + restoreOut.encodedLength());
    }

    // ── Sends ────────────────────────────────────────────────────────────────────────────────────────────────

    private boolean canPublishNow() {
        return app != null ? app.canPublish() : gateway != null && (gateway.canAccept() || activating);
    }

    private void sendGate() {
        gateOut.wrapAndApplyHeader(out, 0, headerOut)
            .epoch(ledger.epoch())
            .leading((short)(ledger.leading() ? 1 : 0))
            .canPublish((short)(canPublishNow() ? 1 : 0))
            .caughtUp((short)0);
        send(ProxyProtocol.HEADER + gateOut.encodedLength());
    }

    /** Offers {@code out}; no subscriber means no client, which is not worth spinning on. */
    private void send(final int length) {
        final long deadline = System.nanoTime() + BACK_PRESSURE_NS;
        while (true) {
            ProxyProtocol.beat(hostHeartbeat);
            final long result = events.offer(out, 0, length);
            if (result >= 0 || result == Publication.NOT_CONNECTED || result == Publication.CLOSED) {
                return;
            }
            if (System.nanoTime() > deadline) {
                stalled = true;
                return;
            }
            Thread.onSpinWait();
        }
    }

    private static void log(final String format, final Object... args) {
        System.out.printf("[host %tT.%<tL] %s%n", System.currentTimeMillis(), String.format(format, args));
        System.out.flush();
    }
}
