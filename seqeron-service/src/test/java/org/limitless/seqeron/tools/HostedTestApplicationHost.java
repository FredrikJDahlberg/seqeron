package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableRingBuffer;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.GroupDirectory;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.app.SnapshotListener;
import org.limitless.seqeron.app.Tuning;
import org.limitless.seqeron.protocol.Publish;

/**
 * The host of the hosted-façade prototype (doc/hosted-facade-draft.md): the real {@link Application}, started when a
 * client sends {@code Attach} and closed when it is lost, its callbacks relayed over {@link HostedWire}'s streams.
 * Harness code in the test source set, launched by {@code HostedSnapshotTest} beside a member.
 *
 * <p>Properties on top of {@link ClusterProbe}'s {@code probe.memberId} / {@code probe.aeronDir}:
 * {@code probe.sourceId} (16), {@code probe.clientId} (11), {@code probe.snapshotDir}, {@code host.clientTimeoutMs}
 * (2000), {@code host.commandWindow} (65536 bytes: H-5's window, here the encoded length of the {@code Publish}
 * messages not yet placed), {@code host.placePerCycle} (64), {@code host.pendingCapacity} (the façade's tracker, 0: its
 * default), {@code host.spin} (busy-spin the loop rather than back off), {@code host.allocWarmupFrames} (0: off) and {@code host.allocWindowFrames} (20000): once that many frames
 * have crossed the loop, logs the bytes it allocated over the next window, for {@code HostedAllocationTest}.
 */
public final class HostedTestApplicationHost implements Application.Listener, SnapshotListener {
    private static final int EXIT_FENCED = 70;
    private static final long HEARTBEAT_NS = TimeUnit.MILLISECONDS.toNanos(100);
    /** The proposed type id of the host-retired counter (doc/hosted-facade-draft.md §17.2.1). */
    static final int RETIRED_TYPE_ID = 5303;
    private static final long BACK_PRESSURE_NS = TimeUnit.SECONDS.toNanos(1);

    private final int sourceId = Integer.getInteger("probe.sourceId", 16);
    private final int clientId = Integer.getInteger("probe.clientId", 11);
    private final long clientTimeoutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("host.clientTimeoutMs", 2000));
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[128 * 1024]);
    private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[16 * 1024]);
    private final List<byte[]> cutRecords = new ArrayList<>();
    private final FragmentHandler onCommand = this::onCommand;
    private final long commandWindow = Long.getLong("host.commandWindow", 64 * 1024);
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
    private Publication events;
    private Subscription commands;
    private Path snapshotDir;
    private Application app;
    private long attachId;
    private long liveNonce;
    private long epoch;
    private long cutNo;
    private long lastClientNs;
    private long lastHeartbeatNs;
    private long frames;
    private Counter retiredCounter;
    private long retired;
    private long queuedBytes;
    private long peakQueuedBytes;
    private long placed;
    private long epochDrops;
    private long refusedDrops;
    private long headDeclines;
    private long loggedDropsAndDeclines;
    private long loggedPlaced;
    private long lastQueueLogNs;
    private long windowFrames;
    private long windowBytes;
    private long windowReadCost;
    private long windowEpoch;
    private boolean windowOpen;
    private boolean windowDone;
    private boolean leading;
    private boolean lastCanPublish;
    private boolean collecting;
    private boolean endSeen;
    private boolean cutLost;
    private boolean stalled;
    private volatile boolean running = true;
    private volatile boolean fenced;

    public static void main(final String[] args) {
        System.exit(new HostedTestApplicationHost().run());
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
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ClusterProbe.aeronDir()));
        events = aeron.addExclusivePublication(HostedWire.IPC, HostedWire.EVENTS_STREAM);
        commands = aeron.addSubscription(HostedWire.IPC, HostedWire.COMMANDS_STREAM);
        retiredCounter = aeron.addCounter(RETIRED_TYPE_ID, "seqeron.host.retired");
        log("host ready: member %d, sourceId %d, clientId %d", ClusterProbe.memberId(), sourceId, clientId);
        final IdleStrategy idle = Boolean.getBoolean("host.spin") ? new BusySpinIdleStrategy() : new BackoffIdleStrategy();
        while (running && !fenced) {
            int work = commands.poll(onCommand, 16);
            if (app != null) {
                work += app.doWork();
                if (!fenced) {
                    placeCommands();
                    afterDoWork();
                    allocWindow();
                }
            }
            idle.idle(work);
        }
        if (app != null) {
            app.close();
        }
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
            windowEpoch = epoch;
            windowOpen = true;
        } else if (windowOpen && frames - windowFrames >= allocWindowFrames) {
            final long bytes = threads.getThreadAllocatedBytes(id) - windowBytes - windowReadCost;
            windowDone = true;
            log("alloc window: %d bytes over %d frames, gate epoch %d to %d", bytes, frames - windowFrames,
                windowEpoch, epoch);
        }
    }

    private void afterDoWork() {
        final long now = System.nanoTime();
        final boolean canPublish = app.canPublish();
        if (canPublish != lastCanPublish) {
            lastCanPublish = canPublish;
            sendGate();
        }
        if (now - lastHeartbeatNs >= HEARTBEAT_NS) {
            sendHeartbeat(now);
        }
        final long dropsAndDeclines = epochDrops + refusedDrops + headDeclines;
        if (dropsAndDeclines != 0 && (dropsAndDeclines != loggedDropsAndDeclines || placed != loggedPlaced) &&
            now - lastQueueLogNs >= 1_000_000_000L) {
            logQueue();
            loggedDropsAndDeclines = dropsAndDeclines;
            loggedPlaced = placed;
            lastQueueLogNs = now;
        }
        if (stalled) {
            closeApp("CLIENT_STALLED: events stream back-pressured for " + BACK_PRESSURE_NS / 1_000_000 + "ms");
        } else if (now - lastClientNs > clientTimeoutNs) {
            closeApp("CLIENT_LOST: silent for " + (now - lastClientNs) / 1_000_000 + "ms");
        }
    }

    // ── Commands ─────────────────────────────────────────────────────────────────────────────────────────────

    private void onCommand(final DirectBuffer buffer, final int offset, final int length,
                           final io.aeron.logbuffer.Header header) {
        final byte type = buffer.getByte(offset);
        final long id = buffer.getLong(offset + 8);
        if (type == HostedWire.ATTACH) {
            onAttach(buffer, offset);
            return;
        }
        if (app == null || id != attachId) {
            log("dropped stale command type=%d attachId=%x (current %x)", type, id, attachId);
            return;
        }
        lastClientNs = System.nanoTime();
        switch (type) {
        case HostedWire.PUBLISH -> onPublish(buffer, offset, length);
        case HostedWire.SNAPSHOT_RECORD -> {
            if (collecting && buffer.getLong(offset + 16) == cutNo) {
                final int recordLength = buffer.getInt(offset + 28);
                final byte[] record = new byte[recordLength];
                buffer.getBytes(offset + 32, record);
                cutRecords.add(record);
            }
        }
        case HostedWire.SNAPSHOT_END -> {
            if (collecting && buffer.getLong(offset + 16) == cutNo) {
                endSeen = true;
            }
        }
        case HostedWire.DETACH -> {
            if (collecting) {
                cutLost = true;
                endSeen = true;
            } else {
                closeApp("DETACHED");
            }
        }
        default -> {
            // A heartbeat: lastClientNs is all it is for.
        }
        }
    }

    private void onAttach(final DirectBuffer buffer, final int offset) {
        final long nonce = buffer.getLong(offset + 16);
        if (app != null) {
            if (nonce != liveNonce) {
                log("refused Attach from another client: attachment %x is live", attachId);
            }
            return;
        }
        liveNonce = nonce;
        retired = 0;
        queuedBytes = 0;
        retiredCounter.setOrdered(0);
        placed = epochDrops = refusedDrops = headDeclines = peakQueuedBytes = 0;
        attachId = ThreadLocalRandom.current().nextLong() | 1;
        epoch = 0;
        cutNo = 0;
        leading = false;
        lastCanPublish = false;
        cutLost = false;
        stalled = false;
        app = Application.builder()
                  .sourceId(sourceId)
                  .clientId(clientId)
                  .memberId(ClusterProbe.memberId())
                  .egressChannel(ClusterProbe.egressChannel())
                  .ingressEndpoints(ClusterProbe.ingressEndpoints())
                  .listener(this)
                  .tuning(pendingCapacity == 0 ? Tuning.DEFAULT : Tuning.DEFAULT.withPendingCapacity(pendingCapacity))
                  .snapshots(this, snapshotDir)
                  .build();
        app.start(aeron);
        lastClientNs = System.nanoTime();
        HostedWire.header(out, HostedWire.ATTACHED, attachId);
        out.putLong(16, nonce);
        out.putInt(24, retiredCounter.id());
        out.putLong(28, commandWindow);
        send(36);
        log("ATTACHED attachId=%x", attachId);
    }

    /** Queues the message whole; H-5's window decides whether the client may have sent it. */
    private void onPublish(final DirectBuffer buffer, final int offset, final int length) {
        if (queuedBytes + length > commandWindow || !queue.append(buffer, offset, length)) {
            closeApp("CLIENT_WINDOW: " + queuedBytes + " bytes queued, window " + commandWindow);
            return;
        }
        queuedBytes += length;
        peakQueuedBytes = Math.max(peakQueuedBytes, queuedBytes);
    }

    private void placeCommands() {
        if (queuedBytes > 0) {
            queue.consume(place, placePerCycle);
        }
    }

    /** H-3 and H-5: the head is placed in the epoch it was decided in, dropped otherwise, kept if the façade declines. */
    private boolean place(final MutableDirectBuffer buffer, final int offset, final int length, final int head) {
        if (buffer.getLong(offset + 24) != epoch || !leading) {
            epochDrops++;
            retire(length);
            return true;
        }
        final int payloadLength = buffer.getInt(offset + 36);
        buffer.getBytes(offset + 40, scratch, 0, payloadLength);
        final Publish result = app.publish(buffer.getInt(offset + 32), scratch, payloadLength);
        if (result == Publish.Declined) {
            headDeclines++;
            return false;
        }
        if (result == Publish.Published) {
            placed++;
        } else {
            refusedDrops++;
        }
        frames++;
        retire(length);
        return true;
    }

    private void logQueue() {
        log("queue: placed %d, epochDrops %d, refused %d, headDeclines %d, queued %d, peakQueued %d, window %d", placed,
            epochDrops, refusedDrops, headDeclines, queuedBytes, peakQueuedBytes, commandWindow);
    }

    private void retire(final int length) {
        queuedBytes -= length;
        retired += length;
        retiredCounter.setOrdered(retired);
    }

    private void closeApp(final String reason) {
        log("%s", reason);
        logQueue();
        app.close();
        app = null;
        attachId = 0;
        queue.consume(discard, Integer.MAX_VALUE);
        queuedBytes = 0;
        collecting = false;
    }

    // ── Application.Listener ─────────────────────────────────────────────────────────────────────────────────

    @Override
    public void onGroupsResolved(final GroupDirectory directory) {
    }

    @Override
    public void onLeadershipChanged(final boolean nowLeading) {
        leading = nowLeading;
        epoch++;
        log(nowLeading ? "now leading — gate epoch %d, epochDrops %d" : "not leading — gate epoch %d, epochDrops %d", epoch,
            epochDrops);
        sendGate();
    }

    @Override
    public void onSequenced(final Payload payload) {
        frames++;
        HostedWire.header(out, HostedWire.PAYLOAD, attachId);
        out.putLong(16, payload.clusterSeqNo());
        out.putInt(24, payload.sourceId());
        out.putInt(28, payload.payloadId());
        out.putInt(32, payload.templateId());
        out.putInt(36, payload.bodyLength());
        payload.buffer().getBytes(payload.bodyOffset(), out, 40, payload.bodyLength());
        send(40 + payload.bodyLength());
    }

    @Override
    public void onCaughtUp(final long clusterSeqNo) {
        HostedWire.header(out, HostedWire.CAUGHT_UP, attachId);
        out.putLong(16, clusterSeqNo);
        send(24);
        log("host caught up at clusterSeqNo %d", clusterSeqNo);
    }

    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        log("FENCED: %s — %s", fence, detail);
        final byte[] text = (fence + ": " + detail).getBytes(StandardCharsets.UTF_8);
        HostedWire.header(out, HostedWire.FENCED, attachId);
        out.putInt(16, text.length);
        out.putBytes(20, text);
        send(20 + text.length);
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
            lastClientNs = System.nanoTime();
            HostedWire.header(out, HostedWire.SNAPSHOT_CUT, attachId);
            out.putLong(16, cutNo);
            send(24);
            while (!endSeen) {
                commands.poll(onCommand, 16);
                app.keepAlive();
                final long now = System.nanoTime();
                if (now - lastHeartbeatNs >= HEARTBEAT_NS) {
                    sendHeartbeat(now);
                }
                if (now - lastClientNs > clientTimeoutNs) {
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
        HostedWire.header(out, HostedWire.RESTORE_RECORD, attachId);
        out.putInt(16, recordIndex);
        out.putInt(20, formatVersion);
        out.putInt(24, length);
        out.putBytes(28, buffer, 0, length);
        send(28 + length);
    }

    // ── Sends ────────────────────────────────────────────────────────────────────────────────────────────────

    private void sendGate() {
        HostedWire.header(out, HostedWire.GATE, attachId);
        out.putLong(16, epoch);
        out.putByte(24, (byte)(leading ? 1 : 0));
        out.putByte(25, (byte)(app != null && app.canPublish() ? 1 : 0));
        send(26);
    }

    private void sendHeartbeat(final long now) {
        lastHeartbeatNs = now;
        frames++;
        HostedWire.header(out, HostedWire.HEARTBEAT, attachId);
        out.putLong(16, now);
        send(24);
    }

    /** Offers {@code out}; no subscriber means no client, which is not worth spinning on. */
    private void send(final int length) {
        final long deadline = System.nanoTime() + BACK_PRESSURE_NS;
        while (true) {
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
