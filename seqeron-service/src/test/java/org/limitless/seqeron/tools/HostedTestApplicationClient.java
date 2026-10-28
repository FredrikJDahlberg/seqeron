package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;

/**
 * The client of the hosted-façade prototype (doc/hosted-facade-draft.md): {@code TestApplication}'s state and snapshot
 * records, byte for byte, behind {@link HostedWire}'s streams instead of an {@code Application}. Its state is the count
 * of markers sequenced under its {@code sourceId} and a digest of their {@code clusterSeqNo}s, so a direct replica
 * comparing a round against the one this client wrote fences if the two differ (spec A-7).
 *
 * <p>Properties on top of {@link ClusterProbe}'s {@code probe.memberId} / {@code probe.aeronDir}:
 * {@code probe.sourceId} (16), {@code probe.intervalMs} (100), {@code client.stallCutNo} (0: stall in that cut, after
 * printing {@code IN CUT}, until killed), {@code client.slowCutMs} (0: spend that long in every cut, heartbeating,
 * before the record), {@code client.ghost} (send one command under an attachId that is not current), {@code client.corruptRecord} (write a digest one off in every snapshot record), {@code
 * client.hostTimeoutMs} (3000).
 */
public final class HostedTestApplicationClient {
    private static final int EXIT_FENCED = 70;
    private static final long HEARTBEAT_NS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long ATTACH_RETRY_NS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long ATTACH_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(90);

    private final int sourceId = Integer.getInteger("probe.sourceId", 16);
    private final long intervalNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("probe.intervalMs", 100));
    private final long stallCutNo = Long.getLong("client.stallCutNo", 0);
    private final long slowCutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("client.slowCutMs", 0));
    private final boolean ghost = Boolean.getBoolean("client.ghost");
    private final boolean corruptRecord = Boolean.getBoolean("client.corruptRecord");
    private final long hostTimeoutNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("client.hostTimeoutMs", 3000));
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[64 * 1024]);
    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private final FragmentHandler onEvent = this::onEvent;

    private Publication commands;
    private long attachId;
    private long nonce;
    private long markers;
    private long digest;
    private long lastAppliedSeqNo;
    private long epoch;
    private boolean leading;
    private boolean canPublish;
    private boolean fenced;
    private long lastHostNs;
    private long markerSeqNo;
    private long correlationId;
    private long cuts;
    private long declined;
    private long staleDropped;

    public static void main(final String[] args) {
        System.exit(new HostedTestApplicationClient().run());
    }

    private int run() {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ClusterProbe.aeronDir()))) {
            final Subscription events = aeron.addSubscription(HostedWire.IPC, HostedWire.EVENTS_STREAM);
            commands = aeron.addExclusivePublication(HostedWire.IPC, HostedWire.COMMANDS_STREAM);
            if (!attach(events)) {
                log("no ATTACHED within %ds", TimeUnit.NANOSECONDS.toSeconds(ATTACH_TIMEOUT_NS));
                return 3;
            }
            log("attached, attachId=%x", attachId);
            if (ghost) {
                HostedWire.header(out, HostedWire.PUBLISH, attachId ^ 0x5A5A5A5AL);
                out.putLong(16, ++correlationId);
                out.putLong(24, epoch);
                send(40);
                log("sent one command under attachId %x", attachId ^ 0x5A5A5A5AL);
            }
            final IdleStrategy idle = new BackoffIdleStrategy();
            long nextMarkerNs = 0;
            long nextHeartbeatNs = 0;
            long nextStateNs = 0;
            lastHostNs = System.nanoTime();
            while (!fenced) {
                int work = events.poll(onEvent, 16);
                final long now = System.nanoTime();
                if (canPublish && now >= nextMarkerNs) {
                    publishMarker();
                    nextMarkerNs = now + intervalNs;
                    work++;
                }
                if (now >= nextStateNs) {
                    log("state: %d marker(s), digest %016x, published %d, declined %d", markers, digest, markerSeqNo,
                        declined);
                    nextStateNs = now + TimeUnit.SECONDS.toNanos(2);
                }
                if (now >= nextHeartbeatNs) {
                    heartbeat(now);
                    nextHeartbeatNs = now + HEARTBEAT_NS;
                }
                if (now - lastHostNs > hostTimeoutNs) {
                    log("HOST_LOST: silent for %dms", (now - lastHostNs) / 1_000_000);
                    return EXIT_FENCED;
                }
                idle.idle(work);
            }
            return EXIT_FENCED;
        }
    }

    private boolean attach(final Subscription events) {
        nonce = ThreadLocalRandom.current().nextLong();
        final long deadline = System.nanoTime() + ATTACH_TIMEOUT_NS;
        final boolean[] attached = {false};
        final FragmentHandler onAttached = (buffer, offset, length, header) -> {
            if (buffer.getByte(offset) == HostedWire.ATTACHED && buffer.getLong(offset + 16) == nonce) {
                attachId = buffer.getLong(offset + 8);
                attached[0] = true;
            }
        };
        long nextAttachNs = 0;
        while (!attached[0] && System.nanoTime() < deadline) {
            events.poll(onAttached, 16);
            final long now = System.nanoTime();
            if (now >= nextAttachNs) {
                HostedWire.header(out, HostedWire.ATTACH, 0);
                out.putLong(16, nonce);
                out.putInt(24, 11);
                out.putInt(28, 1);
                send(32);
                nextAttachNs = now + ATTACH_RETRY_NS;
            }
            Thread.onSpinWait();
        }
        return attached[0];
    }

    private void onEvent(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final byte type = buffer.getByte(offset);
        if (buffer.getLong(offset + 8) != attachId) {
            staleDropped++;
            log("dropped stale event type=%d attachId=%x", type, buffer.getLong(offset + 8));
            return;
        }
        lastHostNs = System.nanoTime();
        switch (type) {
        case HostedWire.PAYLOAD -> {
            final long clusterSeqNo = buffer.getLong(offset + 16);
            if (buffer.getInt(offset + 24) == sourceId && buffer.getInt(offset + 28) == ClusterProbe.PROBE_PAYLOAD_ID &&
                buffer.getInt(offset + 32) == ProbeMarkerDecoder.TEMPLATE_ID) {
                markers++;
                digest = digest * 31 + clusterSeqNo;
            }
            lastAppliedSeqNo = clusterSeqNo;
        }
        case HostedWire.GATE -> {
            epoch = buffer.getLong(offset + 16);
            final boolean nowLeading = buffer.getByte(offset + 24) != 0;
            canPublish = buffer.getByte(offset + 25) != 0;
            if (nowLeading != leading) {
                leading = nowLeading;
                log(leading ? "now leading — submitting markers" : "not leading — silent");
            }
        }
        case HostedWire.CAUGHT_UP -> log(
            "Caught up — following live at clusterSeqNo %d, %d marker(s) sequenced, digest %016x",
            buffer.getLong(offset + 16), markers, digest);
        case HostedWire.RESTORE_RECORD -> {
            final int recordIndex = buffer.getInt(offset + 16);
            if (recordIndex == 0) {
                markers = buffer.getLong(offset + 28);
                digest = buffer.getLong(offset + 36);
            }
            log("restored record %d (format %d): %d marker(s), digest %016x", recordIndex, buffer.getInt(offset + 20),
                markers, digest);
        }
        case HostedWire.SNAPSHOT_CUT -> onCut(buffer.getLong(offset + 16));
        case HostedWire.VERDICT -> {
            declined++;
            log("declined: result %d at epoch %d", buffer.getInt(offset + 32), buffer.getLong(offset + 24));
        }
        case HostedWire.FENCED -> {
            final byte[] text = new byte[buffer.getInt(offset + 16)];
            buffer.getBytes(offset + 20, text);
            log("FENCED: %s", new String(text, StandardCharsets.UTF_8));
            fenced = true;
        }
        default -> {
            // A heartbeat: lastHostNs is all it is for.
        }
        }
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
        while (System.nanoTime() < slowEndNs) {
            heartbeat(System.nanoTime());
            LockSupport.parkNanos(HEARTBEAT_NS);
        }
        HostedWire.header(out, HostedWire.SNAPSHOT_RECORD, attachId);
        out.putLong(16, cutNo);
        out.putInt(24, 0);
        out.putInt(28, 2 * Long.BYTES);
        out.putLong(32, markers);
        out.putLong(40, corruptRecord ? digest + 1 : digest);
        send(48);
        HostedWire.header(out, HostedWire.SNAPSHOT_END, attachId);
        out.putLong(16, cutNo);
        out.putInt(24, 1);
        send(28);
    }

    private void heartbeat(final long now) {
        HostedWire.header(out, HostedWire.CLIENT_HEARTBEAT, attachId);
        out.putLong(16, now);
        out.putLong(24, lastAppliedSeqNo);
        send(32);
    }

    private void publishMarker() {
        final int length = marker.encodePayload(++markerSeqNo, ClusterProbe.NO_FILLER);
        HostedWire.header(out, HostedWire.PUBLISH, attachId);
        out.putLong(16, ++correlationId);
        out.putLong(24, epoch);
        out.putInt(32, ClusterProbe.PROBE_PAYLOAD_ID);
        out.putInt(36, length);
        marker.payload().getBytes(0, out, 40, length);
        send(40 + length);
    }

    /** Offers {@code out}; with no host subscribed there is nothing to wait for, and the host's silence says so. */
    private void send(final int length) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (true) {
            final long result = commands.offer(out, 0, length);
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
