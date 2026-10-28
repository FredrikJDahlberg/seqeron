package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Consumer;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;

/**
 * Prototype of the hosted-façade hop (doc/hosted-facade-draft.md), not a test and not wired into any suite. The
 * host runs the real {@link Application} on the leader member's driver and relays it over two IPC streams to a client
 * thread; the other phase runs the same {@link Application} directly. A direct {@link Consumer} beside each phase
 * gives a per-{@code clusterSeqNo} reference for the receive path. The wire format here is bench-only, not schema
 * 218: a one-byte type, then fields.
 *
 * <p>Run off the integrationTest classpath with {@code SEQERON_PORT_BASE=9500}: {@code HostHopBench <rate/s> <count>}.
 */
final class HostHopBench {
    private static final int SOURCE_ID = 16;
    private static final int APP_CLIENT_ID = 11;
    private static final int CONSUMER_CLIENT_ID = 12;
    private static final int EVENTS_STREAM = 301;
    private static final int COMMANDS_STREAM = 302;
    private static final String IPC = "aeron:ipc";
    private static final byte EVENT = 1;
    private static final byte VERDICT = 2;
    private static final byte GATE = 3;
    private static final byte PUBLISH = 4;
    private static final int BODY = 16;
    static volatile long declines;
    static volatile long gateNs;
    static volatile long firstEventNs;
    static volatile long t0Ns;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(90);

    public static void main(final String[] args) throws Exception {
        final long rate = Long.parseLong(args[0]);
        final int count = Integer.parseInt(args[1]);
        final int warm = Math.max(2000, count / 5);
        final String externalDir = System.getProperty("bench.aeronDir");
        if (externalDir != null) {
            // A cluster already running as separate processes: member bench.memberId's driver, which leads.
            measure(externalDir, Integer.getInteger("bench.memberId", 0), rate, count, warm);
        } else {
            final Path base = Files.createTempDirectory("hop-bench");
            try (InProcessCluster cluster = new InProcessCluster(base)) {
                cluster.withApplication(SOURCE_ID);
                cluster.startAll();
                final int leader = cluster.awaitLeader();
                Thread.sleep(2000);
                measure(cluster.aeronDir(leader), leader, rate, count, warm);
            }
        }
        System.exit(0);
    }

    private static void measure(final String aeronDir, final int leader, final long rate, final int count,
                                final int warm) throws Exception {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
            final Phase direct = new Phase(rate, count, warm);
            final Phase hosted = new Phase(rate, count, warm);
            if (Boolean.getBoolean("bench.hostedFirst")) {
                runHosted(aeron, leader, hosted);
                runDirect(aeron, leader, direct);
            } else {
                runDirect(aeron, leader, direct);
                runHosted(aeron, leader, hosted);
            }
            report(direct, hosted);
        }
    }

    /** One phase's samples; every array is written by one thread and read after it ends. */
    private static final class Phase {
        final long rate;
        final int count;
        final int warm;
        final long[] confirmLatency;
        volatile int confirmed;
        final Stamps consumer;
        final Stamps host;
        final Stamps client;
        final Stamps receipts;
        volatile boolean failed;

        Phase(final long rate, final int count, final int warm) {
            this.rate = rate;
            this.count = count;
            this.warm = warm;
            confirmLatency = new long[count + warm];
            final int capacity = (count + warm) * 2;
            consumer = new Stamps(capacity);
            host = new Stamps(capacity);
            client = new Stamps(capacity);
            receipts = new Stamps(capacity);
        }

        int total() {
            return count + warm;
        }
    }

    /** (clusterSeqNo, time) pairs in arrival order. */
    private static final class Stamps {
        final long[] seqNos;
        final long[] times;
        final long[] extras;
        volatile int size;

        Stamps(final int capacity) {
            seqNos = new long[capacity];
            times = new long[capacity];
            extras = new long[capacity];
        }

        void add(final long seqNo, final long time) {
            add(seqNo, time, 0);
        }

        void add(final long seqNo, final long time, final long extra) {
            final int i = size;
            seqNos[i] = seqNo;
            times[i] = time;
            extras[i] = extra;
            size = i + 1;
        }

        Map<Long, Long> toMap() {
            final Map<Long, Long> map = new HashMap<>();
            for (int i = 0; i < size; i++) {
                map.put(seqNos[i], times[i]);
            }
            return map;
        }
    }

    /** Stamps are intended send times, so anything before the phase began is an earlier phase's, replayed. */
    private static volatile long phaseStartNs;
    private static final ThreadLocal<ProbeMarkerDecoder> DECODERS = ThreadLocal.withInitial(ProbeMarkerDecoder::new);

    private static boolean isOwnProbe(final Payload p) {
        return p.sourceId() == SOURCE_ID && p.payloadId() == ClusterProbe.PROBE_PAYLOAD_ID &&
            p.templateId() == ProbeMarkerDecoder.TEMPLATE_ID && stampOf(p, DECODERS.get()) >= phaseStartNs;
    }

    private static long stampOf(final Payload p, final ProbeMarkerDecoder decoder) {
        return decoder.wrap(p.buffer(), p.bodyOffset(), p.blockLength(), p.version()).seqNo();
    }

    // ── Direct ───────────────────────────────────────────────────────────────────────────────────────────────

    private static void runDirect(final Aeron aeron, final int leader, final Phase phase) throws Exception {
        phaseStartNs = System.nanoTime();
        final AtomicBoolean stop = new AtomicBoolean();
        final Thread consumerThread = startConsumer(aeron, leader, phase, stop);
        if (Boolean.getBoolean("bench.extraSpinner")) {
            // The hosted phase runs a host and a client thread where this runs one; match its spinning threads.
            final Thread spinner = new Thread(() -> {
                while (!stop.get()) {
                    Thread.onSpinWait();
                }
            }, "spinner");
            spinner.setDaemon(true);
            spinner.start();
        }
        final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
        final int[] seen = {0};
        final long[] stampSeqNo = new long[1];
        final Application.Listener listener = new Application.Listener() {
            @Override
            public void onLeadershipChanged(final boolean leading) {
            }

            @Override
            public void onSequenced(final Payload p) {
                if (!isOwnProbe(p)) {
                    return;
                }
                final long now = System.nanoTime();
                final int i = seen[0]++;
                phase.confirmLatency[i] = now - stampOf(p, decoder);
                phase.host.add(p.clusterSeqNo(), now, stampOf(p, decoder));
                phase.confirmed = seen[0];
            }

            @Override
            public void onFenced(final ClusterError fence, final String detail) {
                phase.failed = true;
                System.err.println("direct fenced: " + fence + " " + detail);
            }
        };
        try (Application app = application(listener, leader)) {
            app.start(aeron);
            long t0 = 0;
            long next = 0;
            long due = 0;
            boolean haveDue = false;
            final long interval = 1_000_000_000L / phase.rate;
            final long deadline = System.nanoTime() + TIMEOUT_NS + 2 * phase.total() * interval;
            while (phase.confirmed < phase.total() && !phase.failed && System.nanoTime() < deadline) {
                app.doWork();
                if (t0 == 0 && app.canPublish() && app.isCaughtUp()) {
                    t0 = System.nanoTime() + 200_000_000L;
                }
                if (t0 != 0 && next < phase.total()) {
                    if (!haveDue) {
                        due = t0 + next * interval;
                        haveDue = true;
                    }
                    if (System.nanoTime() >= due && app.canPublish()) {
                        final int length = marker.encodePayload(due, ClusterProbe.NO_FILLER);
                        if (app.publish(ClusterProbe.PROBE_PAYLOAD_ID, marker.payload(), length) == Publish.Published) {
                            next++;
                            haveDue = false;
                        }
                    }
                }
            }
        }
        stop.set(true);
        consumerThread.join();
        if (phase.confirmed < phase.total()) {
            throw new IllegalStateException("direct phase confirmed " + phase.confirmed + " of " + phase.total());
        }
    }

    // ── Hosted ───────────────────────────────────────────────────────────────────────────────────────────────

    private static void runHosted(final Aeron aeron, final int leader, final Phase phase) throws Exception {
        phaseStartNs = System.nanoTime();
        final AtomicBoolean stop = new AtomicBoolean();
        final Thread consumerThread = startConsumer(aeron, leader, phase, stop);

        final Publication events = aeron.addExclusivePublication(IPC, EVENTS_STREAM);
        final Subscription eventsIn = aeron.addSubscription(IPC, EVENTS_STREAM);
        final Publication commands = aeron.addExclusivePublication(IPC, COMMANDS_STREAM);
        final Subscription commandsIn = aeron.addSubscription(IPC, COMMANDS_STREAM);
        while (!events.isConnected() || !eventsIn.isConnected() || !commands.isConnected() || !commandsIn.isConnected()) {
            Thread.onSpinWait();
        }

        final Thread host = new Thread(() -> hostLoop(aeron, leader, phase, events, commandsIn, stop), "host");
        host.start();
        final Thread client = new Thread(() -> clientLoop(phase, eventsIn, commands, stop), "client");
        client.start();
        client.join();
        stop.set(true);
        host.join();
        consumerThread.join();
        events.close();
        eventsIn.close();
        commands.close();
        commandsIn.close();
        if (phase.confirmed < phase.total()) {
            throw new IllegalStateException("hosted phase confirmed " + phase.confirmed + " of " + phase.total());
        }
    }

    private static void hostLoop(final Aeron aeron, final int leader, final Phase phase, final Publication events,
                                 final Subscription commandsIn, final AtomicBoolean stop) {
        final UnsafeBuffer out = new UnsafeBuffer(new byte[4096]);
        final UnsafeBuffer scratch = new UnsafeBuffer(new byte[4096]);
        final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        final Application[] holder = new Application[1];
        final Application.Listener listener = new Application.Listener() {
            @Override
            public void onLeadershipChanged(final boolean leading) {
                out.putByte(0, GATE);
                out.putLong(8, leading ? 1 : 0);
                offer(events, out, BODY, stop);
            }

            @Override
            public void onSequenced(final Payload p) {
                if (!isOwnProbe(p)) {
                    return;
                }
                phase.host.add(p.clusterSeqNo(), System.nanoTime(), stampOf(p, decoder));
                out.putByte(0, EVENT);
                out.putLong(8, p.clusterSeqNo());
                p.buffer().getBytes(p.bodyOffset(), out, BODY, p.bodyLength());
                offer(events, out, BODY + p.bodyLength(), stop);
            }

            @Override
            public void onFenced(final ClusterError fence, final String detail) {
                phase.failed = true;
                System.err.println("host fenced: " + fence + " " + detail);
            }
        };
        final FragmentHandler onCommand = (buffer, offset, length, header) -> {
            if (buffer.getByte(offset) != PUBLISH) {
                return;
            }
            final long stamp = buffer.getLong(offset + 8);
            phase.receipts.add(stamp, System.nanoTime());
            final int payloadLength = length - BODY;
            buffer.getBytes(offset + BODY, scratch, 0, payloadLength);
            if (holder[0].publish(ClusterProbe.PROBE_PAYLOAD_ID, scratch, payloadLength) != Publish.Published) {
                out.putByte(0, VERDICT);
                out.putLong(8, stamp);
                offer(events, out, BODY, stop);
            }
        };
        try (Application app = application(listener, leader)) {
            holder[0] = app;
            app.start(aeron);
            while (!stop.get() && !phase.failed) {
                app.doWork();
                commandsIn.poll(onCommand, 16);
            }
        }
    }

    private static void clientLoop(final Phase phase, final Subscription eventsIn, final Publication commands,
                                   final AtomicBoolean stop) {
        final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
        final UnsafeBuffer out = new UnsafeBuffer(new byte[4096]);
        final ArrayDeque<Long> retries = new ArrayDeque<>();
        final boolean[] gateOpen = {false};
        final int[] seen = {0};
        final FragmentHandler onEvent = (buffer, offset, length, header) -> {
            final byte type = buffer.getByte(offset);
            if (type == GATE) {
                gateOpen[0] = buffer.getLong(offset + 8) != 0;
                gateNs = System.nanoTime();
            } else if (type == VERDICT) {
                declines++;
                retries.add(buffer.getLong(offset + 8));
            } else if (type == EVENT) {
                final long now = System.nanoTime();
                final long stamp =
                    decoder.wrap(buffer, offset + BODY, ProbeMarkerDecoder.BLOCK_LENGTH, ProbeMarkerDecoder.SCHEMA_VERSION)
                        .seqNo();
                final int i = seen[0]++;
                if (i == 0) {
                    firstEventNs = now;
                }
                phase.confirmLatency[i] = now - stamp;
                phase.client.add(buffer.getLong(offset + 8), now);
                phase.confirmed = seen[0];
            }
        };
        final long interval = 1_000_000_000L / phase.rate;
        long t0 = 0;
        long next = 0;
        final long deadline = System.nanoTime() + TIMEOUT_NS + 2 * phase.total() * interval;
        while (phase.confirmed < phase.total() && !phase.failed && System.nanoTime() < deadline) {
            eventsIn.poll(onEvent, 16);
            if (!gateOpen[0]) {
                continue;
            }
            if (t0 == 0) {
                t0 = System.nanoTime() + 200_000_000L;
                t0Ns = t0;
            }
            final Long retry = retries.peekFirst();
            if (retry != null) {
                retries.pollFirst();
                send(commands, marker, out, retry, stop);
            } else if (next < phase.total() && System.nanoTime() >= t0 + next * interval) {
                send(commands, marker, out, t0 + next * interval, stop);
                next++;
            }
        }
        stop.set(true);
    }

    private static void send(final Publication commands, final ClusterProbe.MarkerEncoder marker,
                             final UnsafeBuffer out, final long stamp, final AtomicBoolean stop) {
        final int length = marker.encodePayload(stamp, ClusterProbe.NO_FILLER);
        out.putByte(0, PUBLISH);
        out.putLong(8, stamp);
        marker.payload().getBytes(0, out, BODY, length);
        offer(commands, out, BODY + length, stop);
    }

    // ── Shared ───────────────────────────────────────────────────────────────────────────────────────────────

    private static Application application(final Application.Listener listener, final int leader) {
        return Application.builder()
            .sourceId(SOURCE_ID)
            .clientId(APP_CLIENT_ID)
            .memberId(leader)
            .egressChannel("aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0")
            .ingressEndpoints(PortLayout.ingressEndpoints(InProcessCluster.MEMBERS))
            .listener(listener)
            .build();
    }

    private static Thread startConsumer(final Aeron aeron, final int leader, final Phase phase,
                                        final AtomicBoolean stop) {
        final Thread thread = new Thread(() -> {
            if (!Boolean.parseBoolean(System.getProperty("bench.consumer", "true"))) {
                return;
            }
            final Consumer consumer = Consumer.builder()
                .clientId(CONSUMER_CLIENT_ID)
                .memberId(leader)
                .groupId(InProcessCluster.GROUP_ID)
                .listener(new Consumer.Listener() {
                    @Override
                    public void onSequenced(final Payload p) {
                        if (isOwnProbe(p)) {
                            phase.consumer.add(p.clusterSeqNo(), System.nanoTime());
                        }
                    }

                    @Override
                    public void onFenced(final ClusterError fence, final String detail) {
                        System.err.println("consumer fenced: " + fence + " " + detail);
                    }
                })
                .build();
            try {
                consumer.start(aeron);
                while (!stop.get()) {
                    consumer.doWork();
                }
            } finally {
                consumer.close();
            }
        }, "consumer");
        thread.start();
        return thread;
    }

    private static void offer(final Publication publication, final UnsafeBuffer buffer, final int length,
                              final AtomicBoolean stop) {
        while (publication.offer(buffer, 0, length) < 0 && !stop.get()) {
            Thread.onSpinWait();
        }
    }

    // ── Report ───────────────────────────────────────────────────────────────────────────────────────────────

    private static void report(final Phase direct, final Phase hosted) {
        System.out.printf("%n== %d msg/s, %d measured (after %d warm-up), one 3-member cluster in this JVM%n",
                          direct.rate, direct.count, direct.warm);
        System.out.println("publish() -> own frame sequenced, back to the application (the whole loop), ns:");
        summary("  direct ", Arrays.copyOfRange(direct.confirmLatency, direct.warm, direct.total()));
        summary("  hosted ", Arrays.copyOfRange(hosted.confirmLatency, hosted.warm, hosted.total()));

        System.out.printf("hosted: declines=%d, first event %d ms after t0, gate seen %d ms before t0%n", declines,
                          (firstEventNs - t0Ns) / 1_000_000, (t0Ns - gateNs) / 1_000_000);
        final Map<Long, Long> receipt = hosted.receipts.toMap();
        final long[] sendHop = new long[hosted.host.size];
        final long[] commit = new long[hosted.host.size];
        final long[] seqVsIntended = new long[hosted.host.size];
        final long[] directCommit = new long[direct.host.size];
        int n = 0;
        for (int i = hosted.warm; i < hosted.host.size; i++) {
            final Long r = receipt.get(hosted.host.extras[i]);
            if (r != null) {
                sendHop[n] = r - hosted.host.extras[i];
                commit[n] = hosted.host.times[i] - r;
                seqVsIntended[n++] = hosted.host.times[i] - hosted.host.extras[i];
            }
        }
        int m = 0;
        for (int i = direct.warm; i < direct.host.size; i++) {
            directCommit[m++] = direct.host.times[i] - direct.host.extras[i];
        }
        System.out.println("where the loop goes (host/application sees its own frame sequenced), ns:");
        summary("  direct: sequenced - intended send                  ", Arrays.copyOf(directCommit, m));
        summary("  hosted: sequenced - intended send                  ", Arrays.copyOf(seqVsIntended, n));
        summary("  hosted:   host got command - intended send (send hop)", Arrays.copyOf(sendHop, n));
        summary("  hosted:   sequenced - host got command (commit)    ", Arrays.copyOf(commit, n));
        System.out.println("receive path, per clusterSeqNo, same run, ns:");
        final Map<Long, Long> dConsumer = direct.consumer.toMap();
        summary("  direct: Application - Consumer (same path, sanity)", deltas(direct.host, dConsumer, direct.warm));
        final Map<Long, Long> hConsumer = hosted.consumer.toMap();
        summary("  hosted: host Application - Consumer (sanity)      ", deltas(hosted.host, hConsumer, hosted.warm));
        final Map<Long, Long> hHost = hosted.host.toMap();
        summary("  hosted: client - host Application (the hop)       ", deltas(hosted.client, hHost, hosted.warm));
        summary("  hosted: client - Consumer (hop vs a direct reader)", deltas(hosted.client, hConsumer, hosted.warm));
    }

    /** {@code a[seqNo] - b[seqNo]} for the frames past warm-up that both saw. */
    private static long[] deltas(final Stamps a, final Map<Long, Long> b, final int warm) {
        final long[] out = new long[a.size];
        int n = 0;
        for (int i = warm; i < a.size; i++) {
            final Long other = b.get(a.seqNos[i]);
            if (other != null) {
                out[n++] = a.times[i] - other;
            }
        }
        return Arrays.copyOf(out, n);
    }

    private static void summary(final String label, final long[] samples) {
        if (samples.length == 0) {
            System.out.println(label + " no samples");
            return;
        }
        final long[] s = samples.clone();
        Arrays.sort(s);
        System.out.printf("%s n=%d p50=%d p90=%d p99=%d p99.9=%d max=%d%n", label, s.length, p(s, .5), p(s, .9),
                          p(s, .99), p(s, .999), s[s.length - 1]);
    }

    private static long p(final long[] sorted, final double q) {
        return sorted[(int)Math.min(sorted.length - 1, (long)(q * sorted.length))];
    }

    private HostHopBench() {
    }
}
