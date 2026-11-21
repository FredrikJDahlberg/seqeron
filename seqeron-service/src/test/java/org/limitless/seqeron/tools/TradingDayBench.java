package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;

/**
 * Replays a {@link TradingDay} profile's message rates through an {@link Application} on the leader's driver, every
 * message a 50-byte {@code ProbeMarker} stamped with its intended send time, and times each from then until the
 * application sees it sequenced. Sends follow the schedule, not the replies, so a stall shows as latency. Harness code
 * in the test source set, run by {@code node.sh trading-day}.
 *
 * <p>Output: one line per phase and a last {@code phase=day} line for the whole run, in microseconds:
 * {@code phase=<name> seconds=<s> sent=<n> seen=<n> declines=<n> behind_max=<us> p50= p99= p99.9= max= (us) hist=}.
 * {@code behind_max} is how far sends fell behind the schedule; {@code hist} is bucket:count, 32 buckets to each power
 * of two of the nanoseconds, as the hop benches print it, and the percentiles are its buckets' lower edges.
 *
 * <p>Usage: {@code TradingDayBench <profile>}, with system properties {@code day.memberId} (the leader, default 0),
 * {@code day.scale} (multiplies every rate, default 1) and {@code day.seed} (default 1).
 */
final class TradingDayBench {
    static final int PAYLOAD_BYTES = 50;
    private static final int SOURCE_ID = 16;
    private static final int CLIENT_ID = 12;
    private static final int BUCKETS = 60 * 32;
    private static final long READY_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(60);
    private static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    private final TradingDay day;
    private final long[] sent;
    private final long[] seen;
    private final long[] declines;
    private final long[] behindMaxNs;
    private final long[][] hist;
    private long startNs;
    private boolean failed;

    private TradingDayBench(final TradingDay day) {
        this.day = day;
        final int phases = day.phases().size();
        sent = new long[phases];
        seen = new long[phases];
        declines = new long[phases];
        behindMaxNs = new long[phases];
        hist = new long[phases][BUCKETS];
    }

    public static void main(final String[] args) throws Exception {
        final TradingDay day = TradingDay.parse(Files.readAllLines(Path.of(args[0])),
                                                Double.parseDouble(System.getProperty("day.scale", "1")));
        final TradingDayBench bench = new TradingDayBench(day);
        final boolean complete = bench.run(Integer.getInteger("day.memberId", 0), Long.getLong("day.seed", 1));
        bench.report();
        System.exit(complete ? 0 : 1);
    }

    private boolean run(final int memberId, final long seed) {
        final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        final Application.Listener listener = new Application.Listener() {
            @Override
            public void onLeadershipChanged(final boolean leading) {
                if (!leading && startNs != 0) {
                    fail("leadership lost");
                }
            }

            @Override
            public void onSequenced(final Payload p) {
                if (p.sourceId() != SOURCE_ID || p.payloadId() != ClusterProbe.PROBE_PAYLOAD_ID ||
                    p.templateId() != ProbeMarkerDecoder.TEMPLATE_ID || startNs == 0) {
                    return;
                }
                final long stamp = decoder.wrap(p.buffer(), p.bodyOffset(), p.blockLength(), p.version()).seqNo();
                if (stamp >= startNs) {
                    final int phase = day.phaseAt(stamp - startNs);
                    seen[phase]++;
                    hist[phase][bucket(System.nanoTime() - stamp)]++;
                }
            }

            @Override
            public void onFenced(final ClusterError fence, final String detail) {
                fail("fenced: " + fence + " " + detail);
            }
        };
        final String egressHost =
            memberId < PortLayout.HOSTS.size() ? PortLayout.HOSTS.get(memberId) : PortLayout.DEFAULT_HOST;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(PortLayout.aeronDir(memberId)));
             Application app = Application.builder()
                                   .sourceId(SOURCE_ID)
                                   .clientId(CLIENT_ID)
                                   .memberId(memberId)
                                   .egressChannel("aeron:udp?endpoint=" + egressHost + ":0")
                                   .ingressEndpoints(PortLayout.ingressEndpoints())
                                   .listener(listener)
                                   .build()) {
            app.start(aeron);
            final long readyDeadline = System.nanoTime() + READY_TIMEOUT_NS;
            while (!(app.canPublish() && app.isCaughtUp())) {
                app.doWork();
                if (failed || System.nanoTime() > readyDeadline) {
                    fail("not ready to publish after 60 s");
                    return false;
                }
            }
            send(app, new SplittableRandom(seed));
            final long drainDeadline = System.nanoTime() + DRAIN_TIMEOUT_NS;
            while (!failed && total(seen) < total(sent) && System.nanoTime() < drainDeadline) {
                app.doWork();
            }
        }
        return !failed && total(seen) == total(sent);
    }

    private void send(final Application app, final SplittableRandom random) {
        final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
        final byte[] filler = new byte[PAYLOAD_BYTES - marker.encodePayload(0, ClusterProbe.NO_FILLER)];
        startNs = System.nanoTime();
        long offset = day.nextArrival(0, random);
        while (offset >= 0 && !failed) {
            app.doWork();
            final long due = startNs + offset;
            final long now = System.nanoTime();
            if (now < due) {
                continue;
            }
            final int phase = day.phaseAt(offset);
            final Publish verdict =
                app.publish(ClusterProbe.PROBE_PAYLOAD_ID, marker.payload(), marker.encodePayload(due, filler));
            if (verdict == Publish.Published) {
                sent[phase]++;
                behindMaxNs[phase] = Math.max(behindMaxNs[phase], now - due);
                offset = day.nextArrival(offset, random);
            } else if (verdict == Publish.Declined) {
                declines[phase]++;
            } else {
                fail("publish refused");
            }
        }
    }

    private void fail(final String reason) {
        if (!failed) {
            failed = true;
            System.out.println("FAILED: " + reason);
        }
    }

    private void report() {
        final List<TradingDay.Phase> phases = day.phases();
        final long[] all = new long[BUCKETS];
        long behindMax = 0;
        for (int i = 0; i < phases.size(); i++) {
            final TradingDay.Phase phase = phases.get(i);
            line(phase.name(), phase.seconds(), sent[i], seen[i], declines[i], behindMaxNs[i], hist[i]);
            for (int b = 0; b < BUCKETS; b++) {
                all[b] += hist[i][b];
            }
            behindMax = Math.max(behindMax, behindMaxNs[i]);
        }
        line("day", day.durationNs() / 1e9, total(sent), total(seen), total(declines), behindMax, all);
    }

    private static void line(final String name, final double seconds, final long sent, final long seen,
                             final long declines, final long behindMaxNs, final long[] hist) {
        final StringBuilder buckets = new StringBuilder();
        for (int b = 0; b < hist.length; b++) {
            if (hist[b] != 0) {
                buckets.append(buckets.isEmpty() ? "" : ",").append(b).append(':').append(hist[b]);
            }
        }
        System.out.printf(Locale.ROOT,
                          "phase=%s seconds=%.0f sent=%d seen=%d declines=%d behind_max=%.1f p50=%.1f p99=%.1f "
                              + "p99.9=%.1f max=%.1f (us) hist=%s%n",
                          name, seconds, sent, seen, declines, behindMaxNs / 1e3, percentile(hist, 0.5) / 1e3,
                          percentile(hist, 0.99) / 1e3, percentile(hist, 0.999) / 1e3, percentile(hist, 1.0) / 1e3,
                          buckets);
    }

    /** The lower edge of the bucket holding quantile {@code q}, in nanoseconds; 0 for an empty histogram. */
    static long percentile(final long[] hist, final double q) {
        final long samples = total(hist);
        long count = 0;
        for (int b = 0; b < hist.length; b++) {
            count += hist[b];
            if (count > 0 && count >= q * samples) {
                return bucketFloor(b);
            }
        }
        return 0;
    }

    static int bucket(final long ns) {
        if (ns < 32) {
            return (int)Math.max(0, ns);
        }
        final int log = 63 - Long.numberOfLeadingZeros(ns);
        return Math.min(BUCKETS - 1, (log - 4) * 32 + (int)(ns >> (log - 5)) - 32);
    }

    static long bucketFloor(final int bucket) {
        if (bucket < 32) {
            return bucket;
        }
        return (32L + bucket % 32) << (bucket / 32 + 4 - 5);
    }

    private static long total(final long[] values) {
        long sum = 0;
        for (final long value : values) {
            sum += value;
        }
        return sum;
    }
}
