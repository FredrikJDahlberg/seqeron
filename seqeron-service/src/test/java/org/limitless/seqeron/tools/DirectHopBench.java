package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.util.IdleStrategies;

/**
 * The hop through the Java {@link Application} on the leader's driver (doc/ec2-tests.md, Test 1b): an open-loop
 * publish of a ProbeMarker of {@code --payload} bytes, timed from its intended send until this process sees its own
 * frame sequenced. It prints the line {@code hosted_hop_bench} does, so {@code hop-knee.sh} reads either.
 *
 * <pre>
 *   DirectHopBench --rate R/s --count N [--member M] [--aeron-dir D] [--payload B] [--warmup-ms W]
 *                  [--source-id S] [--client-id C]
 * </pre>
 *
 * <p>The first {@code --warmup-ms} of the run (default a fifth of it, at least 2000 markers) is discarded, and so
 * are the declines before it. The idle strategy is {@code -Dbench.idle} (backoff, yielding or busyspin).
 */
final class DirectHopBench {
    private static final int PROBE_PAYLOAD_ID = 5;
    private static final int PROBE_MARKER_TEMPLATE_ID = 1;
    private static final int PROBE_SCHEMA_ID = 214;
    private static final int MARKER_BLOCK_LENGTH = 8;
    private static final int HEADER = 8;
    private static final int MIN_PAYLOAD = HEADER + MARKER_BLOCK_LENGTH + Short.BYTES;
    private static final int MAX_PAYLOAD = 8875;
    // Set in the seqNo of every marker to tell them from other sources'; bits 28-39 salt it per run.
    private static final long OWN_TAG = 1L << 40;
    private static final int SALT_SHIFT = 28;
    private static final long INDEX_MASK = (1L << SALT_SHIFT) - 1;
    private static final long TIMEOUT_NS = 90_000_000_000L;
    private static final long START_DELAY_NS = 200_000_000L;

    private DirectHopBench() {
    }

    public static void main(final String[] args) {
        final Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            options.put(args[i].replaceFirst("^-+", ""), args[i + 1]);
        }
        try {
            System.exit(run(options));
        } catch (final Exception ex) {
            System.err.println("DirectHopBench failed: " + ex);
            System.exit(1);
        }
    }

    private static int run(final Map<String, String> options) {
        final long rate = Long.parseLong(options.getOrDefault("rate", "5000"));
        final int count = Integer.parseInt(options.getOrDefault("count", "30000"));
        final int member = Integer.parseInt(options.getOrDefault("member", "0"));
        final int payload = Integer.parseInt(options.getOrDefault("payload", Integer.toString(MIN_PAYLOAD)));
        final int sourceId = Integer.parseInt(options.getOrDefault("source-id", "16"));
        if (rate <= 0 || count <= 0 || count > INDEX_MASK || payload < MIN_PAYLOAD || payload > MAX_PAYLOAD) {
            throw new IllegalArgumentException("rate and count must be positive and payload within 18..8875");
        }
        final Run run = new Run(rate, count);
        final long warmupMs = Long.parseLong(options.getOrDefault("warmup-ms", "-1"));
        final int warm = (int)Math.min(count, warmupMs >= 0 ? warmupMs * 1_000_000L / run.interval
                                                            : Math.max(2000, count / 5));
        final Application.Listener listener = new Application.Listener() {
            @Override
            public void onLeadershipChanged(final boolean leading) {
            }

            @Override
            public void onSequenced(final Payload p) {
                if (p.sourceId() == sourceId && p.payloadId() == PROBE_PAYLOAD_ID &&
                    p.templateId() == PROBE_MARKER_TEMPLATE_ID) {
                    final long seqNo = p.buffer().getLong(p.bodyOffset());
                    if ((seqNo & OWN_TAG) != 0) {
                        run.received(seqNo, System.nanoTime());
                    }
                }
            }

            @Override
            public void onFenced(final ClusterError fence, final String detail) {
                run.fault = fence + ": " + detail;
            }
        };
        final String aeronDir = options.getOrDefault("aeron-dir", PortLayout.aeronDir(member));
        final UnsafeBuffer marker = new UnsafeBuffer(new byte[payload]);
        marker.putShort(0, (short)MARKER_BLOCK_LENGTH);
        marker.putShort(2, (short)PROBE_MARKER_TEMPLATE_ID);
        marker.putShort(4, (short)PROBE_SCHEMA_ID);
        final IdleStrategy idle = IdleStrategies.fromProperty("bench.idle").get();
        long declinesAtWarm = 0;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             Application app = Application.builder()
                 .sourceId(sourceId)
                 .clientId(Integer.parseInt(options.getOrDefault("client-id", "12")))
                 .memberId(member)
                 .listener(listener)
                 .build()) {
            app.start(aeron);
            while (!run.done(System.nanoTime()) && run.fault == null) {
                int work = app.doWork();
                final long now = System.nanoTime();
                if (run.t0 == 0) {
                    if (app.canPublish()) {
                        run.t0 = now + START_DELAY_NS;
                        run.lastNs = run.t0;
                    }
                } else if (run.sent < count && now >= run.due(run.sent) && app.canPublish()) {
                    marker.putLong(HEADER, OWN_TAG | run.salt | (run.sent + 1));
                    final Publish outcome = app.publish(PROBE_PAYLOAD_ID, marker, payload);
                    if (outcome == Publish.Published) {
                        run.sent++;
                        work++;
                        if (run.sent == warm) {
                            declinesAtWarm = run.declines;
                        }
                    } else if (outcome == Publish.Refused) {
                        throw new IllegalStateException("a ProbeMarker the sequencer would refuse");
                    } else {
                        run.declines++;
                    }
                }
                idle.idle(work);
            }
        }
        if (run.fault != null) {
            System.out.println("FENCED " + run.fault);
            return 1;
        }
        report(run, rate, payload, warm, declinesAtWarm);
        return 0;
    }

    /** One run's open-loop schedule and the latencies it produced; marker i is sent at t0 + i * interval. */
    private static final class Run {
        final long count;
        final long interval;
        final long salt;
        final long[] latency;
        long t0;
        long sent;
        long seen;
        long declines;
        long lastNs;
        String fault;

        Run(final long rate, final long count) {
            this.count = count;
            interval = 1_000_000_000L / rate;
            salt = ((System.nanoTime() / 1_000_000_000L) & 0xFFF) << SALT_SHIFT;
            latency = new long[(int)count];
            Arrays.fill(latency, -1);
        }

        long due(final long index) {
            return t0 + index * interval;
        }

        void received(final long seqNo, final long now) {
            final long index = (seqNo & INDEX_MASK) - 1;
            if ((seqNo & (OWN_TAG - 1) & ~INDEX_MASK) != salt || index < 0 || index >= sent ||
                latency[(int)index] >= 0) {
                return;
            }
            latency[(int)index] = now - due(index);
            seen++;
            lastNs = now;
        }

        boolean done(final long now) {
            return seen >= count || (t0 != 0 && now > t0 + TIMEOUT_NS + count * interval);
        }
    }

    /** The samples as bucket:count pairs, as {@code hosted_hop_bench} writes them: 32 buckets to each power of two. */
    private static String histogram(final long[] samples) {
        final TreeMap<Integer, Long> buckets = new TreeMap<>();
        for (final long value : samples) {
            final int log = value < 32 ? 0 : 63 - Long.numberOfLeadingZeros(value);
            final int bucket = value < 32 ? (int)value : (log - 4) * 32 + (int)((value >>> (log - 5)) & 31);
            buckets.merge(bucket, 1L, Long::sum);
        }
        final StringBuilder text = new StringBuilder();
        buckets.forEach((bucket, n) -> text.append(text.isEmpty() ? "" : ",").append(bucket).append(':').append(n));
        return text.toString();
    }

    private static void report(final Run run, final long rate, final int payload, final int warm,
                               final long declinesAtWarm) {
        final long[] samples = Arrays.stream(run.latency, warm, run.latency.length).filter(ns -> ns >= 0).sorted()
            .toArray();
        if (samples.length == 0) {
            System.out.printf(Locale.ROOT, "arm=java rate=%d: no samples (seen %d of %d)%n", rate, run.seen, run.count);
            return;
        }
        System.out.printf(Locale.ROOT, "arm=java rate=%d payload=%d n=%d seen=%d declines=%d warmup_declines=%d " +
                          "p50=%.1f p99=%.1f p99.9=%.1f max=%.1f (us) hist=%s%n",
                          rate, payload, samples.length, run.seen, run.declines - declinesAtWarm, declinesAtWarm,
                          at(samples, 0.5), at(samples, 0.99), at(samples, 0.999), at(samples, 1.0),
                          histogram(samples));
    }

    private static double at(final long[] sorted, final double quantile) {
        return sorted[(int)(quantile * (sorted.length - 1))] / 1000.0;
    }
}
