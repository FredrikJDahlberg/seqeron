package org.limitless.seqeron.tools;

import io.aeron.CncFileDescriptor;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

/**
 * Samples every counter of one Aeron driver, off its CnC file, and prints what changed. Harness code in the test
 * source set, started by {@code node.sh counter-sampler}; it opens the file read-only and registers no client, so
 * the driver does no work for it.
 *
 * <p>Output on stdout: {@code # <id> <label>} the first time a counter is seen, then one line per sample in which
 * any counter changed, {@code t=<wall ms> <id>=<value> …} listing the changed ones (every one, in the first line).
 * A counter that is freed and reallocated under the same id is relabelled.
 *
 * <p>System properties:
 * <pre>
 *   sampler.aeronDir    — the driver's Aeron directory (required)
 *   sampler.intervalMs  — time between samples; default 10
 * </pre>
 */
public final class CounterSampler {
    private static final long WAIT_FOR_CNC_MS = 30_000;

    private CounterSampler() {}

    /**
     * Runs until it is killed.
     *
     * @param args unused
     * @throws Exception if the CnC file does not appear within 30 s
     */
    public static void main(final String[] args) throws Exception {
        final File cnc = new File(System.getProperty("sampler.aeronDir"), CncFileDescriptor.CNC_FILE);
        final long intervalNs = Long.getLong("sampler.intervalMs", 10) * 1_000_000;
        final long deadline = System.currentTimeMillis() + WAIT_FOR_CNC_MS;
        while (!cnc.exists() || cnc.length() == 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("no " + cnc + " after " + WAIT_FOR_CNC_MS + " ms");
            }
            Thread.sleep(100);
        }
        final MappedByteBuffer mapped = IoUtil.mapExistingFile(cnc, "cnc");
        final UnsafeBuffer cncMetaData = CncFileDescriptor.createMetaDataBuffer(mapped);
        final CountersReader reader =
            new CountersReader(CncFileDescriptor.createCountersMetaDataBuffer(mapped, cncMetaData),
                               CncFileDescriptor.createCountersValuesBuffer(mapped, cncMetaData));
        final PrintStream out = new PrintStream(new BufferedOutputStream(System.out, 1 << 16), false);
        final StringBuilder line = new StringBuilder(4096);
        long[] last = new long[0];
        String[] labels = new String[0];
        while (true) {
            final long wallMs = System.currentTimeMillis();
            final int max = reader.maxCounterId();
            if (max >= last.length) {
                last = Arrays.copyOf(last, max + 1);
                labels = Arrays.copyOf(labels, max + 1);
            }
            line.setLength(0);
            for (int id = 0; id <= max; id++) {
                if (reader.getCounterState(id) != CountersReader.RECORD_ALLOCATED) {
                    labels[id] = null;
                    continue;
                }
                if (labels[id] == null) {
                    labels[id] = reader.getCounterLabel(id);
                    out.println("# " + id + " " + labels[id]);
                    last[id] = Long.MIN_VALUE;
                }
                final long value = reader.getCounterValue(id);
                if (value != last[id]) {
                    last[id] = value;
                    line.append(' ').append(id).append('=').append(value);
                }
            }
            if (line.length() > 0) {
                out.println("t=" + wallMs + line);
            }
            out.flush();
            LockSupport.parkNanos(intervalNs);
        }
    }
}
