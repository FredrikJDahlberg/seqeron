package org.limitless.seqeron.metrics;

import static org.junit.jupiter.api.Assertions.*;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the exporter's Prometheus rendering. No Aeron runtime: {@code CountersManager} is
 * Agrona's own, so a counters region can be built over plain buffers and the exporter reads it exactly
 * as it reads a node's CnC file.
 *
 * <p>What they pin is the one thing the format itself makes fatal: {@code HELP}/{@code TYPE} belongs to
 * a metric <b>name</b>, and a name carries one counter per co-located replica. A repeated HELP is
 * rejected by Prometheus for the whole body, so a second app replica used to take every other metric on
 * the node down with it.
 */
class MetricsExporterTest {
    private static final int COUNTER_COUNT = 16;
    private static final int MEMBER = 1;

    private final CountersManager counters = newCountersManager();

    @Test
    @DisplayName("two co-located replicas' app counters render under one HELP/TYPE header")
    void appCountersOfOneTypeShareOneHeader() {
        addAppCounter(SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID, MEMBER, 9, 0);
        addAppCounter(SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID, MEMBER, 10, 1);

        final String body = new MetricsExporter(counters).renderMetrics();

        assertEquals(1, count(body, "# HELP seqeron_app_recovery_stalled "),
                     "a repeated HELP line makes Prometheus reject the entire scrape");
        assertEquals(1, count(body, "# TYPE seqeron_app_recovery_stalled "));
        assertTrue(body.contains("seqeron_app_recovery_stalled{member=\"1\",client=\"9\"} 0"), body);
        assertTrue(body.contains("seqeron_app_recovery_stalled{member=\"1\",client=\"10\"} 1"), body);
    }

    @Test
    @DisplayName("each metric keeps its own header, and its samples stay together under it")
    void distinctMetricsKeepDistinctHeaders() {
        addAppCounter(SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID, MEMBER, 9, 0);
        addCounter(SeqeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID, MEMBER, 42);
        addAppCounter(SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID, MEMBER, 10, 1);

        final String body = new MetricsExporter(counters).renderMetrics();

        assertEquals(1, count(body, "# HELP seqeron_app_recovery_stalled "));
        assertEquals(1, count(body, "# HELP seqeron_sequencer_global_seq_no "));
        // Interleaved in the counters region, grouped in the body: a metric's samples must all follow
        // its own header, not another metric's.
        assertTrue(body.indexOf("client=\"10\"") < body.indexOf("# HELP seqeron_sequencer_global_seq_no ")
                       || body.indexOf("# HELP seqeron_sequencer_global_seq_no ") < body.indexOf("client=\"9\""),
                   body);
    }

    @Test
    @DisplayName("a node-scoped counter carries no client label")
    void nodeScopedCounterHasNoClientLabel() {
        addCounter(SeqeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID, MEMBER, 42);

        assertTrue(new MetricsExporter(counters).renderMetrics()
                       .contains("seqeron_sequencer_global_seq_no{member=\"1\"} 42"));
    }

    @Test
    @DisplayName("a counter seqeron does not own is skipped")
    void unknownTypeIdIsSkipped() {
        counters.newCounter("something.else", 999, keyBuffer -> keyBuffer.putInt(0, MEMBER));

        assertEquals("", new MetricsExporter(counters).renderMetrics());
    }

    @Test
    @DisplayName("a consumer's app counter core has no metadata for renders under its own label")
    void unknownAppCounterIsNamedFromItsLabel() {
        counters.newCounter("simdfixgw.fix.sessionsUp member=1 client=3", SeqeronCounters.APP_TYPE_ID_MIN + 42,
                            keyBuffer -> {
                                keyBuffer.putInt(SeqeronCounters.KEY_MEMBER_ID_OFFSET, MEMBER);
                                keyBuffer.putInt(SeqeronCounters.KEY_CLIENT_ID_OFFSET, 3);
                            })
            .set(7);

        final String body = new MetricsExporter(counters).renderMetrics();

        assertEquals(1, count(body, "# TYPE simdfixgw_fix_sessionsUp untyped"), body);
        assertTrue(body.contains("simdfixgw_fix_sessionsUp{member=\"1\",client=\"3\"} 7"), body);
    }

    @Test
    @DisplayName("an app counter whose label cannot be a metric name falls back to its type id")
    void unusableLabelFallsBackToTheTypeId() {
        counters.newCounter("7up member=1 client=0", SeqeronCounters.APP_TYPE_ID_MIN + 1,
                            keyBuffer -> keyBuffer.putInt(SeqeronCounters.KEY_MEMBER_ID_OFFSET, MEMBER))
            .set(1);

        final String body = new MetricsExporter(counters).renderMetrics();

        assertTrue(body.contains("seqeron_app_counter_5201{member=\"1\",client=\"0\"} 1"), body);
        assertFalse(body.contains("7up"), body);
    }

    private void addCounter(final int typeId, final int memberId, final long value) {
        counters.newCounter("seqeron member=" + memberId, typeId,
                            keyBuffer -> keyBuffer.putInt(SeqeronCounters.KEY_MEMBER_ID_OFFSET, memberId))
            .set(value);
    }

    private void addAppCounter(final int typeId, final int memberId, final int clientId, final long value) {
        counters
            .newCounter("seqeron member=" + memberId + " client=" + clientId, typeId,
                        keyBuffer -> {
                            keyBuffer.putInt(SeqeronCounters.KEY_MEMBER_ID_OFFSET, memberId);
                            keyBuffer.putInt(SeqeronCounters.KEY_CLIENT_ID_OFFSET, clientId);
                        })
            .set(value);
    }

    private static CountersManager newCountersManager() {
        // Direct and cache-line aligned: CountersManager rejects a byte[]-backed AtomicBuffer.
        return new CountersManager(aligned(COUNTER_COUNT * CountersManager.METADATA_LENGTH),
                                   aligned(COUNTER_COUNT * CountersManager.COUNTER_LENGTH));
    }

    private static UnsafeBuffer aligned(final int length) {
        return new UnsafeBuffer(BufferUtil.allocateDirectAligned(length, BitUtil.CACHE_LINE_LENGTH));
    }

    private static int count(final String body, final String needle) {
        int found = 0;
        for (int at = body.indexOf(needle); at >= 0; at = body.indexOf(needle, at + needle.length())) {
            ++found;
        }
        return found;
    }
}
