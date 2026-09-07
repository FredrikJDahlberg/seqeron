package org.limitless.phixeron.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the aggregator's two parsing seams. No HTTP and no Aeron: both are pure functions over
 * a scraped body and the targets property.
 *
 * <p>What the parsing tests pin is blast radius. A scraped body is remote input, but one unparseable
 * line used to throw all the way out of {@code renderMetrics}, and the 500 that followed dropped every
 * healthy node's metrics and the {@code phixeron_node_up} gauge with it — so a single bad node blinded
 * the operator to the whole cluster.
 */
class MetricsAggregatorTest {
    private final Map<String, String> help = new LinkedHashMap<>();
    private final Map<String, String> type = new LinkedHashMap<>();
    private final Map<String, StringBuilder> samples = new LinkedHashMap<>();

    @Test
    @DisplayName("an unlabelled sample is kept, not thrown on")
    void unlabelledSampleParses() {
        MetricsAggregator.parseResponse("# HELP some_metric Docs.\n# TYPE some_metric gauge\nsome_metric 42\n", help,
                                        type, samples);

        assertEquals("Docs.", help.get("some_metric"));
        assertEquals("some_metric 42\n", samples.get("some_metric").toString());
    }

    @Test
    @DisplayName("a comment that is neither HELP nor TYPE is dropped, not read as a sample")
    void otherCommentsAreDropped() {
        MetricsAggregator.parseResponse("# UNIT some_metric seconds\n#\n# EOF\n", help, type, samples);

        assertTrue(samples.isEmpty(), samples.toString());
    }

    @Test
    @DisplayName("a HELP with no docstring names its metric and carries empty text")
    void helpWithoutDocstringParses() {
        MetricsAggregator.parseResponse("# HELP some_metric\n# TYPE some_metric gauge\n", help, type, samples);

        assertEquals("", help.get("some_metric"));
        assertEquals("gauge", type.get("some_metric"));
    }

    @Test
    @DisplayName("a line that is no sample at all is dropped rather than re-emitted")
    void junkLineIsDropped() {
        // It would go into the aggregate body verbatim, where Prometheus rejects it — the same
        // whole-scrape loss by another route.
        MetricsAggregator.parseResponse("<html>\n", help, type, samples);

        assertTrue(samples.isEmpty(), samples.toString());
    }

    @Test
    @DisplayName("the labelled samples the exporter actually emits still group by name")
    void labelledSamplesGroupByName() {
        MetricsAggregator.parseResponse("phixeron_app_recovery_stalled{member=\"0\",client=\"9\"} 0\n"
                                            + "phixeron_app_recovery_stalled{member=\"0\",client=\"10\"} 1\n",
                                        help, type, samples);

        assertEquals(1, samples.size());
        assertEquals("phixeron_app_recovery_stalled{member=\"0\",client=\"9\"} 0\n"
                         + "phixeron_app_recovery_stalled{member=\"0\",client=\"10\"} 1\n",
                     samples.get("phixeron_app_recovery_stalled").toString());
    }

    @Test
    @DisplayName("a targets entry with no separator stops start-up and names itself")
    void targetWithoutSeparatorIsRejected() {
        // Skipping it would drop the node from the aggregate entirely — not even phixeron_node_up 0 —
        // so the operator reads a full house while a node is never scraped.
        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class, () -> MetricsAggregator.parseTargets("0=localhost:9400,localhost:9401"));

        assertTrue(ex.getMessage().contains("localhost:9401"), ex.getMessage());
        assertTrue(ex.getMessage().contains("metricsAggregator.targets"), ex.getMessage());
    }

    @Test
    @DisplayName("a targets entry with a non-numeric memberId stops start-up and names itself")
    void targetWithNonNumericMemberIdIsRejected() {
        final IllegalArgumentException ex =
            assertThrows(IllegalArgumentException.class,
                         () -> MetricsAggregator.parseTargets("0=localhost:9400,one=localhost:9401"));

        assertTrue(ex.getMessage().contains("one=localhost:9401"), ex.getMessage());
    }

    @Test
    @DisplayName("well-formed targets parse to memberId order")
    void wellFormedTargetsParse() {
        assertEquals(Map.of(0, "localhost:9400", 1, "localhost:9401", 2, "localhost:9402"),
                     MetricsAggregator.parseTargets("0=localhost:9400, 1=localhost:9401 ,2=localhost:9402"));
    }
}
