package org.limitless.phixeron.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.aeron.Aeron;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.status.CountersReader;

/**
 * Node-local Prometheus exporter (see {@code doc/ops.md}): serves phixeron's operator counters
 * ({@code SequencerService}/{@code ReplayerService}, see {@link PhixeronCounters}) as
 * {@code /metrics} in Prometheus text exposition format, read live off the co-located Aeron
 * directory's CnC file — the same
 * connection-less, no-cluster-session access pattern as {@code clusterctl counters}, except this
 * process stays resident so a scraper can poll it on an interval instead of invoking a one-shot CLI.
 *
 * <p>Covers both {@code SequencerService} and {@code ReplayerService} counters from one endpoint,
 * since both processes share one node's Aeron directory. A counter whose owning process isn't up
 * yet simply doesn't appear in the scrape — no special-casing needed.
 */
public final class MetricsExporter {
    // ── Configuration (mirrors ClusterCtl's property-naming convention for co-location) ──
    private static final int MEMBER_ID = Integer.getInteger("metricsExporter.memberId", 0);
    private static final String AERON_DIR = System.getProperty(
        "metricsExporter.aeronDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + MEMBER_ID);
    private static final int PORT = Integer.getInteger("metricsExporter.port", 9400 + MEMBER_ID);

    private record MetricMeta(String name, String help, String type) { }

    private static final Map<Integer, MetricMeta> METRICS_BY_TYPE_ID = buildMetricsByTypeId();

    private static Map<Integer, MetricMeta> buildMetricsByTypeId() {
        final List<Map.Entry<Integer, MetricMeta>> entries = List.of(
            Map.entry(PhixeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_global_seq_no", "Last globalSeqNo emitted on this node's tap.",
                                     "gauge")),
            Map.entry(PhixeronCounters.SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_tap_backpressure_alerts_total",
                                     "Count of times the tap-emit back-pressure alert threshold has fired.",
                                     "counter")),
            Map.entry(PhixeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_rejected_ingress_total",
                                     "Count of malformed ingress messages skipped by Sequencer.sequenceMessage.",
                                     "counter")),
            Map.entry(PhixeronCounters.SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_leadership_change_total",
                                     "Count of leadership changes this node has observed and sequenced.", "counter")),
            Map.entry(PhixeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_current_leader_member_id",
                                     "memberId of the leader last recorded by this node's Sequencer.", "gauge")),
            Map.entry(PhixeronCounters.SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_last_tick_timestamp_ms",
                                     "Consensus timestamp of the last 1Hz ClusterHeartbeat emitted.", "gauge")),
            Map.entry(
                PhixeronCounters.SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID,
                new MetricMeta("phixeron_sequencer_gateway_promotion_total",
                               "Count of standby-promotion GatewayActive frames emitted on a gateway session close.",
                               "counter")),
            Map.entry(PhixeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_bootstrap_activated",
                                     "1 once the bootstrap GatewayActive has been emitted for the trading day, else 0.",
                                     "gauge")),
            Map.entry(
                PhixeronCounters.SEQUENCER_TAP_STALLED_TYPE_ID,
                new MetricMeta("phixeron_sequencer_tap_stalled",
                               "1 while tap-emit back-pressure has been sustained past the stall threshold, else 0.",
                               "gauge")),
            Map.entry(PhixeronCounters.SEQUENCER_CONNECTED_CLIENTS_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_connected_clients",
                                     "Current count of TCP clients connected across every gateway.", "gauge")),
            Map.entry(PhixeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID,
                      new MetricMeta("phixeron_sequencer_ingress_messages_total",
                                     "Count of ingress messages successfully sequenced.", "counter")),
            Map.entry(PhixeronCounters.REPLAYER_STALLED_TYPE_ID,
                      new MetricMeta("phixeron_replayer_stalled",
                                     "1 while the local archive is unreachable for replay, else 0.", "gauge")),
            Map.entry(
                PhixeronCounters.REPLAYER_READY_TYPE_ID,
                new MetricMeta("phixeron_replayer_ready",
                               "1 once the co-located tap recording is visible and replay requests are being served.",
                               "gauge")),
            Map.entry(PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID,
                      new MetricMeta("phixeron_replayer_active_slots", "Current count of in-flight replays.", "gauge")),
            Map.entry(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID,
                      new MetricMeta("phixeron_replayer_pending_requests",
                                     "Current count of replay requests waiting for a free slot.", "gauge")),
            Map.entry(PhixeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID,
                      new MetricMeta("phixeron_replayer_replays_served_total",
                                     "Count of replays started since this node came up.", "counter")),
            Map.entry(PhixeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID,
                      new MetricMeta("phixeron_replayer_idle_ttl_reclaimed_total",
                                     "Count of replay slots reclaimed by the idle-TTL backstop.", "counter")),
            Map.entry(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID,
                      new MetricMeta(
                          "phixeron_replayer_integrity_failure",
                          "1 once this node's oldest tap recording failed the startup gseq-1 integrity check, else 0.",
                          "gauge")),
            Map.entry(PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID,
                      new MetricMeta(
                          "phixeron_replayer_control_replies_dropped_total",
                          "Count of control replies dropped rather than spun on because an app stopped draining the "
                              + "control stream. Each costs that app one resend interval and nothing else, so a rising "
                              + "rate — not the absolute value — is what identifies a wedged replica.",
                          "counter")),
            Map.entry(
                PhixeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID,
                new MetricMeta("phixeron_replayer_client_id_collision",
                               "1 once two co-located apps were seen sharing one PHIXERON_REPLAYER_CLIENT_ID, else 0. "
                                   + "They stop each other's replays and neither catches up until it is corrected.",
                               "gauge")),
            Map.entry(PhixeronCounters.APP_RECOVERY_STALLED_TYPE_ID,
                      new MetricMeta(
                          "phixeron_app_recovery_stalled",
                          "1 while a co-located replica's recovery has dispatched nothing for 30s while not caught "
                              + "up, else 0. It is holding, which is correct — but it is not serving, and this is the "
                              + "only signal that says so.",
                          "gauge")));
        final Map<Integer, MetricMeta> map = new HashMap<>();
        for (final Map.Entry<Integer, MetricMeta> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    /** The co-located node's CnC counters — the only thing this reads; an Aeron client is not needed. */
    private final CountersReader reader;

    MetricsExporter(final CountersReader reader) {
        this.reader = reader;
    }

    public static void main(final String[] args) throws IOException {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
        final MetricsExporter exporter = new MetricsExporter(aeron.countersReader());

        final HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/metrics", exporter::handleMetrics);
        server.setExecutor(null);
        server.start();

        System.out.printf("[MetricsExporter/%d] serving /metrics on port %d from aeronDir=%s%n", MEMBER_ID, PORT,
                          AERON_DIR);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            aeron.close();
        }));
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        final byte[] body;
        try {
            body = renderMetrics().getBytes(StandardCharsets.UTF_8);
        } catch (final Exception ex) {
            final byte[] error =
                ("aeron counters unavailable: " + ex.getMessage() + "\n").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, error.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(error);
            }
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /**
     * Renders every phixeron counter this node's CnC file holds as one Prometheus exposition body.
     *
     * <p>Samples are grouped by metric first, because {@code HELP}/{@code TYPE} belongs to a metric
     * <b>name</b> and a name may carry more than one counter: a node runs several co-located replicas,
     * each with its own client-labelled counter of the same type id (see {@link
     * PhixeronCounters#addAppCounter}). Emitted per counter instead, the second header line makes the
     * whole scrape unparseable — Prometheus rejects a repeated HELP for one name, so every other
     * metric in the body goes with it.
     */
    String renderMetrics() {
        final Map<MetricMeta, StringBuilder> samplesByMetric = new LinkedHashMap<>();
        reader.forEach((counterId, typeId, keyBuffer, label) -> {
            final MetricMeta meta = METRICS_BY_TYPE_ID.get(typeId);
            if (meta == null) {
                return;
            }
            final StringBuilder samples = samplesByMetric.computeIfAbsent(meta, name -> new StringBuilder());
            samples.append(meta.name())
                .append("{member=\"")
                .append(keyBuffer.getInt(PhixeronCounters.KEY_MEMBER_ID_OFFSET))
                .append('"');
            if (typeId >= PhixeronCounters.APP_TYPE_ID_MIN && typeId <= PhixeronCounters.APP_TYPE_ID_MAX) {
                samples.append(",client=\"").append(keyBuffer.getInt(PhixeronCounters.KEY_CLIENT_ID_OFFSET)).append('"');
            }
            samples.append("} ").append(reader.getCounterValue(counterId)).append('\n');
        });

        final StringBuilder body = new StringBuilder();
        for (final Map.Entry<MetricMeta, StringBuilder> entry : samplesByMetric.entrySet()) {
            final MetricMeta meta = entry.getKey();
            body.append("# HELP ").append(meta.name()).append(' ').append(meta.help()).append('\n');
            body.append("# TYPE ").append(meta.name()).append(' ').append(meta.type()).append('\n');
            body.append(entry.getValue());
        }
        return body.toString();
    }
}
