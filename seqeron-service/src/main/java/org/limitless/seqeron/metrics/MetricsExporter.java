package org.limitless.seqeron.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.aeron.AeronCounters;
import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * Node-local Prometheus exporter (see {@code doc/ops.md}): converts seqeron's counters, and Aeron Cluster's own, into
 * Prometheus text format
 */
public final class MetricsExporter {
    // ── Configuration (mirrors ClusterCtl's property-naming convention for co-location) ──
    private static final int MEMBER_ID = Integer.getInteger("metricsExporter.memberId", 0);
    private static final String AERON_DIR =
        System.getProperty("metricsExporter.aeronDir", PortLayout.aeronDir(MEMBER_ID));
    private static final int PORT = Integer.getInteger("metricsExporter.port", 9400 + MEMBER_ID);

    private record MetricMeta(String name, String help, String type) { }

    private static final Map<Integer, MetricMeta> METRICS_BY_TYPE_ID = Map.ofEntries(
        Map.entry(SeqeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_global_seq_no", "Last globalSeqNo emitted on this node's tap.",
                                 "gauge")),
        Map.entry(SeqeronCounters.SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_tap_backpressure_alerts_total",
                                 "Count of times the tap-emit back-pressure alert threshold has fired.",
                                 "counter")),
        Map.entry(SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_rejected_ingress_total",
                                 "Count of malformed ingress messages skipped by Sequencer.sequenceMessage.",
                                 "counter")),
        Map.entry(SeqeronCounters.SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_leadership_change_total",
                                 "Count of leadership changes this node has observed and sequenced.", "counter")),
        Map.entry(SeqeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_current_leader_member_id",
                                 "memberId of the leader last recorded by this node's Sequencer.", "gauge")),
        Map.entry(SeqeronCounters.SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_last_tick_timestamp_ms",
                                 "Consensus timestamp of the last 1Hz ClusterHeartbeat emitted.", "gauge")),
        Map.entry(
            SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID,
            new MetricMeta("seqeron_sequencer_gateway_promotion_total",
                           "Count of standby-promotion GatewayActive frames emitted on a gateway session close.",
                           "counter")),
        Map.entry(SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_gateway_promotion_failed_total",
                                 "Count of gateways left with no active instance and no standby to promote.",
                                 "counter")),
        Map.entry(SeqeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_bootstrap_activated",
                                 "1 once the bootstrap GatewayActive has been emitted in this cluster's log, else 0.",
                                 "gauge")),
        Map.entry(
            SeqeronCounters.SEQUENCER_TAP_STALLED_TYPE_ID,
            new MetricMeta("seqeron_sequencer_tap_stalled",
                           "1 while tap-emit back-pressure has been sustained past the stall threshold, else 0.",
                           "gauge")),
        Map.entry(SeqeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID,
                  new MetricMeta("seqeron_sequencer_ingress_messages_total",
                                 "Count of ingress messages successfully sequenced.", "counter")),
        Map.entry(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_replayer_archive_errors_total",
                                 "Count of local-archive calls that threw; a sustained rate means the archive is "
                                     + "not serving replays.",
                                 "counter")),
        Map.entry(
            SeqeronCounters.REPLAYER_READY_TYPE_ID,
            new MetricMeta("seqeron_replayer_ready",
                           "1 once the co-located tap recording is visible and replay requests are being served.",
                           "gauge")),
        Map.entry(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID,
                  new MetricMeta("seqeron_replayer_active_slots", "Current count of in-flight replays.", "gauge")),
        Map.entry(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID,
                  new MetricMeta("seqeron_replayer_pending_requests",
                                 "Current count of replay requests waiting for a free slot.", "gauge")),
        Map.entry(SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_replayer_replays_served_total",
                                 "Count of replays started since this node came up.", "counter")),
        Map.entry(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID,
                  new MetricMeta(
                      "seqeron_replayer_control_replies_dropped_total",
                      "Count of control replies dropped rather than spun on because an app stopped draining the "
                          + "control stream. Each costs that app one resend interval and nothing else, so a rising "
                          + "rate — not the absolute value — is what identifies a wedged replica.",
                      "counter")),
        Map.entry(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_replayer_throttled_requests_total",
                                 "Count of replay requests held because the tap recording trailed the tap while a "
                                     + "replay ran, resends included.",
                                 "counter")),
        Map.entry(SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID,
                  new MetricMeta(
                      "seqeron_app_recovery_stalled",
                      "1 while a co-located replica's recovery has dispatched nothing for 30s while not caught "
                          + "up, else 0. It is holding, which is correct — but it is not serving, and this is the "
                          + "only signal that says so.",
                      "gauge")),
        Map.entry(SeqeronCounters.APP_SNAPSHOT_ROUND_TYPE_ID,
                  new MetricMeta("seqeron_app_snapshot_round",
                                 "The last snapshot round this client confirmed. A client whose round lags the "
                                     + "others' is missing rounds.",
                                 "gauge")),
        Map.entry(SeqeronCounters.GATEWAY_CONNECTIONS_TYPE_ID,
                  new MetricMeta("seqeron_gateway_connections",
                                 "Connections this gateway instance holds open; 0 on a standby.", "gauge")));

    /**
     * Aeron Cluster's own counters, from the member's consensus module and service container. Their key holds the
     * cluster id rather than a member id, so they carry this exporter's.
     */
    private static final Map<Integer, MetricMeta> CLUSTER_METRICS_BY_TYPE_ID = Map.ofEntries(
        Map.entry(AeronCounters.CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_cluster_errors_total",
                                 "Count of errors the consensus module has raised; clusterctl errors lists them.",
                                 "counter")),
        Map.entry(AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_cluster_service_errors_total",
                                 "Count of errors the service container has raised; clusterctl errors lists them.",
                                 "counter")),
        Map.entry(AeronCounters.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID,
                  new MetricMeta("seqeron_cluster_timed_out_clients_total",
                                 "Count of client sessions the cluster closed for missing keep-alives.", "counter")),
        Map.entry(AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID,
                  new MetricMeta("seqeron_cluster_commit_position", "The log position this member knows committed.",
                                 "gauge")),
        Map.entry(AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID,
                  new MetricMeta("seqeron_cluster_node_role", "This member's role: 0 follower, 1 candidate, 2 leader.",
                                 "gauge")),
        Map.entry(
            AeronCounters.CLUSTER_ELECTION_STATE_TYPE_ID,
            new MetricMeta("seqeron_cluster_election_state",
                           "This member's election state, Aeron's ElectionState code; 17 (CLOSED) between elections.",
                           "gauge")),
        Map.entry(AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
                  new MetricMeta("seqeron_cluster_module_state",
                                 "The consensus module's state, Aeron's ConsensusModule.State code; 1 (ACTIVE) when "
                                     + "serving.",
                                 "gauge")));

    /** The co-located node's CnC counters — the only thing this reads; an Aeron client is not needed. */
    private final CountersReader reader;

    /** The node these counters are on, for the counters whose key does not say. */
    private final int memberId;

    MetricsExporter(final CountersReader reader, final int memberId) {
        this.reader = reader;
        this.memberId = memberId;
    }

    public static void main(final String[] args) throws IOException {
        final File aeronDir = new File(AERON_DIR);
        final HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/metrics", exchange -> handleMetrics(exchange, aeronDir, MEMBER_ID));
        server.setExecutor(null);
        server.start();

        System.out.printf("[MetricsExporter/%d] serving /metrics on port %d from aeronDir=%s%n", MEMBER_ID, PORT,
                          AERON_DIR);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    }

    /**
     * Renders the counters of the driver running in {@code aeronDir}, mapping its CnC file for this scrape alone.
     * Mapped afresh each time rather than held by an Aeron client, so a member's restart, which replaces the file,
     * costs the scrapes while it is down and none after.
     *
     * @param aeronDir the member's Aeron directory
     * @param memberId the member's id
     * @throws IllegalStateException if no live driver holds that directory
     */
    static String scrape(final File aeronDir, final int memberId) {
        final MappedByteBuffer cnc =
            IoUtil.mapExistingFile(new File(aeronDir, CncFileDescriptor.CNC_FILE), CncFileDescriptor.CNC_FILE);
        try {
            if (!CommonContext.isDriverActive(CommonContext.DRIVER_TIMEOUT_MS, message -> { }, cnc)) {
                throw new IllegalStateException("no live media driver in " + aeronDir);
            }
            final UnsafeBuffer metaData = CncFileDescriptor.createMetaDataBuffer(cnc);
            return new MetricsExporter(new CountersReader(CncFileDescriptor.createCountersMetaDataBuffer(cnc, metaData),
                                                          CncFileDescriptor.createCountersValuesBuffer(cnc, metaData)),
                                       memberId)
                .renderMetrics();
        } finally {
            BufferUtil.free(cnc);
        }
    }

    private static void handleMetrics(final HttpExchange exchange, final File aeronDir, final int memberId)
        throws IOException {
        final byte[] body;
        try {
            body = scrape(aeronDir, memberId).getBytes(StandardCharsets.UTF_8);
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
     * Renders every seqeron and Aeron Cluster counter this node's CnC file holds as one Prometheus exposition body. A
     * counter in the app range core has no name for is a consumer's (doc/ops.md, "Counter type ids"), named from its
     * label.
     */
    String renderMetrics() {
        final Map<MetricMeta, StringBuilder> samplesByMetric = new LinkedHashMap<>();
        reader.forEach((counterId, typeId, keyBuffer, label) -> {
            final MetricMeta clusterMeta = CLUSTER_METRICS_BY_TYPE_ID.get(typeId);
            MetricMeta meta = clusterMeta != null ? clusterMeta : METRICS_BY_TYPE_ID.get(typeId);
            if (meta == null) {
                if (typeId < SeqeronCounters.APP_TYPE_ID_MIN || typeId > SeqeronCounters.APP_TYPE_ID_MAX) {
                    return;
                }
                meta = appMetricMeta(typeId, label);
            }
            final StringBuilder samples = samplesByMetric.computeIfAbsent(meta, name -> new StringBuilder());
            samples.append(meta.name())
                .append("{member=\"")
                .append(clusterMeta != null ? memberId : keyBuffer.getInt(SeqeronCounters.KEY_MEMBER_ID_OFFSET))
                .append('"');
            if (typeId >= SeqeronCounters.APP_TYPE_ID_MIN && typeId <= SeqeronCounters.APP_TYPE_ID_MAX) {
                samples.append(",client=\"").append(keyBuffer.getInt(SeqeronCounters.KEY_CLIENT_ID_OFFSET)).append('"');
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

    /**
     * A consumer counter's metric name: its label's first token, sanitised (doc/ops.md, "Counter type ids"). The same
     * for every counter of one type id, so the replicas publishing it share one HELP/TYPE header.
     */
    private static MetricMeta appMetricMeta(final int typeId, final String label) {
        final String sanitised = label.split(" ", 2)[0].replaceAll("[^a-zA-Z0-9_:]", "_");
        final String name = sanitised.isEmpty() || Character.isDigit(sanitised.charAt(0))
            ? "seqeron_app_counter_" + typeId
            : sanitised;
        return new MetricMeta(name, "Consumer counter, typeId " + typeId + ".", "untyped");
    }
}
