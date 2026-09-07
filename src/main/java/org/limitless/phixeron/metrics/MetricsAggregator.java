package org.limitless.phixeron.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Central ops-server aggregator (see {@code doc/ops.md}): pulls every node's {@code /metrics}
 * (see {@link MetricsExporter})
 * over HTTP and re-exposes one combined {@code /metrics} endpoint — the "aggregating proxy" topology,
 * so Prometheus itself only ever talks to this one process rather than needing network reach to every
 * node. Purely an HTTP client/server; unlike {@link MetricsExporter} it has no Aeron dependency at all.
 *
 * <p>Also synthesizes {@code phixeron_node_up{member="N"}}: Prometheus's own built-in {@code up{}}
 * metric would only reflect reachability to this aggregator, not to each individual node, under this
 * topology — so this process records its own per-node scrape success/failure as a gauge instead.
 */
public final class MetricsAggregator {
    // ── Configuration ──
    private static final int PORT = Integer.getInteger("metricsAggregator.port", 9500);
    // memberId=host:port, comma-separated — mirrors clusterctl's ingressEndpoints "id=endpoint" format.
    private static final String TARGETS = System.getProperty("metricsAggregator.targets", "0=localhost:9400");

    private static final Duration SCRAPE_TIMEOUT = Duration.ofSeconds(3);
    private static final String NODE_UP_NAME = "phixeron_node_up";

    private final Map<Integer, String> targets;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(SCRAPE_TIMEOUT).build();

    private MetricsAggregator(final Map<Integer, String> targets) {
        this.targets = targets;
    }

    /**
     * Main entry point
     * @param args arguments
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
        final Map<Integer, String> targets = parseTargets(TARGETS);
        final MetricsAggregator aggregator = new MetricsAggregator(targets);
        final HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/metrics", aggregator::handleMetrics);
        server.setExecutor(null);
        server.start();

        System.out.printf("[MetricsAggregator] serving /metrics on port %d, scraping targets=%s%n", PORT, targets);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    }

    /**
     * Parse end-point
     *
     * <p>A malformed entry stops start-up rather than being skipped: a dropped target is a node that
     * appears nowhere in the aggregate, not even as {@code phixeron_node_up 0}, so the operator would
     * read a full house while a node was never scraped at all.
     * @param targets end-point
     * @return end-points by identity
     */
    static Map<Integer, String> parseTargets(final String targets) {
        final Map<Integer, String> map = new TreeMap<>();
        for (final String entry : targets.split(",")) {
            final int equals = entry.indexOf('=');
            if (equals < 0) {
                throw new IllegalArgumentException(malformedTarget(targets, entry));
            }
            try {
                map.put(Integer.parseInt(entry.substring(0, equals).trim()), entry.substring(equals + 1).trim());
            } catch (final NumberFormatException ex) {
                throw new IllegalArgumentException(malformedTarget(targets, entry), ex);
            }
        }
        return map;
    }

    private static String malformedTarget(final String targets, final String entry) {
        return "-DmetricsAggregator.targets=\"" + targets + "\": entry \"" + entry + "\" is not memberId=host:port";
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        final byte[] body;
        try {
            body = renderMetrics().getBytes(StandardCharsets.UTF_8);
        } catch (final Exception ex) {
            final byte[] error = ("aggregation failed: " + ex.getMessage() + "\n").getBytes(StandardCharsets.UTF_8);
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
     * Scrapes every target, then re-renders as one Prometheus body: HELP/TYPE emitted once per metric
     * name (each node's exporter emits identical HELP/TYPE text, so first-seen wins), followed by every
     * node's sample lines for that metric — plus the synthesized {@code phixeron_node_up} gauge.
     */
    private String renderMetrics() {
        final Map<String, String> helpByName = new LinkedHashMap<>();
        final Map<String, String> typeByName = new LinkedHashMap<>();
        final Map<String, StringBuilder> samplesByName = new LinkedHashMap<>();
        final Map<Integer, Boolean> targetByResponse = new TreeMap<>();
        for (final Map.Entry<Integer, String> target : targets.entrySet()) {
            final String response = scrape(target.getValue());
            targetByResponse.put(target.getKey(), response != null);
            if (response != null) {
                parseResponse(response, helpByName, typeByName, samplesByName);
            }
        }

        final StringBuilder body = new StringBuilder();
        body.append("# HELP ")
            .append(NODE_UP_NAME)
            .append(" 1 if the aggregator's last scrape of this node's exporter succeeded, else 0.\n");
        body.append("# TYPE ").append(NODE_UP_NAME).append(" gauge\n");
        for (final Map.Entry<Integer, Boolean> entry : targetByResponse.entrySet()) {
            body.append(NODE_UP_NAME)
                .append("{member=\"")
                .append(entry.getKey())
                .append("\"} ")
                .append(entry.getValue() ? 1 : 0)
                .append('\n');
        }
        for (final String name : helpByName.keySet()) {
            body.append("# HELP ").append(name).append(' ').append(helpByName.get(name)).append('\n');
            body.append("# TYPE ").append(name).append(' ').append(typeByName.get(name)).append('\n');
            body.append(samplesByName.get(name));
        }
        return body.toString();
    }

    /**
     * Returns the scraped body, or {@code null} on any failure (connect/timeout/non-200).
     * @param hostPort host port
     * @return response or null
     */
    private String scrape(final String hostPort) {
        try {
            final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + hostPort + "/metrics"))
                                            .timeout(SCRAPE_TIMEOUT)
                                            .GET()
                                            .build();
            final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : null;
        } catch (final IOException error) {
            return null;
        } catch (final InterruptedException error) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Parse response
     * @param response string
     * @param helpByName help
     * @param typeByName type
     * @param samplesByName sample
     */
    static void parseResponse(final String response,
                              final Map<String, String> helpByName,
                              final Map<String, String> typeByName,
                              final Map<String, StringBuilder> samplesByName) {
        for (final String line : response.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            if (line.charAt(0) == '#') {
                // Every other comment form is dropped, not parsed as a sample. Read as one, a line with
                // no '{' threw out of renderMetrics, and the 500 that followed took the whole aggregate
                // with it: every healthy node's metrics, and the phixeron_node_up gauge that is the one
                // thing meant to survive a bad node.
                if (line.startsWith("# HELP ")) {
                    putHeader(helpByName, line.substring("# HELP ".length()));
                } else if (line.startsWith("# TYPE ")) {
                    putHeader(typeByName, line.substring("# TYPE ".length()));
                }
                continue;
            }
            final String name = sampleName(line);
            if (!name.isEmpty()) {
                samplesByName.computeIfAbsent(name, k -> new StringBuilder()).append(line).append('\n');
            }
        }
    }

    /** Splits a HELP/TYPE body into name and text; first node seen wins, and an empty text is legal. */
    private static void putHeader(final Map<String, String> byName, final String rest) {
        final int sp = rest.indexOf(' ');
        byName.putIfAbsent(sp < 0 ? rest : rest.substring(0, sp), sp < 0 ? "" : rest.substring(sp + 1));
    }

    /**
     * A sample line's metric name: everything before its label brace or its value. Empty when the line
     * carries neither — not a sample, so the caller drops it rather than re-emitting it into a body
     * Prometheus has to parse.
     */
    private static String sampleName(final String line) {
        int end = 0;
        while (end < line.length() && line.charAt(end) != '{' && line.charAt(end) != ' ') {
            ++end;
        }
        return end == line.length() ? "" : line.substring(0, end);
    }
}
