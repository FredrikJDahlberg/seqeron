package org.limitless.phixeron.tools;

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

    private static Map<Integer, String> parseTargets(final String spec) {
        final Map<Integer, String> map = new TreeMap<>();
        for (final String entry : spec.split(",")) {
            final String[] parts = entry.split("=", 2);
            map.put(Integer.parseInt(parts[0].trim()), parts[1].trim());
        }
        return map;
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
        final Map<Integer, Boolean> up = new TreeMap<>();

        for (final Map.Entry<Integer, String> target : targets.entrySet()) {
            final String scraped = scrape(target.getValue());
            up.put(target.getKey(), scraped != null);
            if (scraped != null) {
                parseInto(scraped, helpByName, typeByName, samplesByName);
            }
        }

        final StringBuilder body = new StringBuilder();
        body.append("# HELP ").append(NODE_UP_NAME)
            .append(" 1 if the aggregator's last scrape of this node's exporter succeeded, else 0.\n");
        body.append("# TYPE ").append(NODE_UP_NAME).append(" gauge\n");
        for (final Map.Entry<Integer, Boolean> entry : up.entrySet()) {
            body.append(NODE_UP_NAME).append("{member=\"").append(entry.getKey()).append("\"} ")
                .append(entry.getValue() ? 1 : 0).append('\n');
        }

        for (final String name : helpByName.keySet()) {
            body.append("# HELP ").append(name).append(' ').append(helpByName.get(name)).append('\n');
            body.append("# TYPE ").append(name).append(' ').append(typeByName.get(name)).append('\n');
            body.append(samplesByName.get(name));
        }
        return body.toString();
    }

    /** Returns the scraped body, or {@code null} on any failure (connect/timeout/non-200) — never throws. */
    private String scrape(final String hostPort) {
        try {
            final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + hostPort + "/metrics"))
                .timeout(SCRAPE_TIMEOUT)
                .GET()
                .build();
            final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : null;
        } catch (final IOException ex) {
            return null;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static void parseInto(final String scraped, final Map<String, String> helpByName,
                                   final Map<String, String> typeByName, final Map<String, StringBuilder> samplesByName) {
        for (final String line : scraped.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("# HELP ")) {
                final String rest = line.substring("# HELP ".length());
                final int sp = rest.indexOf(' ');
                helpByName.putIfAbsent(rest.substring(0, sp), rest.substring(sp + 1));
            } else if (line.startsWith("# TYPE ")) {
                final String rest = line.substring("# TYPE ".length());
                final int sp = rest.indexOf(' ');
                typeByName.putIfAbsent(rest.substring(0, sp), rest.substring(sp + 1));
            } else {
                final String name = line.substring(0, line.indexOf('{'));
                samplesByName.computeIfAbsent(name, k -> new StringBuilder()).append(line).append('\n');
            }
        }
    }
}
