package org.limitless.phixeron.tools;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.phixeron.metrics.PhixeronCounters;
import org.limitless.phixeron.replayer.client.SequencedFrameDecoder;
import org.limitless.phixeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.phixeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.phixeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.phixeron.sbe.frame.ClusterStoppedEncoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveEncoder;
import org.limitless.phixeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.phixeron.sequencer.CoreFrame;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.phixeron.sequencer.SequencerServer;
import org.limitless.phixeron.sequencer.SequencerService;

/**
 * clusterctl — the operator cluster life-cycle tool (see clusterctl.md). Node-local: run co-located
 * on a {@code SequencerServer} host, sharing that node's Aeron directory (to reach the co-located tap
 * over {@code aeron:ipc}) and {@code clusterDir} (for {@link ClusterTool}).
 *
 * <p>Named to mirror the {@code clusterctl.sh} launcher and to avoid shadowing Aeron's own
 * {@code io.aeron.cluster.ClusterControl} toggle class that {@link #shutdown()} drives via
 * {@link ClusterTool}.
 *
 * <p>Commands:
 * <ul>
 *   <li><b>start</b> — publishes an unsequenced {@code ClusterStarted} marker (with a correlationId)
 *       to cluster ingress and waits for its own sequenced echo on the tap; exits non-zero if the
 *       cluster has no elected leader (the ingress connect times out) or the echo never arrives. It
 *       records that the system is up — it does not start any process.</li>
 *   <li><b>shutdown</b> — safe to fire on every node. On a follower it is a no-op (leader gate via
 *       {@link ClusterTool#isLeader}); on the leader it publishes a {@code ClusterStopped} marker,
 *       waits (best-effort) for its sequenced echo, then requests {@link ClusterTool#abort} — a
 *       consensus-coordinated, snapshot-free termination of every node. Because {@code SequencerServer}
 *       wires its termination hook, that abort unwinds each node cleanly through try-with-resources,
 *       closing the Archive and draining the tap recording (including the just-observed
 *       {@code ClusterStopped}) to disk — so the log stays replayable/analysable afterwards. Best
 *       effort: if the echo does not arrive (an unhealthy cluster — often why one stops early), it
 *       aborts anyway, still via {@code ABORT} rather than SIGKILL, so the log is preserved.</li>
 *   <li><b>activate &lt;gatewayId&gt;</b> — manual standby promotion: publishes an unsequenced
 *       {@code GatewayActive(gatewayId)} to cluster ingress and waits for its sequenced echo on the
 *       tap. No special-casing needed on the sequencer side — {@code GatewayActive} passes through
 *       {@code Sequencer.sequenceMessage} like any other message, the same path the sequencer's own
 *       bootstrap/promotion activations take. Every gateway instance reacts identically regardless of
 *       which of the two publishes it: the instance whose {@code gatewayId}/{@code gatewaySourceId}
 *       matches opens its accept gate, the others stay standby.</li>
 *   <li><b>load-topology &lt;file&gt;</b> — publishes the gateway roster: one unsequenced
 *       {@code GatewayRegistered} per row of a {@code name,gatewayId,gatewaySourceId,preferenceRank}
 *       CSV file, {@code remaining} counting down to 0 on the last, then waits for that last row's
 *       sequenced echo. The roster is a deployment assertion, the same kind of act as {@code
 *       activate}, which is why it lives here rather than riding along in the reference-data load:
 *       it changes when you deploy, where the comp-id table and the calendar change daily (see
 *       doc/future-arch.md §3.6). The {@code remaining == 0} row is the sequencer's completeness
 *       edge — it synthesizes one bootstrap {@code GatewayActive} per logical gateway behind it —
 *       so this tool, which counted the rows it read, is what authors that edge. Re-running is safe:
 *       the sequencer de-dups rows on {@code gatewayId} and latches the bootstrap once.
 *       <b>Run it before the reference-data load</b>: a session row whose {@code ownerSourceId} no
 *       roster row claims is dropped by every gateway on ingest, so a load that beats the roster in
 *       leaves the gateways with no sessions (fail closed, but a dead cluster).</li>
 *   <li><b>counters</b> — lists this node's phixeron operator counters ({@link
 *       org.limitless.phixeron.metrics.PhixeronCounters}), read directly off the co-located Aeron
 *       directory's CnC file. No cluster connection, so it works with no elected leader and is
 *       safe on every node.</li>
 *   <li><b>help</b> — usage.</li>
 *   <li><i>anything else</i> — passed through to {@link ClusterTool} against this node's
 *       {@code clusterDir} (describe, errors, list-members, recording-log, …).</li>
 * </ul>
 *
 * <p>Configuration mirrors {@code SequencerServer}'s defaults so co-location with the default
 * single-node cluster works with no arguments (see {@link #usage()} / {@code clusterctl.sh}).
 */
public final class ClusterCtl {
    // ── Configuration (mirrors SequencerServer's property defaults for co-location) ──
    private static final int MEMBER_ID = Integer.getInteger("clusterctl.memberId", 0);
    private static final String BASE_DIR =
        System.getProperty("clusterctl.baseDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq");
    private static final String AERON_DIR = System.getProperty(
        "clusterctl.aeronDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + MEMBER_ID);
    private static final File CLUSTER_DIR = new File(BASE_DIR + "/cluster-" + MEMBER_ID);
    private static final String INGRESS_ENDPOINTS =
        System.getProperty("clusterctl.ingressEndpoints", "0=" + SequencerServer.ingressEndpoint(0));

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long OFFER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** header.sourceId/connectionId for markers this tool submits: no gateway process/TCP connection. */
    private static final int NO_ID = -1;

    /** Roster-row field widths, from sbe-unsequenced.xml's gatewayName type and preferenceRank uint8. */
    private static final int GATEWAY_NAME_LENGTH = 32;
    private static final int MAX_PREFERENCE_RANK = 255;

    private static final IdleStrategy IDLE = new YieldingIdleStrategy();
    private static final EgressListener NULL_EGRESS = (sessionId, timestamp, buffer, offset, length, header) -> { };

    private ClusterCtl() {
    }

    public static void main(final String[] args) {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        switch (args[0]) {
        case "help":
        case "-h":
        case "--help":
            usage();
            break;
        case "snapshot":
            System.out.println("[clusterctl] snapshot: command is not supported.");
            break;
        case "start":
            System.exit(start());
            break;
        case "shutdown":
            System.exit(shutdown());
            break;
        case "activate":
            System.exit(activate(args));
            break;
        case "load-topology":
            System.exit(loadTopology(args));
            break;
        case "counters":
            System.exit(counters());
            break;
        default:
            passthrough(args); // io.aeron.cluster.ClusterTool
            break;
        }
    }

    private static int start() {
        final long correlationId = System.nanoTime();
        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo =
                publishMarkerAndAwaitEcho(cluster, ClusterStartedEncoder.TEMPLATE_ID, correlationId);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] start: no sequenced ClusterStarted echo within timeout");
                return 1;
            }
            System.out.printf("[clusterctl] start: system-started recorded at globalSeqNo=%d%n", globalSeqNo);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] start: no elected leader / cluster unreachable (" + ex.getMessage() + ")");
            return 1;
        }
    }

    private static int shutdown() {
        if (ClusterTool.isLeader(System.out, CLUSTER_DIR) != 0) {
            System.out.println("[clusterctl] shutdown: this node is not the leader — nothing to do");
            return 0;
        }

        final long correlationId = System.nanoTime();
        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo =
                publishMarkerAndAwaitEcho(cluster, ClusterStoppedEncoder.TEMPLATE_ID, correlationId);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] shutdown: no ClusterStopped echo within timeout — aborting anyway");
            } else {
                System.out.printf("[clusterctl] shutdown: system-stopped recorded at globalSeqNo=%d%n", globalSeqNo);
            }
        } catch (final Exception ex) {
            System.err.println("[clusterctl] shutdown: could not publish ClusterStopped (" + ex.getMessage() +
                               ") — aborting anyway");
        }

        if (!ClusterTool.abort(CLUSTER_DIR, System.out)) {
            System.err.println("[clusterctl] shutdown: ClusterTool.abort failed");
            return 1;
        }
        System.out.println("[clusterctl] shutdown: cluster abort requested");
        return 0;
    }

    /**
     * Manual standby promotion: publishes {@code GatewayActive(gatewayId)} to cluster ingress and
     * waits for its sequenced echo, mirroring {@link #start()}'s connect/publish/await-echo shape.
     * No leader gate — routing to the leader is cluster ingress's job, same as {@code start}.
     */
    private static int activate(final String[] args) {
        if (args.length < 2) {
            System.err.println("[clusterctl] activate: missing <gatewayId>");
            return 2;
        }
        final int gatewayId;
        try {
            gatewayId = Integer.parseInt(args[1]);
        } catch (final NumberFormatException ex) {
            System.err.println("[clusterctl] activate: <gatewayId> must be an integer, got '" + args[1] + "'");
            return 2;
        }

        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo = publishGatewayActiveAndAwaitEcho(cluster, gatewayId);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] activate: no sequenced GatewayActive echo within timeout");
                return 1;
            }
            System.out.printf("[clusterctl] activate: GatewayActive(gatewayId=%d) recorded at globalSeqNo=%d%n",
                              gatewayId, globalSeqNo);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] activate: no elected leader / cluster unreachable (" + ex.getMessage() +
                               ")");
            return 1;
        }
    }

    /** One roster row as read from the topology file. */
    private record TopologyRow(String gatewayName, int gatewayId, int gatewaySourceId, int preferenceRank) { }

    /**
     * Publishes the gateway roster read from {@code args[1]} — one {@code GatewayRegistered} per row,
     * {@code remaining} counting down to 0 — and waits for the last row's sequenced echo.
     *
     * <p>Validated before a byte is published, because the checks are what the log cannot make for
     * itself: the sequencer de-dups on {@code gatewayId} and elects the rank-0 row of each {@code
     * gatewaySourceId}, so a duplicate id silently drops an instance and a missing (or second) rank-0
     * leaves a logical gateway with no primary (or an arbitrary one). These used to be {@code
     * static_assert}s over the hardcoded table in {@code BasicDataConstants.hpp}; a file read at load
     * time is where they belong now.
     */
    private static int loadTopology(final String[] args) {
        if (args.length < 2) {
            System.err.println("[clusterctl] load-topology: missing <file>");
            return 2;
        }
        final List<TopologyRow> rows;
        try {
            rows = readTopology(new File(args[1]));
            validateTopology(rows);
        } catch (final IOException | IllegalArgumentException ex) {
            System.err.println("[clusterctl] load-topology: " + args[1] + ": " + ex.getMessage());
            return 2;
        }

        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo = publishRosterAndAwaitEcho(cluster, rows);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] load-topology: no sequenced GatewayRegistered echo within timeout");
                return 1;
            }
            System.out.printf("[clusterctl] load-topology: %d gateway row(s) recorded, roster complete at "
                              + "globalSeqNo=%d%n", rows.size(), globalSeqNo);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] load-topology: no elected leader / cluster unreachable (" +
                               ex.getMessage() + ")");
            return 1;
        }
    }

    /** Reads {@code name,gatewayId,gatewaySourceId,preferenceRank} rows; blank lines and {@code #} skipped. */
    private static List<TopologyRow> readTopology(final File file) throws IOException {
        final List<TopologyRow> rows = new ArrayList<>();
        int lineNo = 0;
        for (final String raw : Files.readAllLines(file.toPath())) {
            lineNo++;
            final String line = raw.trim();
            if (line.isEmpty() || line.charAt(0) == '#') {
                continue;
            }
            final String[] fields = line.split(",");
            if (fields.length != 4) {
                throw new IllegalArgumentException(
                    "line " + lineNo + ": expected name,gatewayId,gatewaySourceId,preferenceRank, got '" + line + "'");
            }
            try {
                rows.add(new TopologyRow(fields[0].trim(), Integer.parseInt(fields[1].trim()),
                                         Integer.parseInt(fields[2].trim()), Integer.parseInt(fields[3].trim())));
            } catch (final NumberFormatException ex) {
                throw new IllegalArgumentException("line " + lineNo + ": " + ex.getMessage());
            }
        }
        return rows;
    }

    private static void validateTopology(final List<TopologyRow> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("no rows — an empty roster elects nobody");
        }
        for (int i = 0; i < rows.size(); i++) {
            final TopologyRow row = rows.get(i);
            if (row.gatewayName().isEmpty() || row.gatewayName().length() > GATEWAY_NAME_LENGTH) {
                throw new IllegalArgumentException(
                    "gatewayName '" + row.gatewayName() + "' must be 1.." + GATEWAY_NAME_LENGTH + " chars");
            }
            if (row.preferenceRank() < 0 || row.preferenceRank() > MAX_PREFERENCE_RANK) {
                throw new IllegalArgumentException(
                    row.gatewayName() + ": preferenceRank must be 0.." + MAX_PREFERENCE_RANK);
            }
            for (int j = i + 1; j < rows.size(); j++) {
                if (rows.get(j).gatewayId() == row.gatewayId()) {
                    throw new IllegalArgumentException("duplicate gatewayId " + row.gatewayId());
                }
                if (rows.get(j).gatewayName().equals(row.gatewayName())) {
                    throw new IllegalArgumentException("duplicate gatewayName '" + row.gatewayName() + "'");
                }
            }
        }
        for (final TopologyRow row : rows) {
            int primaries = 0;
            for (final TopologyRow other : rows) {
                if (other.gatewaySourceId() == row.gatewaySourceId() && other.preferenceRank() == 0) {
                    primaries++;
                }
            }
            if (primaries != 1) {
                throw new IllegalArgumentException("gatewaySourceId " + row.gatewaySourceId() + " has " + primaries +
                                                   " preferenceRank-0 row(s), needs exactly 1");
            }
        }
    }

    /**
     * Offers every roster row to cluster ingress, then reads this node's co-located tap for the sequenced
     * echo of the last one. Returns its globalSeqNo, or -1 on timeout. Matched on the last row's {@code
     * gatewayId}: that is the row the sequencer bootstraps behind, so its echo is exactly the "roster is
     * in the log" edge the caller is waiting for.
     */
    private static long publishRosterAndAwaitEcho(final AeronCluster cluster, final List<TopologyRow> rows) {
        final Subscription tap = awaitTap(cluster);
        if (tap == null) {
            return -1;
        }

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(128);
        final GatewayRegisteredEncoder encoder = new GatewayRegisteredEncoder();
        for (int i = 0; i < rows.size(); i++) {
            final TopologyRow row = rows.get(i);
            encoder.wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder());
            encoder.remaining(rows.size() - 1 - i)
                   .gatewayId(row.gatewayId())
                   .gatewaySourceId(row.gatewaySourceId())
                   .gatewayName(row.gatewayName())
                   .preferenceRank((short)row.preferenceRank());
            offer(cluster, buffer, wrapCore(buffer, payload, encoder.encodedLength()));
        }

        final RosterEchoHandler handler = new RosterEchoHandler(rows.get(rows.size() - 1).gatewayId());
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            cluster.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    /** Matches the sequenced echo of the roster's last row by gatewayId. */
    private static final class RosterEchoHandler implements FragmentHandler {
        private final int gatewayId;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final GatewayRegisteredDecoder decoder = new GatewayRegisteredDecoder();
        private boolean found;
        private long globalSeqNo;

        RosterEchoHandler(final int gatewayId) {
            this.gatewayId = gatewayId;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found) {
                return;
            }
            if (!isCore(view, buffer, offset, length, GatewayRegisteredDecoder.TEMPLATE_ID)) {
                return;
            }
            decoder.wrap(buffer, view.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH, view.blockLength(),
                         view.version());
            if (decoder.gatewayId() == gatewayId && decoder.remaining() == 0) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    /**
     * Publishes an unsequenced {@code GatewayActive(gatewayId)} marker, then reads this node's
     * co-located tap for the matching sequenced echo. Returns the assigned globalSeqNo, or -1 on
     * timeout (tap unavailable, or no echo within {@link #ECHO_TIMEOUT_NS}). Structured like {@link
     * #publishMarkerAndAwaitEcho} but kept separate: {@code GatewayActive} has no correlationId to
     * match on (it carries only {@code gatewayId}), so it matches the echo by {@code gatewayId} instead.
     */
    private static long publishGatewayActiveAndAwaitEcho(final AeronCluster cluster, final int gatewayId) {
        final Subscription tap = awaitTap(cluster);
        if (tap == null) {
            return -1;
        }

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final GatewayActiveEncoder encoder = new GatewayActiveEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder());
        encoder.gatewayId(gatewayId);
        offer(cluster, buffer, wrapCore(buffer, payload, encoder.encodedLength()));

        final GatewayActiveEchoHandler handler = new GatewayActiveEchoHandler(gatewayId);
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            cluster.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    /** Matches the sequenced {@code GatewayActive} echo of our own marker by gatewayId. */
    private static final class GatewayActiveEchoHandler implements FragmentHandler {
        private final int gatewayId;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final GatewayActiveDecoder decoder = new GatewayActiveDecoder();
        private boolean found;
        private long globalSeqNo;

        GatewayActiveEchoHandler(final int gatewayId) {
            this.gatewayId = gatewayId;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found) {
                return;
            }
            if (!isCore(view, buffer, offset, length, GatewayActiveDecoder.TEMPLATE_ID)) {
                return;
            }
            decoder.wrap(buffer, view.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH, view.blockLength(),
                         view.version());
            if (decoder.gatewayId() == gatewayId) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    /**
     * Lists this node's phixeron operator counters (see {@link PhixeronCounters}) — the
     * {@code SequencerService}/{@code ReplayerService} gauges and event counts, plus whatever the
     * co-located C++ replicas publish — read directly off the co-located Aeron directory's CnC file. No cluster
     * connection needed, so this works whether or not this node holds an elected leader, and is safe to run on every
     * node.
     */
    private static int counters() {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR))) {
            final CountersReader reader = aeron.countersReader();
            final boolean[] found = { false };
            reader.forEach((counterId, typeId, keyBuffer, label) -> {
                if (typeId < PhixeronCounters.MIN_TYPE_ID || typeId > PhixeronCounters.MAX_TYPE_ID) {
                    return;
                }
                found[0] = true;
                System.out.printf("%-55s = %d%n", label, reader.getCounterValue(counterId));
            });
            if (!found[0]) {
                System.out.println("[clusterctl] counters: none found under " + AERON_DIR +
                                   " — is a SequencerServer/ReplayerServer running there?");
            }
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] counters: could not connect to " + AERON_DIR + " (" + ex.getMessage() +
                               ")");
            return 1;
        }
    }

    private static void passthrough(final String[] args) {
        // ClusterTool.main expects args[0] = clusterDir, args[1..] = command + its arguments.
        final String[] toolArgs = new String[args.length + 1];
        toolArgs[0] = CLUSTER_DIR.getAbsolutePath();
        System.arraycopy(args, 0, toolArgs, 1, args.length);
        ClusterTool.main(toolArgs);
    }

    private static AeronCluster connectCluster() {
        return AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(AERON_DIR)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(INGRESS_ENDPOINTS)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(NULL_EGRESS)
            .messageTimeoutNs(CONNECT_TIMEOUT_NS));
    }

    /**
     * Publishes the unsequenced marker for {@code templateId} with {@code correlationId} to cluster
     * ingress, then reads this node's co-located tap for the matching sequenced echo. Returns the
     * assigned globalSeqNo, or -1 on timeout (tap unavailable, or no echo within {@link #ECHO_TIMEOUT_NS}).
     */
    private static long publishMarkerAndAwaitEcho(final AeronCluster cluster, final int templateId,
                                                  final long correlationId) {
        final Subscription tap = awaitTap(cluster);
        if (tap == null) {
            return -1;
        }

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final int length = encodeMarker(buffer, templateId, correlationId);
        offer(cluster, buffer, length);

        final EchoHandler handler = new EchoHandler(templateId, correlationId);
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            cluster.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    private static int encodeMarker(final ExpandableArrayBuffer buffer, final int templateId,
                                    final long correlationId) {
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        if (templateId == ClusterStartedEncoder.TEMPLATE_ID) {
            final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
            encoder.wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder());
            encoder.correlationId(correlationId);
            return wrapCore(buffer, payload, encoder.encodedLength());
        }
        final ClusterStoppedEncoder encoder = new ClusterStoppedEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder());
        encoder.correlationId(correlationId);
        return wrapCore(buffer, payload, encoder.encodedLength());
    }

    /**
     * Whether the fragment is a core frame carrying {@code templateId}. Every echo handler asks this: a
     * template id names nothing without the protocol it belongs to, and the tap carries other protocols.
     */
    private static boolean isCore(final SequencedFrameDecoder view, final DirectBuffer buffer, final int offset,
                                  final int length, final int templateId) {
        return view.wrap(buffer, offset, length) && view.payloadId() == SequencedFrameDecoder.CORE_PAYLOAD_ID &&
               view.templateId() == templateId;
    }

    /**
     * Wraps a core payload in an {@code Unsequenced} frame. clusterctl is not a gateway, so it publishes
     * under no {@code sourceId} and no connection — {@code NO_ID} for both, which is also what the
     * operator markers have always carried.
     */
    private static int wrapCore(final ExpandableArrayBuffer frame, final ExpandableArrayBuffer payload,
                                final int encodedLength) {
        return CoreFrame.wrap(frame, NO_ID, NO_ID, NO_ID, payload,
                              MessageHeaderEncoder.ENCODED_LENGTH + encodedLength);
    }

    /**
     * Subscribes to this node's co-located tap and waits for it to connect, which is where every
     * await-my-own-echo path starts. Returns null (having said why) if it never does.
     */
    private static Subscription awaitTap(final AeronCluster cluster) {
        final Subscription tap = cluster.context().aeron().addSubscription(SequencerService.FEEDER_CHANNEL,
                                                                          SequencerService.FEEDER_STREAM_ID);
        final long connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= connectDeadline) {
                System.err.printf("[clusterctl] tap (aeron:ipc/%d) not available — co-located with a SequencerServer?%n",
                                  SequencerService.FEEDER_STREAM_ID);
                return null;
            }
            cluster.pollEgress();
            IDLE.idle();
        }
        return tap;
    }

    private static void offer(final AeronCluster cluster, final DirectBuffer buffer, final int length) {
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        long result;
        while ((result = cluster.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("cluster ingress offer failed: " + result);
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("cluster ingress offer timed out (back-pressure / not connected)");
            }
            cluster.pollEgress();
            IDLE.idle();
        }
    }

    /**
     * Matches the sequenced echo of our own marker by schema/template id and correlationId. Decodes with
     * {@link ClusterStartedDecoder} for either marker — ClusterStarted/ClusterStopped are byte-identical
     * past the header, so correlationId and header.globalSeqNo are at the same offsets for both.
     */
    private static final class EchoHandler implements FragmentHandler {
        private final int templateId;
        private final long correlationId;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final ClusterStartedDecoder marker = new ClusterStartedDecoder();
        private boolean found;
        private long globalSeqNo;

        EchoHandler(final int templateId, final long correlationId) {
            this.templateId = templateId;
            this.correlationId = correlationId;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found) {
                return;
            }
            if (!isCore(view, buffer, offset, length, templateId)) {
                return;
            }
            marker.wrap(buffer, view.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH, view.blockLength(),
                        view.version());
            if (marker.correlationId() == correlationId) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    private static void usage() {
        System.out.println("""
            clusterctl — cluster life-cycle tool (node-local; run co-located with a SequencerServer)

            Usage: clusterctl.sh <command> [args]

              start        record a "system started" marker (requires an elected leader)
              shutdown     orderly stop; safe to run on every node, no-op on followers
              activate <gatewayId>
                           manual standby promotion; publishes GatewayActive(gatewayId) and
                           waits for its sequenced echo (requires an elected leader)
              load-topology <file>
                           publish the gateway roster from a CSV file
                           (name,gatewayId,gatewaySourceId,preferenceRank); run once per
                           cluster lifetime, BEFORE the reference-data load
              counters     list this node's phixeron operator counters (SequencerService/
                           ReplayerService); no cluster connection needed, safe on every node
              snapshot     this operation is not supported
              help         show this help
              <other>      passed through to io.aeron.cluster.ClusterTool (describe, errors,
                           list-members, recording-log, …) against this node's cluster dir

            Config (system properties; clusterctl.sh maps the CLUSTERCTL_* env vars onto them):
              clusterctl.memberId          co-located member id             (default 0)
              clusterctl.baseDir           cluster data dir root            (default $TMPDIR/phixeron-seq)
              clusterctl.aeronDir          co-located member's Aeron dir     (default $TMPDIR/phixeron-seq-aeron-<id>)
              clusterctl.ingressEndpoints  member ingress endpoints          (default 0=localhost:9302)""");
    }
}
