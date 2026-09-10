package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterTool;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.ParserConfigurationException;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.metrics.SeqeronCounters;
import org.limitless.seqeron.replayer.client.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.ClusterStoppedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sequencer.ClusterStreamSender;
import org.limitless.seqeron.sequencer.IngressPublisher;
import org.limitless.seqeron.sequencer.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder;
import org.limitless.seqeron.sequencer.SequencerServer;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.tools.TopologyDocument.ApplicationRow;
import org.limitless.seqeron.tools.TopologyDocument.ProtocolRow;
import org.limitless.seqeron.tools.TopologyDocument.TopologyRow;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

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
 *       {@code GatewayActivationRequested(gatewayId)} to cluster ingress and waits for the
 *       {@code GatewayActive} the sequencer synthesizes behind it. The operator's act is what is
 *       recorded and the designation stays the cluster's, through the same path bootstrap and both
 *       promotions take — which is also what gets the manual path the list validation it would
 *       otherwise lack, since a {@code gatewayId} no list row names is rejected on ingress. Every
 *       gateway instance reacts to the resulting {@code GatewayActive} identically however it was
 *       triggered: the instance whose {@code gatewayId} matches opens its accept gate, the others stay
 *       standby.</li>
 *   <li><b>load-topology &lt;file&gt;</b> — publishes the deployment's topology document
 *       (doc/seqeron-protocol-spec.md §6.4; XML, validated against the packaged {@code topology.xsd}).
 *       Its {@code <gateways>} section becomes one unsequenced {@code GatewayRegistered} per row,
 *       {@code remaining} counting down to 0 on the last; its optional {@code <applications>} and
 *       {@code <protocols>} sections become one {@code ApplicationRegistered} and one {@code
 *       PayloadIdRegistered} per row behind them, carrying no countdown of their own — labelling for
 *       {@code SbeLogPrinter} and nothing more, since the sequencer never decodes them and
 *       registration gates no frame (§6.3, <b>C-2</b>). Between them the three sections put the whole
 *       deployment in the log: the producers that are elected, the producers that are not (§5), and
 *       what the shared payloadIds are called. Then waits for the last list row's
 *       sequenced echo. The sections are one deployment assertion, the same kind of act as {@code
 *       activate}, which is why they live here rather than riding along in the reference-data load:
 *       it changes when you deploy, where the comp-id table and the calendar change daily.
 *       The {@code remaining == 0} row is the sequencer's completeness
 *       edge — it synthesizes one bootstrap {@code GatewayActive} per logical gateway behind it —
 *       so this tool, which counted the rows it read, is what authors that edge. Re-running is safe:
 *       the sequencer de-dups rows on {@code gatewayId} and latches the bootstrap once.
 *       <b>Run it before the reference-data load</b>: a session row whose {@code ownerSourceId} no
 *       list row claims is dropped by every gateway on ingest, so a load that beats the list in
 *       leaves the gateways with no sessions (fail closed, but a dead cluster).</li>
 *   <li><b>counters</b> — lists this node's seqeron operator counters ({@link
 *       org.limitless.seqeron.metrics.SeqeronCounters}), read directly off the co-located Aeron
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
        System.getProperty("clusterctl.baseDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq");
    private static final String AERON_DIR = System.getProperty(
        "clusterctl.aeronDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + MEMBER_ID);
    private static final File CLUSTER_DIR = new File(BASE_DIR + "/cluster-" + MEMBER_ID);
    private static final String INGRESS_ENDPOINTS =
        System.getProperty("clusterctl.ingressEndpoints", "0=" + SequencerServer.ingressEndpoint(0));

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** Ingress is tried over this node's own aeron:ipc first; a follower answers on neither, so keep it short. */
    private static final long IPC_CONNECT_TIMEOUT_MS = 500;

    /** Ephemeral: this tool runs for one command and needs no port of its own (doc/registries.md §2). */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /** header.connectionId/sessionId for markers this tool submits: no gateway process/TCP connection. */
    private static final int NO_ID = -1;

    /** The topology document's namespace, fixed by topology.xsd. */
    private static final String TOPOLOGY_NS = "http://limitless.org/seqeron/topology/1";

    /**
     * §5's other reserved sourceId — clusterctl's own, stamped on every marker it submits; -1 the XSD
     * refuses on its own. It cannot be {@link #NO_ID}: that value is the cluster's (<b>F-4</b>) and the
     * sequencer refuses it on ingress (§9.2, condition 6).
     */
    private static final int RESERVED_SOURCE_ID = 2;

    private static final IdleStrategy IDLE = new YieldingIdleStrategy();

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
        try (Session session = new Session()) {
            final long globalSeqNo =
                publishMarkerAndAwaitEcho(session, SystemFrame.CLUSTER_STARTED, correlationId);
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
        try (Session session = new Session()) {
            final long globalSeqNo =
                publishMarkerAndAwaitEcho(session, SystemFrame.CLUSTER_STOPPED, correlationId);
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
     * Manual standby promotion: publishes {@code GatewayActivationRequested(gatewayId)} to cluster
     * ingress and waits for the {@code GatewayActive} synthesized behind it, mirroring {@link #start()}'s
     * connect/publish/await-echo shape.
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

        try (Session session = new Session()) {
            final long globalSeqNo = publishGatewayActiveAndAwaitEcho(session, gatewayId);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] activate: no synthesized GatewayActive within timeout — is "
                               + "<gatewayId> a list row?");
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

    /**
     * Publishes the topology document read from {@code args[1]}: one {@code GatewayRegistered} per
     * list row, {@code remaining} counting down to 0, then one {@code PayloadIdRegistered} per
     * protocol row, then waits for the last list row's sequenced echo.
     *
     * <p>The whole document is validated before a byte is published — a file that fails half-way
     * leaves a list the log has already closed. All of that is {@link TopologyDocument}'s.
     */
    private static int loadTopology(final String[] args) {
        if (args.length < 2) {
            System.err.println("[clusterctl] load-topology: missing <file>");
            return 2;
        }
        final TopologyDocument topology;
        try {
            topology = TopologyDocument.read(new File(args[1]));
        } catch (final SAXParseException ex) {
            System.err.println("[clusterctl] load-topology: " + args[1] + ":" + ex.getLineNumber() + ":" +
                               ex.getColumnNumber() + ": " + ex.getMessage());
            return 2;
        } catch (final IOException | SAXException | ParserConfigurationException |
                       IllegalArgumentException ex) {
            System.err.println("[clusterctl] load-topology: " + args[1] + ": " + ex.getMessage());
            return 2;
        }

        try (Session session = new Session()) {
            final long globalSeqNo = publishTopologyAndAwaitEcho(session, topology);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] load-topology: no sequenced GatewayRegistered echo within timeout");
                return 1;
            }
            System.out.printf("[clusterctl] load-topology: %d gateway row(s) recorded, list complete at "
                              + "globalSeqNo=%d; %d application row(s) and %d protocol row(s) registered%n",
                              topology.gateways().size(), globalSeqNo, topology.applications().size(),
                              topology.protocols().size());
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] load-topology: no elected leader / cluster unreachable (" +
                               ex.getMessage() + ")");
            return 1;
        }
    }

    /**
     * Offers every list row to cluster ingress and every protocol row behind them, then reads this
     * node's co-located tap for the sequenced echo of the last list row. Returns its globalSeqNo,
     * or -1 on timeout. Matched on the last row's {@code gatewayId}: that is the row the sequencer
     * bootstraps behind, so its echo is exactly the "list is in the log" edge the caller waits for.
     *
     * <p>The application and protocol rows are offered before the wait rather than after it, so they are
     * ordered behind the list on the one session — nothing may fall <em>between</em> the list rows, and
     * neither carries a countdown of its own (§6.4). Neither is waited on: only the gateway list has a
     * completeness edge, because only the gateway list is something the sequencer acts on.
     */
    private static long publishTopologyAndAwaitEcho(final Session session, final TopologyDocument topology) {
        final Subscription tap = awaitTap(session);
        if (tap == null) {
            return -1;
        }

        final List<TopologyRow> rows = topology.gateways();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(128);
        final GatewayRegisteredEncoder encoder = new GatewayRegisteredEncoder();
        for (int i = 0; i < rows.size(); i++) {
            final TopologyRow row = rows.get(i);
            encoder.wrap(payload, 0);
            encoder.remaining(rows.size() - 1 - i)
                   .gatewayId(row.gatewayId())
                   .gatewaySourceId(row.gatewaySourceId())
                   .gatewayName(row.gatewayName())
                   .preferenceRank((short)row.preferenceRank());
            publish(session, SystemFrame.GATEWAY_REGISTERED, payload, encoder.encodedLength());
        }

        final ApplicationRegisteredEncoder applicationEncoder = new ApplicationRegisteredEncoder();
        for (final ApplicationRow row : topology.applications()) {
            applicationEncoder.wrap(payload, 0);
            applicationEncoder.applicationSourceId(row.sourceId())
                              .applicationName(row.applicationName());
            publish(session, SystemFrame.APPLICATION_REGISTERED, payload, applicationEncoder.encodedLength());
        }

        final PayloadIdRegisteredEncoder protocolEncoder = new PayloadIdRegisteredEncoder();
        for (final ProtocolRow row : topology.protocols()) {
            protocolEncoder.wrap(payload, 0);
            protocolEncoder.payloadId(row.payloadId())
                           .protocolVersion(row.protocolVersion())
                           .protocolName(row.protocolName());
            publish(session, SystemFrame.PAYLOAD_ID_REGISTERED, payload, protocolEncoder.encodedLength());
        }

        final ListEchoHandler handler = new ListEchoHandler(rows.get(rows.size() - 1).gatewayId());
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            session.sender.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    /** Matches the sequenced echo of the list's last row by gatewayId. */
    private static final class ListEchoHandler implements FragmentHandler {
        private final int gatewayId;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final GatewayRegisteredDecoder decoder = new GatewayRegisteredDecoder();
        private boolean found;
        private long globalSeqNo;

        ListEchoHandler(final int gatewayId) {
            this.gatewayId = gatewayId;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found) {
                return;
            }
            if (!isSystem(view, buffer, offset, length, SystemFrame.GATEWAY_REGISTERED)) {
                return;
            }
            decoder.wrap(buffer, view.payloadOffset(), GatewayRegisteredDecoder.BLOCK_LENGTH,
                         MessageHeaderDecoder.SCHEMA_VERSION);
            if (decoder.gatewayId() == gatewayId && decoder.remaining() == 0) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    /**
     * Publishes an unsequenced {@code GatewayActivationRequested(gatewayId)}, then reads this node's
     * co-located tap for the {@code GatewayActive} the sequencer synthesizes behind it. Returns that
     * frame's globalSeqNo, or -1 on timeout (tap unavailable, no list row names the instance, or no
     * echo within {@link #ECHO_TIMEOUT_NS}). Structured like {@link #publishMarkerAndAwaitEcho} but kept
     * separate: neither message has a correlationId to match on, so it matches by {@code gatewayId}.
     */
    private static long publishGatewayActiveAndAwaitEcho(final Session session, final int gatewayId) {
        final Subscription tap = awaitTap(session);
        if (tap == null) {
            return -1;
        }

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final GatewayActivationRequestedEncoder encoder = new GatewayActivationRequestedEncoder();
        encoder.wrap(payload, 0);
        encoder.gatewayId(gatewayId);
        publish(session, SystemFrame.GATEWAY_ACTIVATION_REQUESTED, payload, encoder.encodedLength());

        final GatewayActiveEchoHandler handler = new GatewayActiveEchoHandler(gatewayId);
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            session.sender.pollEgress();
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
            if (!isSystem(view, buffer, offset, length, SystemFrame.GATEWAY_ACTIVE)) {
                return;
            }
            // Synthesized, so its gatewayId is inline in the frame's own block rather than in a body.
            decoder.wrap(buffer, view.payloadOffset(), view.blockLength(), view.version());
            if (decoder.gatewayId() == gatewayId) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    /**
     * Lists this node's seqeron operator counters (see {@link SeqeronCounters}) — the
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
                if (typeId < SeqeronCounters.MIN_TYPE_ID || typeId > SeqeronCounters.MAX_TYPE_ID) {
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

    /**
     * This tool's cluster session, and the media driver it borrows to reach both the cluster and the tap.
     * Co-located by construction: {@code clusterctl} runs on a node, against that node's {@code clusterDir}.
     */
    private static final class Session implements AutoCloseable {
        private final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
        private final ClusterStreamSender sender = new ClusterStreamSender();
        private final IngressPublisher publisher = new IngressPublisher();

        private Session() {
            sender.setIngressEndpoints(INGRESS_ENDPOINTS);
            try {
                sender.connectColocated(aeron, MEMBER_ID, IPC_CONNECT_TIMEOUT_MS, EGRESS_CHANNEL);
            } catch (final RuntimeException ex) {
                aeron.close();
                throw ex;
            }
        }

        @Override
        public void close() {
            sender.close();
            aeron.close();
        }
    }

    /**
     * Publishes one system event on this session, or fails the command. {@code Declined} is the sender
     * having spun through back-pressure and an election and found no session left at the end of it — a
     * marker half-published is not something a caller here can carry on from.
     */
    private static void publish(final Session session, final int systemEventType,
                                final ExpandableArrayBuffer body, final int bodyLength) {
        final IngressPublisher.Publish outcome =
            session.publisher.publishSystem(session.sender, RESERVED_SOURCE_ID, NO_ID, systemEventType, body,
                                            bodyLength);
        if (outcome != IngressPublisher.Publish.Published) {
            throw new IllegalStateException("cluster ingress " + outcome + " a " + systemEventType + " marker");
        }
    }

    /**
     * Publishes the unsequenced marker for {@code systemEventType} with {@code correlationId} to cluster
     * ingress, then reads this node's co-located tap for the matching sequenced echo. Returns the
     * assigned globalSeqNo, or -1 on timeout (tap unavailable, or no echo within {@link #ECHO_TIMEOUT_NS}).
     */
    private static long publishMarkerAndAwaitEcho(final Session session, final int systemEventType,
                                                  final long correlationId) {
        final Subscription tap = awaitTap(session);
        if (tap == null) {
            return -1;
        }

        publishMarker(session, systemEventType, correlationId);

        final EchoHandler handler = new EchoHandler(systemEventType, correlationId);
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            session.sender.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    /** Encodes and publishes the ClusterStarted/ClusterStopped marker for {@code systemEventType}. */
    private static void publishMarker(final Session session, final int systemEventType, final long correlationId) {
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        if (systemEventType == SystemFrame.CLUSTER_STARTED) {
            final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
            encoder.wrap(payload, 0);
            encoder.correlationId(correlationId);
            publish(session, systemEventType, payload, encoder.encodedLength());
            return;
        }
        final ClusterStoppedEncoder encoder = new ClusterStoppedEncoder();
        encoder.wrap(payload, 0);
        encoder.correlationId(correlationId);
        publish(session, systemEventType, payload, encoder.encodedLength());
    }

    /**
     * Whether the fragment is a system frame carrying {@code systemEventType}. Every echo handler asks
     * this: the same 2-byte field is an application payloadId on the other family, and the tap carries
     * both.
     */
    private static boolean isSystem(final SequencedFrameDecoder view, final DirectBuffer buffer, final int offset,
                                    final int length, final int systemEventType) {
        return view.wrap(buffer, offset, length) && view.isSystem() && view.systemEventType() == systemEventType;
    }

    /**
     * Subscribes to this node's co-located tap and waits for it to connect, which is where every
     * await-my-own-echo path starts. Returns null (having said why) if it never does.
     */
    private static Subscription awaitTap(final Session session) {
        final Subscription tap = session.aeron.addSubscription(SequencerService.FEEDER_CHANNEL,
                                                               SequencerService.FEEDER_STREAM_ID);
        final long connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= connectDeadline) {
                System.err.printf("[clusterctl] tap (aeron:ipc/%d) not available — co-located with a SequencerServer?%n",
                                  SequencerService.FEEDER_STREAM_ID);
                return null;
            }
            session.sender.pollEgress();
            IDLE.idle();
        }
        return tap;
    }

    /**
     * Matches the sequenced echo of our own marker by schema/template id and correlationId. Decodes with
     * {@link ClusterStartedDecoder} for either marker — ClusterStarted/ClusterStopped are byte-identical
     * past the header, so correlationId and header.globalSeqNo are at the same offsets for both.
     */
    private static final class EchoHandler implements FragmentHandler {
        private final int systemEventType;
        private final long correlationId;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final ClusterStartedDecoder marker = new ClusterStartedDecoder();
        private boolean found;
        private long globalSeqNo;

        EchoHandler(final int systemEventType, final long correlationId) {
            this.systemEventType = systemEventType;
            this.correlationId = correlationId;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found) {
                return;
            }
            if (!isSystem(view, buffer, offset, length, systemEventType)) {
                return;
            }
            marker.wrap(buffer, view.payloadOffset(), ClusterStartedDecoder.BLOCK_LENGTH,
                        MessageHeaderDecoder.SCHEMA_VERSION);
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
                           publish the topology document (XML, validated against the
                           packaged topology.xsd): the gateway list, then the
                           co-located applications, then the protocol registry; run
                           once per cluster lifetime, BEFORE the reference-data load
              counters     list this node's seqeron operator counters (SequencerService/
                           ReplayerService); no cluster connection needed, safe on every node
              snapshot     this operation is not supported
              help         show this help
              <other>      passed through to io.aeron.cluster.ClusterTool (describe, errors,
                           list-members, recording-log, …) against this node's cluster dir

            Config (system properties; clusterctl.sh maps the CLUSTERCTL_* env vars onto them):
              clusterctl.memberId          co-located member id             (default 0)
              clusterctl.baseDir           cluster data dir root            (default $TMPDIR/seqeron-seq)
              clusterctl.aeronDir          co-located member's Aeron dir     (default $TMPDIR/seqeron-seq-aeron-<id>)
              clusterctl.ingressEndpoints  member ingress endpoints          (default 0=localhost:9302)""");
    }
}
