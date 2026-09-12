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
import org.limitless.seqeron.sequencer.FrameLayer;
import org.limitless.seqeron.metrics.SeqeronCounters;
import org.limitless.seqeron.replayer.client.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sequencer.PortLayout;
import org.limitless.seqeron.sequencer.ClusterStreamSender;
import org.limitless.seqeron.sequencer.IngressPublisher;
import org.limitless.seqeron.sequencer.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder;
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
 *   <li><b>shutdown</b> — safe to execute on every node.
 *   <li><b>activate &lt;gatewayId&gt;</b> — manual standby promotion: publishes an unsequenced
 *       {@code GatewayActivationRequested(gatewayId)} to cluster ingress and waits for the
 *       {@code GatewayActive} the sequencer synthesizes behind it. The operator's act is what is
 *       recorded and the designation stays the cluster's, through the same path bootstrap and both
 *       promotions take — which is also what gets the manual path the list validation it would
 *       otherwise lack, since a {@code gatewayId} no list row names is rejected on ingress. Every
 *       gateway instance reacts to the resulting {@code GatewayActive} identically however it was
 *       triggered: the instance whose {@code gatewayId} matches opens its accept gate, the others stay
 *       standby.</li>
 *   <li><b>load-topology &lt;file&gt;</b> — publishes the deployment's topology document validated XML.
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
        System.getProperty("clusterctl.ingressEndpoints", "0=" + PortLayout.ingressEndpoint(0));

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** Ingress is tried over this node's own aeron:ipc first; a follower answers on neither, so keep it short. */
    private static final long IPC_CONNECT_TIMEOUT_MS = 500;

    /** Ephemeral: this tool runs for one command and needs no port of its own (doc/registries.md §2). */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /** header.connectionId/sessionId for markers this tool submits: no gateway process/TCP connection. */
    private static final int NO_ID = -1;

    /**
     * §5's other reserved sourceId — clusterctl's own, stamped on every marker it submits; -1 the XSD
     * refuses on its own. It cannot be {@link #NO_ID}: that value is the cluster's (<b>F-4</b>) and the
     * sequencer refuses it on ingress (§9.2, condition 6). Read from {@link TopologyDocument}, which
     * refuses a document claiming it — the two must name the same number.
     */
    private static final int RESERVED_SOURCE_ID = TopologyDocument.RESERVED_SOURCE_ID;

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
     * Publishes the topology document.
     * @param session cluster session
     * @param topology topology definition
     */
    private static long publishTopologyAndAwaitEcho(final Session session, final TopologyDocument topology) {
        final Subscription tap = awaitTap(session);
        if (tap == null) {
            return -1;
        }

        final List<TopologyRow> rows = topology.gateways();
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

        return awaitEcho(session, tap, new ListEchoHandler(rows.get(rows.size() - 1).gatewayId()));
    }

    /** Matches the sequenced echo of the list's last row by gatewayId. */
    private static final class ListEchoHandler extends EchoHandler {
        private final int gatewayId;
        private final GatewayRegisteredDecoder decoder = new GatewayRegisteredDecoder();

        ListEchoHandler(final int gatewayId) {
            super(SystemFrame.GATEWAY_REGISTERED);
            this.gatewayId = gatewayId;
        }

        @Override
        boolean matches(final DirectBuffer buffer) {
            decoder.wrap(buffer, view.payloadOffset(), GatewayRegisteredDecoder.BLOCK_LENGTH,
                         MessageHeaderDecoder.SCHEMA_VERSION);
            return decoder.gatewayId() == gatewayId && decoder.remaining() == 0;
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

        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final GatewayActivationRequestedEncoder encoder = new GatewayActivationRequestedEncoder();
        encoder.wrap(payload, 0);
        encoder.gatewayId(gatewayId);
        publish(session, SystemFrame.GATEWAY_ACTIVATION_REQUESTED, payload, encoder.encodedLength());

        return awaitEcho(session, tap, new GatewayActiveEchoHandler(gatewayId));
    }

    /** Matches the sequenced {@code GatewayActive} echo of our own marker by gatewayId. */
    private static final class GatewayActiveEchoHandler extends EchoHandler {
        private final int gatewayId;
        private final GatewayActiveDecoder decoder = new GatewayActiveDecoder();

        GatewayActiveEchoHandler(final int gatewayId) {
            super(SystemFrame.GATEWAY_ACTIVE);
            this.gatewayId = gatewayId;
        }

        @Override
        boolean matches(final DirectBuffer buffer) {
            // Synthesized, so its gatewayId is inline in the frame's own block rather than in a body.
            decoder.wrap(buffer, view.payloadOffset(), view.blockLength(), view.version());
            return decoder.gatewayId() == gatewayId;
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

        return awaitEcho(session, tap, new MarkerEchoHandler(systemEventType, correlationId));
    }

    /**
     * Encodes and publishes the ClusterStarted/ClusterStopped marker for {@code systemEventType}. One
     * encoder for both: they are byte-identical past the header, which is the same fact {@link
     * MarkerEchoHandler} decodes both with one decoder on.
     */
    private static void publishMarker(final Session session, final int systemEventType, final long correlationId) {
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
        encoder.wrap(payload, 0);
        encoder.correlationId(correlationId);
        publish(session, systemEventType, payload, encoder.encodedLength());
    }

    /**
     * Reads the tap until {@code handler} sees the echo it is waiting for, or {@link #ECHO_TIMEOUT_NS}
     * passes. Egress is polled alongside it: the session that published the marker has to stay alive for
     * the echo to arrive at all.
     * @return the echoed frame's globalSeqNo, or -1 on timeout
     */
    private static long awaitEcho(final Session session, final Subscription tap, final EchoHandler handler) {
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = tap.poll(assembler, 10);
            session.sender.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.globalSeqNo : -1;
    }

    /**
     * Waits for one sequenced echo of a marker this tool published. Subclasses supply only what makes a
     * frame theirs; the family check is common because the same 2-byte field is an application payloadId
     * on the other family and the tap carries both.
     */
    private abstract static class EchoHandler implements FragmentHandler {
        final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final int systemEventType;
        private boolean found;
        private long globalSeqNo;

        EchoHandler(final int systemEventType) {
            this.systemEventType = systemEventType;
        }

        /** Whether this frame — already unwrapped into {@link #view} — is the echo being waited for. */
        abstract boolean matches(DirectBuffer buffer);

        @Override
        public final void onFragment(final DirectBuffer buffer, final int offset, final int length,
                                     final Header header) {
            if (found || !view.wrap(buffer, offset, length) || !view.isSystem() ||
                view.systemEventType() != systemEventType) {
                return;
            }
            if (matches(buffer)) {
                globalSeqNo = view.globalSeqNo();
                found = true;
            }
        }
    }

    /**
     * Subscribes to this node's co-located tap and waits for it to connect, which is where every
     * await-my-own-echo path starts. Returns null (having said why) if it never does.
     */
    private static Subscription awaitTap(final Session session) {
        final Subscription tap = session.aeron.addSubscription(FrameLayer.FEEDER_CHANNEL,
                                                               FrameLayer.FEEDER_STREAM_ID);
        final long connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= connectDeadline) {
                System.err.printf("[clusterctl] tap (aeron:ipc/%d) not available — co-located with a SequencerServer?%n",
                                  FrameLayer.FEEDER_STREAM_ID);
                return null;
            }
            session.sender.pollEgress();
            IDLE.idle();
        }
        return tap;
    }

    /**
     * Matches the sequenced echo of our own marker by correlationId. Decodes with {@link
     * ClusterStartedDecoder} for either marker — ClusterStarted/ClusterStopped are byte-identical past the
     * header, so correlationId and header.globalSeqNo are at the same offsets for both.
     */
    private static final class MarkerEchoHandler extends EchoHandler {
        private final long correlationId;
        private final ClusterStartedDecoder marker = new ClusterStartedDecoder();

        MarkerEchoHandler(final int systemEventType, final long correlationId) {
            super(systemEventType);
            this.correlationId = correlationId;
        }

        @Override
        boolean matches(final DirectBuffer buffer) {
            marker.wrap(buffer, view.payloadOffset(), ClusterStartedDecoder.BLOCK_LENGTH,
                        MessageHeaderDecoder.SCHEMA_VERSION);
            return marker.correlationId() == correlationId;
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
