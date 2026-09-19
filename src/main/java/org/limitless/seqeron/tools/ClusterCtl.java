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
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;
import org.limitless.seqeron.tools.TopologyDocument.ApplicationRow;
import org.limitless.seqeron.tools.TopologyDocument.ProtocolRow;
import org.limitless.seqeron.tools.TopologyDocument.TopologyRow;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * clusterctl — the operator cluster life-cycle tool (doc/clusterctl.md). Node-local: it shares a {@code
 * SequencerServer} host's Aeron directory (to read the tap) and {@code clusterDir} (for {@link ClusterTool}).
 * Named so as not to shadow Aeron's {@code io.aeron.cluster.ClusterControl}.
 *
 * <p>Commands:
 * <ul>
 *   <li><b>start</b> — publishes a {@code ClusterStarted} marker and waits for its echo on the tap;
 *       non-zero if there is no leader or no echo. It records that the system is up; it starts nothing.</li>
 *   <li><b>shutdown</b> — safe to execute on every node.
 *   <li><b>activate &lt;gatewayId&gt;</b> — manual standby promotion: publishes
 *       {@code GatewayActivationRequested} and waits for the {@code GatewayActive} the sequencer synthesizes
 *       behind it, so the designation stays the cluster's and an unlisted {@code gatewayId} is refused.</li>
 *   <li><b>load-topology &lt;file&gt;</b> — publishes the deployment's validated topology document.
 *   <li><b>counters</b> — lists this node's operator counters off the CnC file; needs no leader.</li>
 *   <li><b>help</b> — usage.</li>
 *   <li><i>anything else</i> — passed through to {@link ClusterTool} against this node's
 *       {@code clusterDir} (describe, errors, list-members, recording-log, …).</li>
 * </ul>
 *
 * <p>Defaults mirror {@code SequencerServer}'s, so the default single-node cluster needs no arguments.
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

    /**
     * Ephemeral: this tool runs for one command and needs no port of its own (doc/registries.md §2). The host
     * is what the leader replies to, so on a follower of a multi-host cluster it must be this node's own name.
     */
    private static final String EGRESS_CHANNEL =
        "aeron:udp?endpoint=" + System.getProperty("clusterctl.egressHost", "localhost") + ":0";

    /** header.connectionId/sessionId for markers this tool submits: no gateway process/TCP connection. */
    private static final int NO_ID = -1;

    /**
     * clusterctl's own reserved sourceId (§5), stamped on every marker. Not {@link #NO_ID}, which is the
     * cluster's own (<b>F-4</b>) and refused on ingress; {@link TopologyDocument} refuses a document claiming it.
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
     * Manual standby promotion: publishes {@code GatewayActivationRequested(gatewayId)} and waits for the
     * {@code GatewayActive} synthesized behind it.
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
     * Publishes the topology document read from {@code args[1]} — one {@code GatewayRegistered} per list row,
     * {@code remaining} counting down to 0, then the application and protocol rows — and waits for the last
     * list row's echo. {@link TopologyDocument} validates the whole document before a byte is published.
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
     * Publishes {@code GatewayActivationRequested(gatewayId)} and reads the tap for the {@code GatewayActive}
     * behind it, matched by {@code gatewayId}. Returns its globalSeqNo, or -1 on timeout.
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
     * Lists this node's operator counters (see {@link SeqeronCounters}), including any the co-located
     * replicas publish, off the Aeron directory's CnC file. Needs no cluster connection.
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

    /** This tool's cluster session, and the co-located media driver it reaches the cluster and the tap through. */
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

    /** Publishes one system event on this session, or fails the command: a {@code Declined} marker cannot be resumed. */
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
     * Publishes the marker for {@code systemEventType} with {@code correlationId}, then reads the tap for its
     * echo. Returns the assigned globalSeqNo, or -1 on timeout.
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

    /** Encodes and publishes the ClusterStarted/ClusterStopped marker; the two are byte-identical past the header. */
    private static void publishMarker(final Session session, final int systemEventType, final long correlationId) {
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
        encoder.wrap(payload, 0);
        encoder.correlationId(correlationId);
        publish(session, systemEventType, payload, encoder.encodedLength());
    }

    /**
     * Reads the tap until {@code handler} sees its echo, or {@link #ECHO_TIMEOUT_NS} passes, polling egress
     * alongside so the publishing session stays alive.
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

    /** Waits for one sequenced echo of a marker this tool published; subclasses say which frame is theirs. */
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

    /** Subscribes to this node's tap and waits for it to connect; null (having said why) if it never does. */
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

    /** Matches our marker's echo by correlationId; one decoder serves both byte-identical markers. */
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
              clusterctl.ingressEndpoints  member ingress endpoints          (default 0=localhost:9302)
              clusterctl.egressHost        host the leader replies to        (default localhost)""");
    }
}
