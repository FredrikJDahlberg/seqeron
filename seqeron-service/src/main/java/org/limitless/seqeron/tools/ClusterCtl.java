package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.RecordingLog;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.xml.parsers.ParserConfigurationException;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.server.NodeDriver;
import org.limitless.seqeron.replayer.server.RestartSafeLog;
import org.limitless.seqeron.replayer.server.SegmentPurge;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;
import org.limitless.seqeron.tools.TopologyDocument.ApplicationRow;
import org.limitless.seqeron.tools.TopologyDocument.GroupRow;
import org.limitless.seqeron.tools.TopologyDocument.ProtocolRow;
import org.limitless.seqeron.tools.TopologyDocument.TopologyRow;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * clusterctl — the operator cluster life-cycle tool (doc/clusterctl.md). Node-local: it shares a {@code
 * SequencerServer} host's Aeron directory (to read the default queue, which the node's Replayer names) and {@code
 * clusterDir} (for {@link ClusterTool}).
 * Named so as not to shadow Aeron's {@code io.aeron.cluster.ClusterControl}.
 *
 * <p>Commands:
 * <ul>
 *   <li><b>start</b> — publishes a {@code ClusterStarted} marker and waits for its echo on the queue;
 *       non-zero if there is no leader or no echo. It records that the system is up; it starts nothing.</li>
 *   <li><b>shutdown</b> — safe to execute on every node.
 *   <li><b>activate &lt;gatewayId&gt;</b> — manual standby promotion: publishes
 *       {@code GatewayActivationRequested} and waits for the {@code GatewayActive} the sequencer synthesizes
 *       behind it, so the designation stays the cluster's and an unlisted {@code gatewayId} is refused.</li>
 *   <li><b>request-snapshot</b> — publishes {@code SnapshotRequested} and waits for the
 *       {@code SnapshotStarted} the sequencer synthesizes behind it (doc/snapshot.md §3).</li>
 *   <li><b>load-topology &lt;file&gt;</b> — publishes the deployment's validated topology document, checked
 *       against the log (spec §6.4); the first command an archive accepts.
 *   <li><b>trim-log</b> — purges this member's Raft log and snapshot recordings below its two newest restart-safe
 *       snapshots (doc/snapshot.md §10.7); needs no leader, and is safe to repeat.</li>
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
    private static final String AERON_DIR = System.getProperty("clusterctl.aeronDir", PortLayout.aeronDir(MEMBER_ID));
    private static final File CLUSTER_DIR = new File(BASE_DIR + "/cluster-" + MEMBER_ID);
    private static final String INGRESS_ENDPOINTS =
        System.getProperty("clusterctl.ingressEndpoints", PortLayout.ingressEndpoints());

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** The longest a first load's activation holds the queues up for (spec §7.4). */
    private static final long ACTIVATION_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** How long load-topology may take to replay the default queue before it publishes. */
    private static final long CATCH_UP_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    /** A {@code groupNames} slot. */
    private static final int NAME_LENGTH = 32;

    private static final int NO_GROUP = 0;

    /** Ingress is tried over this node's own aeron:ipc first; a follower answers on neither, so keep it short. */
    private static final long IPC_CONNECT_TIMEOUT_MS = 500;

    /**
     * Ephemeral: this tool runs for one command and needs no port of its own (doc/ops.md, "Ports"). The host
     * is what the leader replies to, so on a follower of a multi-host cluster it must be this node's own name.
     */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint="
        + System.getProperty("clusterctl.egressHost",
                             MEMBER_ID < PortLayout.HOSTS.size() ? PortLayout.HOSTS.get(MEMBER_ID)
                                                                 : PortLayout.DEFAULT_HOST)
        + ":0";

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
        case "request-snapshot":
            System.exit(requestSnapshot());
            break;
        case "load-topology":
            System.exit(loadTopology(args));
            break;
        case "trim-log":
            System.exit(trimLog());
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
            final long clusterSeqNo =
                publishMarkerAndAwaitEcho(session, SystemFrame.CLUSTER_STARTED, correlationId);
            if (clusterSeqNo < 0) {
                System.err.println("[clusterctl] start: no sequenced ClusterStarted echo within timeout");
                return 1;
            }
            System.out.printf("[clusterctl] start: system-started recorded at clusterSeqNo=%d%n", clusterSeqNo);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] start: no elected leader / cluster unreachable (" + ex.getMessage() + ")");
            return 1;
        }
    }

    /**
     * Purges what the member's two newest restart-safe snapshots make unneeded (doc/snapshot.md §10.7). The trim is
     * recorded first: a run that dies half way leaves the log no longer beginning at its first term, and the next
     * start must not fall back to replaying it.
     */
    private static int trimLog() {
        if (!new File(CLUSTER_DIR, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            System.err.println("[clusterctl] trim-log: no recording log in " + CLUSTER_DIR);
            return 1;
        }
        final LogTrimPlan plan;
        try (RecordingLog recordingLog = new RecordingLog(CLUSTER_DIR, false)) {
            plan = LogTrimPlan.of(recordingLog.entries(),
                                  RestartSafeLog.read(CLUSTER_DIR.toPath().resolve(RestartSafeLog.FILE_NAME)));
        }
        if (plan == LogTrimPlan.NOTHING) {
            System.out.println("[clusterctl] trim-log: fewer than two restart-safe snapshots — nothing to trim");
            return 0;
        }
        try (RestartSafeLog safeLog = RestartSafeLog.open(CLUSTER_DIR.toPath())) {
            safeLog.appendTrimmed();
        }

        int failures = 0;
        try (AeronArchive archive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeronDirectoryName(AERON_DIR)
                .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                .controlResponseStreamId(NodeDriver.CLUSTERCTL_ARCHIVE_RESPONSE_STREAM_ID))) {
            for (final long recordingId : plan.snapshotRecordings()) {
                try {
                    archive.purgeRecording(recordingId);
                    System.out.printf("[clusterctl] trim-log: purged snapshot recording %d%n", recordingId);
                } catch (final ArchiveException ex) {
                    if (ex.errorCode() != ArchiveException.UNKNOWN_RECORDING) { // else a run before this one did
                        System.err.printf("[clusterctl] trim-log: snapshot recording %d: %s%n", recordingId,
                                          ex.getMessage());
                        ++failures;
                    }
                }
            }
            for (final long recordingId : plan.logRecordings()) {
                failures += trimLogRecording(archive, recordingId, plan.floor());
            }
        } catch (final RuntimeException ex) {
            System.err.println("[clusterctl] trim-log: the member's archive is unreachable (" + ex.getMessage() + ")");
            return 1;
        }
        if (failures > 0) {
            System.err.println("[clusterctl] trim-log: " + failures + " purge(s) refused; run it again");
            return 1;
        }
        return 0;
    }

    /** Purges a log recording's segments below {@code floor}; 1 if the archive refused. */
    private static int trimLogRecording(final AeronArchive archive, final long recordingId, final long floor) {
        final long[] summary = new long[4]; // start, stop, term length, segment length
        if (archive.listRecording(recordingId, (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                                                startPosition, stopPosition, initialTermId, segmentFileLength,
                                                termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                                                originalChannel, sourceIdentity) -> {
                summary[0] = startPosition;
                summary[1] = stopPosition;
                summary[2] = termBufferLength;
                summary[3] = segmentFileLength;
            }) == 0) {
            return 0; // already gone
        }
        final long end =
            summary[1] != AeronArchive.NULL_POSITION ? summary[1] : archive.getRecordingPosition(recordingId);
        final long target = SegmentPurge.target(summary[0], (int)summary[2], (int)summary[3], Math.min(floor, end));
        if (target == SegmentPurge.NOTHING) {
            return 0;
        }
        try {
            archive.purgeSegments(recordingId, target);
            System.out.printf("[clusterctl] trim-log: purged log recording %d below position %d%n", recordingId, target);
            return 0;
        } catch (final ArchiveException ex) {
            System.err.printf("[clusterctl] trim-log: log recording %d below position %d: %s%n", recordingId, target,
                              ex.getMessage());
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
            final long clusterSeqNo =
                publishMarkerAndAwaitEcho(session, SystemFrame.CLUSTER_STOPPED, correlationId);
            if (clusterSeqNo < 0) {
                System.err.println("[clusterctl] shutdown: no ClusterStopped echo within timeout — aborting anyway");
            } else {
                System.out.printf("[clusterctl] shutdown: system-stopped recorded at clusterSeqNo=%d%n", clusterSeqNo);
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
            final long clusterSeqNo = publishGatewayActiveAndAwaitEcho(session, gatewayId);
            if (clusterSeqNo < 0) {
                System.err.println("[clusterctl] activate: no synthesized GatewayActive within timeout — is "
                               + "<gatewayId> a list row?");
                return 1;
            }
            System.out.printf("[clusterctl] activate: GatewayActive(gatewayId=%d) recorded at clusterSeqNo=%d%n",
                              gatewayId, clusterSeqNo);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] activate: no elected leader / cluster unreachable (" + ex.getMessage() +
                               ")");
            return 1;
        }
    }

    /**
     * Publishes {@code SnapshotRequested} and waits for the {@code SnapshotStarted} of the round it calls for; none
     * follows when the loaded topology has no {@code <snapshots>}.
     */
    private static int requestSnapshot() {
        final long correlationId = System.nanoTime();
        try (Session session = new Session()) {
            final Subscription queue = awaitQueue(session);
            if (queue == null) {
                return 1;
            }
            publishMarker(session, SystemFrame.SNAPSHOT_REQUESTED, correlationId);
            final SnapshotStartedEchoHandler started = new SnapshotStartedEchoHandler(correlationId);
            final long cut = awaitEcho(session, queue, started);
            if (cut < 0) {
                System.err.println(started.requestSeqNo < 0
                    ? "[clusterctl] request-snapshot: no sequenced SnapshotRequested echo within timeout"
                    : "[clusterctl] request-snapshot: no SnapshotStarted followed — does the loaded topology have a "
                      + "<snapshots> element?");
                return 1;
            }
            System.out.printf("[clusterctl] request-snapshot: round %d started at clusterSeqNo=%d%n", started.round,
                              cut);
            return 0;
        } catch (final Exception ex) {
            System.err.println("[clusterctl] request-snapshot: no elected leader / cluster unreachable (" +
                               ex.getMessage() + ")");
            return 1;
        }
    }

    /**
     * Matches our {@code SnapshotRequested} by correlationId, then the first {@code SnapshotStarted} behind it: the
     * round the request called for, which the cluster's snapshot action takes at the leader's next callback. One
     * handler for both, since one poll can deliver the two together.
     */
    private static final class SnapshotStartedEchoHandler extends EchoHandler {
        private final ClusterStartedDecoder marker = new ClusterStartedDecoder();
        private final SnapshotStartedDecoder decoder = new SnapshotStartedDecoder();
        private final long correlationId;
        private long requestSeqNo = -1;
        private long round;

        SnapshotStartedEchoHandler(final long correlationId) {
            super(SystemFrame.SNAPSHOT_STARTED);
            this.correlationId = correlationId;
        }

        @Override
        boolean accepts() {
            return super.accepts() || view.isSystem() && view.systemEventType() == SystemFrame.SNAPSHOT_REQUESTED;
        }

        @Override
        boolean matches(final DirectBuffer buffer) {
            if (view.systemEventType() == SystemFrame.SNAPSHOT_REQUESTED) {
                marker.wrap(buffer, view.payloadOffset(), ClusterStartedDecoder.BLOCK_LENGTH,
                            MessageHeaderDecoder.SCHEMA_VERSION);
                if (marker.correlationId() == correlationId) {
                    requestSeqNo = view.clusterSeqNo();
                }
                return false;
            }
            if (requestSeqNo < 0 || view.clusterSeqNo() < requestSeqNo) {
                return false;
            }
            decoder.wrap(buffer, view.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                         SnapshotStartedDecoder.SCHEMA_VERSION);
            round = decoder.round();
            return true;
        }
    }

    /**
     * Publishes the topology document read from {@code args[1]} in spec §6.4's order, checked against the default
     * queue before a row is published. {@link TopologyDocument} validates the whole document before a byte is.
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
            return publishTopology(session, topology);
        } catch (final Exception ex) {
            System.err.println("[clusterctl] load-topology: no elected leader / cluster unreachable (" +
                               ex.getMessage() + ")");
            return 1;
        }
    }

    /**
     * Spec §6.4's publish order on one session, which the {@code GroupsRegistered} makes the topology session:
     * compare the groups with the active ones, publish them, check the rows' groups against those already
     * registered, publish the rows, and wait for every frame published to appear on the default queue.
     * @return the exit status
     */
    private static int publishTopology(final Session session, final TopologyDocument topology) {
        final int active = DefaultQueue.resolve(session.aeron, CONNECT_TIMEOUT_NS, false);
        if (active == DefaultQueue.NO_REPLAYER) {
            System.err.println("[clusterctl] load-topology: no Replayer answered on " + AERON_DIR +
                               " — co-located with a SequencerServer?");
            return 1;
        }
        // Before activation the default queue is the file's lowest group's.
        final int groupId = active == DefaultQueue.NOT_ACTIVE ? topology.groups().getFirst().groupId() : active;
        final TopologyLog log = new TopologyLog(session.sender.clusterSessionId());
        try (ReplayerStreamReceiver receiver =
                 new ReplayerStreamReceiver(RESERVED_SOURCE_ID, log::onSequenced, null, null)) {
            receiver.start(session.aeron, MEMBER_ID, groupId);
            if (active != DefaultQueue.NOT_ACTIVE) {
                if (!poll(session, receiver, receiver::isCaughtUp, CATCH_UP_TIMEOUT_NS)) {
                    System.err.printf("[clusterctl] load-topology: could not read group %d's queue to its end%n",
                                      groupId);
                    return 1;
                }
                if (!Arrays.equals(log.groupNames, topology.groupNames())) {
                    System.err.println("[clusterctl] load-topology: the file's groups differ from the active ones, "
                                       + "which are fixed for the life of the log (spec S-8); nothing published");
                    return 1;
                }
            }

            final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(GroupsRegisteredEncoder.BLOCK_LENGTH);
            final byte[] groupNames = new byte[GroupsRegisteredEncoder.groupNamesLength()];
            for (final GroupRow group : topology.groups()) {
                final byte[] name = group.groupName().getBytes(StandardCharsets.US_ASCII);
                System.arraycopy(name, 0, groupNames, (group.groupId() - 1) * NAME_LENGTH, name.length);
            }
            final GroupsRegisteredEncoder groupsEncoder = new GroupsRegisteredEncoder();
            groupsEncoder.wrap(payload, 0).putGroupNames(groupNames, 0);
            publish(session, log, SystemFrame.GROUPS_REGISTERED, payload, groupsEncoder.encodedLength());
            if (!poll(session, receiver, () -> log.matched > 0, ECHO_TIMEOUT_NS + ACTIVATION_TIMEOUT_NS)) {
                System.err.printf("[clusterctl] load-topology: the GroupsRegistered did not appear on group %d's "
                                      + "queue: another load-topology holds the topology session, or the file's "
                                      + "groups differ from the active ones%n",
                                  groupId);
                return 1;
            }
            final String moved = log.movedSource(topology);
            if (moved != null) {
                System.err.println("[clusterctl] load-topology: " + moved + "; a source's group is fixed (spec S-8)");
                return 1;
            }

            final List<TopologyRow> rows = topology.gateways();
            final GatewayRegisteredEncoder encoder = new GatewayRegisteredEncoder();
            for (int i = 0; i < rows.size(); i++) {
                final TopologyRow row = rows.get(i);
                encoder.wrap(payload, 0);
                encoder.remaining(rows.size() - 1 - i)
                    .gatewayId(row.gatewayId())
                    .gatewaySourceId(row.gatewaySourceId())
                    .gatewayName(row.gatewayName())
                    .preferenceRank((short)row.preferenceRank())
                    .snapshot((short)(row.snapshot() ? 1 : 0))
                    .groupId((short)row.groupId());
                publish(session, log, SystemFrame.GATEWAY_REGISTERED, payload, encoder.encodedLength());
            }

            final ApplicationRegisteredEncoder applicationEncoder = new ApplicationRegisteredEncoder();
            for (final ApplicationRow row : topology.applications()) {
                applicationEncoder.wrap(payload, 0);
                applicationEncoder.applicationSourceId(row.sourceId())
                    .applicationName(row.applicationName())
                    .snapshot((short)(row.snapshot() ? 1 : 0))
                    .groupId((short)row.groupId());
                publish(session, log, SystemFrame.APPLICATION_REGISTERED, payload, applicationEncoder.encodedLength());
            }

            final PayloadIdRegisteredEncoder protocolEncoder = new PayloadIdRegisteredEncoder();
            for (final ProtocolRow row : topology.protocols()) {
                protocolEncoder.wrap(payload, 0);
                protocolEncoder.payloadId(row.payloadId())
                    .protocolVersion(row.protocolVersion())
                    .protocolName(row.protocolName());
                publish(session, log, SystemFrame.PAYLOAD_ID_REGISTERED, payload, protocolEncoder.encodedLength());
            }

            if (topology.snapshots() != null) {
                final SnapshotPolicyRegisteredEncoder policyEncoder = new SnapshotPolicyRegisteredEncoder();
                policyEncoder.wrap(payload, 0)
                    .intervalSeconds(topology.snapshots().intervalSeconds())
                    .bytes(topology.snapshots().bytes());
                publish(session, log, SystemFrame.SNAPSHOT_POLICY_REGISTERED, payload, policyEncoder.encodedLength());
            }

            if (!poll(session, receiver, () -> log.matched == log.published.size(), ECHO_TIMEOUT_NS)) {
                System.err.printf("[clusterctl] load-topology: frame %d of %d, a %s, did not appear on group %d's "
                                      + "queue: the sequencer refused it%n",
                                  log.matched + 1, log.published.size(),
                                  frameName(log.published.get(log.matched).systemEventType()), groupId);
                return 1;
            }
            System.out.printf("[clusterctl] load-topology: %d group(s), %d gateway row(s), %d application row(s) and "
                                  + "%d protocol row(s) recorded, the last at clusterSeqNo=%d%s%n",
                              topology.groups().size(), rows.size(), topology.applications().size(),
                              topology.protocols().size(), log.lastClusterSeqNo,
                              topology.snapshots() == null
                                  ? ""
                                  : "; snapshot interval " + topology.snapshots().intervalSeconds() + " s");
            return 0;
        }
    }

    /**
     * Polls {@code receiver} until {@code done} or {@code timeoutNs} passes, polling egress alongside so the
     * session stays alive.
     */
    private static boolean poll(final Session session, final ReplayerStreamReceiver receiver,
                                final BooleanSupplier done, final long timeoutNs) {
        final long deadline = System.nanoTime() + timeoutNs;
        while (!done.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            IDLE.idle(receiver.poll() + session.sender.pollEgress());
        }
        return true;
    }

    /** Publishes one topology frame, and expects it on the queue in the order published. */
    private static void publish(final Session session, final TopologyLog log, final int systemEventType,
                                final ExpandableArrayBuffer body, final int bodyLength) {
        log.published.add(new PublishedFrame(systemEventType, Arrays.copyOf(body.byteArray(), bodyLength)));
        publish(session, systemEventType, body, bodyLength);
    }

    private static String frameName(final int systemEventType) {
        return switch (systemEventType) {
            case SystemFrame.GROUPS_REGISTERED -> "GroupsRegistered";
            case SystemFrame.GATEWAY_REGISTERED -> "GatewayRegistered";
            case SystemFrame.APPLICATION_REGISTERED -> "ApplicationRegistered";
            case SystemFrame.PAYLOAD_ID_REGISTERED -> "PayloadIdRegistered";
            default -> "SnapshotPolicyRegistered";
        };
    }

    /** A frame {@code load-topology} published: its event and its payload, which the queue carries unchanged. */
    private record PublishedFrame(int systemEventType, byte[] body) { }

    /**
     * What {@code load-topology} reads off the default queue: the active groups and each source's group, from
     * the frames before its {@code GroupsRegistered}, and its own frames, matched in the order published.
     */
    private static final class TopologyLog {
        private final long sessionId;
        private final GroupsRegisteredDecoder groups = new GroupsRegisteredDecoder();
        private final GatewayRegisteredDecoder gateway = new GatewayRegisteredDecoder();
        private final ApplicationRegisteredDecoder application = new ApplicationRegisteredDecoder();
        private final byte[] groupNameBytes = new byte[GroupsRegisteredDecoder.groupNamesLength()];
        private final Int2IntHashMap sourceGroups = new Int2IntHashMap(NO_GROUP);
        private final List<PublishedFrame> published = new ArrayList<>();

        /** The latest {@code GroupsRegistered}'s names, slot by slot; all null before one. */
        private final String[] groupNames = new String[FrameLayer.MAX_GROUPS];
        private int matched;
        private long lastClusterSeqNo;

        private TopologyLog(final long sessionId) {
            this.sessionId = sessionId;
        }

        private void onSequenced(final SequencedEvent event) {
            if (!event.isSystem()) {
                return;
            }
            if (matched < published.size() && isPublished(event, published.get(matched))) {
                matched++;
                lastClusterSeqNo = event.clusterSeqNo();
                return;
            }
            if (matched > 0) {
                return; // past this load's GroupsRegistered, which S-9 keeps any other load's rows from following
            }
            switch (event.systemEventType()) {
            case SystemFrame.GROUPS_REGISTERED -> {
                groups.wrap(event.buffer(), event.payloadOffset(), GroupsRegisteredDecoder.BLOCK_LENGTH,
                            GroupsRegisteredDecoder.SCHEMA_VERSION);
                groups.getGroupNames(groupNameBytes, 0);
                for (int slot = 0; slot < FrameLayer.MAX_GROUPS; slot++) {
                    int length = 0;
                    while (length < NAME_LENGTH && groupNameBytes[slot * NAME_LENGTH + length] != 0) {
                        length++;
                    }
                    groupNames[slot] = length == 0
                        ? null
                        : new String(groupNameBytes, slot * NAME_LENGTH, length, StandardCharsets.US_ASCII);
                }
            }
            case SystemFrame.GATEWAY_REGISTERED -> {
                gateway.wrap(event.buffer(), event.payloadOffset(), GatewayRegisteredDecoder.BLOCK_LENGTH,
                             GatewayRegisteredDecoder.SCHEMA_VERSION);
                sourceGroups.put(gateway.gatewaySourceId(), gateway.groupId());
            }
            case SystemFrame.APPLICATION_REGISTERED -> {
                application.wrap(event.buffer(), event.payloadOffset(), ApplicationRegisteredDecoder.BLOCK_LENGTH,
                                 ApplicationRegisteredDecoder.SCHEMA_VERSION);
                sourceGroups.put(application.applicationSourceId(), application.groupId());
            }
            default -> {
            }
            }
        }

        private boolean isPublished(final SequencedEvent event, final PublishedFrame frame) {
            if (event.sourceSessionId() != sessionId || event.systemEventType() != frame.systemEventType() ||
                event.payloadLength() != frame.body().length) {
                return false;
            }
            for (int i = 0; i < frame.body().length; i++) {
                if (event.buffer().getByte(event.payloadOffset() + i) != frame.body()[i]) {
                    return false;
                }
            }
            return true;
        }

        /** The first of the file's sources the log registered with another group, described; null if none. */
        private String movedSource(final TopologyDocument topology) {
            for (final TopologyRow row : topology.gateways()) {
                final String moved = moved(row.gatewayName(), row.gatewaySourceId(), row.groupId());
                if (moved != null) {
                    return moved;
                }
            }
            for (final ApplicationRow row : topology.applications()) {
                final String moved = moved(row.applicationName(), row.sourceId(), row.groupId());
                if (moved != null) {
                    return moved;
                }
            }
            return null;
        }

        private String moved(final String name, final int sourceId, final int groupId) {
            final int registered = sourceGroups.get(sourceId);
            return registered == NO_GROUP || registered == groupId ? null
                                                                   : name + " puts sourceId " + sourceId +
                    " in group " + groupId + ", which the log registered in group " + registered;
        }
    }

    /**
     * Publishes {@code GatewayActivationRequested(gatewayId)} and reads the queue for the {@code GatewayActive}
     * behind it, matched by {@code gatewayId}. Returns its clusterSeqNo, or -1 on timeout.
     */
    private static long publishGatewayActiveAndAwaitEcho(final Session session, final int gatewayId) {
        final Subscription queue = awaitQueue(session);
        if (queue == null) {
            return -1;
        }

        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final GatewayActivationRequestedEncoder encoder = new GatewayActivationRequestedEncoder();
        encoder.wrap(payload, 0);
        encoder.gatewayId(gatewayId);
        publish(session, SystemFrame.GATEWAY_ACTIVATION_REQUESTED, payload, encoder.encodedLength());

        return awaitEcho(session, queue, new GatewayActiveEchoHandler(gatewayId));
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
            // Synthesized, so its gatewayId is inline in the frame's own block rather than in a payload.
            decoder.wrap(buffer, view.payloadOffset(), GatewayActiveDecoder.BLOCK_LENGTH,
                         GatewayActiveDecoder.SCHEMA_VERSION);
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

    /** This tool's cluster session, and the co-located media driver it reaches the cluster and the queue through. */
    private static final class Session implements AutoCloseable {
        private final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
        private final ClusterStreamSender sender = new ClusterStreamSender();
        private final IngressPublisher publisher = new IngressPublisher();

        private Session() {
            try {
                sender.connectColocated(aeron, MEMBER_ID, IPC_CONNECT_TIMEOUT_MS, EGRESS_CHANNEL,
                                        INGRESS_ENDPOINTS);
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
        final Publish outcome =
            session.publisher.publishSystem(session.sender, RESERVED_SOURCE_ID, NO_ID, systemEventType, body,
                                            bodyLength);
        if (outcome != Publish.Published) {
            throw new IllegalStateException("cluster ingress " + outcome + " a " + systemEventType + " marker");
        }
    }

    /**
     * Publishes the marker for {@code systemEventType} with {@code correlationId}, then reads the queue for its
     * echo. Returns the assigned clusterSeqNo, or -1 on timeout.
     */
    private static long publishMarkerAndAwaitEcho(final Session session, final int systemEventType,
                                                  final long correlationId) {
        final Subscription queue = awaitQueue(session);
        if (queue == null) {
            return -1;
        }

        publishMarker(session, systemEventType, correlationId);

        return awaitEcho(session, queue, new MarkerEchoHandler(systemEventType, correlationId));
    }

    /**
     * Encodes and publishes a ClusterStarted, ClusterStopped or SnapshotRequested marker; the three are
     * byte-identical past the header.
     */
    private static void publishMarker(final Session session, final int systemEventType, final long correlationId) {
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(64);
        final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
        encoder.wrap(payload, 0);
        encoder.correlationId(correlationId);
        publish(session, systemEventType, payload, encoder.encodedLength());
    }

    /**
     * Reads the queue until {@code handler} sees its echo, or {@link #ECHO_TIMEOUT_NS} passes, polling egress
     * alongside so the publishing session stays alive.
     * @return the echoed frame's clusterSeqNo, or -1 on timeout
     */
    private static long awaitEcho(final Session session, final Subscription queue, final EchoHandler handler) {
        final FragmentAssembler assembler = new FragmentAssembler(handler);
        final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
        while (!handler.found && System.nanoTime() < deadline) {
            final int fragments = queue.poll(assembler, 10);
            session.sender.pollEgress();
            IDLE.idle(fragments);
        }
        return handler.found ? handler.clusterSeqNo : -1;
    }

    /** Waits for one sequenced echo of a marker this tool published; subclasses say which frame is theirs. */
    private abstract static class EchoHandler implements FragmentHandler {
        final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final int systemEventType;
        private boolean found;
        private long clusterSeqNo;

        EchoHandler(final int systemEventType) {
            this.systemEventType = systemEventType;
        }

        /** Whether this frame — already unwrapped into {@link #view} — is the echo being waited for. */
        abstract boolean matches(DirectBuffer buffer);

        /** Whether {@link #matches} should see this frame at all; by default, this handler's event type alone. */
        boolean accepts() {
            return view.isSystem() && view.systemEventType() == systemEventType;
        }

        @Override
        public final void onFragment(final DirectBuffer buffer, final int offset, final int length,
                                     final Header header) {
            if (found || !view.wrap(buffer, offset, length) || !accepts()) {
                return;
            }
            if (matches(buffer)) {
                clusterSeqNo = view.clusterSeqNo();
                found = true;
            }
        }
    }

    /**
     * Subscribes to this node's default queue (spec §3.1), untethered, and waits for it to connect; null (having
     * said why) if the Replayer names none or it never connects. Tethered, a stopped or killed clusterctl would hold
     * the queue back until its member terminated.
     */
    private static Subscription awaitQueue(final Session session) {
        final int groupId = DefaultQueue.resolve(session.aeron, CONNECT_TIMEOUT_NS, false);
        if (groupId == DefaultQueue.NO_REPLAYER) {
            System.err.println("[clusterctl] no Replayer answered on " + AERON_DIR +
                               " — co-located with a SequencerServer?");
            return null;
        }
        if (groupId == DefaultQueue.NOT_ACTIVE) {
            System.err.println("[clusterctl] no group is active: the cluster refuses everything before the first "
                               + "load-topology");
            return null;
        }
        final Subscription queue = session.aeron.addSubscription(ReplayerStreamReceiver.QUEUE_CONSUMER_CHANNEL,
                                                                 FrameLayer.queueStreamId(groupId));
        final long connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!queue.isConnected()) {
            if (System.nanoTime() >= connectDeadline) {
                System.err.printf(
                    "[clusterctl] queue (aeron:ipc/%d) not available — co-located with a SequencerServer?%n",
                    FrameLayer.queueStreamId(groupId));
                return null;
            }
            session.sender.pollEgress();
            IDLE.idle();
        }
        return queue;
    }

    /** Matches our marker's echo by correlationId; one decoder serves the three byte-identical markers. */
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
              request-snapshot
                           start an application snapshot round now; waits for the
                           SnapshotStarted behind the request (requires an elected leader
                           and a topology with a <snapshots> element)
              load-topology <file>
                           publish the topology document (XML, validated against the
                           packaged topology.xsd): the groups, then the gateway list,
                           the co-located applications, the protocol registry and the
                           snapshot policy; run it first, before start and before any
                           gateway starts: the cluster refuses everything before it
              trim-log     purge this member's Raft log and snapshot recordings below its two
                           newest restart-safe snapshots; per member, safe to repeat, no leader
                           needed (doc/clusterctl.md)
              counters     list this node's seqeron operator counters (SequencerService/
                           ReplayerService); no cluster connection needed, safe on every node
              snapshot     Aeron's cluster snapshot; not supported (see request-snapshot)
              help         show this help
              <other>      passed through to io.aeron.cluster.ClusterTool (describe, errors,
                           list-members, recording-log, …) against this node's cluster dir

            Config (system properties; clusterctl.sh maps the CLUSTERCTL_* env vars onto them):
              clusterctl.memberId          co-located member id             (default 0)
              clusterctl.baseDir           cluster data dir root            (default $TMPDIR/seqeron-seq)
              clusterctl.aeronDir          co-located member's Aeron dir     (default seqeron-seq-aeron-<id> under
                                                                              /dev/shm, else under $TMPDIR)
              clusterctl.ingressEndpoints  member ingress endpoints          (default from SEQERON_HOSTS,
                                                                              else three on localhost)
              clusterctl.egressHost        host the leader replies to        (default this member's host in
                                                                              SEQERON_HOSTS, else localhost)""");
    }
}
