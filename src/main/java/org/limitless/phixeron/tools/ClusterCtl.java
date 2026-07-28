package org.limitless.phixeron.tools;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.phixeron.sbe.sequenced.ClusterStartedDecoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.ClusterStartedEncoder;
import org.limitless.phixeron.sbe.unsequenced.ClusterStoppedEncoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sequencer.SequencerService;

/**
 * clusterctl — the operator cluster life-cycle tool (see clusterctl.md). Node-local: run co-located
 * on a {@code SequencerNode} host, sharing that node's Aeron directory (to reach the co-located tap
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
 *       consensus-coordinated, snapshot-free termination of every node. Because {@code SequencerNode}
 *       wires its termination hook, that abort unwinds each node cleanly through try-with-resources,
 *       closing the Archive and draining the tap recording (including the just-observed
 *       {@code ClusterStopped}) to disk — so the log stays replayable/analysable afterwards. Best
 *       effort: if the echo does not arrive (an unhealthy cluster — often why one stops early), it
 *       aborts anyway, still via {@code ABORT} rather than SIGKILL, so the log is preserved.</li>
 *   <li><b>help</b> — usage.</li>
 *   <li><i>anything else</i> — passed through to {@link ClusterTool} against this node's
 *       {@code clusterDir} (describe, errors, list-members, recording-log, …).</li>
 * </ul>
 *
 * <p>Configuration mirrors {@code SequencerNode}'s defaults so co-location with the default
 * single-node cluster works with no arguments (see {@link #usage()} / {@code clusterctl.sh}).
 */
public final class ClusterCtl {
    // ── Configuration (mirrors SequencerNode's property defaults for co-location) ──
    private static final int MEMBER_ID = Integer.getInteger("clusterctl.memberId", 0);
    private static final String BASE_DIR
        = System.getProperty("clusterctl.baseDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq");
    private static final String AERON_DIR = System.getProperty(
        "clusterctl.aeronDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + MEMBER_ID);
    private static final File CLUSTER_DIR = new File(BASE_DIR + "/cluster-" + MEMBER_ID);
    private static final String INGRESS_ENDPOINTS
        = System.getProperty("clusterctl.ingressEndpoints", "0=localhost:9302");

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long OFFER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** header.sourceId/connectionId for markers this tool submits: no gateway process/TCP connection. */
    private static final int NO_ID = -1;

    private static final IdleStrategy IDLE = new YieldingIdleStrategy();
    private static final EgressListener NULL_EGRESS = (sessionId, timestamp, buffer, offset,
                                                       length, header) -> { };

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
            default:
                passthrough(args); // io.aeron.cluster.ClusterTool
                break;
        }
    }

    // ── start ─────────────────────────────────────────────────────────────────

    private static int start() {
        final long correlationId = System.nanoTime();
        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo
                = publishMarkerAndAwaitEcho(cluster, ClusterStartedEncoder.TEMPLATE_ID, correlationId);
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

    // ── shutdown ────────────────────────────────────────────────────────────────

    private static int shutdown() {
        // Leader gate — node-local, no cluster connection. isLeader() returns 0 only when this node is
        // the elected leader; anything else (follower, or cluster not running) is a no-op so the command
        // is safe to fire on every node.
        if (ClusterTool.isLeader(System.out, CLUSTER_DIR) != 0) {
            System.out.println("[clusterctl] shutdown: this node is not the leader — nothing to do");
            return 0;
        }

        final long correlationId = System.nanoTime();
        try (AeronCluster cluster = connectCluster()) {
            final long globalSeqNo
                = publishMarkerAndAwaitEcho(cluster, ClusterStoppedEncoder.TEMPLATE_ID, correlationId);
            if (globalSeqNo < 0) {
                System.err.println("[clusterctl] shutdown: no ClusterStopped echo within timeout — aborting anyway");
            } else {
                System.out.printf("[clusterctl] shutdown: system-stopped recorded at globalSeqNo=%d%n", globalSeqNo);
            }
        } catch (final Exception ex) {
            System.err.println(
                "[clusterctl] shutdown: could not publish ClusterStopped (" + ex.getMessage() + ") — aborting anyway");
        }

        // Orderly, consensus-coordinated termination of every node. ABORT takes no snapshot (preserving
        // this system's full-log-replay recovery), and SequencerNode's wired terminationHook makes each
        // node unwind through try-with-resources and close its Archive cleanly, draining the tap recording
        // (including the ClusterStopped observed above) to disk — so the log stays analysable. See
        // clusterctl.md.
        if (!ClusterTool.abort(CLUSTER_DIR, System.out)) {
            System.err.println("[clusterctl] shutdown: ClusterTool.abort failed");
            return 1;
        }
        System.out.println("[clusterctl] shutdown: cluster abort requested");
        return 0;
    }

    // ── passthrough ─────────────────────────────────────────────────────────────

    private static void passthrough(final String[] args) {
        // ClusterTool.main expects args[0] = clusterDir, args[1..] = command + its arguments.
        final String[] toolArgs = new String[args.length + 1];
        toolArgs[0] = CLUSTER_DIR.getAbsolutePath();
        System.arraycopy(args, 0, toolArgs, 1, args.length);
        ClusterTool.main(toolArgs);
    }

    // ── Marker publish + echo ────────────────────────────────────────────────────

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
    private static long publishMarkerAndAwaitEcho(
        final AeronCluster cluster, final int templateId, final long correlationId) {
        // Attach to the tap before publishing so the echo cannot be missed.
        final Subscription tap = cluster.context().aeron()
            .addSubscription(SequencerService.FEEDER_CHANNEL, SequencerService.FEEDER_STREAM_ID);
        final long connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= connectDeadline) {
                System.err.printf("[clusterctl] tap (aeron:ipc/%d) not available — co-located with a SequencerNode?%n",
                                  SequencerService.FEEDER_STREAM_ID);
                return -1;
            }
            cluster.pollEgress();
            IDLE.idle();
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

    private static int encodeMarker(final ExpandableArrayBuffer buffer, final int templateId, final long correlationId) {
        // ClusterStarted (10) and ClusterStopped (11) are byte-identical past the header composite, so
        // this differs only in the encoder chosen for the outer template id.
        if (templateId == ClusterStartedEncoder.TEMPLATE_ID) {
            final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
            encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
            encoder.header().sourceId(NO_ID).connectionId(NO_ID).sessionId(NO_ID);
            encoder.correlationId(correlationId);
            return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        }
        final ClusterStoppedEncoder encoder = new ClusterStoppedEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(NO_ID).connectionId(NO_ID).sessionId(NO_ID);
        encoder.correlationId(correlationId);
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
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
        private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
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
            messageHeader.wrap(buffer, offset);
            if (messageHeader.schemaId() != ClusterStartedDecoder.SCHEMA_ID
                || messageHeader.templateId() != templateId) {
                return;
            }
            marker.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                        messageHeader.version());
            if (marker.correlationId() == correlationId) {
                globalSeqNo = marker.header().globalSeqNo();
                found = true;
            }
        }
    }

    private static void usage() {
        System.out.println("""
            clusterctl — cluster life-cycle tool (node-local; run co-located with a SequencerNode)

            Usage: clusterctl.sh <command> [args]

              start        record a "system started" marker (requires an elected leader)
              shutdown     orderly stop; safe to run on every node, no-op on followers
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
