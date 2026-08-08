package org.limitless.phixeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.phixeron.util.Logger;

/**
 * Launches a single Sequencer cluster node.
 *
 * <p>The Sequencer runs as an Aeron Cluster with an embedded MediaDriver, Archive, and
 * ConsensusModule.  Clients connect via the Aeron Cluster ingress protocol to send
 * {@code AppMessage}s.  Every node stamps each message with a global sequence number and
 * a per-source application sequence number, then publishes the result on its node-local
 * {@code aeron:ipc} tap ({@link SequencerService#FEEDER_CHANNEL}) which is simultaneously
 * recorded by the co-located Archive for client replay on startup.
 *
 * <p><b>Port layout</b> (member 0 on base 9300; members 1 and 2 use base+10, base+20):
 * <pre>
 *   +1  archive control   (9301, 9311, 9321)
 *   +2  cluster ingress   (9302, 9312, 9322)
 *   +3  consensus (Raft)  (9303, 9313, 9323)
 *   +4  cluster log       (9304, 9314, 9324)
 *   +5  file transfer     (9305, 9315, 9325)
 * </pre>
 *
 * <p><b>System properties</b>:
 * <pre>
 *   sequencer.memberId        — this node's Raft member ID (0, 1, or 2); default 0
 *   sequencer.nodeCount       — cluster size; used to generate clusterMembers when that
 *                               property is not set explicitly; default 1
 *   sequencer.clusterMembers  — full clusterMembers string (Aeron format); overrides
 *                               nodeCount-based generation when set
 *   sequencer.baseDir         — data directory root; default /tmp/phixeron-seq
 *   sequencer.aeronDir        — Aeron media driver directory
 *   sequencer.idleStrategy    — duty-cycle idle strategy for the driver/archive/consensus/service
 *                               agents: {@code backoff} (default), {@code yielding}, or {@code busyspin}.
 *                               Busy-spin only pays off when each agent owns an isolated core; on an
 *                               oversubscribed host (e.g. this cluster's 3 members plus their co-located
 *                               replayer/consumer/gateway processes sharing one dev machine) it starves
 *                               everything else instead. Backoff spins briefly, then yields, then sleeps
 *                               with escalating backoff — cheap when idle, still prompt when busy.
 * </pre>
 *
 * <p>Gateway topology (which sourceIds are FIX gateways, and which gatewayId is the designated
 * primary) is no longer configured here — the sequencer derives it from the {@code Gateway}
 * basic-data rows in the log (doc/todo.md item 8c).
 *
 * <p>Single-node launch example:
 * <pre>
 *   java -Dsequencer.memberId=0 \
 *        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        -cp phixeron-uber.jar \
 *        org.limitless.phixeron.sequencer.SequencerNode
 * </pre>
 */
public final class SequencerNode {
    private static final String PROP_MEMBER_ID = "sequencer.memberId";
    private static final String PROP_NODE_COUNT = "sequencer.nodeCount";
    private static final String PROP_CLUSTER_MEMBERS = "sequencer.clusterMembers";
    private static final String PROP_BASE_DIR = "sequencer.baseDir";
    private static final String PROP_AERON_DIR = "sequencer.aeronDir";
    private static final String PROP_IDLE_STRATEGY = "sequencer.idleStrategy";

    private static final String DEFAULT_HOST = "localhost";
    private static final int PORT_BASE = 9300;

    /**
     * Control-response stream for this member's own archive clients (ConsensusModule +
     * ClusteredServiceContainer). Must not be 101: that's Aeron Cluster's default
     * {@code ingressStreamId}, and isIpcIngressAllowed(true) makes the leader subscribe to
     * ingress on aeron:ipc/101 too — sharing it with the archive response stream means every
     * archive reply misdecodes as an ingress frame (and vice versa). Also distinct from
     * ReplayerNode's ARCHIVE_CONTROL_RESPONSE_STREAM_ID (120), which shares this member's
     * aeron:ipc driver.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 121;

    /**
     * Exit status of a node that stopped because it could no longer record its tap (see {@code
     * SequencerService.fatalTapFailure}), as opposed to the 0 of an orderly shutdown — the signal process
     * supervision needs to tell "restart me" from "I was told to stop".
     */
    static final int EXIT_TAP_FATAL = 70;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final int nodeCount = Integer.getInteger(PROP_NODE_COUNT, 1);
        final String baseDir = System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") +
            "/phixeron-seq");
        final String aeronDir = System.getProperty(PROP_AERON_DIR, System.getProperty("java.io.tmpdir") +
            "/phixeron-seq-aeron-" + memberId);
        final int portBase = PORT_BASE + memberId * 10;
        final int archivePort = portBase + 1;
        final int ingressPort = portBase + 2;
        final int memberPort = portBase + 3;
        final int logPort = portBase + 4;
        final int transferPort = portBase + 5;

        final String clusterMembers = System.getProperty(PROP_CLUSTER_MEMBERS, buildClusterMembers(nodeCount));

        final File archiveDir = new File(baseDir + "/archive-" + memberId);
        final File clusterDir = new File(baseDir + "/cluster-" + memberId);

        final Supplier<IdleStrategy> idleStrategySupplier = resolveIdleStrategySupplier();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                                                  .aeronDirectoryName(aeronDir)
                                                  .threadingMode(ThreadingMode.DEDICATED)
                                                  .conductorIdleStrategy(idleStrategySupplier.get())
                                                  .senderIdleStrategy(idleStrategySupplier.get())
                                                  .receiverIdleStrategy(idleStrategySupplier.get())
                                                  .dirDeleteOnStart(true);

        final AeronArchive.Context localArchiveCtx = new AeronArchive.Context()
                                                         .lock(NoOpLock.INSTANCE)
                                                         .controlRequestChannel("aeron:ipc")
                                                         .controlRequestStreamId(100)
                                                         .controlResponseChannel("aeron:ipc")
                                                         .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
                                                         .aeronDirectoryName(aeronDir);

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(archiveDir)
            .controlChannel(udp(DEFAULT_HOST, archivePort)) // UDP: remote clients reach the archive here
            .controlStreamId(100) // must match C++ clients
            .localControlChannel("aeron:ipc")
            .localControlStreamId(100)
            .replicationChannel(udp(DEFAULT_HOST, 0))
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(false)
            .idleStrategySupplier(idleStrategySupplier);

        // Wire the ShutdownSignalBarrier to the consensus module's termination hook: a
        // ClusterTool/ClusterControl ABORT otherwise terminates the consensus and service agents
        // without waking barrier.await() below, leaving the ClusteredMediaDriver (and its Archive)
        // unclosed and the recorded log's catalog unflushed. Signalling the barrier drives the clean
        // try-with-resources teardown (Archive.close flushes the catalog), which is what keeps the tap
        // recording replayable/analysable after an orderly stop.
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
            .aeronDirectoryName(aeronDir)
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .clusterDir(clusterDir)
            .ingressChannel(udp(DEFAULT_HOST, ingressPort))
            .replicationChannel(udp(DEFAULT_HOST, 0))
            .archiveContext(localArchiveCtx.clone())
            .isIpcIngressAllowed(true) // Lets a co-located client share aeron directory
            .terminationHook(barrier::signalAll)
            .deleteDirOnStart(false)
            .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
            .leaderHeartbeatTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .electionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
            .electionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
            .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
            .idleStrategySupplier(idleStrategySupplier)
            .errorHandler(t -> {
                Logger.error(Logger.Component.ConsensusModule, Logger.EventCode.ConsensusModuleError,
                        memberId, "%s", t.getMessage());
                t.printStackTrace();
            });

        // A node that can no longer record its own tap must not keep sequencing history it cannot keep
        // (SequencerService.fatalTapFailure): take the same barrier path an operator shutdown takes, so the
        // Archive still gets its clean close, and remember to exit non-zero afterwards so process
        // supervision restarts the node — the restart's full-log replay is what rebuilds its recording.
        final AtomicBoolean tapFatal = new AtomicBoolean();
        final SequencerService service = new SequencerService(() ->
        {
            tapFatal.set(true);
            barrier.signalAll();
        });

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .archiveContext(localArchiveCtx.clone())
            .clusterDir(clusterDir)
            .clusteredService(service)
            // The container's own hook defaults to a no-op, so a service agent that terminates by itself
            // (AgentTerminationException) would otherwise leave this process running headless: media driver
            // and consensus module up, no service behind them.
            .terminationHook(barrier::signalAll)
            .idleStrategySupplier(idleStrategySupplier)
            .errorHandler(t -> {
                Logger.error(Logger.Component.SequencerService, Logger.EventCode.ServiceError, memberId,
                        "%s", t.getMessage());
                t.printStackTrace();
            });

        Logger.info(Logger.Component.SequencerNode, memberId,
                "Starting member %d | ingress=%s | archive=%s | baseDir=%s | idle=%s",
                memberId, udp(DEFAULT_HOST, ingressPort), udp(DEFAULT_HOST, archivePort), baseDir,
                System.getProperty(PROP_IDLE_STRATEGY, "backoff"));

        try (barrier;
             ClusteredMediaDriver cmd = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceCtx)) {
            Logger.info(Logger.Component.SequencerNode, memberId, "Running — Ctrl-C to stop");
            barrier.await();
        } finally {
            Logger.info(Logger.Component.SequencerNode, memberId, "Shutdown complete");
        }
        if (tapFatal.get()) {
            System.exit(EXIT_TAP_FATAL);
        }
    }

    /**
     * Resolves the driver/archive/consensus/service idle strategy from sequencer.idleStrategy
     * (case-insensitive); see the class Javadoc for why the default is backoff.
     * @return idle strategy supplier — one instance is created per agent thread, never shared
     */
    private static Supplier<IdleStrategy> resolveIdleStrategySupplier() {
        final String name = System.getProperty(PROP_IDLE_STRATEGY, "backoff");
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "busyspin" -> BusySpinIdleStrategy::new;
            case "yielding" -> YieldingIdleStrategy::new;
            case "backoff" -> BackoffIdleStrategy::new;
            default -> throw new IllegalArgumentException(
                "Unknown " + PROP_IDLE_STRATEGY + "=" + name + " (expected 'backoff', 'yielding', or 'busyspin')");
        };
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    /**
     * A member's cluster-ingress endpoint ("host:port"), per this class's {@link #PORT_BASE}
     * port-layout formula. Exposed so other callers co-located with the default single-node
     * cluster (e.g. {@code ClusterCtl}) can derive their default from here instead of restating
     * the port number.
     */
    public static String ingressEndpoint(final int memberId) {
        return DEFAULT_HOST + ":" + (PORT_BASE + memberId * 10 + 2);
    }

    /**
     * Generates the Aeron {@code clusterMembers} string for a {@code nodeCount}-member cluster,
     * all on {@link #DEFAULT_HOST}, using this class's {@link #PORT_BASE} port-layout formula
     * (see the class Javadoc). This is the single source of truth other launchers (shell scripts)
     * should defer to rather than restating the port numbers themselves.
     */
    static String buildClusterMembers(final int nodeCount) {
        final StringBuilder members = new StringBuilder();
        for (int id = 0; id < nodeCount; id++) {
            final int base = PORT_BASE + id * 10;
            members.append(id).append(',')
                   .append(DEFAULT_HOST).append(':').append(base + 2).append(',')
                   .append(DEFAULT_HOST).append(':').append(base + 3).append(',')
                   .append(DEFAULT_HOST).append(':').append(base + 4).append(',')
                   .append(DEFAULT_HOST).append(':').append(base + 5).append(',')
                   .append(DEFAULT_HOST).append(':').append(base + 1).append('|');
        }
        return members.toString();
    }
}
