package org.limitless.seqeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * Launches one Sequencer cluster node: an Aeron Cluster with an embedded media driver, archive and
 * consensus module, running {@link SequencerService}.
 *
 * <p><b>Port layout</b> (member 0 on the base; members 1 and 2 use base+10, base+20). The base is 9300
 * unless {@code SEQERON_PORT_BASE} overrides it (see {@link PortLayout}); the reserved block is wider than
 * what three members bind (doc/registries.md §2):
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
 *   sequencer.host            — hostname the archive control, ingress and replication channels bind to
 *                               and advertise; default localhost. With one member per host it must be
 *                               this member's resolvable name.
 *   sequencer.baseDir         — data directory root; default /tmp/seqeron-seq
 *   sequencer.aeronDir        — Aeron media driver directory
 *   sequencer.idleStrategy    — idle strategy of every agent: {@code backoff} (default), {@code yielding}
 *                               or {@code busyspin}. Busy-spin pays only when each agent owns a core.
 *   sequencer.sessionTimeoutMs — how long the cluster keeps a client session with no keep-alives; default
 *                               1000. A gateway that misses it is replaced by its standby, so raise it on
 *                               an oversubscribed host.
 * </pre>
 *
 * <p>Single-node launch example:
 * <pre>
 *   java -Dsequencer.memberId=0 \
 *        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        -cp seqeron-uber.jar \
 *        org.limitless.seqeron.sequencer.SequencerServer
 * </pre>
 */
public final class SequencerServer {
    private static final String PROP_MEMBER_ID = "sequencer.memberId";
    private static final String PROP_NODE_COUNT = "sequencer.nodeCount";
    private static final String PROP_CLUSTER_MEMBERS = "sequencer.clusterMembers";
    private static final String PROP_HOST = "sequencer.host";
    private static final String PROP_BASE_DIR = "sequencer.baseDir";
    private static final String PROP_AERON_DIR = "sequencer.aeronDir";
    private static final String PROP_IDLE_STRATEGY = "sequencer.idleStrategy";
    private static final String PROP_SESSION_TIMEOUT_MS = "sequencer.sessionTimeoutMs";

    private static final long DEFAULT_SESSION_TIMEOUT_MS = 1000;

    /**
     * Control-response stream of this member's own archive clients. Must not be 101, the cluster's IPC
     * ingress stream, or archive replies misdecode as ingress; nor ReplayerServer's 120 on the same driver.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 121;

    /** Exit status of a node that stopped because it could no longer record its tap; restart it. */
    static final int EXIT_TAP_FATAL = 70;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final int nodeCount = Integer.getInteger(PROP_NODE_COUNT, 1);
        final String host = System.getProperty(PROP_HOST, PortLayout.DEFAULT_HOST);
        final String baseDir =
            System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq");
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);
        final int archivePort = PortLayout.archivePort(memberId);
        final int ingressPort = PortLayout.ingressPort(memberId);

        final String clusterMembers = System.getProperty(PROP_CLUSTER_MEMBERS, buildClusterMembers(nodeCount));

        final File archiveDir = new File(baseDir + "/archive-" + memberId);
        final File clusterDir = new File(baseDir + "/cluster-" + memberId);

        final Supplier<IdleStrategy> idleStrategySupplier = IdleStrategies.fromProperty(PROP_IDLE_STRATEGY);
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                                                  .aeronDirectoryName(aeronDir)
                                                  .threadingMode(ThreadingMode.DEDICATED)
                                                  .conductorIdleStrategy(idleStrategySupplier.get())
                                                  .senderIdleStrategy(idleStrategySupplier.get())
                                                  .receiverIdleStrategy(idleStrategySupplier.get())
                                                  .dirDeleteOnStart(true);

        final AeronArchive.Context localArchiveCtx = new AeronArchive.Context()
                                                         .lock(NoOpLock.INSTANCE)
                                                         .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                         .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                                         .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                         .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
                                                         .aeronDirectoryName(aeronDir);

        final Archive.Context archiveCtx =
            new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDir(archiveDir)
                .controlChannel(udp(host, archivePort)) // UDP: remote clients reach the archive here
                .controlStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                .localControlChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                .localControlStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                .replicationChannel(udp(host, 0))
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(false)
                .idleStrategySupplier(idleStrategySupplier);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ConsensusModule.Context consensusCtx =
            new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir)
                .clusterMemberId(memberId)
                .clusterMembers(clusterMembers)
                .clusterClock(new NanosecondClusterClock())
                .clusterDir(clusterDir)
                .ingressChannel(udp(host, ingressPort))
                .replicationChannel(udp(host, 0))
                .archiveContext(localArchiveCtx.clone())
                .isIpcIngressAllowed(true) // Lets a co-located client share aeron directory
                .terminationHook(barrier::signalAll)
                .deleteDirOnStart(false)
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
                .leaderHeartbeatTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
                .electionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(200))
                .electionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .sessionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(
                    Long.getLong(PROP_SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS)))
                .idleStrategySupplier(idleStrategySupplier)
                .errorHandler(t
                              -> Logger.error(Logger.CoreComponent.ConsensusModule,
                                              Logger.CoreEventCode.ConsensusModuleError,
                                              memberId, "%s", t.getMessage()));

        final AtomicBoolean tapFatal = new AtomicBoolean();
        final SequencerService service = new SequencerService(() -> {
            tapFatal.set(true);
            barrier.signalAll();
        });

        final ClusteredServiceContainer.Context serviceCtx =
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(localArchiveCtx.clone())
                .clusterDir(clusterDir)
                .clusteredService(service)
                .terminationHook(barrier::signalAll)
                .idleStrategySupplier(idleStrategySupplier)
                .errorHandler(t
                              -> Logger.error(Logger.CoreComponent.SequencerService, Logger.CoreEventCode.ServiceError,
                                              memberId, "%s", t.getMessage()));

        Logger.info(Logger.CoreComponent.SequencerServer, memberId,
                    "Starting member %d | ingress=%s | archive=%s | baseDir=%s | idle=%s", memberId,
                    udp(host, ingressPort), udp(host, archivePort), baseDir,
                    System.getProperty(PROP_IDLE_STRATEGY, "backoff"));

        try (barrier; ClusteredMediaDriver cmd = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceCtx)) {
            Logger.info(Logger.CoreComponent.SequencerServer, memberId, "Running — Ctrl-C to stop");
            barrier.await();
        } finally {
            Logger.info(Logger.CoreComponent.SequencerServer, memberId, "Shutdown complete");
        }
        if (tapFatal.get()) {
            System.exit(EXIT_TAP_FATAL);
        }
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    /** The {@code clusterMembers} string for a {@code nodeCount}-member cluster on {@link PortLayout#DEFAULT_HOST}. */
    static String buildClusterMembers(final int nodeCount) {
        final StringBuilder members = new StringBuilder();
        for (int id = 0; id < nodeCount; id++) {
            final String host = PortLayout.DEFAULT_HOST;
            members.append(id)
                .append(',').append(host).append(':').append(PortLayout.ingressPort(id))
                .append(',').append(host).append(':').append(PortLayout.consensusPort(id))
                .append(',').append(host).append(':').append(PortLayout.logPort(id))
                .append(',').append(host).append(':').append(PortLayout.transferPort(id))
                .append(',').append(host).append(':').append(PortLayout.archivePort(id))
                .append('|');
        }
        return members.toString();
    }
}
