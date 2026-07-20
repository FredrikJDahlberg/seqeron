package org.limitless.phixeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;

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
 *   sequencer.clusterMembers  — full clusterMembers string (Aeron format);
 *                               default: single-node localhost
 *   sequencer.baseDir         — data directory root; default /tmp/phixeron-seq
 *   sequencer.aeronDir        — Aeron media driver directory
 * </pre>
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
    private static final String PROP_CLUSTER_MEMBERS = "sequencer.clusterMembers";
    private static final String PROP_BASE_DIR = "sequencer.baseDir";
    private static final String PROP_AERON_DIR = "sequencer.aeronDir";

    private static final String DEFAULT_HOST = "localhost";
    private static final int PORT_BASE = 9300;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String baseDir
            = System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/phixeron-seq");
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + memberId);

        final int portBase = PORT_BASE + memberId * 10;
        final int archivePort = portBase + 1;
        final int ingressPort = portBase + 2;
        final int memberPort = portBase + 3;
        final int logPort = portBase + 4;
        final int xferPort = portBase + 5;

        final String clusterMembers = System.getProperty(
            PROP_CLUSTER_MEMBERS, buildSingleNodeMembers(ingressPort, memberPort, logPort, xferPort, archivePort));

        final File archiveDir = new File(baseDir + "/archive-" + memberId);
        final File clusterDir = new File(baseDir + "/cluster-" + memberId);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                                                  .aeronDirectoryName(aeronDir)
                                                  .threadingMode(ThreadingMode.DEDICATED)
                                                  .conductorIdleStrategy(new BusySpinIdleStrategy())
                                                  .senderIdleStrategy(new BusySpinIdleStrategy())
                                                  .receiverIdleStrategy(new BusySpinIdleStrategy())
                                                  .dirDeleteOnStart(true);

        final AeronArchive.Context localArchiveCtx = new AeronArchive.Context()
                                                         .lock(NoOpLock.INSTANCE)
                                                         .controlRequestChannel("aeron:ipc")
                                                         .controlRequestStreamId(100)
                                                         .controlResponseChannel("aeron:ipc")
                                                         .controlResponseStreamId(101)
                                                         .aeronDirectoryName(aeronDir);

        final Archive.Context archiveCtx
            = new Archive.Context()
                  .aeronDirectoryName(aeronDir)
                  .archiveDir(archiveDir)
                  .controlChannel(udp(DEFAULT_HOST, archivePort)) // UDP: remote clients reach the archive here
                  .controlStreamId(100) // must match C++ clients
                  .localControlChannel("aeron:ipc")
                  .localControlStreamId(100)
                  .replicationChannel(udp(DEFAULT_HOST, 0))
                  .recordingEventsEnabled(false)
                  .deleteArchiveOnStart(false)
                  .idleStrategySupplier(YieldingIdleStrategy::new);

        // Wire the ShutdownSignalBarrier to the consensus module's termination hook: a
        // ClusterTool/ClusterControl ABORT otherwise terminates the consensus and service agents
        // without waking barrier.await() below, leaving the ClusteredMediaDriver (and its Archive)
        // unclosed and the recorded log's catalog unflushed. Signalling the barrier drives the clean
        // try-with-resources teardown (Archive.close flushes the catalog), which is what keeps the tap
        // recording replayable/analysable after an orderly stop.
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final ConsensusModule.Context consensusCtx
            = new ConsensusModule.Context()
                  .aeronDirectoryName(aeronDir)
                  .clusterMemberId(memberId)
                  .clusterMembers(clusterMembers)
                  .clusterDir(clusterDir)
                  .ingressChannel(udp(DEFAULT_HOST, ingressPort))
                  .replicationChannel(udp(DEFAULT_HOST, 0))
                  .archiveContext(localArchiveCtx.clone())
                  // Lets a co-located client (sharing this member's Aeron directory, e.g.
                  // OrderExecClient) reach ingress over "aeron:ipc" while this member is leader,
                  // on top of the normal UDP ingressChannel above — never instead of it, and only
                  // while leader (see ConsensusModuleAgent.connectIngress()).
                  .isIpcIngressAllowed(true)
                  .terminationHook(barrier::signalAll)
                  .deleteDirOnStart(false)
                  .idleStrategySupplier(YieldingIdleStrategy::new)
                  .errorHandler(t -> {
                      System.err.printf("[ConsensusModule/%d] %s%n", memberId, t.getMessage());
                      t.printStackTrace();
                  });

        final ClusteredServiceContainer.Context serviceCtx
            = new ClusteredServiceContainer.Context()
                  .aeronDirectoryName(aeronDir)
                  .archiveContext(localArchiveCtx.clone())
                  .clusterDir(clusterDir)
                  .clusteredService(new SequencerService())
                  .idleStrategySupplier(YieldingIdleStrategy::new)
                  .errorHandler(t -> {
                      System.err.printf("[SequencerService/%d] %s%n", memberId, t.getMessage());
                      t.printStackTrace();
                  });

        System.out.printf("[SequencerNode] Starting member %d | ingress=%s | archive=%s | baseDir=%s%n", memberId,
                          udp(DEFAULT_HOST, ingressPort), udp(DEFAULT_HOST, archivePort), baseDir);

        try (barrier;
             ClusteredMediaDriver cmd = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.printf("[SequencerNode/%d] Running — Ctrl-C to stop%n", memberId);
            barrier.await();
        } finally {
            System.out.printf("[SequencerNode/%d] Shutdown complete%n", memberId);
        }
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    private static String buildSingleNodeMembers(final int ingressPort, final int memberPort, final int logPort,
                                                 final int xferPort, final int archivePort) {
        return "0," + DEFAULT_HOST + ":" + ingressPort + "," + DEFAULT_HOST + ":" + memberPort + "," + DEFAULT_HOST
            + ":" + logPort + "," + DEFAULT_HOST + ":" + xferPort + "," + DEFAULT_HOST + ":" + archivePort + "|";
    }
}
