package org.limitless.phixeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

/**
 * Launches a single FIX Sequencer cluster node.
 * <p>
 * Three-node cluster port layout (member 0 shown; members 1 and 2 follow the same
 * pattern on the same or different hosts, with memberId and ports adjusted):
 * <p>
 *   Base: 9100 + memberId × 10
 *     +0  (reserved)
 *     +1  archive control       (9101, 9111, 9121)
 *     +2  ingress / cluster     (9102, 9112, 9122)
 *     +3  consensus             (9103, 9113, 9123)
 *     +4  cluster log           (9104, 9114, 9124)
 *     +5  file transfer         (9105, 9115, 9125)
 * <p>
 * System properties (all required in multi-node mode):
 *   phixeron.memberId          — this node's Raft member ID (0, 1, or 2)
 *   phixeron.clusterMembers    — full clusterMembers string (Aeron format)
 *                                Default: single-node localhost configuration
 *   phixeron.baseDir           — data directory root (default: /tmp/phixeron)
 *   phixeron.aeronDir          — Aeron media driver directory
 * <p>
 * Example single-node launch:
 *   java -Dphixeron.memberId=0 -cp phixeron-uber.jar \
 *        org.limitless.phixeron.sequencer.FixSequencerNode
 */
public final class FixSequencerNode {
    private static final String PROP_MEMBER_ID       = "phixeron.memberId";
    private static final String PROP_CLUSTER_MEMBERS = "phixeron.clusterMembers";
    private static final String PROP_BASE_DIR        = "phixeron.baseDir";
    private static final String PROP_AERON_DIR       = "phixeron.aeronDir";

    private static final String DEFAULT_HOST         = "localhost";
    private static final int    PORT_BASE            = 9100;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String baseDir = System.getProperty(PROP_BASE_DIR,
            System.getProperty("java.io.tmpdir") + "/phixeron");
        final String aeronDir = System.getProperty(PROP_AERON_DIR,
            System.getProperty("java.io.tmpdir") + "/phixeron-aeron-" + memberId);

        final int portBase = PORT_BASE + memberId * 10;
        final int archivePort = portBase + 1;
        final int ingressPort = portBase + 2;
        final int memberPort = portBase + 3;
        final int logPort = portBase + 4;
        final int xferPort = portBase + 5;

        final String clusterMembers = System.getProperty(
            PROP_CLUSTER_MEMBERS,
            buildSingleNodeMembers(ingressPort, memberPort, logPort, xferPort, archivePort));

        final File archiveDir = new File(baseDir + "/archive-" + memberId);
        final File clusterDir = new File(baseDir + "/cluster-" + memberId);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
            .senderIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
            .receiverIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
            .dirDeleteOnStart(true);

        final AeronArchive.Context localArchiveCtx = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel("aeron:ipc")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:ipc")
            .controlResponseStreamId(101)
            .aeronDirectoryName(aeronDir);

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(archiveDir)
            .controlChannel(udp(DEFAULT_HOST, archivePort))
            .localControlChannel("aeron:ipc")
            .localControlStreamId(100)
            .replicationChannel(udp(DEFAULT_HOST, 0))
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(false);   // preserve on restart for failover recovery

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
            .aeronDirectoryName(aeronDir)
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .clusterDir(clusterDir)
            .ingressChannel(udp(DEFAULT_HOST, ingressPort))
            .replicationChannel(udp(DEFAULT_HOST, 0))
            .archiveContext(localArchiveCtx.clone())
            .deleteDirOnStart(false)        // preserve across restarts
            .errorHandler(t -> {
                System.err.printf("[ConsensusModule/%d] %s%n", memberId, t.getMessage());
                t.printStackTrace(System.err);
            });

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .archiveContext(localArchiveCtx.clone())
            .clusterDir(clusterDir)
            .clusteredService(new FixAeronHandler())
            .errorHandler(t -> {
                System.err.printf("[ClusteredService/%d] %s%n", memberId, t.getMessage());
                t.printStackTrace(System.err);
            });

        System.out.printf("[FixSequencerNode] Starting member %d | ingress=%s | baseDir=%s%n",
            memberId, udp(DEFAULT_HOST, ingressPort), baseDir);

        try (final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
             ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.printf("[FixSequencerNode/%d] Cluster node started — Ctrl-C to stop%n", memberId);
            barrier.await();
        }
        finally {
            System.out.printf("[FixSequencerNode/%d] Shutting down%n", memberId);
        }
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    private static String buildSingleNodeMembers(final int ingressPort,
                                                 final int memberPort,
                                                 final int logPort,
                                                 final int xferPort,
                                                 final int archivePort)
    {
        return "0," + DEFAULT_HOST + ":" + ingressPort
             + "," + DEFAULT_HOST + ":" + memberPort
             + "," + DEFAULT_HOST + ":" + logPort
             + "," + DEFAULT_HOST + ":" + xferPort
             + "," + DEFAULT_HOST + ":" + archivePort + "|";
    }
}