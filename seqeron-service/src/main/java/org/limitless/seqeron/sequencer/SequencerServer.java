package org.limitless.seqeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.server.NodeDriver;
import org.limitless.seqeron.replayer.server.ReplayerServer;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * Launches one Sequencer cluster node: an Aeron Cluster with an embedded media driver, archive and
 * consensus module, running {@link SequencerService}, and the node's {@link ReplayerServer} on the same driver.
 *
 * <p><b>Port layout</b> (member m on base + 10m, for up to seven members; three shown). The base is 9300
 * unless {@code SEQERON_PORT_BASE} overrides it (see {@link PortLayout}); the reserved block is wider than
 * what seven members bind (doc/ops.md, "Ports"):
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
 *   sequencer.hosts           — every member's host name, comma-separated in member-id order; generates
 *                               clusterMembers. Default SEQERON_HOSTS, else one member on localhost. With
 *                               more than one member, sequencer.baseDir is required.
 *   sequencer.host            — hostname the archive control, ingress and replication channels bind to
 *                               and advertise; default this member's entry in sequencer.hosts, else
 *                               localhost
 *   sequencer.baseDir         — data directory root; default /tmp/seqeron-seq
 *   sequencer.aeronDir        — Aeron media driver directory
 *   sequencer.idleStrategy    — idle strategy of every agent, the Replayer's included: {@code backoff}
 *                               (default), {@code yielding} or {@code busyspin}. Busy-spin pays only when
 *                               each agent owns a core.
 *   sequencer.sessionTimeoutMs — how long the cluster keeps a client session with no keep-alives; default
 *                               1000. A gateway that misses it is replaced by its standby, so raise it on
 *                               an oversubscribed host.
 *   aeron.ipc.term.buffer.length — the embedded driver's IPC term length; default 16m (doc/ops.md, "Term lengths")
 *   aeron.timer.interval, aeron.untethered.window.limit.timeout, aeron.untethered.linger.timeout — default
 *                               10ms, 100ms, 100ms (doc/ops.md, "Untethered subscribers")
 * </pre>
 *
 * <p>Launch example, member 0 of three:
 * <pre>
 *   java -Dsequencer.memberId=0 -Dsequencer.hosts=h0,h1,h2 -Dsequencer.baseDir=/var/lib/seqeron \
 *        -jar seqeron-uber.jar
 * </pre>
 */
public final class SequencerServer {
    private static final String PROP_MEMBER_ID = "sequencer.memberId";
    private static final String PROP_HOSTS = "sequencer.hosts";
    private static final String PROP_HOST = "sequencer.host";
    private static final String PROP_BASE_DIR = "sequencer.baseDir";
    private static final String PROP_AERON_DIR = "sequencer.aeronDir";
    private static final String PROP_IDLE_STRATEGY = "sequencer.idleStrategy";
    private static final String PROP_SESSION_TIMEOUT_MS = "sequencer.sessionTimeoutMs";

    private static final long DEFAULT_SESSION_TIMEOUT_MS = 1000;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final List<String> hosts = resolveHosts(System.getProperty(PROP_HOSTS),
                                                System.getProperty(PROP_BASE_DIR) != null, PortLayout.HOSTS, memberId);
        final String host = System.getProperty(PROP_HOST, hosts.get(memberId));
        final String baseDir =
            System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq");
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);
        final int archivePort = PortLayout.archivePort(memberId);
        final int ingressPort = PortLayout.ingressPort(memberId);

        final String clusterMembers = buildClusterMembers(hosts);

        final File archiveDir = new File(baseDir + "/archive-" + memberId);
        final File clusterDir = new File(baseDir + "/cluster-" + memberId);

        final Supplier<IdleStrategy> idleStrategySupplier = IdleStrategies.fromProperty(PROP_IDLE_STRATEGY);
        final MediaDriver.Context driverCtx = NodeDriver.context(aeronDir, idleStrategySupplier);

        final AeronArchive.Context localArchiveCtx = new AeronArchive.Context()
                                                         .lock(NoOpLock.INSTANCE)
                                                         .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                         .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                                         .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                         .controlResponseStreamId(NodeDriver.SEQUENCER_ARCHIVE_RESPONSE_STREAM_ID)
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

        final AtomicBoolean fatal = new AtomicBoolean();
        final Runnable fatalHandler = () -> {
            fatal.set(true);
            barrier.signalAll();
        };
        final SequencerService service = new SequencerService(fatalHandler);

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

        final boolean replayerStopped;
        try (barrier; ClusteredMediaDriver cmd = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceCtx)) {
            final ReplayerServer replayer =
                ReplayerServer.launch(memberId, aeronDir, idleStrategySupplier, fatalHandler);
            Logger.info(Logger.CoreComponent.SequencerServer, memberId, "Running — Ctrl-C to stop");
            barrier.await();
            replayerStopped = replayer.stop();
        } finally {
            Logger.info(Logger.CoreComponent.SequencerServer, memberId, "Shutdown complete");
        }
        if (fatal.get()) {
            System.exit(NodeDriver.EXIT_FATAL);
        }
        if (!replayerStopped) {
            System.exit(NodeDriver.EXIT_SHUTDOWN_TIMEOUT);
        }
    }

    /**
     * This node's member hosts: {@code sequencer.hosts}, else {@code SEQERON_HOSTS}, else one member on
     * {@link PortLayout#DEFAULT_HOST}.
     *
     * @param hostsProperty {@code sequencer.hosts}, or {@code null}
     * @param baseDirSet    whether {@code sequencer.baseDir} is set
     * @param envHosts      {@link PortLayout#HOSTS}
     * @param memberId      this node's member id
     * @throws IllegalArgumentException if the list has no entry for {@code memberId}, or it names several members
     *                                  and no baseDir is set
     */
    static List<String> resolveHosts(final String hostsProperty, final boolean baseDirSet,
                                     final List<String> envHosts, final int memberId) {
        final List<String> hosts = hostsProperty != null ? PortLayout.parseHosts(hostsProperty)
            : envHosts.isEmpty() ? List.of(PortLayout.DEFAULT_HOST) : envHosts;
        if (memberId < 0 || memberId >= hosts.size()) {
            throw new IllegalArgumentException(
                "the host list " + hosts + " names " + hosts.size() + " member(s), so it has no member " + memberId);
        }
        if (hosts.size() > 1 && !baseDirSet) {
            throw new IllegalArgumentException(
                PROP_BASE_DIR + " is required for a multi-member cluster: its default is under java.io.tmpdir, "
                    + "which is not persistent storage for the Raft log and archive");
        }
        return hosts;
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    /** The {@code clusterMembers} string for a cluster whose member {@code i} runs on {@code hosts.get(i)}. */
    static String buildClusterMembers(final List<String> hosts) {
        final StringBuilder members = new StringBuilder();
        for (int id = 0; id < hosts.size(); id++) {
            final String host = hosts.get(id);
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
