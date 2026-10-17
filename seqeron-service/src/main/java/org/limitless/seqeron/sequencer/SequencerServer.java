package org.limitless.seqeron.sequencer;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import java.io.File;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
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
 * {@link #main} launches one from system properties; {@link #launch} launches one in the calling process.
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
 *   sequencer.aeronDir        — Aeron media driver directory; default PortLayout.aeronDir(memberId)
 *   sequencer.idleStrategy    — idle strategy of every agent, the Replayer's included: {@code backoff}
 *                               (default), {@code yielding} or {@code busyspin}. Busy-spin pays only when
 *                               each agent owns a core.
 *   sequencer.sessionTimeoutMs — how long the cluster keeps a client session with no keep-alives; default
 *                               5000. A gateway that misses it is replaced by its standby. Keep it well above
 *                               the leader heartbeat timeout (doc/ops.md, "Consensus timeouts").
 *   sequencer.leaderHeartbeatIntervalMs — how often the leader heartbeats its followers; default 200
 *   sequencer.leaderHeartbeatTimeoutMs — how long a follower waits for a leader heartbeat before it starts an
 *                               election, and how long a member copying the log waits on progress; default
 *                               2000
 *   sequencer.electionTimeoutMs — how long an election stage waits on the other members; default 1000
 *   aeron.cluster.max.sessions — the most client sessions the cluster holds at once; default 64 rather than
 *                               Aeron's 10 (doc/ops.md, "Cluster sessions")
 *   aeron.threading.mode       — the embedded driver's threading mode; default DEDICATED (doc/ops.md, "Driver threads")
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
    private static final String PROP_LEADER_HEARTBEAT_INTERVAL_MS = "sequencer.leaderHeartbeatIntervalMs";
    private static final String PROP_LEADER_HEARTBEAT_TIMEOUT_MS = "sequencer.leaderHeartbeatTimeoutMs";
    private static final String PROP_ELECTION_TIMEOUT_MS = "sequencer.electionTimeoutMs";

    private static final long DEFAULT_SESSION_TIMEOUT_MS = 5000;
    private static final long DEFAULT_LEADER_HEARTBEAT_INTERVAL_MS = 200;
    private static final long DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MS = 2000;
    private static final long DEFAULT_ELECTION_TIMEOUT_MS = 1000;

    /** Session limit when {@code aeron.cluster.max.sessions} is not set; see doc/ops.md, "Cluster sessions". */
    private static final int DEFAULT_MAX_CONCURRENT_SESSIONS = 64;

    /**
     * How long the graceful shutdown after a fatal may take before the process is halted: it closes the archive
     * that may be what failed. A healthy one fits inside chaos-runner's 10s queue-stall deadline.
     */
    private static final long FATAL_SHUTDOWN_BACKSTOP_MS = 15_000;

    /** The positions in the event the consensus module raises as it truncates its log. */
    private static final Pattern TRUNCATION =
        Pattern.compile("Truncating Cluster Log - .* this\\.logPosition=(-?\\d+) .* newPosition=(-?\\d+)$");

    private final ClusteredMediaDriver driver;
    private final ClusteredServiceContainer container;
    private final ReplayerServer replayer;

    /** Which UDP frames a node's driver drops, for test harnesses. */
    @FunctionalInterface
    public interface UdpFaults {
        /**
         * Called for each UDP frame the driver sends or receives, on the driver's threads.
         *
         * @param publication true for a frame of one of the node's publications: the data it sends and the status
         *                    messages it receives for it; false for one of its subscriptions
         * @param streamId    the frame's stream id
         * @return whether to drop it
         */
        boolean drops(boolean publication, int streamId);
    }

    /**
     * One node's settings, which {@link #fromProperties()} reads from the system properties above.
     *
     * @param memberId                  this node's Raft member id
     * @param hosts                     every member's host, in member-id order
     * @param host                      the host this node's channels bind to and advertise
     * @param baseDir                   the root of the archive and cluster directories
     * @param aeronDir                  the embedded media driver's directory
     * @param idleStrategies            the idle strategy of every agent, the Replayer's included
     * @param sessionTimeoutMs          how long the cluster keeps a client session with no keep-alives
     * @param leaderHeartbeatIntervalMs how often the leader heartbeats its followers
     * @param leaderHeartbeatTimeoutMs  how long a follower waits for a leader heartbeat before an election
     * @param electionTimeoutMs         how long an election stage waits on the other members
     */
    public record Config(int memberId, List<String> hosts, String host, String baseDir, String aeronDir,
                         Supplier<IdleStrategy> idleStrategies, long sessionTimeoutMs, long leaderHeartbeatIntervalMs,
                         long leaderHeartbeatTimeoutMs, long electionTimeoutMs) {
        /**
         * Reads the settings from the system properties, with their documented defaults.
         *
         * @return the settings
         * @throws IllegalArgumentException if the host list has no entry for the member id, or it names several
         *                                  members and no base directory is set
         */
        public static Config fromProperties() {
            final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
            final List<String> hosts = resolveHosts(
                System.getProperty(PROP_HOSTS), System.getProperty(PROP_BASE_DIR) != null, PortLayout.HOSTS, memberId);
            return new Config(memberId, hosts, System.getProperty(PROP_HOST, hosts.get(memberId)),
                              System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq"),
                              System.getProperty(PROP_AERON_DIR, PortLayout.aeronDir(memberId)),
                              IdleStrategies.fromProperty(PROP_IDLE_STRATEGY),
                              Long.getLong(PROP_SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS),
                              Long.getLong(PROP_LEADER_HEARTBEAT_INTERVAL_MS, DEFAULT_LEADER_HEARTBEAT_INTERVAL_MS),
                              Long.getLong(PROP_LEADER_HEARTBEAT_TIMEOUT_MS, DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MS),
                              Long.getLong(PROP_ELECTION_TIMEOUT_MS, DEFAULT_ELECTION_TIMEOUT_MS));
        }
    }

    private SequencerServer(final ClusteredMediaDriver driver, final ClusteredServiceContainer container,
                            final ReplayerServer replayer) {
        this.driver = driver;
        this.container = container;
        this.replayer = replayer;
    }

    public static void main(final String[] args) {
        final Config config = Config.fromProperties();
        final int memberId = config.memberId();

        final AtomicBoolean fatal = new AtomicBoolean();
        final boolean replayerStopped;
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier()) {
            final Runnable fatalHandler = () -> {
                if (fatal.compareAndSet(false, true)) {
                    startFatalShutdownBackstop(memberId);
                }
                barrier.signalAll();
            };

            Logger.info(Logger.CoreComponent.SequencerServer, memberId,
                        "Starting member %d | ingress=%s | archive=%s | baseDir=%s | idle=%s", memberId,
                        udp(config.host(), PortLayout.ingressPort(memberId)),
                        udp(config.host(), PortLayout.archivePort(memberId)), config.baseDir(),
                        System.getProperty(PROP_IDLE_STRATEGY, "backoff"));

            final SequencerServer node = launch(config, barrier::signalAll, fatalHandler);
            Logger.info(Logger.CoreComponent.SequencerServer, memberId, "Running — Ctrl-C to stop");
            barrier.await();
            replayerStopped = node.stop();
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
     * Launches one node in this process; {@link #main} is this over {@link Config#fromProperties()}.
     *
     * @param config          the node's settings
     * @param terminationHook run when the consensus module or the service container terminates the node
     * @param fatalHandler    run when the node cannot go on: its queue recording stopped, its Replayer failed, or
     *                        its log was truncated below what it applied
     * @return the running node, which {@link #stop()} stops
     */
    public static SequencerServer launch(final Config config, final Runnable terminationHook,
                                         final Runnable fatalHandler) {
        return launch(config, terminationHook, fatalHandler, null);
    }

    /**
     * As above, dropping every UDP frame the node's driver sends or receives that {@code udpFaults} names, which can
     * cut the node off from its peers and remote clients alike, or from some streams, or in one direction. Test
     * harnesses only.
     *
     * @param udpFaults the frames to drop; null drops nothing
     */
    public static SequencerServer launch(final Config config, final Runnable terminationHook,
                                         final Runnable fatalHandler, final UdpFaults udpFaults) {
        final int memberId = config.memberId();
        final String host = config.host();
        final String aeronDir = config.aeronDir();
        final Supplier<IdleStrategy> idleStrategySupplier = config.idleStrategies();
        final int archivePort = PortLayout.archivePort(memberId);
        final int ingressPort = PortLayout.ingressPort(memberId);

        final String clusterMembers = buildClusterMembers(config.hosts());

        final File archiveDir = new File(config.baseDir() + "/archive-" + memberId);
        final File clusterDir = new File(config.baseDir() + "/cluster-" + memberId);

        final MediaDriver.Context driverCtx = NodeDriver.context(aeronDir, idleStrategySupplier);
        if (udpFaults != null) {
            final LossGenerator dropSent = (address, buffer, length) -> udpFaults.drops(true, streamId(buffer));
            final LossGenerator dropReceived = (address, buffer, length) -> udpFaults.drops(false, streamId(buffer));
            final SendChannelEndpointSupplier send =
                (udp, status, ctx) -> new DebugSendChannelEndpoint(udp, status, ctx, dropSent, dropSent);
            final ReceiveChannelEndpointSupplier receive =
                (udp, dispatcher, status,
                 ctx) -> new DebugReceiveChannelEndpoint(udp, dispatcher, status, ctx, dropReceived, dropReceived);
            driverCtx.sendChannelEndpointSupplier(send).receiveChannelEndpointSupplier(receive);
        }
        NodeDriver.warnUnlessTmpfs(Logger.CoreComponent.SequencerServer, memberId, aeronDir);
        NodeDriver.warnIfTmpfs(Logger.CoreComponent.SequencerServer, memberId, config.baseDir());

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
        NodeDriver.recordChecksums(archiveCtx, Logger.CoreComponent.SequencerServer, memberId);

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
                .terminationHook(terminationHook)
                .deleteDirOnStart(false)
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(config.leaderHeartbeatIntervalMs()))
                .leaderHeartbeatTimeoutNs(TimeUnit.MILLISECONDS.toNanos(config.leaderHeartbeatTimeoutMs()))
                .electionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(config.electionTimeoutMs()))
                .electionStatusIntervalNs(TimeUnit.MILLISECONDS.toNanos(config.electionTimeoutMs() / 10))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .sessionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(config.sessionTimeoutMs()))
                .idleStrategySupplier(idleStrategySupplier)
                .errorHandler(t -> {
                    Logger.error(Logger.CoreComponent.ConsensusModule, Logger.CoreEventCode.ConsensusModuleError,
                                 memberId, "%s", t.toString());
                    if (truncatesAppliedLog(t)) {
                        Logger.fault(Logger.CoreComponent.SequencerServer, Logger.CoreEventCode.AppliedLogTruncated,
                                     memberId,
                                     "FATAL: the log was truncated below what this node applied — terminating this "
                                         + "node; its peers keep quorum and its restart replays the log they kept");
                        fatalHandler.run();
                    }
                });
        if (System.getProperty(ConsensusModule.Configuration.MAX_CONCURRENT_SESSIONS_PROP_NAME) == null) {
            consensusCtx.maxConcurrentSessions(DEFAULT_MAX_CONCURRENT_SESSIONS);
        }

        final SequencerService service = new SequencerService(memberId, localArchiveCtx.clone(), fatalHandler);

        final ClusteredServiceContainer.Context serviceCtx =
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(localArchiveCtx.clone())
                .clusterDir(clusterDir)
                .clusteredService(service)
                .terminationHook(terminationHook)
                .idleStrategySupplier(idleStrategySupplier)
                .errorHandler(t
                              -> Logger.error(Logger.CoreComponent.SequencerService, Logger.CoreEventCode.ServiceError,
                                              memberId, "%s", t.toString()));

        ClusteredMediaDriver driver = null;
        ClusteredServiceContainer container = null;
        try {
            driver = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
            container = ClusteredServiceContainer.launch(serviceCtx);
            return new SequencerServer(driver, container,
                                       ReplayerServer.launch(memberId, aeronDir, idleStrategySupplier, fatalHandler));
        } catch (final RuntimeException ex) {
            CloseHelper.quietCloseAll(container, driver);
            throw ex;
        }
    }

    /**
     * Stops the node: the Replayer, then the service container, then the media driver, archive and consensus module.
     *
     * @return false if the Replayer did not stop within its timeout
     */
    public boolean stop() {
        try {
            return replayer.stop();
        } finally {
            CloseHelper.closeAll(container, driver);
        }
    }

    /**
     * Halts the process {@link #FATAL_SHUTDOWN_BACKSTOP_MS} from now, on a daemon thread, so it covers whichever
     * thread raised the fatal and whatever the shutdown wedges on, and holds up no exit that completes.
     * @param memberId this node, for the log
     */
    private static void startFatalShutdownBackstop(final int memberId) {
        final Thread backstop = new Thread(() -> {
            try {
                Thread.sleep(FATAL_SHUTDOWN_BACKSTOP_MS);
            } catch (final InterruptedException ignore) {
                // nothing interrupts it, and the node is committed to dying either way
            }
            Logger.fault(Logger.CoreComponent.SequencerServer, Logger.CoreEventCode.ShutdownTimeout, memberId,
                         "shutdown after a fatal did not complete within %dms: halting", FATAL_SHUTDOWN_BACKSTOP_MS);
            Runtime.getRuntime().halt(NodeDriver.EXIT_FATAL);
        }, "seqeron-fatal-backstop");
        backstop.setDaemon(true);
        backstop.start();
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

    /**
     * Whether {@code t} is the consensus module truncating its log below the position it has applied, which Aeron
     * Cluster 1.53.2 does to entries it committed (doc/fault-tolerance.md §1.4). The services then hold state the log
     * no longer has, and only a restart rebuilds it.
     */
    static boolean truncatesAppliedLog(final Throwable t) {
        if (!(t instanceof ClusterEvent) || t.getMessage() == null) {
            return false;
        }
        final Matcher truncation = TRUNCATION.matcher(t.getMessage());
        return truncation.find() && Long.parseLong(truncation.group(2)) < Long.parseLong(truncation.group(1));
    }

    private static String udp(final String host, final int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    /** A UDP frame's stream id, which data, pad and setup frames carry after their term offset. */
    private static int streamId(final DirectBuffer frame) {
        final int type = frame.getShort(HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final boolean afterTermOffset = type == HeaderFlyweight.HDR_TYPE_DATA || type == HeaderFlyweight.HDR_TYPE_PAD ||
            type == HeaderFlyweight.HDR_TYPE_SETUP;
        return frame.getInt(afterTermOffset ? DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET
                                            : ErrorFlyweight.STREAM_ID_FIELD_OFFSET,
                            ByteOrder.LITTLE_ENDIAN);
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
