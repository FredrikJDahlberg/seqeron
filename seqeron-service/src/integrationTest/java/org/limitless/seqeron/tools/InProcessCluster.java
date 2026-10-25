package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.Tests;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.server.RelayedCut;
import org.limitless.seqeron.replayer.server.ReplayerServer;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredEncoder;
import org.limitless.seqeron.sequencer.SequencerServer;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;

/**
 * Three members launched in this JVM, each on its own directories under one base, each one's UDP able to be cut,
 * wholly, all but its consensus traffic, or only the consensus messages it sends, each one's consensus clock able to be
 * stepped, and optionally a gateway host beside them.
 */
final class InProcessCluster implements AutoCloseable {
    static final int MEMBERS = 3;
    static final int NO_MEMBER = -1;
    /** The gateway host's node id, one no member uses. */
    static final int GATEWAY_HOST = MEMBERS;
    /** The producer's group, and the lowest: the default queue. */
    static final int GROUP_ID = 1;
    /** The group a producer also addresses, frame by frame, so that a second queue is recorded and relayed. */
    static final int OTHER_GROUP_ID = 2;
    /** Every group, ascending: each one's queue is on every archive. */
    static final List<Integer> GROUP_IDS = List.of(GROUP_ID, OTHER_GROUP_ID);
    private static final long ACTIVATION_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    private final Path baseDir;
    private final AtomicReferenceArray<SequencerServer> nodes = new AtomicReferenceArray<>(MEMBERS);
    private final AtomicBoolean[] cut = new AtomicBoolean[MEMBERS];
    private final AtomicBoolean[] cutAllButConsensus = new AtomicBoolean[MEMBERS];
    private final AtomicBoolean[] cutArchive = new AtomicBoolean[MEMBERS];
    private final AtomicBoolean[] muted = new AtomicBoolean[MEMBERS];
    /** Each member's consensus clock, which outlives a restart of it, as its host's does. */
    private final SteppableClusterClock[] clocks = new SteppableClusterClock[MEMBERS];
    /** Each member's Replayer, and the gateway host's last, which fail or stall on cue and outlive a restart of it. */
    private final FaultableReplayer[] replayers = new FaultableReplayer[MEMBERS + 1];
    private final AtomicBoolean[] fatal = new AtomicBoolean[MEMBERS + 1];
    private final AtomicBoolean terminated = new AtomicBoolean();
    private ReplayerServer gatewayHost;
    private ExpandableArrayBuffer gatewayRow;
    private int gatewayRowLength;

    InProcessCluster(final Path baseDir) {
        this.baseDir = baseDir;
        for (int m = 0; m < MEMBERS; m++) {
            cut[m] = new AtomicBoolean();
            cutAllButConsensus[m] = new AtomicBoolean();
            cutArchive[m] = new AtomicBoolean();
            muted[m] = new AtomicBoolean();
            clocks[m] = new SteppableClusterClock();
        }
        for (int m = 0; m <= MEMBERS; m++) {
            replayers[m] = new FaultableReplayer();
            fatal[m] = new AtomicBoolean();
        }
    }

    /** Launches every member and activates the groups, since the cluster sequences nothing before them (spec §7.4). */
    void startAll() {
        startAll(NO_SNAPSHOT_POLICY, 0);
    }

    /** No {@code <snapshots>} policy: {@link #startAll()}'s. */
    static final int NO_SNAPSHOT_POLICY = -1;

    /**
     * As {@link #startAll()}, loading a snapshot policy on the session that activates the groups, the one session that
     * may register a topology row (<b>S-9</b>).
     * @param snapshotIntervalSeconds seconds between rounds, 0 for operator requests only, or {@link #NO_SNAPSHOT_POLICY}
     * @param snapshotBytes           sequenced bytes between rounds, 0 for none
     */
    void startAll(final int snapshotIntervalSeconds, final long snapshotBytes) {
        for (int m = 0; m < MEMBERS; m++) {
            start(m);
        }
        activateGroups(snapshotIntervalSeconds, snapshotBytes);
    }

    /** Publishes the group table from a session of its own, and waits until every member's Replayer serves it. */
    private void activateGroups(final int snapshotIntervalSeconds, final long snapshotBytes) {
        final byte[] names = new byte[GroupsRegisteredEncoder.groupNamesLength()];
        final int slot = names.length / FrameLayer.MAX_GROUPS;
        for (final int groupId : GROUP_IDS) {
            final byte[] name = ("group-" + groupId).getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(name, 0, names, (groupId - 1) * slot, name.length);
        }
        final ExpandableArrayBuffer body = new ExpandableArrayBuffer(GroupsRegisteredEncoder.BLOCK_LENGTH);
        final int length = new GroupsRegisteredEncoder().wrap(body, 0).putGroupNames(names, 0).encodedLength();
        final IngressPublisher publisher = new IngressPublisher();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(awaitLeader())));
             ClusterStreamSender sender = new ClusterStreamSender()) {
            sender.connect(aeron, "aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0",
                           PortLayout.ingressEndpoints(MEMBERS));
            while (publisher.publishSystem(sender, TopologyDocument.RESERVED_SOURCE_ID, -1,
                                           SystemFrame.GROUPS_REGISTERED, body, length) != Publish.Published) {
                Tests.yield();
            }
            if (gatewayRow != null) {
                while (publisher.publishSystem(sender, TopologyDocument.RESERVED_SOURCE_ID, -1,
                                               SystemFrame.GATEWAY_REGISTERED, gatewayRow, gatewayRowLength)
                       != Publish.Published) {
                    Tests.yield();
                }
            }
            if (snapshotIntervalSeconds != NO_SNAPSHOT_POLICY) {
                final ExpandableArrayBuffer policy = new ExpandableArrayBuffer(SnapshotPolicyRegisteredEncoder.BLOCK_LENGTH);
                final int policyLength = new SnapshotPolicyRegisteredEncoder()
                                             .wrap(policy, 0)
                                             .intervalSeconds(snapshotIntervalSeconds)
                                             .bytes(snapshotBytes)
                                             .encodedLength();
                while (publisher.publishSystem(sender, TopologyDocument.RESERVED_SOURCE_ID, -1,
                                               SystemFrame.SNAPSHOT_POLICY_REGISTERED, policy, policyLength)
                       != Publish.Published) {
                    Tests.yield();
                }
            }
            for (int m = 0; m < MEMBERS; m++) {
                try (Aeron member = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(m)))) {
                    if (DefaultQueue.resolve(member, ACTIVATION_TIMEOUT_NS, true) != GROUP_ID) {
                        throw new IllegalStateException("member " + m + " did not activate group " + GROUP_ID);
                    }
                }
            }
        }
    }

    /**
     * Has the next {@link #startAll} register a gateway row that takes part in snapshots, from the session that
     * registers the groups (<b>S-9</b>). With {@code last} the row ends the topology and the cluster designates the
     * gateway; without, the designation is withheld.
     */
    void withGateway(final int gatewayId, final int gatewaySourceId, final String name, final boolean last) {
        final ExpandableArrayBuffer body = new ExpandableArrayBuffer(GatewayRegisteredEncoder.BLOCK_LENGTH);
        gatewayRowLength = new GatewayRegisteredEncoder()
                               .wrap(body, 0)
                               .remaining(last ? 0 : 1)
                               .gatewayId(gatewayId)
                               .gatewaySourceId(gatewaySourceId)
                               .gatewayName(name)
                               .preferenceRank((short)0)
                               .snapshot((short)1)
                               .groupId((short)GROUP_ID)
                               .encodedLength();
        gatewayRow = body;
    }

    /** Launches member {@code m} on its directories, which a previous run of it may have left. */
    void start(final int m) {
        final SequencerServer.Config config = new SequencerServer.Config(
            m, Collections.nCopies(MEMBERS, PortLayout.DEFAULT_HOST), PortLayout.DEFAULT_HOST, baseDir.toString(),
            aeronDir(m), BackoffIdleStrategy::new, SequencerServer.DEFAULT_SESSION_TIMEOUT_MS,
            SequencerServer.DEFAULT_LEADER_HEARTBEAT_INTERVAL_MS, SequencerServer.DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MS,
            SequencerServer.DEFAULT_ELECTION_TIMEOUT_MS);
        final AtomicBoolean isCut = cut[m];
        final AtomicBoolean isCutAllButConsensus = cutAllButConsensus[m];
        final AtomicBoolean isMuted = muted[m];
        final AtomicBoolean isArchiveCut = cutArchive[m];
        final SequencerServer.UdpFaults udpFaults = (publication, streamId)
            -> isCut.get() || (isArchiveCut.get() && RELAY_STREAM_IDS.contains(streamId)) ||
            (streamId == ConsensusModule.Configuration.CONSENSUS_STREAM_ID_DEFAULT ? publication && isMuted.get()
                                                                                   : isCutAllButConsensus.get());
        final AtomicBoolean isFatal = fatal[m];
        nodes.set(
            m,
            SequencerServer.launch(config, () -> terminated.set(true), () -> isFatal.set(true), udpFaults, clocks[m],
                                   replayers[m]::wrap));
    }

    /** Launches the gateway host: its own driver and archive, relaying a member's queue of each group onto its own. */
    void startGatewayHost() {
        startGatewayHost(null);
    }

    /** As {@link #startGatewayHost()}, with the host's relays reporting the markers they publish to {@code cutReports}. */
    void startGatewayHost(final Queue<RelayedCut> cutReports) {
        final List<String> archives = IntStream.range(0, MEMBERS)
                                          .mapToObj(m -> PortLayout.DEFAULT_HOST + ":" + PortLayout.archivePort(m))
                                          .toList();
        gatewayHost = ReplayerServer.launchGatewayHost(
            new ReplayerServer.GatewayHostConfig(GATEWAY_HOST, aeronDir(GATEWAY_HOST), baseDir.toString(), archives,
                                                 PortLayout.DEFAULT_HOST, GROUP_IDS, BackoffIdleStrategy::new),
            () -> fatal[GATEWAY_HOST].set(true), cutReports, replayers[GATEWAY_HOST]::wrap);
    }

    /** Stops the gateway host, whose archive and Aeron directory stay for the next {@link #startGatewayHost()}. */
    void stopGatewayHost() {
        gatewayHost.stop();
        gatewayHost = null;
    }

    /** Stops member {@code m}, which a member stopped in-process does by closing rather than by dying. */
    void stop(final int m) {
        final SequencerServer node = nodes.getAndSet(m, null);
        if (node != null) {
            node.stop();
        }
    }

    boolean isRunning(final int m) {
        return nodes.get(m) != null;
    }

    /** Drops, or stops dropping, every UDP frame member {@code m} sends or receives. */
    void cut(final int m, final boolean isCut) {
        cut[m].set(isCut);
    }

    /** As {@link #cut}, but member {@code m} keeps its consensus traffic: it still votes, but copies no log. */
    void cutAllButConsensus(final int m, final boolean isCut) {
        cutAllButConsensus[m].set(isCut);
    }

    /**
     * What a member sends a gateway host's relay: the archive's responses (122) and the replay (206). The relay's
     * requests (stream 100, which is also the cluster's log) are left, so the relay hears nothing and the cluster
     * keeps its quorum.
     */
    private static final Set<Integer> RELAY_STREAM_IDS = Set.of(122, 206);

    /** Drops, or stops dropping, what member {@code m} sends a gateway host's relay, and nothing else. */
    void cutArchive(final int m, final boolean isCut) {
        cutArchive[m].set(isCut);
    }

    /** Drops, or stops dropping, member {@code m}'s consensus messages to its peers, though it still hears theirs. */
    void mute(final int m, final boolean isMuted) {
        muted[m].set(isMuted);
    }

    /** Member {@code m}'s Replayer, or the gateway host's for {@link #GATEWAY_HOST}, to make fail or stall. */
    FaultableReplayer replayer(final int m) {
        return replayers[m];
    }

    /** Steps member {@code m}'s consensus clock by {@code millis}, backward when negative. */
    void stepClock(final int m, final long millis) {
        clocks[m].step(millis);
    }

    /** Member {@code m}'s Aeron directory, or the gateway host's for {@link #GATEWAY_HOST}. */
    String aeronDir(final int m) {
        return baseDir.resolve("aeron-" + m).toString();
    }

    /** Each running member's driver directory, and the gateway host's if launched, by name. */
    Map<String, String> archives() {
        final Map<String, String> archives = new LinkedHashMap<>();
        for (int m = 0; m < MEMBERS; m++) {
            if (isRunning(m)) {
                archives.put("member " + m, aeronDir(m));
            }
        }
        if (gatewayHost != null) {
            archives.put("the gateway host", aeronDir(GATEWAY_HOST));
        }
        return archives;
    }

    boolean fatal() {
        for (final AtomicBoolean isFatal : fatal) {
            if (isFatal.get()) {
                return true;
            }
        }
        return false;
    }

    /** Whether member {@code m}, or the gateway host for {@link #GATEWAY_HOST}, raised a fatal, in any run of it. */
    boolean fatal(final int m) {
        return fatal[m].get();
    }

    boolean terminated() {
        return terminated.get();
    }

    /** The one running member whose consensus module reports itself leader, once there is exactly one. */
    int awaitLeader() {
        while (true) {
            int leader = NO_MEMBER;
            int leaders = 0;
            for (int m = 0; m < MEMBERS; m++) {
                if (isRunning(m) && role(m) == Cluster.Role.LEADER) {
                    leader = m;
                    leaders++;
                }
            }
            if (leaders == 1) {
                return leader;
            }
            Tests.sleep(50);
        }
    }

    Cluster.Role role(final int m) {
        return Cluster.Role.get(counter(m, ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID));
    }

    ElectionState electionState(final int m) {
        return ElectionState.get(counter(m, ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID));
    }

    long commitPosition(final int m) {
        return counter(m, ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID);
    }

    /** Member {@code m}'s consensus-module counter of the given type, or 0 before the module allocates it. */
    private long counter(final int m, final int typeId) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(m)))) {
            final CountersReader counters = aeron.countersReader();
            final MutableLong value = new MutableLong();
            counters.forEach((counterId, counterTypeId, keyBuffer, label) -> {
                if (counterTypeId == typeId) {
                    value.set(counters.getCounterValue(counterId));
                }
            });
            return value.get();
        }
    }

    @Override
    public void close() {
        if (gatewayHost != null) {
            gatewayHost.stop();
        }
        for (int m = 0; m < MEMBERS; m++) {
            stop(m);
        }
    }
}
