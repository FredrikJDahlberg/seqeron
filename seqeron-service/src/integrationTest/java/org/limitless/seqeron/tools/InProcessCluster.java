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
import org.limitless.seqeron.replayer.server.ReplayerServer;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;
import org.limitless.seqeron.sequencer.SequencerServer;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;

/**
 * Three members launched in this JVM, each on its own directories under one base, each one's UDP able to be cut,
 * wholly, all but its consensus traffic, or only the consensus messages it sends, and optionally a gateway host beside
 * them.
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
    private final AtomicBoolean[] muted = new AtomicBoolean[MEMBERS];
    private final AtomicBoolean[] fatal = new AtomicBoolean[MEMBERS + 1];
    private final AtomicBoolean terminated = new AtomicBoolean();
    private ReplayerServer gatewayHost;

    InProcessCluster(final Path baseDir) {
        this.baseDir = baseDir;
        for (int m = 0; m < MEMBERS; m++) {
            cut[m] = new AtomicBoolean();
            cutAllButConsensus[m] = new AtomicBoolean();
            muted[m] = new AtomicBoolean();
        }
        for (int m = 0; m <= MEMBERS; m++) {
            fatal[m] = new AtomicBoolean();
        }
    }

    /** Launches every member and activates the groups, since the cluster sequences nothing before them (spec §7.4). */
    void startAll() {
        for (int m = 0; m < MEMBERS; m++) {
            start(m);
        }
        activateGroups();
    }

    /** Publishes the group table from a session of its own, and waits until every member's Replayer serves it. */
    private void activateGroups() {
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
            for (int m = 0; m < MEMBERS; m++) {
                try (Aeron member = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(m)))) {
                    if (DefaultQueue.resolve(member, ACTIVATION_TIMEOUT_NS, true) != GROUP_ID) {
                        throw new IllegalStateException("member " + m + " did not activate group " + GROUP_ID);
                    }
                }
            }
        }
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
        final SequencerServer.UdpFaults udpFaults = (publication, streamId)
            -> isCut.get() ||
            (streamId == ConsensusModule.Configuration.CONSENSUS_STREAM_ID_DEFAULT ? publication && isMuted.get()
                                                                                   : isCutAllButConsensus.get());
        final AtomicBoolean isFatal = fatal[m];
        nodes.set(m, SequencerServer.launch(config, () -> terminated.set(true), () -> isFatal.set(true), udpFaults));
    }

    /** Launches the gateway host: its own driver and archive, relaying a member's queue of each group onto its own. */
    void startGatewayHost() {
        final List<String> archives = IntStream.range(0, MEMBERS)
                                          .mapToObj(m -> PortLayout.DEFAULT_HOST + ":" + PortLayout.archivePort(m))
                                          .toList();
        gatewayHost = ReplayerServer.launchGatewayHost(
            new ReplayerServer.GatewayHostConfig(GATEWAY_HOST, aeronDir(GATEWAY_HOST), baseDir.toString(), archives,
                                                 PortLayout.DEFAULT_HOST, GROUP_IDS, BackoffIdleStrategy::new),
            () -> fatal[GATEWAY_HOST].set(true));
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

    /** Drops, or stops dropping, member {@code m}'s consensus messages to its peers, though it still hears theirs. */
    void mute(final int m, final boolean isMuted) {
        muted[m].set(isMuted);
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
