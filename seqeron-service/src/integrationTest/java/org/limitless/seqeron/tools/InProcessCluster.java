package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.server.ReplayerServer;
import org.limitless.seqeron.sequencer.SequencerServer;

/**
 * Three members launched in this JVM, each on its own directories under one base, each one's UDP able to be cut, and
 * optionally a gateway host beside them.
 */
final class InProcessCluster implements AutoCloseable {
    static final int MEMBERS = 3;
    static final int NO_MEMBER = -1;
    /** The gateway host's node id, one no member uses. */
    static final int GATEWAY_HOST = MEMBERS;

    private final Path baseDir;
    private final AtomicReferenceArray<SequencerServer> nodes = new AtomicReferenceArray<>(MEMBERS);
    private final AtomicBoolean[] cut = new AtomicBoolean[MEMBERS];
    private final AtomicBoolean fatal = new AtomicBoolean();
    private final AtomicBoolean terminated = new AtomicBoolean();
    private ReplayerServer gatewayHost;

    InProcessCluster(final Path baseDir) {
        this.baseDir = baseDir;
        for (int m = 0; m < MEMBERS; m++) {
            cut[m] = new AtomicBoolean();
        }
    }

    void startAll() {
        for (int m = 0; m < MEMBERS; m++) {
            start(m);
        }
    }

    /** Launches member {@code m} on its directories, which a previous run of it may have left. */
    void start(final int m) {
        final SequencerServer.Config config = new SequencerServer.Config(
            m, Collections.nCopies(MEMBERS, PortLayout.DEFAULT_HOST), PortLayout.DEFAULT_HOST, baseDir.toString(),
            aeronDir(m), BackoffIdleStrategy::new, 1000, 20, 200, 200); // SequencerServer's default timings
        final AtomicBoolean isCut = cut[m];
        nodes.set(m, SequencerServer.launch(config, () -> terminated.set(true), () -> fatal.set(true), isCut::get));
    }

    /** Launches the gateway host: its own driver and archive, relaying a member's queue onto its own. */
    void startGatewayHost() {
        final List<String> archives = IntStream.range(0, MEMBERS)
                                          .mapToObj(m -> PortLayout.DEFAULT_HOST + ":" + PortLayout.archivePort(m))
                                          .toList();
        gatewayHost = ReplayerServer.launchGatewayHost(
            new ReplayerServer.GatewayHostConfig(GATEWAY_HOST, aeronDir(GATEWAY_HOST), baseDir.toString(), archives,
                                                 PortLayout.DEFAULT_HOST, BackoffIdleStrategy::new),
            () -> fatal.set(true));
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
        return fatal.get();
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

    private Cluster.Role role(final int m) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(m)))) {
            final CountersReader counters = aeron.countersReader();
            final MutableReference<Cluster.Role> role = new MutableReference<>(Cluster.Role.FOLLOWER);
            counters.forEach((counterId, typeId, keyBuffer, label) -> {
                if (typeId == ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID) {
                    role.set(Cluster.Role.get(counters.getCounterValue(counterId)));
                }
            });
            return role.get();
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
