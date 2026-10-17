package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.probe.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.sequencer.SequencerServer;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.PendingSends;

/**
 * failover-test.sh's confirm producer in one JVM: three members, a producer co-located with a follower sending
 * through {@link PendingSends}, and the leader stopped mid-stream. The producer's queue must show every frame
 * exactly once, in order.
 */
@ExtendWith(InterruptingTestCallback.class)
class FailoverConfirmTest {
    private static final int MEMBERS = 3;
    private static final int COUNT = 5_000;
    private static final long PACING_NS = TimeUnit.MICROSECONDS.toNanos(100); // failover-test.sh's
    private static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(15); // ClusterProbe confirm's
    private static final int CLIENT_ID = 9;
    private static final int NO_ID = -1;

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private final SequencerServer[] nodes = new SequencerServer[MEMBERS];
    private final AtomicBoolean fatal = new AtomicBoolean();
    private final AtomicBoolean terminated = new AtomicBoolean();

    @AfterEach
    void stopNodes() {
        for (final SequencerServer node : nodes) {
            if (node != null) {
                node.stop();
            }
        }
    }

    @Test
    @InterruptAfter(120)
    void pendingSendsShowsEveryFrameOnceAcrossALeaderStop() throws Exception {
        watcher.dataCollector().add(baseDir);
        for (int m = 0; m < MEMBERS; m++) {
            nodes[m] = SequencerServer.launch(config(m), () -> terminated.set(true), () -> fatal.set(true));
        }
        final int leader = awaitLeader();
        final int member = (leader + 1) % MEMBERS;

        final PendingSends pending = new PendingSends(4096);
        final LongArrayList seqNos = new LongArrayList();
        final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        final AtomicInteger lastLeader = new AtomicInteger(NO_ID);
        final AtomicBoolean caughtUp = new AtomicBoolean();
        final CompletableFuture<Void> leaderStopped;
        int resent = 0;
        boolean drained = false;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir(member)));
             ClusterStreamSender sender = new ClusterStreamSender()) {
            final ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                CLIENT_ID,
                event
                -> {
                    pending.onSequenced(event);
                    if (isMarker(event)) {
                        decoder.wrap(event.buffer(), event.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH,
                                     event.blockLength(), event.version());
                        seqNos.addLong(decoder.seqNo());
                    }
                },
                (leaderMemberId, leadershipTermId, globalSeqNo)
                    -> {
                    pending.onLeadershipChanged(leadershipTermId);
                    lastLeader.set(leaderMemberId);
                },
                () -> caughtUp.set(true));
            try {
                receiver.start(aeron, member);
                while (!caughtUp.get()) {
                    receiver.poll();
                    Tests.yield();
                }
                sender.setIngressHold(pending);
                sender.connect(aeron, "aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0",
                               PortLayout.ingressEndpoints(MEMBERS));

                final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
                final byte[] noFiller = new byte[0];
                CompletableFuture<Void> stopping = null;
                long next = 1;
                long nextSendNs = 0;
                long lastProgressNs = System.nanoTime();
                while (true) {
                    final long now = System.nanoTime();
                    final int seen = seqNos.size();
                    receiver.poll();
                    sender.pollEgress();
                    sender.keepAlive();
                    if (sender.isSessionLost() || pending.isFaulted()) {
                        fail(pending.isFaulted() ? "PendingSends faulted" : "cluster session lost");
                    }
                    final int resentNow = pending.resendMissing(sender);
                    resent += resentNow;
                    if (next <= COUNT && !pending.isHolding() && !pending.isFull() && now >= nextSendNs) {
                        final int length = marker.encode(next, NO_ID, ClusterProbe.PROBE_SOURCE_ID, noFiller);
                        if (sender.send(marker.frame(), length)) {
                            pending.track(marker.frame(), length, sender.clusterSessionId(), sender.leadershipTermId());
                            next++;
                            nextSendNs = now + PACING_NS;
                            lastProgressNs = now;
                        }
                    }
                    if (stopping == null && next > COUNT / 2) {
                        final SequencerServer stopped = nodes[leader];
                        nodes[leader] = null;
                        stopping = CompletableFuture.runAsync(stopped::stop);
                    }
                    if (resentNow > 0 || seqNos.size() > seen) {
                        lastProgressNs = now;
                    }
                    drained = next > COUNT && pending.size() == 0;
                    if (drained || now - lastProgressNs > DRAIN_TIMEOUT_NS) {
                        break;
                    }
                    Tests.yield();
                }
                leaderStopped = stopping;
            } finally {
                receiver.close();
            }
        }

        leaderStopped.get(30, TimeUnit.SECONDS);
        // Reported rather than asserted, as failover-test.sh does: a stop can land between two frames.
        System.out.printf("member %d stopped, member %d leads; resent %d frame(s)%n", leader, lastLeader.get(), resent);
        assertTrue(drained, "PendingSends still held " + pending.size() + " frame(s) after the drain timeout");
        assertNotEquals(leader, lastLeader.get(), "no new leader took over from member " + leader);
        assertExactlyOnceInOrder(seqNos, resent);
        assertFalse(fatal.get(), "a member raised a fatal");
        assertFalse(terminated.get(), "a member terminated");
    }

    private static boolean isMarker(final SequencedEvent event) {
        return !event.isSystem() && event.payloadId() == ClusterProbe.PROBE_PAYLOAD_ID &&
            event.templateId() == ProbeMarkerDecoder.TEMPLATE_ID;
    }

    private static void assertExactlyOnceInOrder(final LongArrayList seqNos, final int resent) {
        final LongArrayList expected = new LongArrayList();
        for (long seqNo = 1; seqNo <= COUNT; seqNo++) {
            expected.addLong(seqNo);
        }
        assertEquals(expected, seqNos,
                     "the queue showed " + seqNos.size() + " of " + COUNT + " frame(s), not 1.." + COUNT +
                         " in order; resent " + resent);
    }

    private SequencerServer.Config config(final int memberId) {
        // SequencerServer's defaults for the session, heartbeat and election timings.
        return new SequencerServer.Config(memberId, Collections.nCopies(MEMBERS, PortLayout.DEFAULT_HOST),
                                          PortLayout.DEFAULT_HOST, baseDir.toString(), aeronDir(memberId),
                                          BackoffIdleStrategy::new, 1000, 20, 200, 200);
    }

    private String aeronDir(final int memberId) {
        return baseDir.resolve("aeron-" + memberId).toString();
    }

    /** The one member whose consensus module reports itself leader, once there is exactly one. */
    private int awaitLeader() {
        while (true) {
            int leader = NO_ID;
            int leaders = 0;
            for (int m = 0; m < MEMBERS; m++) {
                if (role(aeronDir(m)) == Cluster.Role.LEADER) {
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

    private static Cluster.Role role(final String aeronDir) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
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
}
