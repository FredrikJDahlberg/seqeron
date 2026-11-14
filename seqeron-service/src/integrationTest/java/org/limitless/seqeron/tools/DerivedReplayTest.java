package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.status.StreamCounter;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.server.DerivingReplayer;

/**
 * A client's cold start served off the Raft log: a member records no queue and its
 * Replayer derives the replay through {@link DerivingReplayer}, and an unchanged {@link ReplayerStreamReceiver} must
 * walk each queue from groupSeqNo 1 in order, catch up at the queue's tip and follow it live, and print its rate. A
 * gateway host's walk is off its own recording.
 */
@ExtendWith(InterruptingTestCallback.class)
class DerivedReplayTest {
    private static final int HISTORY_FRAMES = 200_000;
    /** With the frame's envelope, a payload of the size production carries. */
    private static final int FILLER = 150;
    private static final int LIVE_FRAMES = 1_000;
    private static final int HOST_HISTORY_FRAMES = 20_000;
    private static final long CONVERGE_TIMEOUT_MS = 60_000;
    /** Well under the time the shortest replay here takes. */
    private static final long DERIVED_LOOK_INTERVAL_MS = 5;
    private static final int CLIENT_ID = 31;
    private static final int RESPONSE_STREAM_ID = 125; // beside QueueFrames' and NodeDriver's

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;

    @BeforeEach
    void collect() {
        watcher.dataCollector().add(baseDir);
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @InterruptAfter(240)
    void aColdStartWalksEachQueueOffTheLog() {
        walkEachQueue();
    }

    /**
     * A gateway host cannot relay a queue recording its members do not make: it derives its queues from a member's
     * log, and goes on from the next member's when that one stops.
     */
    @Test
    @InterruptAfter(240)
    void aGatewayHostDerivesItsQueuesFromAMembersLogAndGoesOnFromTheNextMembers() {
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        final long tip = send(cluster.awaitLeader(), HOST_HISTORY_FRAMES);
        cluster.startGatewayHost();
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            awaitRelayed(groupId);
            walk(InProcessCluster.GATEWAY_HOST, groupId, tip, false);
        }

        cluster.stop(0); // the member the host reads first
        final long later = send(cluster.awaitLeader(), LIVE_FRAMES);
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            walk(InProcessCluster.GATEWAY_HOST, groupId, later, false);
        }
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /**
     * Waits until the host's recording of a queue holds the history: a client that reads the queue from before its
     * first frame takes it all live and leaves the replay it asked for unread, which the archive logs as an error.
     */
    private void awaitRelayed(final int groupId) {
        final long deadlineMs = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        try (Aeron aeron =
                 Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(InProcessCluster.GATEWAY_HOST)));
             AeronArchive archive = connectArchive(aeron)) {
            final long[] recordingId = { Aeron.NULL_VALUE };
            long position = -1;
            while (true) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "the host relayed nothing of group " + groupId);
                Tests.sleep(200);
                archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
                                             (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                                              startPosition, stopPosition, initialTermId, segmentFileLength,
                                              termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                                              originalChannel, sourceIdentity) -> recordingId[0] = id);
                final long now = recordingId[0] == Aeron.NULL_VALUE ? -1 : archive.getRecordingPosition(recordingId[0]);
                if (now > 0 && now == position) {
                    return;
                }
                position = now;
            }
        }
    }

    private static AeronArchive connectArchive(final Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
                                        .aeron(aeron)
                                        .ownsAeronClient(false)
                                        .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                        .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlResponseStreamId(RESPONSE_STREAM_ID));
    }

    private void walkEachQueue() {
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        final long tip = send(leader, HISTORY_FRAMES);
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            walk(member, groupId, tip, true);
            assertEquals(0, queueRecordings(member, groupId), "a member records group " + groupId + "'s queue");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** Cold-starts a client on a node's queue, then sends more and holds it to following live. */
    private void walk(final int member, final int groupId, final long tipClusterSeqNo, final boolean derived) {
        final LongArrayList groupSeqNos = new LongArrayList();
        boolean derivedReplay = false;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID + groupId,
                 (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null, () -> { })) {
            final long startedNs = System.nanoTime();
            receiver.start(aeron, member, groupId);
            derivedReplay = converge(aeron, receiver, tipClusterSeqNo, groupSeqNos);
            final int walked = groupSeqNos.size();
            final double seconds = (System.nanoTime() - startedNs) / 1e9;
            System.out.printf("group %d, %s: %d frame(s) walked in %.0f ms, %.0f frames/s%n", groupId,
                              derived ? "derived from the log" : "replayed from the recording", walked, seconds * 1e3,
                              walked / seconds);

            converge(aeron, receiver, send(cluster.awaitLeader(), LIVE_FRAMES), groupSeqNos);
            assertTrue(groupSeqNos.size() > walked, "nothing reached it live after the walk");
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertEquals(derived, derivedReplay, "whether its replay was derived");
    }

    /**
     * @return whether a derived replay was seen on the way: looked for throughout, since a member that trails its
     *         peers ends the replay long before the client has caught up on its queue
     */
    private static boolean converge(final Aeron aeron, final ReplayerStreamReceiver receiver,
                                    final long tipClusterSeqNo, final LongArrayList groupSeqNos) {
        final long deadlineMs = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        long nextLookMs = 0;
        boolean derivedReplay = false;
        while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
            final long nowMs = System.currentTimeMillis();
            assertTrue(nowMs < deadlineMs,
                       "not caught up after " + CONVERGE_TIMEOUT_MS + " ms, at groupSeqNo " + groupSeqNos.size());
            if (!derivedReplay && nowMs >= nextLookMs) {
                nextLookMs = nowMs + DERIVED_LOOK_INTERVAL_MS;
                derivedReplay = derivedReplay(aeron);
            }
            if (receiver.poll() == 0) {
                Tests.yield();
            }
        }
        return derivedReplay || derivedReplay(aeron);
    }

    /**
     * Whether a {@link DerivingReplayer} is publishing a replay: the driver holds its publication's counters.
     */
    private static boolean derivedReplay(final Aeron aeron) {
        final CountersReader counters = aeron.countersReader();
        final boolean[] found = { false };
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            found[0] |= typeId == AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID &&
                keyBuffer.getInt(StreamCounter.STREAM_ID_OFFSET) == ReplayProtocol.REPLAY_STREAM_ID &&
                keyBuffer.getStringAscii(StreamCounter.CHANNEL_OFFSET).contains(DerivingReplayer.DERIVED_ALIAS);
        });
        return found[0];
    }

    /** How many recordings of a group's queue a member's archive holds. */
    private int queueRecordings(final int member, final int groupId) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             AeronArchive archive = connectArchive(aeron)) {
            return archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
                                                (controlSessionId, correlationId, recordingId, startTimestamp,
                                                 stopTimestamp, startPosition, stopPosition, initialTermId,
                                                 segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                                                 strippedChannel, originalChannel, sourceIdentity) -> { });
        }
    }

    /** @return the clusterSeqNo of the last of {@code frames} confirmed frames sent through {@code member} */
    private long send(final int member, final int frames) {
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, frames, 0, FILLER)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            return producer.lastClusterSeqNo();
        }
    }
}
