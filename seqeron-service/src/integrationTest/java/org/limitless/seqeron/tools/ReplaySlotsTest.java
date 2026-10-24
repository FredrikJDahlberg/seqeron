package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.server.AeronReplayer;
import org.limitless.seqeron.replayer.server.Replayer;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * What the Replayer reads off its driver's counters to cap and throttle replays (spec §10, "Slot limits"):
 * whether a replay's publication is still there, and how far a queue's recording trails it. {@code
 * ReplayerServiceTest} takes both from a fake, so a counter matched wrongly here would pass it.
 */
@ExtendWith(InterruptingTestCallback.class)
class ReplaySlotsTest {
    private static final long TIMEOUT_MS = 20_000;
    private static final int RESPONSE_STREAM_ID = 125; // beside QueueFrames'

    /** Longer than the driver's 5 s publication linger, so a held slot is held by its subscriber alone. */
    private static final int HOLD_CHECKS = 7;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @InterruptAfter(120)
    void aReplayHoldsItsSlotUntilItsSubscriberLetsItGoAndAFifthClientIsHeldUntilThen() {
        final int member = cluster.awaitLeader();
        final List<Client> holders = new ArrayList<>();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)))) {
            serve(aeron, holders);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    private static void serve(final Aeron aeron, final List<Client> holders) {
        try {
            for (int i = 0; i < 4; i++) {
                final Client holder = new Client(aeron);
                holders.add(holder);
                assertEquals(ReplayingDecoder.TEMPLATE_ID, holder.ask(), "holder " + i + " is served");
                holder.attach(); // and never reads it
            }
            final Client fifth = new Client(aeron);
            holders.add(fifth);
            for (int i = 0; i < HOLD_CHECKS; i++) {
                assertEquals(ReplayPendingDecoder.TEMPLATE_ID, fifth.ask(), "every slot is held, check " + i);
                Tests.sleep(1_000);
            }

            holders.getFirst().close(); // lets its replay go

            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (fifth.ask() != ReplayingDecoder.TEMPLATE_ID) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "the slot its subscriber let go never freed");
                Tests.sleep(500);
            }
        } finally {
            CloseHelper.closeAll(holders);
        }
    }

    @Test
    @InterruptAfter(60)
    void eachQueuesBacklogIsFoundInItsMembersCountersAgainstItsOwnRecording() {
        final int member = cluster.awaitLeader();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                                                             .aeron(aeron)
                                                             .ownsAeronClient(false)
                                                             .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                                             .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlResponseStreamId(RESPONSE_STREAM_ID))) {
            final AeronReplayer replayer = new AeronReplayer(aeron, archive, member);
            final List<Replayer.RecordingSpan> spans = new ArrayList<>();
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                final Replayer.RecordingSpan span = activeRecording(replayer, groupId);
                spans.add(span);
                final Replayer.QueueBacklog backlog = replayer.queueBacklog(groupId, span);
                assertNotNull(backlog, "group " + groupId + "'s queue publication was not found");
                assertTrue(backlog.window() > 0, "group " + groupId + ": " + backlog);
                assertTrue(backlog.bytes() >= 0 && backlog.bytes() <= backlog.window(), "group " + groupId + ": " + backlog);
            }
            assertNull(replayer.queueBacklog(InProcessCluster.GROUP_ID, spans.getLast()),
                       "a queue's publication was matched to another queue's recording");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(120)
    void aSecondReplayIsHeldWhileTheRecordingTrailsItsQueueByMoreThanAQuarterOfItsWindow() {
        final int leader = cluster.awaitLeader();
        // A member's recorder records the Raft log too, so holding it would hold commits; the host's records queues only.
        System.setProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME, GatedChecksum.class.getName());
        try {
            cluster.startGatewayHost();
        } finally {
            System.clearProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME);
        }
        final String hostDir = cluster.aeronDir(InProcessCluster.GATEWAY_HOST);
        final List<Client> clients = new ArrayList<>();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(hostDir));
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                                                             .aeron(aeron)
                                                             .ownsAeronClient(false)
                                                             .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                                             .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlResponseStreamId(RESPONSE_STREAM_ID))) {
            final AeronReplayer replayer = new AeronReplayer(aeron, archive, InProcessCluster.GATEWAY_HOST);
            final Replayer.RecordingSpan span = activeRecording(replayer, InProcessCluster.GROUP_ID);
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (replayer.recordingPosition(span.recordingId()) <= span.startPosition()) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "the relay brought the host nothing");
                Tests.sleep(100);
            }
            final Client holder = new Client(aeron);
            clients.add(holder);
            holder.askUntilServed();
            holder.attach(); // and never reads it

            final Client second = new Client(aeron);
            clients.add(second);
            GatedChecksum.shut(true);
            try {
                trailBeyondAQuarter(leader, replayer, span);
                assertEquals(ReplayPendingDecoder.TEMPLATE_ID, second.ask(), "three of four slots are free, but the recording trails");
                assertTrue(counter(aeron, SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID) > 0);
            } finally {
                GatedChecksum.shut(false);
            }
            second.askUntilServed(); // once the recording has caught up
        } finally {
            CloseHelper.closeAll(clients);
        }
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    /**
     * Sends ingress until group {@link InProcessCluster#GROUP_ID}'s recording on the host trails its queue by more than
     * a quarter of the window, and stops there: past the whole window the relay is back-pressured, and stalls.
     */
    private void trailBeyondAQuarter(final int leader, final Replayer replayer, final Replayer.RecordingSpan span) {
        try (ConfirmProducer producer =
                 new ConfirmProducer(cluster.aeronDir(leader), leader, Integer.MAX_VALUE, 0, 256)) {
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            Replayer.QueueBacklog backlog;
            while ((backlog = replayer.queueBacklog(InProcessCluster.GROUP_ID, span)) == null ||
                   backlog.bytes() <= backlog.window() / 4) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "the recording never fell behind: " + backlog);
                producer.doWork();
            }
            assertTrue(backlog.bytes() < backlog.window(), backlog.toString());
        }
    }

    private static long counter(final Aeron aeron, final int typeId) {
        final long[] value = {-1};
        aeron.countersReader().forEach((counterId, type, key, label) -> {
            if (type == typeId) {
                value[0] = aeron.countersReader().getCounterValue(counterId);
            }
        });
        return value[0];
    }

    private static Replayer.RecordingSpan activeRecording(final Replayer replayer, final int groupId) {
        final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
        while (true) {
            for (final Replayer.RecordingSpan span : replayer.listQueueRecordings(groupId)) {
                if (span.active()) {
                    return span;
                }
            }
            assertTrue(System.currentTimeMillis() < deadlineMs, "group " + groupId + " has no active recording");
            Tests.sleep(100);
        }
    }

    /** One client of the member's Replayer, reading the control stream for itself, as an application does. */
    private static final class Client implements AutoCloseable {
        private final Aeron aeron;
        private final long clientId;
        private final Subscription control;
        private final Publication requests;
        private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        private final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final ReplayingDecoder replaying = new ReplayingDecoder();
        private final ReplayPendingDecoder pending = new ReplayPendingDecoder();
        private long requestId;
        private int templateId;
        private long replaySessionId;
        private Subscription replay;

        Client(final Aeron aeron) {
            this.aeron = aeron;
            clientId = aeron.nextCorrelationId();
            control = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
            requests = aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
        }

        /** Asks for a walk of the queue and returns the template of the answer, resending as a client does. */
        int ask() {
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadlineMs) {
                encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .clientId(clientId)
                    .requestId(++requestId)
                    .fromPosition(ReplayProtocol.FROM_START)
                    .queueId(InProcessCluster.GROUP_ID)
                    .cutClusterSeqNo(ReplayProtocol.NO_CUT);
                if (requests.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength()) < 0) {
                    Tests.sleep(10);
                    continue;
                }
                templateId = 0;
                final long resendAtMs = System.currentTimeMillis() + 500;
                while (templateId == 0 && System.currentTimeMillis() < resendAtMs) {
                    if (control.poll(this::onReply, 10) == 0) {
                        Tests.yield();
                    }
                }
                if (templateId != 0) {
                    return templateId;
                }
            }
            throw new AssertionError("client " + clientId + " had no answer");
        }

        /** Asks until it is served, as a client held behind a slot or a not yet ready Replayer does. */
        void askUntilServed() {
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (ask() != ReplayingDecoder.TEMPLATE_ID) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "client " + clientId + " was never served");
                Tests.sleep(500);
            }
        }

        /** Subscribes to the replay it was answered with, as a client does to read it. */
        void attach() {
            replay = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replaySessionId,
                                           ReplayProtocol.REPLAY_STREAM_ID);
        }

        private void onReply(final org.agrona.DirectBuffer buffer, final int offset, final int length,
                             final io.aeron.logbuffer.Header unused) {
            header.wrap(buffer, offset);
            final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
                replaying.wrap(buffer, body, header.blockLength(), header.version());
                if (replaying.clientId() == clientId && replaying.requestId() == requestId) {
                    assertTrue(replaying.replaySessionId() >= 0, "a walk of a queue with frames in it");
                    replaySessionId = replaying.replaySessionId();
                    templateId = ReplayingDecoder.TEMPLATE_ID;
                }
            } else if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
                pending.wrap(buffer, body, header.blockLength(), header.version());
                if (pending.clientId() == clientId && pending.requestId() == requestId) {
                    templateId = ReplayPendingDecoder.TEMPLATE_ID;
                }
            }
        }

        @Override
        public void close() {
            CloseHelper.closeAll(replay, control, requests);
            replay = null;
        }
    }
}
