package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * The Replayer's timer boundaries, crossed exactly: a member's Replayer over its real archive and streams, made by
 * {@link FaultableReplayer} to back-pressure its control stream, read no requests, start a replay slowly or not at all,
 * or end or freeze its replays. The scripts approach each of these by timing.
 */
@ExtendWith(InterruptingTestCallback.class)
class ReplayerFaultsTest {
    private static final long TIMEOUT_MS = 20_000;

    /** {@code ReplayerService.STALLED_REPLAY_TIMEOUT_MS}, which it keeps to itself. */
    private static final long STALLED_REPLAY_TIMEOUT_MS = 10_000;

    private static final long RESEND_MS = 500;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;
    private int member;
    private FaultableReplayer replayer;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        member = cluster.awaitLeader();
        replayer = cluster.replayer(member);
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @InterruptAfter(60)
    void aControlStreamThatBackPressuresDropsTheReplyAtTheSpinBoundAndTheResendIsServed() {
        try (Aeron aeron = aeron(); Client client = new Client(aeron)) {
            final long dropped = counter(aeron, SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID);
            replayer.backPressureControl(true);
            client.newRequest();
            assertEquals(0, client.ask(RESEND_MS), "a reply the control stream refused arrived");
            awaitCounterAbove(aeron, SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID, dropped);
            replayer.backPressureControl(false);
            assertEquals(ReplayingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS), "the resend was not served");
        } finally {
            replayer.backPressureControl(false);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(60)
    void aReplayerThatReadsNoRequestsAnswersNothingAndServesTheRequestOnceItReads() {
        try (Aeron aeron = aeron(); Client client = new Client(aeron)) {
            replayer.starveRequests(true);
            client.newRequest();
            assertEquals(0, client.ask(3_000), "a starved Replayer answered");
            replayer.starveRequests(false);
            assertEquals(ReplayingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS), "the queued request was not served");
        } finally {
            replayer.starveRequests(false);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(60)
    void resendsOfOneRequestQueuedBehindASlowStartReplayStartOneReplay() {
        try (Aeron aeron = aeron(); Client client = new Client(aeron)) {
            final long served = counter(aeron, SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID);
            replayer.slowStartReplay(3 * RESEND_MS + RESEND_MS / 2);
            client.newRequest();
            assertEquals(ReplayingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS), "a slow start was not answered");
            assertTrue(client.resends() >= 3, "the request was not resent behind the slow start: " + client.resends());
            replayer.slowStartReplay(0);
            Tests.sleep(RESEND_MS);
            assertEquals(served + 1, counter(aeron, SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID),
                         "a resend started another replay");
        } finally {
            replayer.slowStartReplay(0);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(60)
    void aStartReplayThatThrowsIsCountedAndAnsweredPendingAndTheResendIsServed() {
        try (Aeron aeron = aeron(); Client client = new Client(aeron)) {
            final long errors = counter(aeron, SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID);
            replayer.failStartReplay(true);
            client.newRequest();
            assertEquals(ReplayPendingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS), "a refused replay was not pending");
            assertTrue(counter(aeron, SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID) > errors,
                       "the archive error was not counted");
            replayer.failStartReplay(false);
            client.newRequest();
            assertEquals(ReplayingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS), "the next request was not served");
        } finally {
            replayer.failStartReplay(false);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(90)
    void aReplayWhoseProgressStopsLosesItsSlotOnlyAfterTheStallTimeout() {
        try (Aeron aeron = aeron(); Client client = new Client(aeron)) {
            client.newRequest();
            assertEquals(ReplayingDecoder.TEMPLATE_ID, client.ask(TIMEOUT_MS));
            client.attach();
            assertEquals(1, counter(aeron, SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));

            final long startedMs = System.currentTimeMillis();
            replayer.holdReplayProgress(true);
            try {
                // The slot is held while the limit is unmoved for less than the timeout, though the image is read.
                Tests.sleep(STALLED_REPLAY_TIMEOUT_MS - 3_000);
                assertEquals(1, counter(aeron, SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID),
                             "the slot was reclaimed before the stall timeout");
                final long deadlineMs = startedMs + STALLED_REPLAY_TIMEOUT_MS + 5_000;
                while (counter(aeron, SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID) != 0) {
                    assertTrue(System.currentTimeMillis() < deadlineMs, "the stalled replay never lost its slot");
                    Tests.sleep(100);
                }
                assertTrue(System.currentTimeMillis() - startedMs >= STALLED_REPLAY_TIMEOUT_MS,
                           "the slot was reclaimed before the stall timeout");
            } finally {
                replayer.holdReplayProgress(false);
            }
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(60)
    void replaysThatEndFreeTheirSlotsAtOnceAndAFifthClientIsServed() {
        final List<Client> clients = new ArrayList<>();
        try (Aeron aeron = aeron()) {
            for (int i = 0; i < 4; i++) {
                final Client holder = new Client(aeron);
                clients.add(holder);
                holder.newRequest();
                assertEquals(ReplayingDecoder.TEMPLATE_ID, holder.ask(TIMEOUT_MS), "holder " + i + " is served");
                holder.attach(); // and stops reading, its client alive
            }
            final Client fifth = new Client(aeron);
            clients.add(fifth);
            fifth.newRequest();
            assertEquals(ReplayPendingDecoder.TEMPLATE_ID, fifth.ask(TIMEOUT_MS), "every slot is held");

            replayer.endReplays(true);
            try {
                // Far inside the stall timeout, which is not what freed them.
                assertEquals(ReplayingDecoder.TEMPLATE_ID, fifth.ask(STALLED_REPLAY_TIMEOUT_MS / 2),
                             "the ended replays' slots never freed");
            } finally {
                replayer.endReplays(false);
            }
        } finally {
            CloseHelper.closeAll(clients);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    private Aeron aeron() {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
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

    private static void awaitCounterAbove(final Aeron aeron, final int typeId, final long above) {
        final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
        while (counter(aeron, typeId) <= above) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "counter " + typeId + " never rose above " + above);
            Tests.sleep(50);
        }
    }

    /**
     * One client of the member's Replayer, reading the control stream for itself. It resends a request under its own
     * id every {@link #RESEND_MS}, as a client does (R-1), until it is answered.
     */
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
        private int resends;
        private long replaySessionId;
        private Subscription replay;

        Client(final Aeron aeron) {
            this.aeron = aeron;
            clientId = aeron.nextCorrelationId();
            control = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
            requests = aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
        }

        /** Starts a new request, which {@link #ask} sends and resends. */
        void newRequest() {
            ++requestId;
            templateId = 0;
            resends = 0;
            send();
        }

        int resends() {
            return resends;
        }

        /**
         * Waits for the current request's answer, resending it under its id every {@link #RESEND_MS}.
         * @return the answer's template, or 0 if none came in {@code budgetMs}
         */
        int ask(final long budgetMs) {
            final long deadlineMs = System.currentTimeMillis() + budgetMs;
            long resendAtMs = System.currentTimeMillis() + RESEND_MS;
            while (templateId == 0 && System.currentTimeMillis() < deadlineMs) {
                if (control.poll(this::onReply, 10) == 0) {
                    Tests.yield();
                }
                if (templateId == 0 && System.currentTimeMillis() >= resendAtMs) {
                    ++resends;
                    send();
                    resendAtMs = System.currentTimeMillis() + RESEND_MS;
                }
            }
            final int answered = templateId;
            templateId = 0; // a later ask() of the same request, such as after a fault is lifted, waits for a new answer
            return answered;
        }

        private void send() {
            encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .requestId(requestId)
                .fromPosition(ReplayProtocol.FROM_START)
                .queueId(InProcessCluster.GROUP_ID)
                .cutClusterSeqNo(ReplayProtocol.NO_CUT);
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
            while (requests.offer(buffer, 0, length) < 0) {
                Tests.sleep(10);
            }
        }

        /** Subscribes to the replay it was answered with, as a client does to read it. */
        void attach() {
            replay = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replaySessionId,
                                           ReplayProtocol.REPLAY_STREAM_ID);
        }

        private void onReply(final DirectBuffer buf, final int offset, final int length,
                             final io.aeron.logbuffer.Header unused) {
            header.wrap(buf, offset);
            final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
                replaying.wrap(buf, body, header.blockLength(), header.version());
                if (replaying.clientId() == clientId && replaying.requestId() == requestId) {
                    replaySessionId = replaying.replaySessionId();
                    templateId = ReplayingDecoder.TEMPLATE_ID;
                }
            } else if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
                pending.wrap(buf, body, header.blockLength(), header.version());
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
