package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.QueueRequestEncoder;
import org.limitless.seqeron.sbe.replay.QueueResolvedDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;
import org.limitless.seqeron.sbe.replay.SnapshotConfirmedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotJoinedEncoder;
import org.limitless.seqeron.util.Logger;

/**
 * The replay protocol's own decisions — which recording answers a request, when the archive cannot be
 * believed, when to hold, when to refuse, when to stall — driven against {@link
 * FakeReplayer} rather than a live node. Requests go in as real
 * SBE bytes on the request stream and replies are decoded off the control stream, so the wire format
 * is under test alongside the logic.
 *
 * <p>Every assertion the {@code Logger} carries goes through a recording sink: the fault codes are the
 * operator-visible half of these paths, and an anomaly that reports itself twice (or not at all) is a
 * defect in its own right.
 */
class ReplayerServiceTest {
    private static final int MEMBER_ID = 0;
    private static final long CLIENT = 7;
    private static final long OTHER_CLIENT = 8;
    private static final long FROM_START = ReplayProtocol.FROM_START;
    private static final int GROUP_ID = FakeReplayer.GROUP_ID;
    private static final int OTHER_GROUP_ID = 2;

    /**
     * One decoded control reply. {@code catchUpPosition} is unset on non-Replaying replies, and {@code queueId} on
     * all but QueueResolved.
     */
    private record
        Reply(int templateId, long clientId, long requestId, long replaySessionId, long catchUpPosition, int queueId) {
    }

    private final List<Logger.LoggerEvent> events = new ArrayList<>();
    private final AtomicInteger fatalCount = new AtomicInteger();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(128);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    private final Queue<byte[]> topologyRows = new ConcurrentLinkedQueue<>();
    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;

    @BeforeEach
    void setUp() {
        Logger.install(events::add);
        fakeReplayer = new FakeReplayer();
        serve(QueueResolver.EVERY_GROUP, new TopologyLoad().groups(GROUP_ID, "group-1"));
    }

    /** Replaces the service with one relaying {@code relayedGroups}, handed {@code load}'s rows. */
    private void serve(final int relayedGroups, final TopologyLoad load) {
        topologyRows.clear();
        topologyRows.addAll(load.rows());
        replayerService = new ReplayerService(fakeReplayer, MEMBER_ID, NoOpIdleStrategy.INSTANCE, topologyRows,
                                              relayedGroups, fatalCount::incrementAndGet);
    }

    @AfterEach
    void tearDown() {
        Logger.reset();
    }

    // ── Readiness ───────────────────────────────────────────────────────────────

    @Test
    void aVisibleActiveRecordingMakesTheNodeReady() {
        replayerService.poll();
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID), "nothing recorded yet");

        fakeReplayer.addRecording(6, 0, true, 4096);
        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));
        assertTrue(fakeReplayer.startedReplays().isEmpty(), "readiness replays nothing");
    }

    @Test
    void aWalkOfAnActiveRecordingWithNothingInItYetHasNothingToReplay() {
        fakeReplayer.addRecording(6, 0, true, 0); // recording armed, sequencer has published nothing yet
        makeReady();

        final Reply reply = request(CLIENT, 1, FROM_START);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertTrue(noClientReplayStarted());
    }

    @Test
    void requestsArrivingBeforeReadinessAreAnsweredWithoutBecomingPending() {
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        replayerService.poll();

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, lastReply().templateId());
        // The app's own resend timer is the retry — holding it here would only re-try the same not-ready state.
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
    }

    @Test
    void aRequestArrivingInTheCycleReadinessFlipsIsHeldUntilTheAppsNextResend() {
        fakeReplayer.addRecording(6, 0, true, 4096);

        // poll() answers requests BEFORE checking readiness, because a not-ready Replayer still owes every
        // request an answer and answering costs nothing, whereas the check makes archive control calls. This
        // one-cycle delay is that ordering's whole cost.
        final Reply held = request(CLIENT, 1, FROM_START);
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, held.templateId());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 2, FROM_START).templateId());
    }

    @Test
    void aMessageOfAnotherSchemaOrAnUnknownTemplateIsNotAnswered() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        final byte[] otherSchema = replayRequest(CLIENT, 1, FROM_START);
        new MessageHeaderEncoder().wrap(new UnsafeBuffer(otherSchema), 0).schemaId(ReplayRequestEncoder.SCHEMA_ID + 1);
        fakeReplayer.offerRequest(otherSchema);
        replayerService.poll();
        final byte[] unknownTemplate = replayRequest(CLIENT, 2, FROM_START);
        new MessageHeaderEncoder().wrap(new UnsafeBuffer(unknownTemplate), 0).templateId(99);
        fakeReplayer.offerRequest(unknownTemplate);
        replayerService.poll();

        assertTrue(fakeReplayer.controlReplies().isEmpty());
        assertTrue(noClientReplayStarted());
    }

    @Test
    void aSnapshotSignalToAReplayerThatTracksNoCutsIsIgnored() {
        // A gateway or application whose snapshot="true" row has no <snapshots> policy beside it still signals.
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        final SnapshotJoinedEncoder joined = new SnapshotJoinedEncoder();
        joined.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(CLIENT).sourceId(16).round(1);
        fakeReplayer.offerRequest(encoded(joined.encodedLength()));
        replayerService.poll();
        final SnapshotConfirmedEncoder confirmed = new SnapshotConfirmedEncoder();
        confirmed.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(CLIENT).sourceId(16).round(1);
        fakeReplayer.offerRequest(encoded(confirmed.encodedLength()));
        replayerService.poll();

        assertTrue(fakeReplayer.controlReplies().isEmpty());
        assertEquals(0, fatalCount.get());
    }

    // ── Cold-start walk ─────────────────────────────────────────────────────────

    @Test
    void aWalkServesTheActiveRecordingFromItsOwnStartPosition() {
        fakeReplayer.addRecording(5, 0, false, 4096);
        fakeReplayer.addRecording(7, 4096, true, 9000);
        makeReady();

        assertEquals(9000, request(CLIENT, 1, FROM_START).catchUpPosition());

        final FakeReplayer.StartedReplay replay = lastClientReplay();
        assertEquals(7, replay.recordingId());
        assertEquals(4096, replay.position()); // the recording's own startPosition, not 0
        assertEquals(9000 - 4096, replay.length());
        assertTrue(noClientReplayFor(5));
    }

    @Test
    void aTipNeitherCounterCanReportHoldsTheRequestInsteadOfAnsweringIt() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.hideTip(6); // RecordingPos gone, stopPosition not written yet

        final Reply reply = request(CLIENT, 1, FROM_START);

        // Reported as a tip, this transient read would tell a walking app it is caught up.
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID),
                     "the client's resend is the retry");
        assertTrue(noClientReplayFor(6));
    }

    @Test
    void aQueueWithNoActiveRecordingAnymoreHoldsTheRequestWithoutQueueingIt() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.stopRecording(6); // nothing records the queue now

        final Reply reply = request(CLIENT, 1, FROM_START);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID),
                     "the client's resend is the retry");
        assertTrue(noClientReplayStarted());
    }

    @Test
    void moreThanOneActiveRecordingIsReportedOnceAndTheNewestIsServed() {
        fakeReplayer.addRecording(5, 0, true, 1000); // left unstopped by an unclean shutdown
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        // Picking the stale one would walk or resume an app into a prefix.
        request(CLIENT, 1, FROM_START);
        assertEquals(6, lastClientReplay().recordingId());
        request(CLIENT, 2, 0);
        assertEquals(6, lastClientReplay().recordingId());

        assertTrue(loggedOnce(Logger.CoreEventCode.StaleActiveRecording));
    }

    @Test
    void aSecondStaleActiveRecordingIsReportedAgainAfterTheFirstWasRepaired() {
        fakeReplayer.addRecording(5, 0, true, 1000); // left unstopped by an unclean shutdown
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, FROM_START);

        fakeReplayer.stopRecording(5); // operator repairs it
        request(CLIENT, 2, FROM_START);
        assertEquals(1, logCount(Logger.CoreEventCode.StaleActiveRecording), "the repair is not a new episode");

        fakeReplayer.orphanRecording(6); // a later unclean shutdown leaves 6 unstopped
        fakeReplayer.addRecording(7, 4096, true, 5000);
        request(CLIENT, 3, FROM_START);

        // This anomaly has no counter, so a latch that never re-arms makes every later episode silent.
        assertEquals(2, logCount(Logger.CoreEventCode.StaleActiveRecording));
        assertEquals(7, lastClientReplay().recordingId());
    }

    @Test
    void theActiveRecordingIsListedOnceWhileItKeepsRecording() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, FROM_START);
        final int listings = fakeReplayer.listings();

        // Each listing is an archive round trip on the one thread that serves every app.
        request(CLIENT, 2, FROM_START);
        request(CLIENT, 3, 2048);
        assertEquals(listings, fakeReplayer.listings());

        fakeReplayer.orphanRecording(6); // the archive stopped writing it
        request(CLIENT, 4, FROM_START);
        assertEquals(listings + 1, fakeReplayer.listings());
    }

    // ── Steady-state resume ─────────────────────────────────────────────────────

    @Test
    void aResumeReplaysTheActiveRecordingFromTheAppsOwnPosition() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        final Reply reply = request(CLIENT, 1, 2048);

        assertEquals(4096, reply.catchUpPosition());
        assertEquals(6, lastClientReplay().recordingId());
        assertEquals(2048, lastClientReplay().position());
    }

    @Test
    void aResumeTheRecordingHasNotReachedYetIsHeld() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        // The client resumes from a frame it read off the queue, which the recording may still trail.
        final Reply reply = request(CLIENT, 1, 4096);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertTrue(noClientReplayStarted());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID),
                     "the client's resend is the retry");
    }

    @Test
    void aResumeBelowTheActiveRecordingsStartIsRefused() {
        fakeReplayer.addRecording(6, 2048, true, 4096);
        makeReady();

        final Reply reply = request(CLIENT, 1, 1024);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertTrue(noClientReplayStarted());
    }

    // ── Restoring at a snapshot's cut ───────────────────────────────────────────

    /** A recording from 4096 holding frames of clusterSeqNo 40, 45, 50 and 55, one per KiB. */
    private void recordingWithCuts() {
        fakeReplayer.addRecording(6, 4096, true, 8192);
        fakeReplayer.addFrames(6, 4096, 40, 5120, 45, 6144, 50, 7168, 55);
        makeReady();
    }

    @Test
    void aCutAtTheHintedPositionIsFoundByReadingOneFrame() {
        recordingWithCuts();

        final Reply reply = cutRequest(CLIENT, 1, 6144, 50);

        assertEquals(8192, reply.catchUpPosition());
        assertEquals(6144, lastClientReplay().position());
        assertEquals(List.of(6144L), fakeReplayer.scans());
    }

    @Test
    void aHintThatIsNoFrameBoundaryFallsBackToReadingFromTheStart() {
        recordingWithCuts();

        cutRequest(CLIENT, 1, 6000, 50);

        assertEquals(6144, lastClientReplay().position());
        assertEquals(List.of(6000L, 4096L), fakeReplayer.scans());
    }

    @Test
    void aHintPastTheCutFallsBackToReadingFromTheStart() {
        recordingWithCuts();

        cutRequest(CLIENT, 1, 7168, 50);

        assertEquals(6144, lastClientReplay().position(), "the cut is where its frame is, not where the file says");
        assertEquals(List.of(7168L, 4096L), fakeReplayer.scans());
    }

    @Test
    void aCutKeptWhenItWasRecordedIsFoundWithoutReading() {
        recordingWithCuts();
        replayerService.keepCut(GROUP_ID, 50, 6144);

        cutRequest(CLIENT, 1, 0, 50);

        assertEquals(6144, lastClientReplay().position());
        assertTrue(fakeReplayer.scans().isEmpty());
    }

    @Test
    void aCutFoundByReadingIsKeptSoTheResendDoesNotReadAgain() {
        recordingWithCuts();
        cutRequest(CLIENT, 1, 6000, 50);

        cutRequest(CLIENT, 2, 6000, 50);

        assertEquals(6144, lastClientReplay().position());
        assertEquals(List.of(6000L, 4096L), fakeReplayer.scans());
    }

    @Test
    void aCutSearchTheArchiveStallsIsHeldForTheResend() {
        recordingWithCuts();
        fakeReplayer.stallScans();

        final Reply reply = cutRequest(CLIENT, 1, 6144, 50);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId(), "not refused, which costs the snapshot");
        assertTrue(noClientReplayStarted());
        assertEquals(List.of(6144L, 4096L), fakeReplayer.scans(), "a stalled hint falls back to the start");
    }

    @Test
    void aCutWhoseHintedScanStallsIsFoundByReadingFromTheStart() {
        recordingWithCuts();
        fakeReplayer.stallNextScan();

        final Reply reply = cutRequest(CLIENT, 1, 6144, 50);

        assertEquals(8192, reply.catchUpPosition());
        assertEquals(6144, lastClientReplay().position());
        assertEquals(List.of(6144L, 4096L), fakeReplayer.scans());
    }

    @Test
    void aCutBeforeTheRecordingsFirstFrameIsRefused() {
        recordingWithCuts();

        final Reply reply = cutRequest(CLIENT, 1, 4096, 30);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertTrue(noClientReplayStarted());
    }

    @Test
    void aCutTheRecordingPassesWithoutAFrameAtItIsRefused() {
        recordingWithCuts();

        final Reply reply = cutRequest(CLIENT, 1, 5120, 47);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, reply.replaySessionId(), "another log's cut");
        assertTrue(noClientReplayStarted());
    }

    @Test
    void aCutTheRecordingHasNotReachedYetIsHeld() {
        recordingWithCuts();

        final Reply reply = cutRequest(CLIENT, 1, 7168, 70);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertTrue(noClientReplayStarted());
    }

    @Test
    void aKeptCutBelowTheRecordingsStartIsRefused() {
        recordingWithCuts();
        replayerService.keepCut(GROUP_ID, 20, 1024);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, cutRequest(CLIENT, 1, 1024, 20).replaySessionId());
    }

    @Test
    void aCutRequestWaitingForASlotStillNamesItsCut() {
        recordingWithCuts();
        for (int client = 100; client < 100 + ReplayerService.MAX_CONCURRENT_REPLAYS; client++) {
            request(client, 1, 4096);
        }

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, cutRequest(CLIENT, 1, 6144, 50).templateId());
        fakeReplayer.endReplay(fakeReplayer.startedReplays().getFirst().replaySessionId());
        replayerService.poll();

        assertEquals(6144, lastClientReplay().position());
    }

    // ── Local-archive errors ────────────────────────────────────────────────────

    @Test
    void anArchiveThatRefusesAWalkReplayHoldsTheClientAndIsCounted() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));

        final Reply reply = request(CLIENT, 1, FROM_START);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId(), "the client's resend is the retry");
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
    }

    @Test
    void everyArchiveErrorIsCountedButReportedOncePerEpisode() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));

        request(CLIENT, 1, FROM_START);
        request(CLIENT, 2, FROM_START);
        request(CLIENT, 3, FROM_START);
        assertEquals(3, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
        assertEquals(1, archiveErrorReports(), "one episode is one line, not one per resend");

        fakeReplayer.advanceMillis(60_000);
        request(CLIENT, 4, FROM_START);
        assertEquals(2, archiveErrorReports(), "a distinct episode names itself");
    }

    @Test
    void anArchiveThatComesBackServesTheNextResend() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));
        request(CLIENT, 1, FROM_START);

        fakeReplayer.serveReplaysAgain();
        final Reply reply = request(CLIENT, 2, FROM_START);

        assertEquals(ReplayingDecoder.TEMPLATE_ID, reply.templateId());
    }

    @Test
    void aResumeTheArchiveRefusesIsRefusedRatherThanCountedAsAnError() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive refused the position"));

        final Reply reply = request(CLIENT, 1, 2048);

        // The archive still answers, so it is the position that is wrong — one the client supplied, in
        // range but not on a frame boundary. Answering ReplayPending to a position that can never work would
        // hold that app forever.
        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
    }

    @Test
    void aResumeAnArchiveOutageRefusesIsHeldRatherThanRefused() {
        // The same exception, the opposite fault. Reading every refused resume as a bad position would fail a
        // whole node's worth of apps because the archive was down.
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        replayerService.poll(); // opens the snapshot index's replay, the Replayer's own archive call
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));

        final Reply reply = request(CLIENT, 1, 2048);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId(), "hold at the gap, keep the position");
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
    }

    @Test
    void theReplayersOwnArchiveCallsWaitAfterOneThrows() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));
        replayerService.poll();
        final int listings = fakeReplayer.listings();

        for (int cycle = 0; cycle < 10; ++cycle) {
            replayerService.poll();
        }

        assertEquals(listings, fakeReplayer.listings(), "a dead archive must not be hammered every cycle");
        fakeReplayer.advanceMillis(1_500);
        replayerService.poll();
        assertEquals(listings + 1, fakeReplayer.listings(), "one attempt per interval, though");
    }

    @Test
    void anArchiveThatRefusesTheStartupListingIsReportedRatherThanLookingMerelySlow() {
        // A node whose archive never answered would otherwise look like one that was merely slow to become ready.
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));

        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));
        assertEquals(0, fatalCount.get(), "a transient archive fault must not take the duty cycle down");

        fakeReplayer.answerArchiveAgain();
        fakeReplayer.advanceMillis(1_500);
        makeReady();
    }

    @Test
    void everyDutyCycleReadsTheArchivesControlResponses() {
        // The archive pings each control session and closes one whose responses go unread, so an idle
        // node that never read them lost its session and could serve no replay again.
        replayerService.poll();
        replayerService.poll();
        replayerService.poll();

        assertEquals(3, fakeReplayer.archivePolls());
    }

    @Test
    void anArchiveThatClosedTheControlSessionTakesTheDutyCycleDownLoudly() {
        fakeReplayer.closeArchiveSession();

        assertThrows(IllegalStateException.class, () -> replayerService.poll());
    }

    @Test
    void anErrorAnsweringOneRequestIsCountedAndTheDutyCycleRunsOn() {
        // A replay whose client left before it connected is answered with an error, and the session stays open.
        fakeReplayer.answerArchiveRequestWithError("no connection established for replayChannel=aeron:ipc");

        replayerService.poll();
        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
        assertEquals(0, fatalCount.get());
    }

    // ── Slots ───────────────────────────────────────────────────────────────────

    @Test
    void aClientBeyondTheConcurrencyCapIsHeldAndServedWhenASlotFrees() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        for (int client = 1; client <= ReplayerService.MAX_CONCURRENT_REPLAYS; client++) {
            assertEquals(ReplayingDecoder.TEMPLATE_ID, request(client, 1, FROM_START).templateId());
        }
        final long firstSession = fakeReplayer.startedReplays().getFirst().replaySessionId();
        final int lateClient = ReplayerService.MAX_CONCURRENT_REPLAYS + 1;

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(lateClient, 1, FROM_START).templateId());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID));

        fakeReplayer.endReplay(firstSession);
        replayerService.poll();

        final Reply served = lastReply();
        assertEquals(ReplayingDecoder.TEMPLATE_ID, served.templateId());
        assertEquals(lateClient, served.clientId());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
        assertEquals(ReplayerService.MAX_CONCURRENT_REPLAYS,
                     fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
    }

    @Test
    void whileTheQueueRecordingIsBehindASecondReplayWaitsForTheFirstToEnd() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        final long window = FakeReplayer.TERM_BUFFER_LENGTH / 2;
        fakeReplayer.queueBacklog(new Replayer.QueueBacklog(window / 4 + 1, window));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 1, FROM_START).templateId());
        final long session = lastClientReplay().replaySessionId();
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(OTHER_CLIENT, 1, FROM_START).templateId());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID));

        fakeReplayer.endReplay(session);
        replayerService.poll();

        final Reply served = lastReply();
        assertEquals(ReplayingDecoder.TEMPLATE_ID, served.templateId());
        assertEquals(OTHER_CLIENT, served.clientId());
    }

    @Test
    void aQueueRecordingThatCatchesUpLetsTheNextResendRunAlongside() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        final long window = FakeReplayer.TERM_BUFFER_LENGTH / 2;
        fakeReplayer.queueBacklog(new Replayer.QueueBacklog(window / 4 + 1, window));
        request(CLIENT, 1, FROM_START);
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(OTHER_CLIENT, 1, FROM_START).templateId());

        fakeReplayer.queueBacklog(new Replayer.QueueBacklog(window / 4, window));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(OTHER_CLIENT, 2, FROM_START).templateId());
        assertEquals(2, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID));
    }

    @Test
    void aBacklogTheCountersCannotBeReadForThrottlesNothing() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failQueueBacklog(new IllegalStateException("counters gone"));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 1, FROM_START).templateId());
        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(OTHER_CLIENT, 1, FROM_START).templateId());

        assertEquals(2, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID));
    }

    @Test
    void aSlotIsHeldForAsLongAsItsReplayRunsAndFreedWhenItEnds() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, FROM_START);
        final long session = lastClientReplay().replaySessionId();

        // A replay has no time limit: a full-log walk over a log that grows with uptime can run long.
        fakeReplayer.advanceMillis(60_000);
        replayerService.poll();
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));

        fakeReplayer.endReplay(session); // the archive ended it, or its client died
        replayerService.poll();

        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertFalse(fakeReplayer.stoppedReplays().contains(session), "nothing is left to stop");
    }

    @Test
    void aResendWhoseLastReplayTheArchiveCannotStopIsServedAnyway() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, FROM_START);
        final long superseded = lastClientReplay().replaySessionId();
        fakeReplayer.failStops(new IllegalStateException("unknown replay session")); // it ended on its own

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 2, FROM_START).templateId());

        assertTrue(lastClientReplay().replaySessionId() != superseded, "a replay of its own");
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
    }

    @Test
    void shutdownStopsEveryReplayItStillHolds() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, FROM_START);
        final long session = lastClientReplay().replaySessionId();

        replayerService.run(new AtomicBoolean(false));

        assertTrue(fakeReplayer.stoppedReplays().contains(session));
    }

    // ── Control-stream back-pressure ────────────────────────────────────────────

    @Test
    void aReplyToAnAppThatStoppedReadingIsDroppedAndCounted() {
        fakeReplayer.controlOfferResult(Publication.BACK_PRESSURED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        replayerService.poll();

        assertTrue(fakeReplayer.controlReplies().isEmpty());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
        assertTrue(loggedOnce(Logger.CoreEventCode.ControlReplyDropped));
    }

    @Test
    void aFreshBurstOfDroppedRepliesIsReportedAgainAfterAQuietPeriod() {
        fakeReplayer.controlOfferResult(Publication.BACK_PRESSURED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));
        replayerService.poll();

        // Same episode: a wedged app is answered on every resend, and the counter carries the rate.
        fakeReplayer.advanceMillis(1_000);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 2, FROM_START));
        replayerService.poll();
        assertEquals(1, logCount(Logger.CoreEventCode.ControlReplyDropped));

        fakeReplayer.advanceMillis(60_000);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 3, FROM_START));
        replayerService.poll();

        assertEquals(2, logCount(Logger.CoreEventCode.ControlReplyDropped), "a distinct episode names itself");
        assertEquals(3, fakeReplayer.counter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
    }

    @Test
    void aReplyToAnAppNotSubscribedYetIsDroppedWithoutBeingCounted() {
        fakeReplayer.controlOfferResult(Publication.NOT_CONNECTED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        replayerService.poll();

        // Ordinary: an app's first request can beat its own control subscription, and its resend covers it.
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
        assertFalse(loggedOnce(Logger.CoreEventCode.ControlReplyDropped));
    }

    @Test
    void aClosedControlPublicationTakesTheDutyCycleDownLoudly() {
        fakeReplayer.controlOfferResult(Publication.CLOSED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        assertThrows(IllegalStateException.class, () -> replayerService.poll());
    }

    @Test
    void aControlPublicationAtItsMaximumPositionTakesTheDutyCycleDownLoudly() {
        fakeReplayer.controlOfferResult(Publication.MAX_POSITION_EXCEEDED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        assertThrows(IllegalStateException.class, () -> replayerService.poll());
    }

    @Test
    void aDutyCycleThatDiesClearsReadinessAndSignalsTheNode() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.controlOfferResult(Publication.CLOSED);
        fakeReplayer.offerRequest(replayRequest(CLIENT, 1, FROM_START));

        replayerService.run(new AtomicBoolean(true));

        assertEquals(1, fatalCount.get());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));
        assertTrue(loggedOnce(Logger.CoreEventCode.ReplayDutyCycleFailure));
    }

    // ── Queues (R-5) ────────────────────────────────────────────────────────────

    @Test
    void nothingIsListedOrServedBeforeTheGroupTableArrives() {
        serve(QueueResolver.EVERY_GROUP, new TopologyLoad());
        fakeReplayer.addRecording(6, 0, true, 4096); // a previous run's, say: no group is active yet

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(CLIENT, 1, FROM_START).templateId());
        replayerService.poll();

        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));
        assertEquals(0, fakeReplayer.listings(), "until activation a node has no queue to list");
    }

    @Test
    void readinessWaitsForEveryServedQueuesRecording() {
        serve(QueueResolver.EVERY_GROUP, new TopologyLoad().groups(GROUP_ID, "a", OTHER_GROUP_ID, "b"));
        fakeReplayer.addRecording(GROUP_ID, 6, 0, true, 4096);
        replayerService.poll();
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID));

        fakeReplayer.addRecording(OTHER_GROUP_ID, 7, 0, true, 2048);
        makeReady();
    }

    @Test
    void eachQueueIsReplayedFromItsOwnRecording() {
        serve(QueueResolver.EVERY_GROUP, new TopologyLoad().groups(GROUP_ID, "a", OTHER_GROUP_ID, "b"));
        fakeReplayer.addRecording(GROUP_ID, 6, 0, true, 4096);
        fakeReplayer.addRecording(OTHER_GROUP_ID, 7, 0, true, 2048);
        makeReady();

        assertEquals(2048, request(CLIENT, 1, FROM_START, OTHER_GROUP_ID).catchUpPosition());
        assertEquals(7, lastClientReplay().recordingId());
        assertEquals(4096, request(OTHER_CLIENT, 1, FROM_START, GROUP_ID).catchUpPosition());
        assertEquals(6, lastClientReplay().recordingId());
    }

    @Test
    void aRequestForAQueueThisNodeDoesNotServeIsHeld() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        for (final int queueId : new int[] { 0, OTHER_GROUP_ID, 17 }) {
            assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(CLIENT, queueId, FROM_START, queueId).templateId(),
                         "queue " + queueId);
        }
        assertTrue(noClientReplayStarted());
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID),
                     "the client's resend is the retry");
    }

    @Test
    void whileAnyQueueRecordingIsBehindASecondReplayWaits() {
        serve(QueueResolver.EVERY_GROUP, new TopologyLoad().groups(GROUP_ID, "a", OTHER_GROUP_ID, "b"));
        fakeReplayer.addRecording(GROUP_ID, 6, 0, true, 4096);
        fakeReplayer.addRecording(OTHER_GROUP_ID, 7, 0, true, 2048);
        makeReady();
        final long window = FakeReplayer.TERM_BUFFER_LENGTH / 2;
        fakeReplayer.queueBacklog(OTHER_GROUP_ID, new Replayer.QueueBacklog(window / 4 + 1, window));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 1, FROM_START, GROUP_ID).templateId());

        // The lagging recorder shares the disk with every replay, whichever queue it is of.
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(OTHER_CLIENT, 1, FROM_START, GROUP_ID).templateId());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID));
    }

    // ── Queue resolution (R-6) ──────────────────────────────────────────────────

    @Test
    void aQueueRequestResolvesAGatewayAnApplicationOrTheDefaultQueueAndHoldsNoSlot() {
        serve(QueueResolver.EVERY_GROUP,
              new TopologyLoad().groups(3, "pricing", 5, "risk").gatewayRow(5, 9, "GW-A", 5).applicationRow(16, 3));

        assertEquals(5, queueRequest(CLIENT, 1, ReplayProtocol.NO_SOURCE_ID, "GW-A").queueId());
        assertEquals(3, queueRequest(CLIENT, 2, 16, "").queueId());
        assertEquals(3, queueRequest(CLIENT, 3, ReplayProtocol.NO_SOURCE_ID, "").queueId(), "the lowest active group");
        assertEquals(QueueResolvedDecoder.TEMPLATE_ID, lastReply().templateId());
        assertEquals(3, lastReply().requestId());

        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
    }

    @Test
    void aQueueRequestBeforeItsRowIsHeldAndLoggedOnceThenResolved() {
        final TopologyLoad load = new TopologyLoad().groups(GROUP_ID, "a", OTHER_GROUP_ID, "b");
        serve(QueueResolver.EVERY_GROUP, load);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID,
                     queueRequest(CLIENT, 1, ReplayProtocol.NO_SOURCE_ID, "GW-A").templateId());
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID,
                     queueRequest(CLIENT, 2, ReplayProtocol.NO_SOURCE_ID, "GW-A").templateId());
        assertTrue(loggedOnce(Logger.CoreEventCode.QueueUnresolved), "one line per source, not one per resend");
        queueRequest(OTHER_CLIENT, 1, 16, "");
        queueRequest(OTHER_CLIENT, 2, 16, "");
        assertEquals(2, logCount(Logger.CoreEventCode.QueueUnresolved), "and one more for each other source");

        final int handedOver = load.rows().size();
        load.gatewayRow(5, 9, "GW-A", OTHER_GROUP_ID);
        topologyRows.addAll(load.rows().subList(handedOver, load.rows().size()));

        assertEquals(OTHER_GROUP_ID, queueRequest(CLIENT, 3, ReplayProtocol.NO_SOURCE_ID, "GW-A").queueId());
    }

    @Test
    void aGatewayHostHoldsAndLogsARequestForAGroupItDoesNotRelay() {
        serve(0b10,
              new TopologyLoad()
                  .groups(GROUP_ID, "a", OTHER_GROUP_ID, "b")
                  .gatewayRow(5, 9, "GW-A", GROUP_ID)
                  .gatewayRow(6, 10, "GW-B", OTHER_GROUP_ID));

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID,
                     queueRequest(CLIENT, 1, ReplayProtocol.NO_SOURCE_ID, "GW-A").templateId());
        assertTrue(loggedOnce(Logger.CoreEventCode.QueueUnresolved));
        assertEquals(OTHER_GROUP_ID, queueRequest(CLIENT, 2, ReplayProtocol.NO_SOURCE_ID, "GW-B").queueId());
        assertEquals(OTHER_GROUP_ID, queueRequest(CLIENT, 3, ReplayProtocol.NO_SOURCE_ID, "").queueId(),
                     "the lowest queue the host relays");
    }

    @Test
    void aQueueRequestNamingBothAGatewayAndAnApplicationIsLoggedAndNotAnswered() {
        fakeReplayer.offerRequest(queueRequestBytes(CLIENT, 1, 16, "GW-A"));
        replayerService.poll();

        assertTrue(fakeReplayer.controlReplies().isEmpty());
        assertTrue(loggedOnce(Logger.CoreEventCode.QueueUnresolved));
    }

    // ── Fixtures and helpers ────────────────────────────────────────────────────

    /** Polls until the node is serving. */
    private void makeReady() {
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; ++cycle) {
            replayerService.poll();
        }
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID),
                     "fixture failed to make the node ready");
    }

    /** Delivers one ReplayRequest for {@link #GROUP_ID}'s queue and returns the reply it drew. */
    private Reply request(final long clientId, final long requestId, final long fromPosition) {
        return request(clientId, requestId, fromPosition, GROUP_ID);
    }

    private Reply request(final long clientId, final long requestId, final long fromPosition, final int queueId) {
        fakeReplayer.offerRequest(replayRequest(clientId, requestId, fromPosition, queueId));
        replayerService.poll();
        return lastReply();
    }

    /** Delivers a restore's request, naming the cut and hinting at its position. */
    private Reply cutRequest(final long clientId, final long requestId, final long hint, final long cut) {
        fakeReplayer.offerRequest(replayRequest(clientId, requestId, hint, GROUP_ID, cut));
        replayerService.poll();
        return lastReply();
    }

    /** Delivers one QueueRequest and returns the reply it drew. */
    private Reply queueRequest(final long clientId, final long requestId, final int sourceId,
                               final String gatewayName) {
        fakeReplayer.offerRequest(queueRequestBytes(clientId, requestId, sourceId, gatewayName));
        replayerService.poll();
        return lastReply();
    }

    private Reply lastReply() {
        final List<byte[]> replies = fakeReplayer.controlReplies();
        assertFalse(replies.isEmpty(), "no control reply was sent");
        final UnsafeBuffer buffer = new UnsafeBuffer(replies.getLast());
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, 0);
        final int offset = MessageHeaderDecoder.ENCODED_LENGTH;
        if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
            final ReplayingDecoder decoder = new ReplayingDecoder();
            decoder.wrap(buffer, offset, header.blockLength(), header.version());
            return new Reply(header.templateId(), decoder.clientId(), decoder.requestId(), decoder.replaySessionId(),
                             decoder.catchUpPosition(), 0);
        }
        if (header.templateId() == QueueResolvedDecoder.TEMPLATE_ID) {
            final QueueResolvedDecoder decoder = new QueueResolvedDecoder();
            decoder.wrap(buffer, offset, header.blockLength(), header.version());
            return new Reply(header.templateId(), decoder.clientId(), decoder.requestId(), 0, 0, decoder.queueId());
        }
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, header.templateId());
        final ReplayPendingDecoder decoder = new ReplayPendingDecoder();
        decoder.wrap(buffer, offset, header.blockLength(), header.version());
        return new Reply(header.templateId(), decoder.clientId(), decoder.requestId(), 0, 0, 0);
    }

    /** The most recent replay served to an app. */
    private FakeReplayer.StartedReplay lastClientReplay() {
        final List<FakeReplayer.StartedReplay> clientReplays =
            fakeReplayer.startedReplays()
                .stream()
                .filter(replay -> replay.streamId() == ReplayProtocol.REPLAY_STREAM_ID)
                .toList();
        assertFalse(clientReplays.isEmpty(), "no replay was started for an app");
        return clientReplays.getLast();
    }

    private boolean noClientReplayStarted() {
        return fakeReplayer.startedReplays().stream().noneMatch(r -> r.streamId() == ReplayProtocol.REPLAY_STREAM_ID);
    }

    /** No app was served out of {@code recordingId}. */
    private boolean noClientReplayFor(final long recordingId) {
        return fakeReplayer.startedReplays().stream().noneMatch(
            r -> r.recordingId() == recordingId && r.streamId() == ReplayProtocol.REPLAY_STREAM_ID);
    }

    /** Archive-error reports only: they share {@code Info} with every routine line, so only the text separates them. */
    private long archiveErrorReports() {
        return events.stream().filter(event -> event.message().startsWith("the local archive refused")).count();
    }

    private boolean loggedOnce(final Logger.EventCode code) {
        return logCount(code) == 1;
    }

    private long logCount(final Logger.EventCode code) {
        return events.stream().filter(event -> event.code() == code).count();
    }

    // ── Encoders ────────────────────────────────────────────────────────────────

    private byte[] replayRequest(final long clientId, final long requestId, final long fromPosition) {
        return replayRequest(clientId, requestId, fromPosition, GROUP_ID);
    }

    private byte[] replayRequest(final long clientId, final long requestId, final long fromPosition,
                                 final int queueId) {
        return replayRequest(clientId, requestId, fromPosition, queueId, ReplayProtocol.NO_CUT);
    }

    private byte[] replayRequest(final long clientId, final long requestId, final long fromPosition,
                                 final int queueId, final long cutClusterSeqNo) {
        final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .fromPosition(fromPosition)
            .queueId(queueId)
            .cutClusterSeqNo(cutClusterSeqNo);
        return encoded(encoder.encodedLength());
    }

    private byte[] queueRequestBytes(final long clientId, final long requestId, final int sourceId,
                                     final String gatewayName) {
        final QueueRequestEncoder encoder = new QueueRequestEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .sourceId(sourceId)
            .gatewayName(gatewayName);
        return encoded(encoder.encodedLength());
    }

    private byte[] encoded(final int bodyLength) {
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + bodyLength];
        encodeBuffer.getBytes(0, message);
        return message;
    }
}
