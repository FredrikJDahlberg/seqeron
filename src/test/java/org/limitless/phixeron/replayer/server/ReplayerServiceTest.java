package org.limitless.phixeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Publication;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.metrics.PhixeronCounters;
import org.limitless.phixeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.replay.ReplayCompleteEncoder;
import org.limitless.phixeron.sbe.replay.ReplayHeartbeatEncoder;
import org.limitless.phixeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.phixeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.phixeron.sbe.replay.ReplayUnavailableDecoder;
import org.limitless.phixeron.sbe.replay.ReplayingDecoder;
import org.limitless.phixeron.util.Logger;

/**
 * The replay protocol's own decisions — which recording answers a request, when the archive cannot be
 * believed, when to hold, when to refuse, when to stall — driven against {@link
 * FakeReplayer} rather than a live node (review-3.md finding 11). Requests go in as real
 * SBE bytes on the request stream and replies are decoded off the control stream, so the wire format
 * is under test alongside the logic.
 *
 * <p>Every assertion the {@code Logger} carries goes through a recording sink: the fault codes are the
 * operator-visible half of these paths, and an anomaly that reports itself twice (or not at all) is a
 * defect in its own right.
 */
class ReplayerServiceTest {
    private static final int MEMBER_ID = 0;
    private static final int CLIENT = 7;
    private static final int RESUME = -1; // ReplayRequest.segmentIndex < 0 — resume, not a walk step

    /** One decoded control reply. {@code recordingId}/{@code catchUpPosition} are unset on non-Replaying replies. */
    private record Reply(int templateId, int clientId, long requestId, long replaySessionId, long catchUpPosition,
                         long recordingId) { }

    private final List<Logger.LoggerEvent> events = new ArrayList<>();
    private final AtomicInteger fatalCount = new AtomicInteger();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(128);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;

    @BeforeEach
    void setUp() {
        Logger.install(events::add);
        fakeReplayer = new FakeReplayer();
        replayerService =
            new ReplayerService(fakeReplayer, MEMBER_ID, NoOpIdleStrategy.INSTANCE, fatalCount::incrementAndGet);
    }

    @AfterEach
    void tearDown() {
        Logger.reset();
    }

    // ── Startup integrity check ─────────────────────────────────────────────────

    @Test
    void aFirstFrameAtGlobalSeqNoOneMakesTheNodeReady() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(1));

        replayerService.poll(); // opens the self-check
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        replayerService.poll(); // reads its first fragment

        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID));
        // Bounded to the first frame, not the whole recording: it only ever reads one fragment.
        assertEquals(1, fakeReplayer.startedReplays().size());
        assertEquals(ReplayerService.SELF_CHECK_STREAM_ID, fakeReplayer.startedReplays().getFirst().streamId());
    }

    @Test
    void aFirstFrameThatIsNotGlobalSeqNoOneRefusesEveryRequestForGood() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(7));

        replayerService.poll();
        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertTrue(loggedOnce(Logger.EventCode.ArchiveIntegrityFailure));

        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));
        replayerService.poll();
        assertEquals(ReplayUnavailableDecoder.TEMPLATE_ID, lastReply().templateId());

        // Permanent: retrying reads the same bytes, so nothing is re-opened and ready never returns.
        final int selfChecks = fakeReplayer.selfCheckStreamsOpened();
        for (int i = 0; i < 5; i++) {
            replayerService.poll();
        }
        assertEquals(selfChecks, fakeReplayer.selfCheckStreamsOpened());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
    }

    @Test
    void everyRecordingInTheChainIsProvedBeforeReadiness() {
        fakeReplayer.addRecording(5, 0, false, 4096);
        fakeReplayer.addRecording(6, 0, true, 4096);

        makeReady();

        // Both spans, not just the oldest: with no snapshots a healthy recording begins at globalSeqNo 1
        // however late it was created, so the newest is as much a witness to complete history as the oldest.
        final List<Long> checked = fakeReplayer.startedReplays()
                                       .stream()
                                       .filter(r -> r.streamId() == ReplayerService.SELF_CHECK_STREAM_ID)
                                       .map(FakeReplayer.StartedReplay::recordingId)
                                       .toList();
        assertEquals(List.of(5L, 6L), checked);
    }

    @Test
    void aLaterRecordingThatBeginsMidHistoryIsAnIntegrityFailure() {
        fakeReplayer.addRecording(5, 0, false, 4096);
        fakeReplayer.addRecording(6, 0, true, 4096);

        replayerService.poll(); // opens the check on recording 5
        fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(1));
        replayerService.poll(); // recording 5 proves out
        replayerService.poll(); // opens the check on recording 6
        fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(4001)); // resumed mid-history: a hole at the join
        replayerService.poll();

        // The hole itself is invisible from here — only a walking app ever meets it, and then only as a
        // recovery that never converges (review-3.md finding 6). This is where it is catchable.
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertTrue(loggedOnce(Logger.EventCode.ArchiveIntegrityFailure));
        assertEquals(ReplayUnavailableDecoder.TEMPLATE_ID, request(CLIENT, 1, 0, 0).templateId());
    }

    @Test
    void aStoppedRecordingWithNothingInItIsSkippedRatherThanHeldOn() {
        fakeReplayer.addRecording(5, 0, false, 0); // created by an unclean restart, never published to
        fakeReplayer.addRecording(6, 0, true, 4096);

        makeReady();

        // It can never gain a first frame to prove, and serveReplay already skips it by name, so
        // requiring one would wedge readiness for as long as the recording is on disk.
        assertTrue(fakeReplayer.startedReplays().stream().noneMatch(r -> r.recordingId() == 5));
    }

    @Test
    void aLiveRecordingWithNothingInItYetHoldsReadinessRatherThanPassing() {
        fakeReplayer.addRecording(6, 0, true, 0); // recording armed, sequencer has published nothing yet

        for (int cycle = 0; cycle < 5; cycle++) {
            replayerService.poll();
        }

        // Unlike a stopped empty span this is transient, and passing it would declare the node ready on
        // an archive nothing has proved anything about.
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID));
        assertEquals(0, fakeReplayer.selfCheckStreamsOpened());
    }

    @Test
    void aSelfCheckThatDeliversNothingInTimeIsStartedOverRatherThanFailed() {
        fakeReplayer.addRecording(6, 0, true, 4096); // no self-check frame queued

        replayerService.poll();
        assertEquals(1, fakeReplayer.selfCheckStreamsOpened());
        fakeReplayer.advanceMillis(3_000); // past SELF_CHECK_TIMEOUT_NS
        replayerService.poll();
        replayerService.poll();

        assertEquals(2, fakeReplayer.selfCheckStreamsOpened());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
    }

    @Test
    void requestsArrivingBeforeReadinessAreHeldWithoutBeingQueued() {
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));

        replayerService.poll();

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, lastReply().templateId());
        // The app's own resend timer is the retry — queueing here would only re-try the same not-ready state.
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
    }

    @Test
    void aRequestArrivingInTheCycleReadinessFlipsIsHeldUntilTheAppsNextResend() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(1));
        replayerService.poll(); // opens the self-check; readiness flips on the next cycle

        // poll() answers requests BEFORE running the integrity check, because a not-ready Replayer still
        // owes every request an answer and answering costs nothing, whereas the check makes archive
        // control calls. This one-cycle delay is that ordering's whole cost.
        final Reply held = request(CLIENT, 1, 0, 0);
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, held.templateId());
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));

        assertEquals(ReplayingDecoder.TEMPLATE_ID, request(CLIENT, 2, 0, 0).templateId());
    }

    // ── Cold-start walk ─────────────────────────────────────────────────────────

    @Test
    void aWalkServesEachSegmentFromItsOwnStartPosition() {
        threeSegmentChain();
        makeReady();

        assertEquals(4096, request(CLIENT, 1, 0, 0).catchUpPosition());
        assertEquals(5, request(CLIENT, 2, 0, 0).recordingId());

        final Reply third = request(CLIENT, 3, 2, 0);
        assertEquals(7, third.recordingId());
        assertEquals(9000, third.catchUpPosition());

        final FakeReplayer.StartedReplay last = lastClientReplay();
        assertEquals(7, last.recordingId());
        assertEquals(4096, last.position()); // the recording's own startPosition, not 0
        assertEquals(9000 - 4096, last.length());
    }

    @Test
    void onlyTheReplyNamingNoRecordingEndsTheWalk() {
        threeSegmentChain();
        makeReady();

        final Reply terminator = request(CLIENT, 1, 3, 0);

        assertEquals(ReplayerService.NO_REPLAY_NEEDED, terminator.replaySessionId());
        assertEquals(Aeron.NULL_VALUE, terminator.recordingId());
    }

    @Test
    void anEmptySegmentIsRefusedByNameSoTheWalkSkipsItRatherThanEnding() {
        threeSegmentChain(); // segment 1 (recording 6) was created but never written to
        makeReady();

        final Reply empty = request(CLIENT, 1, 1, 0);

        assertEquals(ReplayerService.NO_REPLAY_NEEDED, empty.replaySessionId());
        // Names the recording it found nothing in. That is what tells the app to skip this segment and
        // keep walking instead of declaring itself caught up with segment 2 unreplayed.
        assertEquals(6, empty.recordingId());
        assertTrue(noClientReplayFor(6));
    }

    @Test
    void aTipNeitherCounterCanReportHoldsTheRequestInsteadOfAnsweringIt() {
        fakeReplayer.addRecording(5, 0, false, 1000);
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.hideTip(6); // RecordingPos gone, stopPosition not written yet

        final Reply reply = request(CLIENT, 1, 1, 0);

        // Reported as a tip, this transient read would tell a walking app that segment is empty.
        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
        assertTrue(noClientReplayFor(6));
    }

    @Test
    void moreThanOneActiveRecordingIsReportedOnceAndTheNewestIsServed() {
        fakeReplayer.addRecording(5, 0, true, 1000); // left unstopped by an unclean shutdown
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        // Both selection rules have to agree on which one is live: the walk's (stitch) and the
        // resume's (findActiveRecording). Picking the stale one there resumes an app into a prefix.
        assertEquals(6, request(CLIENT, 1, 0, 0).recordingId());
        assertEquals(6, request(CLIENT, 2, RESUME, 0).recordingId());

        assertTrue(loggedOnce(Logger.EventCode.StaleActiveRecording));
    }

    @Test
    void aSecondStaleActiveRecordingIsReportedAgainAfterTheFirstWasRepaired() {
        fakeReplayer.addRecording(5, 0, true, 1000); // left unstopped by an unclean shutdown
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, 0, 0);

        fakeReplayer.stopRecording(5); // operator repairs it
        request(CLIENT, 2, 0, 0);
        assertEquals(1, logCount(Logger.EventCode.StaleActiveRecording), "the repair is not a new episode");

        fakeReplayer.addRecording(7, 4096, true, 5000); // a later unclean shutdown leaves 6 unstopped
        request(CLIENT, 3, 0, 0);

        // This anomaly has no counter, so a latch that never re-arms makes every later episode silent.
        assertEquals(2, logCount(Logger.EventCode.StaleActiveRecording));
    }

    // ── Steady-state resume ─────────────────────────────────────────────────────

    @Test
    void aResumeReplaysTheActiveRecordingFromTheAppsOwnPosition() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        final Reply reply = request(CLIENT, 1, RESUME, 2048);

        assertEquals(6, reply.recordingId());
        assertEquals(4096, reply.catchUpPosition());
        assertEquals(2048, lastClientReplay().position());
    }

    @Test
    void aResumeBelowTheActiveRecordingsStartIsSteeredOntoTheWalk() {
        fakeReplayer.addRecording(6, 2048, true, 4096);
        makeReady();

        final Reply reply = request(CLIENT, 1, RESUME, 1024);

        assertEquals(ReplayerService.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertEquals(Aeron.NULL_VALUE, reply.recordingId());
        assertTrue(noClientReplayStarted());
    }

    // ── Local-archive stall ─────────────────────────────────────────────────────

    @Test
    void anArchiveThatRefusesAWalkReplayStallsAndHoldsTheClient() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));

        final Reply reply = request(CLIENT, 1, 0, 0);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
    }

    @Test
    void whileStalledFurtherRequestsAreHeldWithoutProbingTheArchiveAgain() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));
        request(CLIENT, 1, 0, 0); // enters STALLED
        request(CLIENT, 2, 0, 0); // the first request after that is itself the paced probe
        final int probes = fakeReplayer.replayAttempts();

        fakeReplayer.advanceMillis(100); // inside STALL_RETRY_INTERVAL_MS
        final Reply reply = request(CLIENT, 3, 0, 0);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(probes, fakeReplayer.replayAttempts(), "a dead archive must not be hammered every request");
    }

    @Test
    void anArchiveThatComesBackClearsTheStallOnTheNextPacedProbe() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));
        request(CLIENT, 1, 0, 0);

        fakeReplayer.serveReplaysAgain();
        fakeReplayer.advanceMillis(1_500); // past STALL_RETRY_INTERVAL_MS
        final Reply reply = request(CLIENT, 2, 0, 0);

        assertEquals(ReplayingDecoder.TEMPLATE_ID, reply.templateId());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
    }

    @Test
    void aResumeTheArchiveRefusesIsSteeredOntoTheWalkRatherThanCalledAStall() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive refused the position"));

        final Reply reply = request(CLIENT, 1, RESUME, 2048);

        // The archive still answers, so it is the position that is wrong — one the client supplied, in
        // range but not on a frame boundary of a recording that has rotated. Answering ReplayPending to a
        // position that can never work would hold that app forever.
        assertEquals(ReplayerService.NO_REPLAY_NEEDED, reply.replaySessionId());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
    }

    @Test
    void aResumeAnArchiveOutageRefusesIsHeldRatherThanSteeredOntoAReWalk() {
        // The same exception, the opposite fault (review-3.md #10). Reading every refused resume as a bad
        // position sent a whole node's worth of apps off their positions and onto full chain re-walks
        // because the archive was down, and reported nothing until one of those walks came back.
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));

        final Reply reply = request(CLIENT, 1, RESUME, 2048);

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, reply.templateId(), "hold at the gap, keep the position");
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
    }

    @Test
    void anArchiveThatComesBackIsNoticedWithNoRequestToNoticeItOn() {
        // A Replayer whose apps have all caught up is asked for nothing, so the stall used to have no
        // way back: phixeron.replayer.stalled stayed at 1 for the rest of the process (review-3.md #10).
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));
        request(CLIENT, 1, 0, 0);
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));

        fakeReplayer.serveReplaysAgain();
        fakeReplayer.advanceMillis(1_500);
        replayerService.poll(); // no request arrives, ever again

        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
        assertTrue(noClientReplayStarted(), "the probe is the node's own, not a replay served to an app");
    }

    @Test
    void theIdleProbeIsPacedLikeARequestedOne() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.failReplays(new IllegalStateException("archive gone"));
        request(CLIENT, 1, 0, 0);
        final int probes = fakeReplayer.replayAttempts();

        for (int cycle = 0; cycle < 10; ++cycle) {
            replayerService.poll();
        }

        assertEquals(probes, fakeReplayer.replayAttempts(), "a dead archive must not be hammered every cycle");
        fakeReplayer.advanceMillis(1_500);
        replayerService.poll();
        assertEquals(probes + 1, fakeReplayer.replayAttempts(), "one probe per interval, though");
    }

    @Test
    void anArchiveThatRefusesTheStartupSelfCheckIsReportedRatherThanLookingMerelySlow() {
        // The self-check used to swallow exactly the faults the replay path reports (review-3.md #10), so
        // a node whose archive never answered looked like one that was merely slow to become ready.
        fakeReplayer.addRecording(6, 0, true, 4096);
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));

        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertEquals(0, fatalCount.get(), "a transient archive fault must not take the duty cycle down");

        fakeReplayer.answerArchiveAgain();
        makeReady();
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID));
    }

    // ── Slots ───────────────────────────────────────────────────────────────────

    @Test
    void aClientBeyondTheConcurrencyCapIsQueuedAndServedWhenASlotFrees() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        for (int client = 1; client <= ReplayerService.MAX_CONCURRENT_REPLAYS; client++) {
            assertEquals(ReplayingDecoder.TEMPLATE_ID, request(client, 1, 0, 0).templateId());
        }
        final int lateClient = ReplayerService.MAX_CONCURRENT_REPLAYS + 1;

        assertEquals(ReplayPendingDecoder.TEMPLATE_ID, request(lateClient, 1, 0, 0).templateId());
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));

        fakeReplayer.enqueueRequest(replayComplete(1));
        replayerService.poll();

        final Reply served = lastReply();
        assertEquals(ReplayingDecoder.TEMPLATE_ID, served.templateId());
        assertEquals(lateClient, served.clientId());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID));
        assertEquals(ReplayerService.MAX_CONCURRENT_REPLAYS,
                     fakeReplayer.counter(PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
    }

    @Test
    void aHeartbeatKeepsASlotWhileSilenceAgesItOut() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, 0, 0);
        final long session = lastClientReplay().replaySessionId();

        final long almostTtl = ReplayerService.REPLAY_SLOT_TTL_MS * 3 / 4;
        fakeReplayer.advanceMillis(almostTtl);
        fakeReplayer.enqueueRequest(replayHeartbeat(CLIENT));
        replayerService.poll();
        // One and a half TTLs since the replay started, but only three quarters of one since the heartbeat.
        fakeReplayer.advanceMillis(almostTtl);
        replayerService.poll();

        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID));

        fakeReplayer.advanceMillis(ReplayerService.REPLAY_SLOT_TTL_MS + 1); // no further sign of life
        replayerService.poll();

        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID));
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID));
        assertTrue(fakeReplayer.stoppedReplays().contains(session));
    }

    @Test
    void shutdownStopsEveryReplayItStillHolds() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        request(CLIENT, 1, 0, 0);
        final long session = lastClientReplay().replaySessionId();

        replayerService.run(new AtomicBoolean(false));

        assertTrue(fakeReplayer.stoppedReplays().contains(session));
    }

    // ── Control-stream back-pressure ────────────────────────────────────────────

    @Test
    void aReplyToAnAppThatStoppedReadingIsDroppedAndCounted() {
        fakeReplayer.controlOfferResult(Publication.BACK_PRESSURED);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));

        replayerService.poll();

        assertTrue(fakeReplayer.controlReplies().isEmpty());
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
        assertTrue(loggedOnce(Logger.EventCode.ControlReplyDropped));
    }

    @Test
    void aFreshBurstOfDroppedRepliesIsReportedAgainAfterAQuietPeriod() {
        fakeReplayer.controlOfferResult(Publication.BACK_PRESSURED);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));
        replayerService.poll();

        // Same episode: a wedged app is answered on every resend, and the counter carries the rate.
        fakeReplayer.advanceMillis(1_000);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 2, 0, 0));
        replayerService.poll();
        assertEquals(1, logCount(Logger.EventCode.ControlReplyDropped));

        fakeReplayer.advanceMillis(60_000);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 3, 0, 0));
        replayerService.poll();

        assertEquals(2, logCount(Logger.EventCode.ControlReplyDropped), "a distinct episode names itself");
        assertEquals(3, fakeReplayer.counter(PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
    }

    @Test
    void aReplyToAnAppNotSubscribedYetIsDroppedWithoutBeingCounted() {
        fakeReplayer.controlOfferResult(Publication.NOT_CONNECTED);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));

        replayerService.poll();

        // Ordinary: an app's first request can beat its own control subscription, and its resend covers it.
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID));
        assertFalse(loggedOnce(Logger.EventCode.ControlReplyDropped));
    }

    @Test
    void aClosedControlPublicationTakesTheDutyCycleDownLoudly() {
        fakeReplayer.controlOfferResult(Publication.CLOSED);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));

        assertThrows(IllegalStateException.class, () -> replayerService.poll());
    }

    @Test
    void aDutyCycleThatDiesClearsReadinessAndSignalsTheNode() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();
        fakeReplayer.controlOfferResult(Publication.CLOSED);
        fakeReplayer.enqueueRequest(replayRequest(CLIENT, 1, 0, 0));

        replayerService.run(new AtomicBoolean(true));

        assertEquals(1, fatalCount.get());
        assertEquals(0, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID));
        assertTrue(loggedOnce(Logger.EventCode.ReplayDutyCycleFailure));
    }

    // ── Client-id collision ─────────────────────────────────────────────────────

    @Test
    void twoAppsSharingAClientIdAreReportedOnce() {
        fakeReplayer.addRecording(6, 0, true, 4096);
        makeReady();

        request(CLIENT, 9, 0, 0);
        request(CLIENT, 8, 0, 0); // a second app's own, lower, requestId sequence
        request(CLIENT, 7, 0, 0);
        request(CLIENT, 6, 0, 0);

        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID));
        assertTrue(loggedOnce(Logger.EventCode.ReplayClientIdCollision));

        request(CLIENT, 5, 0, 0);
        assertTrue(loggedOnce(Logger.EventCode.ReplayClientIdCollision));
    }

    // ── Fixtures and helpers ────────────────────────────────────────────────────

    /**
     * Two stopped tenures and the live one, with segment 1 empty — a recording an unclean restart
     * created before anything was published to it.
     */
    private void threeSegmentChain() {
        fakeReplayer.addRecording(5, 0, false, 4096);
        fakeReplayer.addRecording(6, 0, false, 0);
        fakeReplayer.addRecording(7, 4096, true, 9000);
    }

    /**
     * Passes the startup integrity check, leaving the node serving. The check sweeps the whole chain a
     * span per duty cycle, so this feeds one {@code globalSeqNo} 1 first frame per self-check opened
     * rather than assuming a cycle count.
     */
    private void makeReady() {
        int fed = 0;
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID) == 0; ++cycle) {
            if (fakeReplayer.selfCheckStreamsOpened() > fed) {
                fakeReplayer.enqueueSelfCheckFrame(sequencedFrame(1));
                ++fed;
            }
            replayerService.poll();
        }
        assertEquals(1, fakeReplayer.counter(PhixeronCounters.REPLAYER_READY_TYPE_ID),
                     "fixture failed to make the node ready");
    }

    /** Delivers one ReplayRequest and returns the reply it drew. */
    private Reply request(final int clientId, final long requestId, final int segmentIndex, final long fromPosition) {
        fakeReplayer.enqueueRequest(replayRequest(clientId, requestId, segmentIndex, fromPosition));
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
                             decoder.catchUpPosition(), decoder.recordingId());
        }
        if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
            final ReplayPendingDecoder decoder = new ReplayPendingDecoder();
            decoder.wrap(buffer, offset, header.blockLength(), header.version());
            return new Reply(header.templateId(), decoder.clientId(), decoder.requestId(), 0, 0, 0);
        }
        final ReplayUnavailableDecoder decoder = new ReplayUnavailableDecoder();
        decoder.wrap(buffer, offset, header.blockLength(), header.version());
        return new Reply(header.templateId(), decoder.clientId(), decoder.requestId(), 0, 0, 0);
    }

    /** The most recent replay served to an app, ignoring the startup self-check's own. */
    private FakeReplayer.StartedReplay lastClientReplay() {
        final List<FakeReplayer.StartedReplay> clientReplays =
            fakeReplayer.startedReplays()
                .stream()
                .filter(replay -> replay.streamId() == ReplayerService.REPLAY_STREAM_ID)
                .toList();
        assertFalse(clientReplays.isEmpty(), "no replay was started for an app");
        return clientReplays.getLast();
    }

    private boolean noClientReplayStarted() {
        return fakeReplayer.startedReplays().stream().noneMatch(r -> r.streamId() == ReplayerService.REPLAY_STREAM_ID);
    }

    /** No app was served out of {@code recordingId} — ignoring the startup self-check's own replay of it. */
    private boolean noClientReplayFor(final long recordingId) {
        return fakeReplayer.startedReplays().stream().noneMatch(
            r -> r.recordingId() == recordingId && r.streamId() == ReplayerService.REPLAY_STREAM_ID);
    }

    private boolean loggedOnce(final Logger.EventCode code) {
        return logCount(code) == 1;
    }

    private long logCount(final Logger.EventCode code) {
        return events.stream().filter(event -> event.code() == code).count();
    }

    // ── Encoders ────────────────────────────────────────────────────────────────

    private byte[] replayRequest(final int clientId, final long requestId, final int segmentIndex,
                                 final long fromPosition) {
        final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .fromPosition(fromPosition)
            .segmentIndex(segmentIndex);
        return encoded(encoder.encodedLength());
    }

    private byte[] replayComplete(final int clientId) {
        final ReplayCompleteEncoder encoder = new ReplayCompleteEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(clientId);
        return encoded(encoder.encodedLength());
    }

    private byte[] replayHeartbeat(final int clientId) {
        final ReplayHeartbeatEncoder encoder = new ReplayHeartbeatEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(clientId);
        return encoded(encoder.encodedLength());
    }

    /** One frame off the tap recording, as the self-check reads it back: a core ClusterHeartbeat. */
    private byte[] sequencedFrame(final long globalSeqNo) {
        final org.agrona.ExpandableArrayBuffer payload = new org.agrona.ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder heartbeat = new org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder();
        heartbeat.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());

        final org.limitless.phixeron.sbe.frame.SequencedEncoder frame = new org.limitless.phixeron.sbe.frame.SequencedEncoder();
        frame.wrapAndApplyHeader(encodeBuffer, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        frame.header().sourceId(1).connectionId(0).sessionId(0)
            .payloadId(org.limitless.phixeron.sequencer.CoreFrame.PAYLOAD_ID)
            .globalSeqNo(globalSeqNo).timestamp(0);
        frame.putPayload(payload, 0, org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + heartbeat.encodedLength());
        final byte[] message = new byte[org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + frame.encodedLength()];
        encodeBuffer.getBytes(0, message);
        return message;
    }

    private byte[] encoded(final int bodyLength) {
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + bodyLength];
        encodeBuffer.getBytes(0, message);
        return message;
    }
}
