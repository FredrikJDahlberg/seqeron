// ReplayerRecovery — the walk/resume/gap decision state machine ReplayerStreamReceiver drives.
//
// Locked here: the contiguity/de-dupe decision and the first-frame-must-be-globalSeqNo-1 baseline check
// (doc/todo.md "Replayer / ingress" — a cold start answered NO_REPLAY_NEEDED used to adopt an arbitrary
// mid-stream baseline; globalSeqNo only increases, so once the first frame observed isn't 1 no later one
// ever can be, and it aborts rather than latching a flag a caller might not check), the Replayer
// control-stream transitions, the retained-ahead FIFO, the segment-chain walk, the steady-state resume,
// and the two watchdogs.
//
// ReplayerRecovery holds no Aeron runtime and no clock of its own, so this suite drives it directly: its
// transport is recorded through ReplayerRecoveryActions and its clock is owned by the test. The
// receiver's own Aeron side — subscription polling, image attach, and its guarantee to always drain AND
// dispatch the tap regardless of state — has no seam here and is exercised by
// src/test/scripts/gap-recovery-test.sh.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/phixeron/replayer/client/ReplayerRecovery.hpp"
#include "org/limitless/phixeron/util/Logger.hpp"
#include "org_limitless_phixeron_sbe_sequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayUnavailable.h"
#include "org_limitless_phixeron_sbe_unsequenced/Replaying.h"

namespace org::limitless::phixeron::replayer::client {
namespace {

namespace seq = org::limitless::phixeron::sbe::sequenced;
namespace diag = org::limitless::phixeron::util;

// Captures every LoggerEvent reported while in scope, in place of the installed default
// (StderrLoggerSink) — see Logger.hpp. RAII so a test that ASSERTs out early still restores
// the default sink rather than leaving a dangling pointer installed for later tests.
struct ScopedLoggerSink final : diag::LoggerSink
{
    ScopedLoggerSink()
    {
        diag::Logger::install(*this);
    }

    ~ScopedLoggerSink() override
    {
        diag::Logger::reset();
    }

    void record(const diag::LoggerEvent& event) override
    {
        events.push_back(event);
    }

    std::vector<diag::LoggerEvent> events;
};

// Refusals only: a re-request logs its own (warn) line between two of them, so size() cannot count these.
std::size_t refusalCount(const ScopedLoggerSink& sink)
{
    std::size_t count = 0;
    for (const diag::LoggerEvent& event : sink.events)
    {
        count += event.code == diag::EventCode::ReplayUnavailable ? 1 : 0;
    }
    return count;
}

// Retained-buffer overflows only: every gap/re-walk warn shares EventCode::TapGap, so only the text
// separates them.
std::size_t overflowReports(const ScopedLoggerSink& sink)
{
    std::size_t count = 0;
    for (const diag::LoggerEvent& event : sink.events)
    {
        const std::string text(event.text.data(), event.textLen);
        count += text.find("retained-frame buffer full") != std::string::npos ? 1 : 0;
    }
    return count;
}

constexpr std::int32_t CLIENT_ID = 1;

// Stands in for the wall clock. The absolute value is arbitrary; only the deltas tests apply matter.
constexpr std::int64_t CLOCK_MS = 3 * 60 * 60 * 1000;

// Past ReplayerRecovery's RESEND_INTERVAL_MS (500) and REPLAY_STALL_TIMEOUT_MS (5000) alike, so one duty
// cycle fires whichever of the two the state under test has armed.
constexpr std::int64_t PAST_EVERY_TIMER_MS = 6'000;

// A ReplayerRecovery with its transport recorded rather than performed, and a clock the test owns.
struct Client final : ReplayerRecoveryActions
{
    explicit Client(ReplayerRecovery::OnSequenced onSequenced, ReplayerRecovery::OnCaughtUp onCaughtUp = {}) :
      recovery(CLIENT_ID, *this, std::move(onSequenced), {}, {}, {}, std::move(onCaughtUp))
    {}

    void sendReplayRequest(std::int64_t, std::int32_t, std::int64_t) override
    {
        ++requestsSent;
    }

    // False like a receiver with no publication: every send here is one that never went out, which is
    // the state the ReplayComplete retry tests need.
    bool sendReplayComplete() override
    {
        return false;
    }

    bool sendReplayHeartbeat() override
    {
        return false;
    }

    void openReplay(std::int64_t) override
    {}

    void closeReplay() override
    {}

    void recoveryStalled(bool) override
    {}

    std::int64_t nowMs() override
    {
        return clockMs;
    }

    std::int64_t clockMs = CLOCK_MS;
    int requestsSent = 0;

    ReplayerRecovery recovery;
};

// One Heartbeat frame (template id 48) at the given globalSeqNo — clear of the
// ClientConnected/ClientDisconnected/LeadershipChanged special ids (1/2/5), so it always reaches
// onSequenced rather than being intercepted as a lifecycle/leadership event.
std::vector<std::uint8_t> encodeHeartbeat(const std::int64_t globalSeqNo)
{
    std::vector<std::uint8_t> buf(256, 0);
    seq::Heartbeat enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.header().sourceId(1).connectionId(0).sessionId(1).globalSeqNo(globalSeqNo).timestamp(0);
    enc.direction(seq::Direction::Value::Client);
    enc.seqNum(1).sendingTimeMs(0).possDupFlag(seq::PossDupFlag::Value::NULL_VALUE);
    enc.testReqID()[0] = '\0';
    buf.resize(enc.sbePosition());
    return buf;
}

// Feeds one Heartbeat frame in exactly as the receiver would off the live tap. framePosition is where
// the frame starts in the recording — the default 0 suffices wherever a test does not care, but
// requestResume anchors on the last dispatched frame's position, so a test that checks the anchor must
// space its frames apart.
void deliverLive(Client& client, const std::int64_t globalSeqNo, const std::int64_t framePosition = 0)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), framePosition, /*receiveNs=*/0,
                            /*fromReplay=*/false);
}

// A live tap frame too big for the retained-ahead FIFO to hold: a Heartbeat zero-padded past
// MessagesBlock::SIZE, which retainMessages refuses outright (recordSize > MessagesBlock::SIZE). The
// cheapest of its three overflow triggers to drive — the other two need 65536 frames or 16 MiB.
void deliverLiveTooBigToRetain(Client& client, const std::int64_t globalSeqNo)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    buf.resize(8192, 0); // decoded from the front; the padding only has to make the record oversized
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/false);
}

// One Heartbeat frame from a replay image (fromReplay=true), otherwise identical to deliverLive.
void deliverReplay(Client& client, const std::int64_t globalSeqNo)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/true);
}

// requestId must be the client's CURRENT one (recovery.requestId()) for a reply to be acted on — see
// onControl's correlation check; pass a different value to build the stale reply a resend leaves behind.
// recordingId defaults to -1 ("no expectation") so existing call sites that don't care about the
// walk-recordingId mismatch check (see onControl) are unaffected — a test exercising that check passes
// it explicitly.
std::vector<std::uint8_t> encodeReplaying(const std::int32_t clientId, const std::int64_t requestId,
                                          const std::int64_t replaySessionId, const std::int64_t catchUpPosition,
                                          const std::int64_t recordingId = -1)
{
    std::vector<std::uint8_t> buf(64, 0);
    usq::Replaying enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId)
        .requestId(requestId)
        .replaySessionId(replaySessionId)
        .catchUpPosition(catchUpPosition)
        .recordingId(recordingId);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayPending(const std::int32_t clientId, const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    usq::ReplayPending enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayUnavailable(const std::int32_t clientId, const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    usq::ReplayUnavailable enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

// Feeds one already-encoded control-stream message straight in, exactly as onControl() would decode it
// off the Replayer's control subscription.
void deliverControl(Client& client, std::vector<std::uint8_t> body)
{
    client.recovery.onControl(reinterpret_cast<char*>(body.data()), body.size());
}

// The replay image reaching the bound the Replayer gave it — how a segment completes, since a bounded
// replay of an active recording never closes its image at the bound.
void completeSegment(Client& client)
{
    client.recovery.onReplayPosition(client.recovery.catchUpPosition());
}

// One duty cycle with the clock past every timer in it: an unanswered request is re-sent, and an
// established replay that has delivered nothing is declared stalled. Either way the client re-asks with
// a fresh requestId.
void advancePastTimers(Client& client)
{
    client.clockMs += PAST_EVERY_TIMER_MS;
    client.recovery.doTimers(/*requestPublicationPending=*/false);
}

bool checkProgressAt(Client& client, const std::int64_t nowMs)
{
    client.clockMs = nowMs;
    return client.recovery.checkRecoveryProgress();
}

TEST(ReplayerRecoveryBaseline, FirstFrameAtGlobalSeqNoOneIsAccepted)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);

    EXPECT_EQ(1, delivered);
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryBaseline, FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess)
{
    Client client{ [](const SequencedEvent&) {} };

    EXPECT_DEATH(deliverLive(client, 57), "globalSeqNo=57, expected 1")
        << "must not silently adopt a mid-stream baseline";
}

// Coverage for the gap-recovery/re-walk state machine (doc/todo.md "gap re-walk starves the tap it is
// recovering", fixed 2026-07-31): mid-stream gap detection, de-dup of already-seen replayed frames, the
// Replayer control-stream transitions (Replaying / NO_REPLAY_NEEDED / ReplayPending), and the
// segment-chain walk advancing on completion — the contiguity/de-dupe decision, the isRecovering()
// predicate that separates a real hole from the tap running ahead of an in-flight walk, and the
// isCaughtUp() state transitions consumers gate on.

// Companion to ReplayerStreamReceiverBaseline.FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess, and the
// regression the 2026-08-05 seam fix first introduced: once the tap is dispatched mid-walk, a cold start
// races a live tap already carrying mid-stream globalSeqNos against a replay that has not delivered 1
// yet. That tap frame is neither a valid baseline nor a fault — only the walk may establish the
// baseline. (Caught by gap-recovery-test.sh, where every consumer aborted at startup with "first frame
// observed has globalSeqNo=15"; this locks it where it costs a millisecond instead of a cluster.)
TEST(ReplayerRecoveryBaseline, LiveTapMayNotEstablishTheBaselineWhileTheColdStartWalkIsInFlight)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    // What start() does: request segment 0 before any frame has been seen.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_TRUE(client.recovery.isRecovering());

    deliverLive(client, 15); // the live tip, far ahead of a replay that has not started delivering

    EXPECT_EQ(0, delivered) << "must not adopt the live tap's mid-stream baseline";
    EXPECT_FALSE(client.recovery.isCaughtUp());

    deliverReplay(client, 1); // the walk supplies the real baseline

    EXPECT_EQ(1, delivered);
}

TEST(ReplayerRecoveryGapRecovery, MidStreamGapTriggersReplayRequestAndWithholdsTheOutOfOrderFrame)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);
    ASSERT_FALSE(client.recovery.isAwaitingReplay());

    deliverLive(client, 5); // skips 3, 4 -> gap

    EXPECT_EQ(2, delivered) << "the out-of-order frame itself must not be dispatched";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "a gap must re-request a replay";
    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "no session yet — only a request, until the Replayer answers";
}

TEST(ReplayerRecoveryGapRecovery, TapGapReportsAStructuredDiagnosticEvent)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 2);
    deliverLive(client, 5); // skips 3, 4 -> gap

    ASSERT_EQ(1u, sink.events.size()) << "exactly the one gap detected above, nothing from setup";
    const auto& event = sink.events[0];
    EXPECT_EQ(diag::Component::ReplayerStreamReceiver, event.component);
    EXPECT_EQ(diag::Severity::Warn, event.severity);
    EXPECT_EQ(diag::EventCode::TapGap, event.code);
    const std::string text(event.text.data(), event.textLen);
    EXPECT_EQ("tap gap: expected globalSeqNo=3 got 5 — resuming the recording at globalSeqNo=2", text);
}

// A tap frame that is non-contiguous only because the tap runs ahead of an in-flight replay must not be
// mistaken for a new gap. Now that poll() dispatches the tap mid-walk instead of discarding it (the
// 2026-08-05 seam fix), this is the common case, not a rarity: every tap frame arriving while a walk
// replays history reads as "ahead". Superseding the walk on each one would restart it from segment 0
// forever under any sustained publish rate.
TEST(ReplayerRecoveryGapRecovery, TapFrameAheadOfAnInFlightWalkDoesNotSupersedeIt)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> requestReplay(0, 0), awaiting
    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    ASSERT_EQ(1u, sink.events.size()) << "the genuine steady-state gap";

    // The Replayer answers with an active session: now mid-walk (m_replaySessionId >= 0), not merely
    // awaiting an answer. (The e2e version of this scenario — re-arming a live-tap drop while a real walk
    // is in flight — was attempted and abandoned as impractical: local Aeron IPC replay of a small gap
    // completes too fast for a shell-level poll-then-signal loop to reliably land inside the window; see
    // doc/todo.md's 2026-08-02 note. This state transition is locked down here instead.)
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/99,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(99, client.recovery.replaySessionId());
    ASSERT_FALSE(client.recovery.isAwaitingReplay());
    ASSERT_TRUE(client.recovery.isRecovering());

    // A later frame arrives on the live tap while that walk is still riding history. m_lastGlobalSeqNo is
    // still 1, so it is non-contiguous — but it is the tap running ahead, not a hole the walk won't cover.
    deliverLive(client, 12);

    EXPECT_EQ(99, client.recovery.replaySessionId()) << "the in-flight walk must run to completion, not be restarted";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.walkSegmentIndex()) << "still the resume the gap asked for, not restarted";
    EXPECT_EQ(1, delivered) << "the out-of-order frame itself is still withheld";
    EXPECT_EQ(1u, sink.events.size()) << "and it is not reported as a second gap";
}

// The replay->live seam (doc/review A2): the tap is dispatched throughout a walk, so the frame at
// gseq == last+1 lands the instant the replay reaches it. Before the fix poll() routed tap frames to a
// discard handler while recovering, consuming exactly these frames — so every walk ended one guaranteed
// gap short of live and re-walked the whole chain, converging only if nothing was published during the
// final round trip. NOTE: poll()'s handler *routing* needs a live tap subscription and is exercised only
// by src/test/scripts/gap-recovery-test.sh; what is locked here is the decision logic it feeds.
TEST(ReplayerRecoveryGapRecovery, LiveTapFrameAtTheSeamIsDispatchedWhileTheWalkIsStillInFlight)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    ASSERT_EQ(2, delivered);
    ASSERT_FALSE(client.recovery.isCaughtUp()) << "still riding replayed history";
    ASSERT_TRUE(client.recovery.isRecovering());

    deliverLive(client, 3); // the seam: first tap frame contiguous with what the replay delivered

    EXPECT_EQ(3, delivered) << "the seam frame must be dispatched, not discarded as mid-walk noise";
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "a contiguous frame off the live tap means we are following live";
}

// A steady-state gap resumes the recording at the frame last dispatched (doc/review A9), so repairing a
// dropped frame costs a replay of the hole rather than of the whole trading day. The stale walk index is
// the thing it must never carry into that request: that is a cold-start cursor into the recording chain,
// not a position, and resuming a walk from wherever the last one had got to is unsound.
TEST(ReplayerRecoveryGapRecovery, SteadyStateGapResumesAtTheLastDispatchedFrameNotTheStaleWalkIndex)
{
    Client client{ [](const SequencedEvent&) {} };

    // Move the walk index off 0 first (simulating a cold-start walk already into segment 2).
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/100));
    completeSegment(client);
    completeSegment(client);
    ASSERT_EQ(2, client.recovery.walkSegmentIndex());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));
    ASSERT_TRUE(client.recovery.isCaughtUp());

    deliverLive(client, 1, /*framePosition=*/0);
    deliverLive(client, 2, /*framePosition=*/1024); // the frame the resume must anchor on
    deliverLive(client, 5, /*framePosition=*/2048); // steady-state gap

    EXPECT_EQ(-1, client.recovery.walkSegmentIndex()) << "a gap asks to resume, never to continue the old walk";
    EXPECT_EQ(1024, client.recovery.requestFromPosition()) << "anchored at the last frame DISPATCHED, not at the "
                                                              "out-of-order one that exposed the hole";
}

TEST(ReplayerRecoveryGapRecovery, DuplicateAndStaleFramesFromReplayAreDropped)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);

    deliverReplay(client, 1); // already seen live -> dup
    deliverReplay(client, 2); // already seen live -> dup
    EXPECT_EQ(2, delivered) << "replayed frames already delivered off the live tap must be de-duped";

    deliverReplay(client, 3); // new -> delivered
    EXPECT_EQ(3, delivered);
}

TEST(ReplayerRecoveryGapRecovery, NoReplayNeededMarksCaughtUpAndClearsWalkState)
{
    Client client{ [](const SequencedEvent&) {} };

    ASSERT_FALSE(client.recovery.isCaughtUp());
    // recordingId -1: the walk ran past the last recording. That is the only reply that ends a walk —
    // see NoReplayNeededNamingARecordingSkipsThatSegmentAndKeepsWalking for the other sender.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));

    EXPECT_TRUE(client.recovery.isCaughtUp())
        << "NO_REPLAY_NEEDED means already at the tip of an empty/exhausted chain";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_EQ(-1, client.recovery.walkSegmentIndex()) << "chain exhausted -> steady/resume mode, not mid-walk";
}

TEST(ReplayerRecoveryGapRecovery, ReplayingWithSessionArmsReplayWithoutMarkingCaughtUp)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/42,
                                           /*catchUpPosition=*/1'000));

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "a session id means there IS history to replay first";
    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "onControl always clears awaiting once answered";
    EXPECT_EQ(42, client.recovery.replaySessionId());
}

TEST(ReplayerRecoveryGapRecovery, ReplayingForAnotherClientIdIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/2, client.recovery.requestId(), /*replaySessionId=*/42,
                                           /*catchUpPosition=*/1'000));

    EXPECT_EQ(-1, client.recovery.replaySessionId())
        << "a reply on the shared control stream addressed to a different replica must not be applied";
    EXPECT_FALSE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryGapRecovery, SegmentCompleteAdvancesTheWalkAndReRequestsTheNextSegment)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.recovery.replaySessionId());

    completeSegment(client);

    EXPECT_EQ(1, client.recovery.walkSegmentIndex()) << "segment 0 done -> walk advances to segment 1";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "advancing re-requests the next segment";
    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "no session until the Replayer answers the new request";
}

// ── Request/reply correlation (doc/review A3) ────────────────────────────────────────────────
// clientId alone cannot identify WHICH request a reply answers. A resend makes the Replayer stop the
// in-flight session and start a new one, so both replies sit in order on the one shared control
// publication and the stale one is always processed first. Acting on it attached the client to a
// session that no longer exists — and, because onControl cleared m_awaitingReplay unconditionally, also
// stopped the resend timer, leaving the client with no session, no image and no retry.
TEST(ReplayerRecoveryGapRecovery, ReplyForASupersededRequestIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    const std::int64_t stale = client.recovery.requestId();

    completeSegment(client); // advances the walk -> new request, new requestId
    ASSERT_NE(stale, client.recovery.requestId());
    ASSERT_TRUE(client.recovery.isAwaitingReplay());

    deliverControl(client, encodeReplaying(/*clientId=*/1, stale, /*replaySessionId=*/42, /*catchUpPosition=*/900));

    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "must not attach to the superseded request's stopped session";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "and must keep awaiting, so the resend timer stays armed";
}

// The live reply that follows the stale one must still be taken — correlation must reject the stale
// reply without wedging the client against every later one.
TEST(ReplayerRecoveryGapRecovery, CurrentReplyIsStillAcceptedAfterAStaleOne)
{
    Client client{ [](const SequencedEvent&) {} };
    const std::int64_t stale = client.recovery.requestId();
    completeSegment(client);

    deliverControl(client, encodeReplaying(/*clientId=*/1, stale, /*replaySessionId=*/42, /*catchUpPosition=*/900));
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/43,
                                           /*catchUpPosition=*/1'000));

    EXPECT_EQ(43, client.recovery.replaySessionId()) << "the reply to the current request must be applied";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
}

// A stale ReplayPending must not reset the request clock either: that is what paces the resend, so
// honouring a superseded request's "wait" would defer the retry the client is relying on.
TEST(ReplayerRecoveryGapRecovery, ReplayPendingForASupersededRequestIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    const std::int64_t stale = client.recovery.requestId();
    completeSegment(client); // advances the walk -> a new request, whose resend deadline is what matters
    const int sends = client.requestsSent;

    client.clockMs += 300; // still inside the current request's resend interval
    deliverControl(client, encodeReplayPending(/*clientId=*/1, stale));
    client.clockMs += 300; // ... which has now elapsed, unless the stale "wait" pushed it out
    client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ(sends + 1, client.requestsSent) << "a superseded request's ReplayPending must not "
                                                 "push out the current request's resend deadline";
}

// ── Closed image vs. completed segment (doc/review A4) ───────────────────────────────────────
// A bounded replay of a STOPPED recording closes its image on its own, exactly at the stopPosition it
// was bounded to — the one close that means "segment done".
TEST(ReplayerRecoveryGapRecovery, ReplayImageClosingAtTheBoundCompletesTheSegment)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(0, client.recovery.walkSegmentIndex());

    client.recovery.onReplayImageClosed(/*finalPosition=*/500);

    EXPECT_EQ(1, client.recovery.walkSegmentIndex()) << "closing AT the bound is completion -> advance the walk";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
}

// Any other close (supersede, idle-TTL reclaim, archive fault, Replayer shutdown) leaves the image short
// of the bound. Advancing there skips history that was never replayed — a silent hole, since the walk
// rides on and only a later tap gap could ever expose it.
TEST(ReplayerRecoveryGapRecovery, ReplayImageClosingShortOfTheBoundReRequestsTheSameSegment)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    const std::int64_t requestId = client.recovery.requestId();

    client.recovery.onReplayImageClosed(/*finalPosition=*/312); // stopped under us, mid-segment

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "the walk must NOT advance over a segment it only "
                                                        "partially replayed";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "the same segment is re-requested instead";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_NE(requestId, client.recovery.requestId()) << "re-requesting is a new request, so the reply to the "
                                                         "stopped one is recognisably stale";
    EXPECT_EQ(1u, sink.events.size()) << "a truncated replay is reported, not silently absorbed";
}

// A replay that neither closes nor advances is the third case, and the one nothing used to catch: once
// Replaying arrives the resend timer is disarmed (m_awaitingReplay is false), and a bounded replay of an
// active recording never closes its image, so an image that simply stops — the archive faulted, the
// publication wedged — left the client waiting on it forever. Measured before the fix: a cold start past
// ~32 MiB of history hung permanently with the image attached, open, and frozen.
TEST(ReplayerRecoveryGapRecovery, ReplayThatStopsAdvancingReRequestsTheSameSegment)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    const std::int64_t requestId = client.recovery.requestId();
    ASSERT_FALSE(client.recovery.isAwaitingReplay()) << "a session was assigned, so nothing else is retrying";

    advancePastTimers(client);

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "the walk must NOT advance over a segment that stalled "
                                                        "part-way through";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "the same segment is re-requested instead";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_NE(requestId, client.recovery.requestId()) << "re-requesting is a new request, so the reply to the "
                                                         "stalled one is recognisably stale";
    EXPECT_EQ(1u, sink.events.size()) << "a stalled replay is reported, not silently absorbed";
}

TEST(ReplayerRecoveryGapRecovery, ReplayPendingHoldsAtTheGapWithoutAssigningASession)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay
    ASSERT_TRUE(client.recovery.isAwaitingReplay());

    deliverControl(client, encodeReplayPending(/*clientId=*/1, client.recovery.requestId()));

    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "no free Replayer slot -> keep holding at the gap";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_EQ(1, delivered) << "must not advance past the hole while pending";
}

// A Replayer whose archive failed the globalSeqNo-1 integrity check refuses instead of serving history
// its own check rejected. The refusal must be CONTAINED here: served anyway, the replay's first frame
// would not be 1 and this client would abort (see FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess), taking
// down every co-located app over one node's bad archive. So this holds like ReplayPending — never caught
// up, nothing dispatched, and still resending, so repairing the archive heals the app without a restart.
TEST(ReplayerRecoveryGapRecovery, ReplayUnavailableHoldsWithoutAbortingOrAdvancing)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay
    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    ASSERT_FALSE(client.recovery.isCaughtUp());

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only the refusal below
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));

    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "a refusal must not stop the resend timer";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_EQ(1, delivered) << "must not advance past the hole";
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "consumer gates must stay shut while history is unavailable";

    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Severity::Fault, sink.events[0].severity);
    EXPECT_EQ(diag::EventCode::ReplayUnavailable, sink.events[0].code);

    // The Replayer answers every resend the same way; the fault line must not repeat per reply.
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));
    EXPECT_EQ(1u, sink.events.size()) << "one episode is one line, not one per 500ms resend";
}

// A second, distinct outage hours later must report itself: the latch is on the episode, not on the
// process (review-3.md #12). The intervening reply is what ends the first episode — an operator
// repaired the archive and restarted the Replayer, exactly the recovery onReplayUnavailable describes.
TEST(ReplayerRecoveryGapRecovery, ARefusalAfterTheReplayerRecoveredIsReportedAgain)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only what follows
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));
    ASSERT_EQ(1u, refusalCount(sink));

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/900));
    // Later: that node's archive breaks again, its Replayer restarts refusing, and our replay stops
    // delivering — so we re-request (a new requestId) and are refused a second time.
    advancePastTimers(client);
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));

    EXPECT_EQ(2u, refusalCount(sink)) << "a process-lifetime latch leaves every later outage silent — and "
                                         "this refusal carries no counter, so the log line is all there is";
}

// Being queued ends the episode too: a Replayer with integrityFailed latched answers ReplayUnavailable,
// never ReplayPending, so a pending reply is proof it is serving again — and while queued, a stall report
// naming us "refused" would point at the wrong thing.
TEST(ReplayerRecoveryGapRecovery, AQueuedReplayAlsoEndsTheRefusalEpisode)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only what follows
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));
    deliverControl(client, encodeReplayPending(/*clientId=*/1, client.recovery.requestId()));
    advancePastTimers(client);
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));

    EXPECT_EQ(2u, refusalCount(sink));
}

TEST(ReplayerRecoveryGapRecovery, ReplayUnavailableForASupersededRequestIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 5);
    const std::int64_t stale = client.recovery.requestId() - 1;
    const int sends = client.requestsSent;

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only the refusal below
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, stale));
    client.clockMs += 600; // > RESEND_INTERVAL_MS since the CURRENT request went out
    client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_TRUE(sink.events.empty()) << "a stale refusal says nothing about the Replayer's current state";
    EXPECT_EQ(sends + 1, client.requestsSent) << "and must not reset the resend clock";
}

TEST(ReplayerRecoveryGapRecovery, StuckAwaitingReplayResendsAfterTheIntervalElapses)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> requestReplay(0, 0), arming the resend timer
    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    const int sends = client.requestsSent;

    client.clockMs += 300; // inside RESEND_INTERVAL_MS (500)
    client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_EQ(sends, client.requestsSent) << "must not resend before RESEND_INTERVAL_MS elapses";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());

    client.clockMs += 300; // 600ms since the request — past it
    client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ(sends + 1, client.requestsSent)
        << "the Replayer never answered -> the duty cycle must re-send the same request rather than give up";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "still stuck at the gap — a resend, not a new state";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_EQ(-1, client.recovery.walkSegmentIndex()) << "resend repeats the same request verbatim, still the resume";
}

// isRecovering() is what separates "the tap is ahead of my replay" from "there is a hole" — narrowing it
// to miss a state means a walk supersedes itself on its own in-flight frames (see
// TapFrameAheadOfAnInFlightWalkDoesNotSupersedeIt), widening it means a real gap goes unreported. This
// walks every transition it must track. (It also guarded the discard routing behind doc/todo.md's
// 2026-07-31 "gap re-walk starves the tap it is recovering"; that discard is gone as of 2026-08-05 —
// poll() now always drains AND dispatches the tap — but the predicate itself carries more weight, not
// less.)
TEST(ReplayerRecoveryGapRecovery, RecoveringFlagTracksWalkAndAwaitingReplayState)
{
    Client client{ [](const SequencedEvent&) {} };
    EXPECT_FALSE(client.recovery.isRecovering()) << "nothing in flight yet";

    deliverLive(client, 1);
    deliverLive(client, 5); // skips 3, 4 -> gap, re-requests a replay
    EXPECT_TRUE(client.recovery.isRecovering()) << "awaiting the Replayer's answer is already mid-recovery";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    EXPECT_TRUE(client.recovery.isRecovering()) << "a session id means history remains to replay, not yet caught up";

    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    deliverReplay(client, 2); // closes the hole the resume was asked to cover, draining retained frame 5
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    completeSegment(client);
    EXPECT_FALSE(client.recovery.isRecovering()) << "a resume ends at its bound — there is no next segment to ask for";

    // The walk shape of the same transitions, entered the way production enters it: a resume whose replay
    // opens on a frame other than the one it anchored on falls back to the chain walk.
    deliverLive(client, 9); // another gap -> another resume
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 42);
    ASSERT_EQ(0, client.recovery.walkSegmentIndex());
    EXPECT_TRUE(client.recovery.isRecovering()) << "awaiting the walk's first segment";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/9,
                                           /*catchUpPosition=*/500));
    // The walk has to actually close the hole it re-walked for: 9 is retained behind the missing 6..8,
    // and the terminator below refuses to end recovery while anything is still stranded there
    // (WalkTerminatorWithARetainedHoleStillOpenReWalksInstead).
    deliverReplay(client, 6);
    deliverReplay(client, 7);
    deliverReplay(client, 8); // dispatching 8 drains the retained 9 straight over
    completeSegment(client);
    EXPECT_TRUE(client.recovery.isRecovering()) << "advancing to the next segment stays mid-walk";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));
    EXPECT_FALSE(client.recovery.isRecovering()) << "chain exhausted -> back to steady state";
}

// isCaughtUp() is a state, not a latch (doc/review A1). Consumers gate real decisions on it —
// OrderExecServer/BasicDataServer leader-only emission, and FixGateway's tap-stall watchdog, which
// measures silence in DISPATCHED frames and self-terminates the gateway after 20s. A re-walk dispatches
// nothing until the replay passes the hole, so leaving it latched made the gateway diagnose its own
// recovery as a stalled sequencer and force a standby promotion it did not need.
TEST(ReplayerRecoveryGapRecovery, TapGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.recovery.isCaughtUp());
    ASSERT_EQ(1, caughtUpNotifications);

    deliverLive(client, 5); // skips 3, 4 -> gap (and 5 is retained, not dropped)
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "a hole means we are demonstrably not following live";

    deliverReplay(client, 2); // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 3);
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "4 is still missing — the hole is not closed yet";
    EXPECT_EQ(1, caughtUpNotifications);

    deliverReplay(client, 4); // closes the hole; the retained tap frame 5 then hands straight over

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "the retained frame closes the seam with no further round trip";
    EXPECT_EQ(2, caughtUpNotifications) << "consumers must be able to re-arm on re-convergence, not just "
                                           "on first catch-up";
}

// The retention half of the seam fix (doc/review A2), stated on its own: a frame from beyond the hole is
// held and delivered exactly once when the hole closes — never dropped, never duplicated, always in
// globalSeqNo order. Dropping these is what made every walk end one hole short of live and re-walk.
TEST(ReplayerRecoveryGapRecovery, FramesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses)
{
    std::vector<std::int64_t> delivered;
    Client client{ [&](const SequencedEvent& event) { delivered.push_back(event.globalSeqNo); } };

    deliverLive(client, 1);
    deliverLive(client, 4); // gap -> retained
    deliverLive(client, 5); // still ahead of the hole -> retained
    deliverLive(client, 6); // ditto
    ASSERT_EQ((std::vector<std::int64_t>{ 1 }), delivered) << "nothing past the hole may be dispatched early";

    deliverReplay(client, 1); // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2);
    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2 }), delivered) << "3 is still missing";

    deliverReplay(client, 3); // closes the hole -> 4, 5, 6 drain behind it

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3, 4, 5, 6 }), delivered)
        << "retained frames must drain in order, exactly once, with no re-walk needed";
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

// A hole in REPLAYED history was the one invariant violation that produced no log and no counter
// (doc/review A10): the frame is dropped, the walk rides on, and recovery quietly never converges.
// Nothing unsafe follows — contiguity still holds — so this reports rather than aborts, once per
// episode, since the walk retries against the same chain every 500ms.
TEST(ReplayerRecoveryGapRecovery, GapInReplayedHistoryIsReportedOncePerEpisode)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverReplay(client, 1);
    ASSERT_EQ(1, delivered);

    ScopedLoggerSink sink;    // installed after the baseline, so it captures only the hole below
    deliverReplay(client, 7); // the recording chain does not cover 2..6

    EXPECT_EQ(1, delivered) << "the frame past the hole must not be dispatched";
    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Severity::Warn, sink.events[0].severity);
    EXPECT_EQ(diag::EventCode::TapGap, sink.events[0].code);

    deliverReplay(client, 8);
    EXPECT_EQ(1u, sink.events.size()) << "the walk retries against the same chain — report the episode, not "
                                         "every frame";

    // A later, distinct episode must be reported again rather than swallowed by the latch.
    deliverReplay(client, 2);
    ASSERT_EQ(2, delivered);
    deliverReplay(client, 9);
    EXPECT_EQ(2u, sink.events.size()) << "the latch clears once history goes contiguous again";
}

// The resume's safety check (doc/review A9). A position denotes a frame only within the recording it was
// observed in, and a member restart can leave the app holding one from a recording that is no longer the
// active one. Rather than trying to prove that has not happened, the resumed replay is checked where it
// lands: its first frame must be the frame the position was anchored on. Riding it regardless would walk
// an arbitrary mid-stream point, and a replay-path gap is otherwise swallowed silently.
TEST(ReplayerRecoveryGapRecovery, ResumeOpeningOnTheWrongFrameFallsBackToTheChainWalk)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume anchored on frame 1
    ASSERT_EQ(-1, client.recovery.walkSegmentIndex());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.recovery.replaySessionId());

    deliverReplay(client, 77); // the recording rotated: that position is some other frame entirely

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "fall back to the walk, which needs no position to be sound";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "and let go of the replay it was riding";
    EXPECT_EQ(1, delivered) << "the frame it opened on must not be dispatched";
    EXPECT_FALSE(client.recovery.isCaughtUp());
    ASSERT_EQ(2u, sink.events.size()) << "the gap, then the mismatch";
    EXPECT_EQ(diag::EventCode::TapGap, sink.events[1].code);
}

// The resume anchor is consumed by the first frame of ONE replay episode (doc/review A9) — a retry of
// that same logical resume (image closed short of its bound, or stalled) starts a NEW episode and must
// re-arm the check via requestResume(), not resend the bare fromPosition via requestReplay() directly.
// Without it, the retried replay's first frame rides with no anchor to validate against: if the active
// recording rotated under the anchored position and the frame that now lands there happens to look
// contiguous (globalSeqNo == last + 1), it is silently dispatched as if it were the frame the resume
// asked for, instead of being caught as a mismatch and falling back to the chain walk.
TEST(ReplayerRecoveryGapRecovery, ReplayImageClosingShortOfTheBoundOnAResumeReArmsTheAnchorCheck)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume anchored on frame 1
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    ASSERT_EQ(1, delivered);

    client.recovery.onReplayImageClosed(/*finalPosition=*/300); // stopped under us before frame 2 arrived
    ASSERT_EQ(-1, client.recovery.walkSegmentIndex()) << "still a resume retry, not a walk step";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    // The active recording rotated: the retried resume's first frame is NOT the frame it was anchored
    // on, but it happens to look contiguous (globalSeqNo == last + 1).
    deliverReplay(client, 2);

    EXPECT_EQ(1, delivered) << "must not silently accept an unanchored frame merely because it looks contiguous";
    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "the mismatch must fall back to the chain walk";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_FALSE(client.recovery.isCaughtUp());
}

// Same defect, the other retry trigger: a resume that stalls (never closes, never advances) instead of
// closing short.
TEST(ReplayerRecoveryGapRecovery, ReplayStallOnAResumeReArmsTheAnchorCheck)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume anchored on frame 1
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    ASSERT_EQ(1, delivered);

    advancePastTimers(client); // stopped advancing before frame 2 arrived
    ASSERT_EQ(-1, client.recovery.walkSegmentIndex()) << "still a resume retry, not a walk step";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 2); // rotated recording, looks contiguous but is not the anchored frame

    EXPECT_EQ(1, delivered) << "must not silently accept an unanchored frame merely because it looks contiguous";
    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "the mismatch must fall back to the chain walk";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_FALSE(client.recovery.isCaughtUp());
}

// The same rotation seen one step earlier: the Replayer answers "nothing to replay" because the position
// is already at its recording's tip. For a walk that means caught up; for a resume it cannot, because we
// only resumed on account of a hole we know is open — taking it at face value would close that hole by
// fiat and leave the client silently short of history.
TEST(ReplayerRecoveryGapRecovery, NoReplayNeededOverAnOpenHoleFallsBackToTheChainWalk)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.recovery.isCaughtUp());
    deliverLive(client, 6); // gap -> resume
    ASSERT_EQ(-1, client.recovery.walkSegmentIndex());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "re-walk instead of believing it";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "the hole above globalSeqNo=2 is still open";
}

// A resume ends at the bound it was given and has no next segment to request — so unlike a walk step it
// must declare itself caught up and hand the slot back (ReplayComplete), or the slot sits until the 60s
// idle TTL reclaims it: one of MAX_CONCURRENT_REPLAYS slots, held by nobody.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundCatchesUpWithoutRequestingAnotherSegment)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverLive(client, 1);
    ASSERT_EQ(1, caughtUpNotifications);
    deliverLive(client, 5); // gap -> resume
    ASSERT_FALSE(client.recovery.isCaughtUp());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));

    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    deliverReplay(client, 2); // closes the hole the resume was asked to cover, draining retained frame 5
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    completeSegment(client);

    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "no follow-up request — a resume has no next segment";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "we hold everything the recording had when the request was served";
    EXPECT_EQ(2, caughtUpNotifications) << "re-fired, so consumers re-arm on re-convergence";
}

// review-3.md #9: the release was a fire-and-forget offer, so an offer that did not land was
// indistinguishable from one that did — and nothing re-sent it, leaving the slot to the 60s TTL. There
// is no publication in these tests, so every send here is a send that never went out.
TEST(ReplayerRecoveryGapRecovery, AReplayCompleteThatNeverWentOutStaysPending)
{
    Client client{ [](const SequencedEvent&) {}, [] {} };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    ASSERT_FALSE(client.recovery.completePending()) << "nothing to release until the resume reaches its bound";

    completeSegment(client);

    ASSERT_TRUE(client.recovery.isCaughtUp());
    EXPECT_TRUE(client.recovery.completePending()) << "it never reached the wire — remember it";
    client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_TRUE(client.recovery.completePending()) << "the duty cycle retries the release rather than forgetting it";
}

// An unsent request must NOT be retried per duty cycle, only on the resend timer. Every send does
// ++m_requestId and onControl acts only on a reply carrying the CURRENT id, so a per-poll retry runs the
// counter away and every reply that arrives is discarded as stale — leaving the client awaiting a replay
// that can never correlate, hence never caught up. Cost of getting this wrong is not a slow resend: it is
// a consumer wedged out of isCaughtUp(), which on the leader-co-located replica means no ExecutionReport.
TEST(ReplayerRecoveryGapRecovery, AnUnsentRequestIsNotReSentOncePerPoll)
{
    Client client{ [](const SequencedEvent&) {}, [] {} };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume request; no publication here, so it never goes out
    const std::int64_t before = client.recovery.requestId();

    for (int i = 0; i < 50; ++i)
    {
        client.recovery.doTimers(/*requestPublicationPending=*/false); // duty cycles, no clock advance
    }

    ASSERT_TRUE(client.recovery.isAwaitingReplay()) << "still waiting — nothing answered it";
    EXPECT_LE(client.recovery.requestId() - before, 1)
        << "the resend timer paces this; one id per poll outruns every reply in flight";
}

// ReplayComplete names only the clientId, so one landing after a NEW request took a fresh slot would
// free the slot that replay is riding. A new request supersedes the old slot server-side anyway.
TEST(ReplayerRecoveryGapRecovery, ANewRequestDropsAReplayCompleteThatNeverWentOut)
{
    Client client{ [](const SequencedEvent&) {}, [] {} };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    completeSegment(client);
    ASSERT_TRUE(client.recovery.completePending());

    deliverLive(client, 9); // a second gap -> new request, taking a fresh slot

    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_FALSE(client.recovery.completePending()) << "dropped: releasing now would free the new slot";
}

// The companion defect (doc/review #5): a resume can reach its bound while a retained-ahead frame still
// sits behind a hole the replay never covered — onReplaySegmentComplete used to declare caught up on the
// bound alone, with no check that m_lastGlobalSeqNo had actually reached the frontier the retained FIFO
// knows about. That reading is silently wrong: consumers gate real decisions on isCaughtUp() and nothing
// would rediscover the hole until some later, unrelated tap frame happened to expose it.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundWithARetainedHoleStillOpenReWalksInstead)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverLive(client, 1);
    ASSERT_EQ(1, caughtUpNotifications);
    deliverLive(client, 5); // gap -> resume anchored on frame 1; 5 is retained ahead of the hole
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));

    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    deliverReplay(client, 2); // the replay's bound is reached having covered only up to 2 — 3, 4 unfilled

    completeSegment(client); // reached the bound it was given, but the hole above 2 is still open

    EXPECT_FALSE(client.recovery.isCaughtUp())
        << "must not declare caught up with a retained frame still behind a hole";
    EXPECT_EQ(1, caughtUpNotifications) << "no false re-convergence notification";
    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "falls back to the chain walk instead of trusting the bound";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
}

// The overflow half of the same guard (review-3.md #12). When retainMessages had to DROP tap frames, the
// frontier is short by frames that are gone from the tap for good, and every retained frame draining
// cleanly says nothing about them. drainRetained used to clear the overflow on its first dispatch —
// before this check could read it — so precisely the run that drained successfully was the one that
// declared itself caught up over the hole.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundAfterDroppedTapFramesReWalksInstead)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverLive(client, 1);
    ASSERT_EQ(1, caughtUpNotifications);
    deliverLive(client, 3);               // gap -> resume anchored on frame 1; 3 is retained
    deliverLiveTooBigToRetain(client, 4); // ahead of the hole too, but dropped rather than retained
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));

    deliverReplay(client, 1); // opens on the anchored frame -> anchor consumed, frame deduped
    deliverReplay(client, 2); // closes the hole the resume was asked to cover, draining retained frame 3

    completeSegment(client);

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "frame 4 was dropped — the drained frontier is not the real one";
    EXPECT_EQ(1, caughtUpNotifications) << "no false re-convergence notification";
    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "re-walks now rather than leaving the hole for a later tap gap";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
}

TEST(ReplayerRecoveryGapRecovery, DroppedTapFramesAreReportedOncePerEpisode)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 3); // gap -> resume anchored on frame 1; 3 is retained

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only the overflow lines
    deliverLiveTooBigToRetain(client, 4);
    deliverLiveTooBigToRetain(client, 5);
    ASSERT_EQ(1u, overflowReports(sink)) << "one episode is one line, not one per dropped frame";

    // The re-walk is what covers the dropped frames, so the episode ends when it is requested.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    completeSegment(client);
    ASSERT_EQ(0, client.recovery.walkSegmentIndex());

    deliverLiveTooBigToRetain(client, 9);

    EXPECT_EQ(2u, overflowReports(sink)) << "a later overflow is a distinct episode and names itself";
}

// Retained frames the replay has meanwhile covered are discarded on drain rather than double-delivered —
// the ranges legitimately overlap, since a re-walk restarts from segment 0 while the tap keeps arriving.
TEST(ReplayerRecoveryGapRecovery, RetainedFramesAlreadyCoveredByTheReplayAreNotRedelivered)
{
    std::vector<std::int64_t> delivered;
    Client client{ [&](const SequencedEvent& event) { delivered.push_back(event.globalSeqNo); } };

    deliverLive(client, 1);
    deliverLive(client, 3); // gap -> retained
    deliverLive(client, 4); // retained

    deliverReplay(client, 1); // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2);
    deliverReplay(client, 3); // the replay covers a frame the tap already retained
    deliverReplay(client, 4); // and another

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3, 4 }), delivered) << "each globalSeqNo dispatched exactly once";
}

// ── The walk's terminating NO_REPLAY_NEEDED (review-3 findings 1 and 2) ──────────────────────
// serveReplay sends NO_REPLAY_NEEDED for two different things and tells them apart by recordingId: a
// walk that ran past the last recording (names none, -1) versus a segment that is merely EMPTY (names
// the recording it found nothing in). Only the first ends the walk. And ending it is not by itself
// permission to declare caught up: the frontier is what the retained-ahead FIFO knows, not what the
// chain covered.

TEST(ReplayerRecoveryGapRecovery, WalkTerminatorWithARetainedHoleStillOpenReWalksInstead)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    // Cold-start walk over segment 0, which covers only globalSeqNo 1..2 — while the tap runs ahead and
    // frame 5 is retained behind the hole at 3,4.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500, /*recordingId=*/5));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverLive(client, 5); // ahead of the hole -> retained, not dropped
    completeSegment(client);
    ASSERT_EQ(1, client.recovery.walkSegmentIndex()) << "segment 0 done -> the walk advances";

    // The chain really is exhausted (the terminator names no recording) — but 3 and 4 were never
    // replayed, so the retained frame 5 still sits behind a hole.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "must not declare caught up with a retained frame behind a hole";
    EXPECT_EQ(0, caughtUpNotifications) << "no false convergence notification — this opens the accept gate";
    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "re-walks the chain rather than trusting the terminator";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
}

// The negative control for the guard above: a walk whose retained frames all drained must still finish.
TEST(ReplayerRecoveryGapRecovery, WalkTerminatorWithEveryRetainedFrameDrainedStillCatchesUp)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500, /*recordingId=*/5));
    deliverReplay(client, 1);
    deliverLive(client, 3);   // ahead of the hole at 2 -> retained
    deliverReplay(client, 2); // closes it, so 3 drains straight over
    completeSegment(client);

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "nothing is outstanding — the guard must not fire here";
    EXPECT_EQ(-1, client.recovery.walkSegmentIndex()) << "steady/resume mode, not a spurious re-walk";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
}

// The same guard at the terminator, driven through to convergence: the re-walk must both happen AND be
// able to finish. The overflow is cleared where that re-walk is requested, which is the only clear point
// that does both — clearing it on drain forgets the drop and catches up over the hole, never clearing it
// leaves a client that can never declare itself caught up again.
TEST(ReplayerRecoveryGapRecovery, WalkTerminatorAfterDroppedTapFramesReWalksThenCatchesUp)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500, /*recordingId=*/5));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverLiveTooBigToRetain(client, 4); // the tap runs ahead of the walk, and this one is dropped
    completeSegment(client);

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "the chain was exhausted, but a tap frame above it was dropped";
    EXPECT_EQ(0, caughtUpNotifications) << "no false convergence notification — this opens the accept gate";
    ASSERT_EQ(0, client.recovery.walkSegmentIndex()) << "re-walks the chain rather than trusting the terminator";

    // The re-walk replays what the drop lost, and this time nothing is dropped.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/900, /*recordingId=*/5));
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    completeSegment(client);
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/-1));

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "a re-walk that dropped nothing must be able to finish";
    EXPECT_EQ(1, caughtUpNotifications);
}

// An unclean restart can leave a recording created before anything was published to it. Taking the
// NO_REPLAY_NEEDED that answers it as the walk terminator drops every later segment on the floor — and
// because the re-walk a later tap gap triggers lands on that same empty segment, it never converges.
TEST(ReplayerRecoveryGapRecovery, NoReplayNeededNamingARecordingSkipsThatSegmentAndKeepsWalking)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0, /*recordingId=*/5));

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "an empty segment says nothing about the rest of the chain";
    EXPECT_EQ(1, client.recovery.walkSegmentIndex()) << "skip the empty segment, keep walking";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());

    // Segment 1 holds the history, and the walk proceeds through it normally.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/9,
                                           /*catchUpPosition=*/900, /*recordingId=*/8));

    EXPECT_EQ(9, client.recovery.replaySessionId());
    EXPECT_EQ(8, client.recovery.walkRecordingId());
    deliverReplay(client, 1);
    EXPECT_EQ(1, client.recovery.walkSegmentIndex()) << "still walking segment 1";
}

// ── Walk-segment recordingId mismatch (doc/review #4) ────────────────────────────────────────
// serveReplay re-resolves the recording chain on every request; ReplayRecordings.stitch drops a stale
// still-recording span once a newer one supersedes it, which shifts what a given segmentIndex denotes.
// A retry of the SAME segment (image closed short of its bound, stalled, or simply resent) must land on
// the SAME recording it did originally — the Replayer echoes the recordingId it served precisely so this
// can be checked without relying on globalSeqNo contiguity to notice it later.
TEST(ReplayerRecoveryGapRecovery, WalkSegmentRetryOnTheSameRecordingContinuesNormally)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500, /*recordingId=*/5));
    ASSERT_EQ(5, client.recovery.walkRecordingId());
    const std::int64_t requestId = client.recovery.requestId();

    client.recovery.onReplayImageClosed(/*finalPosition=*/312); // stopped under us, mid-segment -> retried
    ASSERT_EQ(0, client.recovery.walkSegmentIndex());
    ASSERT_NE(requestId, client.recovery.requestId());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/700, /*recordingId=*/5));

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "same recording -> the retry is trusted, not abandoned";
    EXPECT_EQ(5, client.recovery.walkRecordingId());
    EXPECT_EQ(8, client.recovery.replaySessionId());
}

TEST(ReplayerRecoveryGapRecovery, WalkSegmentRetryOnADifferentRecordingAbandonsTheWalkAndRestartsAtSegmentZero)
{
    Client client{ [](const SequencedEvent&) {} };

    // Segment 0 -> recording 5, then advance to segment 1 -> recording 8 (a legitimate chain of two).
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500, /*recordingId=*/5));
    completeSegment(client);
    ASSERT_EQ(1, client.recovery.walkSegmentIndex());
    ASSERT_EQ(-1, client.recovery.walkRecordingId()) << "no expectation yet for the new segment";
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/9,
                                           /*catchUpPosition=*/900, /*recordingId=*/8));
    ASSERT_EQ(8, client.recovery.walkRecordingId());

    client.recovery.onReplayImageClosed(/*finalPosition=*/650); // stopped under us, mid-segment -> retried
    ASSERT_EQ(1, client.recovery.walkSegmentIndex()) << "still segment 1 — re-requested, not advanced";

    // The retry for segment 1 now resolves to a different recording — the chain shifted under us.
    ScopedLoggerSink sink;
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/55,
                                           /*catchUpPosition=*/1'200, /*recordingId=*/42));

    EXPECT_EQ(0, client.recovery.walkSegmentIndex()) << "abandon the walk and restart from segment 0";
    EXPECT_EQ(-1, client.recovery.walkRecordingId());
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "restarting the walk is itself a new request";
    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "must not attach to the mismatched reply's session";
    EXPECT_EQ(1u, sink.events.size()) << "the shift is reported, not silently absorbed";
}

// ── Convergence alarm (review-3.md #6) ───────────────────────────────────────────────────────
// RecoveryProgressPolicyTest covers the verdict itself; what is locked here is the wiring around it —
// that an in-order dispatch counts as progress, that catching up without dispatching does too, and that
// the report carries the state telling the causes apart.

constexpr std::int64_t PAST_DEADLINE_MS = CLOCK_MS + 30'000; // RECOVERY_PROGRESS_TIMEOUT_MS

TEST(ReplayerRecoveryConvergence, RecoveryDeliveringNothingIsReportedOncePerEpisode)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    EXPECT_FALSE(checkProgressAt(client, CLOCK_MS)) << "the first observation only anchors the clock";
    EXPECT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));

    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Component::ReplayerStreamReceiver, sink.events[0].component);
    EXPECT_EQ(diag::Severity::Fault, sink.events[0].severity);
    EXPECT_EQ(diag::EventCode::RecoveryStalled, sink.events[0].code);

    // The condition persists for as long as the archive is broken; the caller logs, so it must not
    // repeat every duty cycle.
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS + 60'000));
    EXPECT_EQ(1u, sink.events.size());
}

TEST(ReplayerRecoveryConvergence, AWalkThatIsStillDeliveringIsNeverReportedHoweverLongItRuns)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    // A cold start replaying a whole trading day: slow, but converging one frame at a time. Measuring
    // elapsed time instead of progress is exactly what would fence this.
    for (std::int64_t gseq = 1; gseq <= 20; ++gseq)
    {
        deliverReplay(client, gseq);
        EXPECT_FALSE(checkProgressAt(client, CLOCK_MS + gseq * 60'000)) << "at globalSeqNo " << gseq;
    }
}

TEST(ReplayerRecoveryConvergence, AGapAfterAHealthyRunStartsItsOwnEpisodeRatherThanInheritingOne)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS)); // an episode anchored during cold start
    deliverLive(client, 1);                          // ... which then converges
    ASSERT_TRUE(client.recovery.isCaughtUp());

    deliverLive(client, 5); // much later, a tap gap: !isCaughtUp() again
    ASSERT_FALSE(client.recovery.isCaughtUp());

    // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk
    // it triggers had any chance to converge.
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS));
    ASSERT_EQ(1u, sink.events.size()) << "the tap-gap warn only — no convergence report";
    EXPECT_EQ(diag::EventCode::TapGap, sink.events[0].code);
}

TEST(ReplayerRecoveryConvergence, ACaughtUpClientThatSimplyGoesQuietIsNotReported)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    ASSERT_TRUE(client.recovery.isCaughtUp());

    // A tap that goes silent is checkTapStall's business (FixGateway), and it is not a convergence
    // failure at all — diagnosing it as one would put a second, wrong explanation on the same event.
    EXPECT_FALSE(checkProgressAt(client, CLOCK_MS));
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS));
    EXPECT_TRUE(sink.events.empty());
}

TEST(ReplayerRecoveryConvergence, TheReportNamesTheStateThatTellsTheCausesApart)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));

    ScopedLoggerSink sink; // installed after the refusal, so it captures only the report below
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS));
    ASSERT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));

    ASSERT_EQ(1u, sink.events.size());
    const std::string text(sink.events[0].text.data(), sink.events[0].textLen);
    // Without these an operator cannot tell a refused node from one whose chain cannot cover the hole.
    EXPECT_NE(std::string::npos, text.find("lastGlobalSeqNo=1")) << text;
    EXPECT_NE(std::string::npos, text.find("awaitingReplay=1")) << text;
    EXPECT_NE(std::string::npos, text.find("replayerUnavailable=1")) << text;
}

TEST(ReplayerRecoveryConvergence, TheReportStopsNamingARefusalOnceTheReplayerServesAgain)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> awaiting a replay
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.recovery.requestId()));
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/900));

    ScopedLoggerSink sink; // installed after the exchange above, so it captures only the report below
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS));
    ASSERT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));

    ASSERT_EQ(1u, sink.events.size());
    const std::string text(sink.events[0].text.data(), sink.events[0].textLen);
    // The field is state, not "was ever refused": a stale 1 points the operator at a Replayer that is
    // answering fine, and away from the attached replay that is actually not delivering.
    EXPECT_NE(std::string::npos, text.find("replayerUnavailable=0")) << text;
}

} // namespace
} // namespace org::limitless::phixeron::replayer::client
