// ReplayerRecovery — the walk/resume/gap decision state machine ReplayerStreamReceiver drives.
//
// Locked here: the contiguity/de-dupe decision and the first-frame-must-be-clusterSeqNo-1 baseline check
// (a cold start answered NO_REPLAY_NEEDED used to adopt an arbitrary
// mid-stream baseline; clusterSeqNo only increases, so once the first frame observed isn't 1 no later one
// ever can be, and it latches a fault the receiver raises from its duty cycle), the Replayer
// control-stream transitions, the retained-ahead FIFO, the walk, the steady-state resume,
// and the two watchdogs.
//
// ReplayerRecovery holds no Aeron runtime and no clock of its own, so this suite drives it directly: its
// transport is recorded through ReplayerRecoveryActions and its clock is owned by the test. The
// receiver's own Aeron side — subscription polling, image attach, and its guarantee to always drain AND
// dispatch the queue regardless of state — has no seam here and is exercised by
// GapRecoveryTest.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/helpers/TempDirectory.hpp"
#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/replayer/client/RestoreFrames.hpp"
#include "org/limitless/seqeron/replayer/client/detail/ReplayerRecovery.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"
#include "org_limitless_seqeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_seqeron_sbe_frame/LeadershipChanged.h"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/Sequenced.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"
#include "org_limitless_seqeron_sbe_replay/Replaying.h"

namespace org::limitless::seqeron::replayer::client::detail {
namespace {

namespace frm = org::limitless::seqeron::sbe::frame;
namespace diag = org::limitless::seqeron::util;
namespace rpl = org::limitless::seqeron::sbe::replay;
using protocol::REPLAYER_FROM_START;
using protocol::REPLAYER_NO_CUT;
using protocol::REPLAYER_NO_REPLAY_NEEDED;
using protocol::SequencedEvent;

// Each owns raw retained blocks or hands its own address out, so a copy or a move would double-free or dangle.
static_assert(!std::is_copy_constructible_v<RetainBlockPool> && !std::is_move_constructible_v<RetainBlockPool>);
static_assert(!std::is_copy_constructible_v<ReplayerRecovery> && !std::is_move_constructible_v<ReplayerRecovery>);
static_assert(!std::is_copy_constructible_v<ReplayerStreamReceiver> &&
              !std::is_move_constructible_v<ReplayerStreamReceiver>);

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

// Retained-buffer overflows only: every gap/resume warn shares eventCode::QueueGap, so only the text
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

constexpr std::int64_t CLIENT_ID = 1;

// Stands in for the wall clock. The absolute value is arbitrary; only the deltas tests apply matter.
constexpr std::int64_t CLOCK_MS = 3 * 60 * 60 * 1000;

// Past ReplayerRecovery's RESEND_INTERVAL_MS (500) and REPLAY_STALL_TIMEOUT_MS (5000) alike, so one duty
// cycle fires whichever of the two the state under test has armed.
constexpr std::int64_t PAST_EVERY_TIMER_MS = 6'000;

// A ReplayerRecovery with its transport recorded rather than performed, and a clock the test owns.
struct Client final : ReplayerRecoveryActions
{
    explicit Client(ReplayerRecovery::OnSequenced onSequenced, ReplayerRecovery::OnCaughtUp onCaughtUp = {},
                    ReplayerRecovery::OnLeadershipChanged onLeadershipChanged = {}) :
      recovery(*this, std::move(onSequenced), {}, {}, std::move(onLeadershipChanged), std::move(onCaughtUp))
    {}

    void sendReplayRequest(std::int64_t, std::int64_t, const std::int64_t cutClusterSeqNo) override
    {
        ++requestsSent;
        lastCutClusterSeqNo = cutClusterSeqNo;
    }

    void openReplay(std::int64_t) override
    {}

    void closeReplay() override
    {}

    void recoveryStalled(const bool stalled) override
    {
        stalledGauge.push_back(stalled);
    }

    std::int64_t clientId() override
    {
        return CLIENT_ID;
    }

    std::int64_t nowMs() override
    {
        return clockMs;
    }

    std::int64_t clockMs = CLOCK_MS;
    int requestsSent = 0;
    std::int64_t lastCutClusterSeqNo = -2;
    std::vector<bool> stalledGauge;

    ReplayerRecovery recovery;
};

// One frame carrying a core ClusterHeartbeat payload (template id 16) at the given clusterSeqNo and groupSeqNo — clear
// of the ConnectionOpened/ConnectionClosed/LeadershipChanged special ids (1/2/5), so it always reaches
// onSequenced rather than being intercepted as a lifecycle/leadership event. The twin of the Java
// ReplayerRecoveryTest's heartbeatBuffer; keep the two in step.
std::vector<std::uint8_t> encodeHeartbeat(const std::int64_t clusterSeqNo, const std::int64_t groupSeqNo)
{
    std::vector<std::uint8_t> buf(256, 0);
    frm::ClusterHeartbeat frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    frame.header()
        .sourceId(1)
        .connectionId(0)
        .sessionId(0)
        .systemEventType(protocol::CLUSTER_HEARTBEAT)
        .clusterSeqNo(clusterSeqNo)
        .timestamp(clusterSeqNo * 1000)
        .groupSeqNo(groupSeqNo);
    buf.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return buf;
}

// The same on a queue that carries every frame, so its groupSeqNo is its clusterSeqNo.
std::vector<std::uint8_t> encodeHeartbeat(const std::int64_t clusterSeqNo)
{
    return encodeHeartbeat(clusterSeqNo, clusterSeqNo);
}

// Feeds one Heartbeat frame in exactly as the receiver would off the live queue. framePosition is where
// the frame starts in the recording — the default 0 suffices wherever a test does not care, but
// requestResume anchors on the last dispatched frame's position, so a test that checks the anchor must
// space its frames apart.
void deliverLive(Client& client, const std::int64_t clusterSeqNo, const std::int64_t framePosition = 0)
{
    auto buf = encodeHeartbeat(clusterSeqNo);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), framePosition, /*receiveNs=*/0,
                            /*fromReplay=*/false);
}

// A live queue frame too big for the retained-ahead FIFO to hold: a Heartbeat zero-padded past
// RetainBlock::SIZE, which retainFrame refuses outright (recordSize > RetainBlock::SIZE). The
// cheapest of its three overflow triggers to drive — the other two need 65536 frames or 16 MiB.
void deliverLiveTooBigToRetain(Client& client, const std::int64_t clusterSeqNo)
{
    auto buf = encodeHeartbeat(clusterSeqNo);
    buf.resize(RetainBlock::SIZE, 0); // decoded from the front; the padding only has to make the record oversized
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/false);
}

// The largest frame the queue carries (§12): a full payload behind the 44-byte sequenced envelope.
constexpr std::size_t LARGEST_FRAME_LENGTH = 44 + protocol::MAX_PAYLOAD_LENGTH;

// A live queue frame of `length` bytes: a Heartbeat, decoded from the front, zero-padded to it.
void deliverLivePadded(Client& client, const std::int64_t clusterSeqNo, const std::size_t length)
{
    auto buf = encodeHeartbeat(clusterSeqNo);
    buf.resize(length, 0);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/false);
}

// One Heartbeat frame from a replay image (fromReplay=true), otherwise identical to deliverLive.
void deliverReplay(Client& client, const std::int64_t clusterSeqNo)
{
    auto buf = encodeHeartbeat(clusterSeqNo);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/true);
}

// A frame at groupSeqNo on a queue that does not carry every frame, starting at its 1024s.
void deliverOnQueue(Client& client, const std::int64_t clusterSeqNo, const std::int64_t groupSeqNo,
                    const bool fromReplay)
{
    auto buf = encodeHeartbeat(clusterSeqNo, groupSeqNo);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), groupSeqNo * 1024, /*receiveNs=*/0,
                            fromReplay);
}

// requestId must be the client's CURRENT one (recovery.requestId()) for a reply to be acted on — see
// onControl's correlation check; pass a different value to build the stale reply a resend leaves behind.
std::vector<std::uint8_t> encodeReplaying(const std::int64_t clientId, const std::int64_t requestId,
                                          const std::int64_t replaySessionId, const std::int64_t catchUpPosition)
{
    std::vector<std::uint8_t> buf(64, 0);
    rpl::Replaying enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId).replaySessionId(replaySessionId).catchUpPosition(catchUpPosition);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayPending(const std::int64_t clientId, const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    rpl::ReplayPending enc;
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

// The replay image reaching the bound the Replayer gave it — how a replay completes, since a bounded
// replay of an active recording never closes its image at the bound.
void completeReplay(Client& client)
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

TEST(ReplayerRecoveryBaseline, ColdStartWalksTheRecordingFromItsStart)
{
    Client client{ [](const SequencedEvent&) {} };

    client.recovery.start();

    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(REPLAYER_FROM_START, client.recovery.requestFromPosition());
}

TEST(ReplayerRecoveryBaseline, FirstFrameAtClusterSeqNoOneIsAccepted)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);

    EXPECT_EQ(1, delivered);
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryBaseline, FirstFrameNotAtClusterSeqNoOneIsAFault)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 57);

    ASSERT_TRUE(client.recovery.fault().has_value()) << "must not silently adopt a mid-stream baseline";
    EXPECT_NE(std::string::npos, client.recovery.fault()->find("groupSeqNo=57, expected 1"));
    EXPECT_EQ(0, delivered);
}

// Coverage for the gap-recovery/re-walk state machine (the "gap re-walk starves the queue it is
// recovering" fault, fixed 2026-07-31): mid-stream gap detection, de-dup of already-seen replayed frames, the
// Replayer control-stream transitions (Replaying / NO_REPLAY_NEEDED / ReplayPending), and the
// walk ending at its bound — the contiguity/de-dupe decision, the isRecovering()
// predicate that separates a real hole from the queue running ahead of an in-flight walk, and the
// isCaughtUp() state transitions consumers gate on.

// Companion to ReplayerRecoveryBaseline.FirstFrameNotAtClusterSeqNoOneIsAFault, and the
// regression the 2026-08-05 seam fix first introduced: once the queue is dispatched mid-walk, a cold start
// races a live queue already carrying mid-stream clusterSeqNos against a replay that has not delivered 1
// yet. That queue frame is neither a valid baseline nor a fault — only the walk may establish the
// baseline. (Caught by GapRecoveryTest, where every consumer aborted at startup with "first frame
// observed has clusterSeqNo=15"; this locks it where it costs a millisecond instead of a cluster.)
TEST(ReplayerRecoveryBaseline, LiveQueueMayNotEstablishTheBaselineWhileTheColdStartWalkIsInFlight)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    // What start() does: request the walk before any frame has been seen.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_TRUE(client.recovery.isRecovering());

    deliverLive(client, 15); // the live tip, far ahead of a replay that has not started delivering

    EXPECT_EQ(0, delivered) << "must not adopt the live queue's mid-stream baseline";
    EXPECT_FALSE(client.recovery.isCaughtUp());

    deliverReplay(client, 1); // the walk supplies the real baseline

    EXPECT_EQ(1, delivered);
}

// ── A queue's continuity: groupSeqNo, across jumps in clusterSeqNo (spec §14 row 12) ─────────────────────────

TEST(ReplayerRecoveryContinuity, AFirstFrameAtGroupSeqNoOneIsTheBaselineWhateverItsClusterSeqNo)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };

    deliverOnQueue(client, 40, 1, /*fromReplay=*/false);

    EXPECT_EQ((std::vector<std::int64_t>{ 40 }), dispatched);
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryContinuity, AFirstFrameNotAtGroupSeqNoOneIsAFaultEvenAtClusterSeqNoOne)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), 11, 4096));

    deliverOnQueue(client, 1, 2, /*fromReplay=*/true);

    ASSERT_TRUE(client.recovery.fault().has_value());
    EXPECT_NE(std::string::npos, client.recovery.fault()->find("groupSeqNo=2, expected 1"));
    EXPECT_EQ(0, delivered);
}

TEST(ReplayerRecoveryContinuity, ContinuityFollowsGroupSeqNoSoAJumpInClusterSeqNoIsNoGap)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };

    deliverOnQueue(client, 1, 1, false);
    deliverOnQueue(client, 5, 2, false);
    deliverOnQueue(client, 9, 3, false);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 5, 9 }), dispatched);
    EXPECT_TRUE(client.recovery.isCaughtUp());
    EXPECT_EQ(0, client.requestsSent) << "nothing to recover";
}

TEST(ReplayerRecoveryContinuity, AMissingGroupSeqNoIsAGapResumedAtTheLastDispatchedFrameAndDrainedInOrder)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };

    deliverOnQueue(client, 1, 1, false);
    deliverOnQueue(client, 9, 3, false);

    EXPECT_FALSE(client.recovery.isCaughtUp());
    EXPECT_EQ(1024, client.recovery.requestFromPosition()) << "resumes at frame 1, where it starts";

    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), 21, 3 * 1024));
    deliverOnQueue(client, 1, 1, true); // the anchor, already dispatched
    deliverOnQueue(client, 5, 2, true);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 5, 9 }), dispatched);
}

TEST(ReplayerRecoveryContinuity, AFrameWhoseClusterSeqNoIsAlreadyPassedIsDropped)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };

    deliverOnQueue(client, 1, 1, false);
    deliverOnQueue(client, 5, 2, false);
    deliverOnQueue(client, 5, 2, false);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 5 }), dispatched);
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

TEST(ReplayerRecoveryGapRecovery, QueueGapReportsAStructuredDiagnosticEvent)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 2);
    deliverLive(client, 5); // skips 3, 4 -> gap

    ASSERT_EQ(1u, sink.events.size()) << "exactly the one gap detected above, nothing from setup";
    const auto& event = sink.events[0];
    EXPECT_EQ(diag::component::ReplayerStreamReceiver, event.component);
    EXPECT_EQ(diag::Severity::Warn, event.severity);
    EXPECT_EQ(diag::eventCode::QueueGap, event.code);
    const std::string text(event.text.data(), event.textLen);
    EXPECT_EQ("queue gap: expected groupSeqNo=3 got 5 — resuming the recording at clusterSeqNo=2", text);
}

TEST(ReplayerRecoveryGapRecovery, ALiveQueueFrameAheadOfAnInFlightWalkIsRetainedNotTreatedAsAGap)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/11,
                                           /*catchUpPosition=*/4096));
    const std::int64_t requestIdBefore = client.recovery.requestId();

    deliverLive(client, 9); // the queue runs ahead of the replay

    EXPECT_TRUE(dispatched.empty()) << "no baseline yet — only the walk may establish one";
    EXPECT_EQ(1u, client.recovery.retainedFrameCount());
    EXPECT_EQ(requestIdBefore, client.recovery.requestId()) << "an in-flight walk must not be superseded";
}

TEST(ReplayerRecoveryGapRecovery, AQueueGapWhileTheWalkIsStillInFlightDoesNotSupersedeIt)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/11,
                                           /*catchUpPosition=*/4096));
    deliverReplay(client, 1); // baseline established, by the walk rather than by the queue
    const std::int64_t requestIdBefore = client.recovery.requestId();

    deliverLive(client, 5); // 2..4 are not lost — they are frames this walk has not reached yet

    EXPECT_EQ(requestIdBefore, client.recovery.requestId()) << "an in-flight walk must run to completion";
    EXPECT_EQ(REPLAYER_FROM_START, client.recovery.requestFromPosition())
        << "a mid-walk queue gap is expected, not a new gap";
    EXPECT_EQ(1u, client.recovery.retainedFrameCount());
}

// A queue frame that is non-contiguous only because the queue runs ahead of an in-flight replay must not be
// mistaken for a new gap. Now that poll() dispatches the queue mid-walk instead of discarding it (the
// 2026-08-05 seam fix), this is the common case, not a rarity: every queue frame arriving while a walk
// replays history reads as "ahead". Superseding the walk on each one would restart it from the start
// forever under any sustained publish rate.
TEST(ReplayerRecoveryGapRecovery, AQueueFrameArrivingDuringAnAssignedReplaySessionDoesNotSupersedeIt)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> requestResume(), awaiting
    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    ASSERT_EQ(1u, sink.events.size()) << "the genuine steady-state gap";

    // The Replayer answers with an active session: now mid-walk (m_replaySessionId >= 0), not merely
    // awaiting an answer. (The e2e version of this scenario — re-arming a live-queue drop while a real walk
    // is in flight — was attempted and abandoned as impractical: local Aeron IPC replay of a small gap
    // completes too fast for a shell-level poll-then-signal loop to reliably land inside the window; see
    // the 2026-08-02 investigation. This state transition is locked down here instead.)
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/99,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(99, client.recovery.replaySessionId());
    ASSERT_FALSE(client.recovery.isAwaitingReplay());
    ASSERT_TRUE(client.recovery.isRecovering());

    // A later frame arrives on the live queue while that walk is still riding history. m_lastClusterSeqNo is
    // still 1, so it is non-contiguous — but it is the queue running ahead, not a hole the walk won't cover.
    deliverLive(client, 12);

    EXPECT_EQ(99, client.recovery.replaySessionId()) << "the in-flight walk must run to completion, not be restarted";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
    EXPECT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition())
        << "still the resume the gap asked for, not restarted";
    EXPECT_EQ(1, delivered) << "the out-of-order frame itself is still withheld";
    EXPECT_EQ(1u, sink.events.size()) << "and it is not reported as a second gap";
}

// The replay->live seam: the queue is dispatched throughout a walk, so the frame at
// gseq == last+1 lands the instant the replay reaches it. Before the fix poll() routed queue frames to a
// discard handler while recovering, consuming exactly these frames — so every walk ended one guaranteed
// gap short of live and re-walked the whole recording, converging only if nothing was published during the
// final round trip. NOTE: poll()'s handler *routing* needs a live queue subscription and is exercised only
// by GapRecoveryTest; what is locked here is the decision logic it feeds.
TEST(ReplayerRecoveryGapRecovery, LiveQueueFrameAtTheSeamIsDispatchedWhileTheWalkIsStillInFlight)
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

    deliverLive(client, 3); // the seam: first queue frame contiguous with what the replay delivered

    EXPECT_EQ(3, delivered) << "the seam frame must be dispatched, not discarded as mid-walk noise";
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "a contiguous frame off the live queue means we are following live";
}

// A steady-state gap resumes the recording at the frame last dispatched, so repairing a
// dropped frame costs a replay of the hole rather than of the whole log.
TEST(ReplayerRecoveryGapRecovery, SteadyStateGapResumesAtTheLastDispatchedFrame)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));
    ASSERT_TRUE(client.recovery.isCaughtUp());

    deliverLive(client, 1, /*framePosition=*/0);
    deliverLive(client, 2, /*framePosition=*/1024); // the frame the resume must anchor on
    deliverLive(client, 5, /*framePosition=*/2048); // steady-state gap

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
    EXPECT_EQ(2, delivered) << "replayed frames already delivered off the live queue must be de-duped";

    deliverReplay(client, 3); // new -> delivered
    EXPECT_EQ(3, delivered);
}

TEST(ReplayerRecoveryGapRecovery, ReplayedHistoryDispatchesInOrderAndDeDupesARedeliveredFrame)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/11,
                                           /*catchUpPosition=*/4096));

    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverReplay(client, 2); // redelivered
    deliverReplay(client, 3);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3 }), dispatched);
    EXPECT_EQ(3, client.recovery.lastClusterSeqNo());
}

TEST(ReplayerRecoveryGapRecovery, NothingToReplayFromTheStartMarksCaughtUp)
{
    Client client{ [](const SequencedEvent&) {} };

    ASSERT_FALSE(client.recovery.isCaughtUp());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "NO_REPLAY_NEEDED for a walk means the recording holds nothing yet";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
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

TEST(ReplayerRecoveryGapRecovery, AWalkReachingItsBoundCatchesUp)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.recovery.replaySessionId());

    completeReplay(client);

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "the recording held the whole log when the request was served";
    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "there is nothing further to ask for";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
}

// ── Request/reply correlation ────────────────────────────────────────────────────────────────
// clientId alone cannot identify WHICH request a reply answers. A new request makes the Replayer stop the
// in-flight session and start a new one, so both replies sit in order on the one shared control
// publication and the stale one is always processed first. Acting on it attached the client to a
// session that no longer exists — and, because onControl cleared m_awaitingReplay unconditionally, also
// stopped the resend timer, leaving the client with no session, no image and no retry.
TEST(ReplayerRecoveryGapRecovery, ReplyForASupersededRequestIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    const std::int64_t stale = client.recovery.requestId();

    client.recovery.start(); // a request, and with it a new requestId
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
    client.recovery.start();

    deliverControl(client, encodeReplaying(/*clientId=*/1, stale, /*replaySessionId=*/42, /*catchUpPosition=*/900));
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/43,
                                           /*catchUpPosition=*/1'000));

    EXPECT_EQ(43, client.recovery.replaySessionId()) << "the reply to the current request must be applied";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
}

// Resends queued behind a slow answer each draw it again, under the same requestId.
TEST(ReplayerRecoveryGapRecovery, ARepeatOfTheAnswerAlreadyTakenIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    client.recovery.start();
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/43,
                                           /*catchUpPosition=*/1'000));

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/44,
                                           /*catchUpPosition=*/2'000));

    EXPECT_EQ(43, client.recovery.replaySessionId()) << "the replay it attached to is kept";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
}

// A stale ReplayPending must not reset the request clock either: that is what paces the resend, so
// honouring a superseded request's "wait" would defer the retry the client is relying on.
TEST(ReplayerRecoveryGapRecovery, ReplayPendingForASupersededRequestIsIgnored)
{
    Client client{ [](const SequencedEvent&) {} };
    const std::int64_t stale = client.recovery.requestId();
    client.recovery.start(); // a new request, whose resend deadline is what matters
    const int sends = client.requestsSent;

    client.clockMs += 300; // still inside the current request's resend interval
    deliverControl(client, encodeReplayPending(/*clientId=*/1, stale));
    client.clockMs += 300; // ... which has now elapsed, unless the stale "wait" pushed it out
    client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ(sends + 1, client.requestsSent) << "a superseded request's ReplayPending must not "
                                                 "push out the current request's resend deadline";
}

// ── Closed image vs. completed replay ────────────────────────────────────────────────────────
// A replay whose recording stopped closes its image on its own, exactly at the position it was bounded
// to — the one close that means "replay done". Any other close (supersede, archive fault, Replayer shutdown)
// leaves the image short of the bound. Completing there skips history that was never replayed — a silent
// hole, since only a later queue gap could ever expose it.
TEST(ReplayerRecoveryGapRecovery, AReplayImageClosingAtItsBoundCompletesTheReplayShortOfItReRequests)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    const std::int64_t requestId = client.recovery.requestId();

    client.recovery.onReplayImageClosed(/*finalPosition=*/312); // stopped under us, mid-replay

    EXPECT_EQ(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "the walk is asked for again";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "a partial replay is not taken for a complete one";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_NE(requestId, client.recovery.requestId()) << "re-requesting is a new request, so the reply to the "
                                                         "stopped one is recognisably stale";
    EXPECT_EQ(1u, sink.events.size()) << "a truncated replay is reported, not silently absorbed";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    client.recovery.onReplayImageClosed(/*finalPosition=*/500);

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "closing AT the bound is completion";
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
}

// A replay that neither closes nor advances is the third case, and the one nothing used to catch: once
// Replaying arrives the resend timer is disarmed (m_awaitingReplay is false), and a bounded replay of an
// active recording never closes its image, so an image that simply stops — the archive faulted, the
// publication wedged — left the client waiting on it forever. Measured before the fix: a cold start past
// ~32 MiB of history hung permanently with the image attached, open, and frozen.
TEST(ReplayerRecoveryGapRecovery, ReplayThatStopsAdvancingReRequestsIt)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    const std::int64_t requestId = client.recovery.requestId();
    ASSERT_FALSE(client.recovery.isAwaitingReplay()) << "a session was assigned, so nothing else is retrying";

    advancePastTimers(client);

    EXPECT_EQ(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "the walk is asked for again";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "a stalled replay is not taken for a complete one";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_NE(requestId, client.recovery.requestId()) << "re-requesting is a new request, so the reply to the "
                                                         "stalled one is recognisably stale";
    EXPECT_EQ(1u, sink.events.size()) << "a stalled replay is reported, not silently absorbed";
}

TEST(ReplayerRecoveryGapRecovery, AWalkThatStallsAfterDispatchingResumesAtItsLastFrame)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), 7, 8192));
    deliverOnQueue(client, 1, 1, /*fromReplay=*/true);
    deliverOnQueue(client, 2, 2, /*fromReplay=*/true);

    advancePastTimers(client);

    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(2 * 1024, client.recovery.requestFromPosition()) << "resumed at the last dispatched frame";
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), 8, 8192));
    deliverOnQueue(client, 2, 2, /*fromReplay=*/true); // the anchor, already dispatched
    deliverOnQueue(client, 3, 3, /*fromReplay=*/true);
    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3 }), dispatched);
    EXPECT_FALSE(client.recovery.fault().has_value());
}

// Session id 0 is an ordinary archive replaySessionId; -1 is the only value that means "no replay".
// doTimers testing >= 1 skipped the stall watchdog for it once it stopped delivering. The Java twin tests
// >= 0, as isRecovering() and the receiver's poll() do here.
TEST(ReplayerRecoveryGapRecovery, ReplaySessionZeroIsWatchdoggedLikeAnyOther)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/0,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(0, client.recovery.replaySessionId()) << "session 0 is a replay this client is riding, not the "
                                                       "absence of one";

    advancePastTimers(client);

    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "its stall watchdog must re-request it";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
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

TEST(ReplayerRecoveryGapRecovery, StuckAwaitingReplayResendsAfterTheIntervalElapses)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> requestReplay(0, 0), arming the resend timer
    ASSERT_TRUE(client.recovery.isAwaitingReplay());
    const int sends = client.requestsSent;
    const std::int64_t requestId = client.recovery.requestId();

    client.clockMs += 300; // inside RESEND_INTERVAL_MS (500)
    client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_EQ(sends, client.requestsSent) << "must not resend before RESEND_INTERVAL_MS elapses";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());

    client.clockMs += 300; // 600ms since the request — past it
    client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ(sends + 1, client.requestsSent)
        << "the Replayer never answered -> the duty cycle must re-send the same request rather than give up";
    EXPECT_EQ(requestId, client.recovery.requestId()) << "a resend keeps its requestId, so its reply is still current";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "still stuck at the gap — a resend, not a new state";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition())
        << "resend repeats the same request verbatim, still the resume";
}

// An unsent request must NOT be retried per duty cycle, only on the resend timer. onControl acts only on a
// reply carrying the CURRENT id, so a retry that took a new id per poll would run the counter away and
// discard every reply that arrives as stale — leaving the client awaiting a replay
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

// isRecovering() is what separates "the queue is ahead of my replay" from "there is a hole" — narrowing it
// to miss a state means a walk supersedes itself on its own in-flight frames (see
// AQueueGapWhileTheWalkIsStillInFlightDoesNotSupersedeIt), widening it means a real gap goes unreported. This
// walks every transition it must track. (It also guarded the discard routing behind the
// 2026-07-31 "gap re-walk starves the queue it is recovering" fault; that discard is gone as of 2026-08-05 —
// poll() now always drains AND dispatches the queue — but the predicate itself carries more weight, not
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
    completeReplay(client);
    EXPECT_FALSE(client.recovery.isRecovering()) << "a resume ends at its bound";

    // A resume whose bound leaves a retained frame stranded behind a hole resumes again
    // (AWalkReachingItsBoundWithARetainedHoleStillOpenResumesInstead).
    deliverLive(client, 9); // another gap -> another resume, anchored on frame 5
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 5);
    deliverReplay(client, 6);
    completeReplay(client); // 7 and 8 not covered: 9 is still stranded
    EXPECT_TRUE(client.recovery.isRecovering()) << "awaiting the next resume";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/9,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 6); // anchored on frame 6, the last dispatched
    deliverReplay(client, 7);
    deliverReplay(client, 8); // dispatching 8 drains the retained 9 straight over
    EXPECT_TRUE(client.recovery.isRecovering()) << "mid-resume until the bound";
    completeReplay(client);
    EXPECT_FALSE(client.recovery.isRecovering()) << "that resume ends at its bound too";
}

// isCaughtUp() is a state, not a latch. Consumers gate real decisions on it — LeaderGate's
// leader-only emission, and a gateway's queue-stall watchdog, which measures silence in DISPATCHED
// frames and fences the gateway after 20s. A resume dispatches
// nothing until the replay passes the hole, so leaving it latched made the gateway diagnose its own
// recovery as a stalled sequencer and force a standby promotion it did not need.
TEST(ReplayerRecoveryGapRecovery, QueueGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain)
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

    deliverReplay(client, 4); // closes the hole; the retained queue frame 5 then hands straight over

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "the retained frame closes the seam with no further round trip";
    EXPECT_EQ(2, caughtUpNotifications) << "consumers must be able to re-arm on re-convergence, not just "
                                           "on first catch-up";
}

// An evicted subscriber hears nothing until its driver rejoins it at the tip, so the hole shows only then.
TEST(ReplayerRecoveryGapRecovery, AnEvictionFromTheQueueRevokesCaughtUpUntilALiveFrameAfterTheRejoin)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };
    client.recovery.onQueueImage(true);
    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.recovery.isCaughtUp());

    client.recovery.onQueueImage(false);
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "nothing live reaches an evicted client";
    EXPECT_EQ(0, client.requestsSent) << "the hole shows on the first live frame after the rejoin";

    client.recovery.onQueueImage(true);
    deliverLive(client, 5);
    EXPECT_FALSE(client.recovery.isCaughtUp());
    EXPECT_EQ(1, client.requestsSent) << "the rejoin's first frame shows the hole, and a resume is requested";

    deliverReplay(client, 2);
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    EXPECT_TRUE(client.recovery.isCaughtUp());
    EXPECT_EQ(2, caughtUpNotifications);
}

TEST(ReplayerRecoveryGapRecovery, AWalkReachingItsBoundWhileEvictedFromTheQueueIsNotCaughtUp)
{
    Client client{ [](const SequencedEvent&) {} };
    client.recovery.onQueueImage(true);
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/3 * 1024));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    client.recovery.onQueueImage(false);

    completeReplay(client);
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "the queue may be any distance past the bound";
    EXPECT_FALSE(client.recovery.isRecovering()) << "nothing is requested before the rejoin";

    client.recovery.onQueueImage(true);
    deliverLive(client, 3);
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "a contiguous live frame after the rejoin closes the seam";
}

TEST(ReplayerRecoveryGapRecovery, FramesRetainedBeforeAnEvictionDoNotCatchTheClientUp)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    client.recovery.onQueueImage(true);
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/11,
                                           /*catchUpPosition=*/8 * 1024));
    deliverReplay(client, 1);
    deliverLive(client, 3); // retained behind the hole at 2
    client.recovery.onQueueImage(false);

    deliverReplay(client, 2); // hands 3 straight over
    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3 }), dispatched);
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "a retained frame is not the live queue";

    client.recovery.onQueueImage(true);
    deliverLive(client, 4);
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryGapRecovery, RetainedQueueFramesHandStraightOverWhenTheReplayReachesThem)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), /*replaySessionId=*/11,
                                           /*catchUpPosition=*/4096));
    deliverLive(client, 3);
    deliverLive(client, 4);

    deliverReplay(client, 1);
    deliverReplay(client, 2);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3, 4 }), dispatched) << "the seam closes with no residual";
    EXPECT_EQ(0u, client.recovery.retainedFrameCount());
}

// The retention half of the seam fix, stated on its own: a frame from beyond the hole is
// held and delivered exactly once when the hole closes — never dropped, never duplicated, always in
// clusterSeqNo order. Dropping these is what made every walk end one hole short of live and re-walk.
TEST(ReplayerRecoveryGapRecovery, FramesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses)
{
    std::vector<std::int64_t> delivered;
    Client client{ [&](const SequencedEvent& event) { delivered.push_back(event.clusterSeqNo); } };

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
        << "retained frames must drain in order, exactly once, with no further resume needed";
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryGapRecovery, LargestFrameAheadOfTheHoleIsRetained)
{
    ScopedLoggerSink sink;
    std::vector<std::int64_t> delivered;
    Client client{ [&](const SequencedEvent& event) { delivered.push_back(event.clusterSeqNo); } };

    deliverLive(client, 1);
    deliverLivePadded(client, 3, LARGEST_FRAME_LENGTH); // gap -> retained
    ASSERT_EQ(0U, overflowReports(sink));

    deliverReplay(client, 1); // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2); // closes the hole -> 3 drains behind it

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3 }), delivered);
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "an overflow would have forced a resume";
}

TEST(ReplayerRecoveryGapRecovery, FramesAheadOfTheHoleAreRetainedUpToTheFrameCapAndTheNextIsDropped)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    ScopedLoggerSink sink;
    const auto last = static_cast<std::int64_t>(2 + ReplayerRecovery::MAX_RETAINED_FRAMES); // 2 is the hole
    for (std::int64_t clusterSeqNo = 3; clusterSeqNo <= last; ++clusterSeqNo)
    {
        deliverLive(client, clusterSeqNo);
    }
    ASSERT_EQ(ReplayerRecovery::MAX_RETAINED_FRAMES, client.recovery.retainedFrameCount());
    ASSERT_EQ(0u, overflowReports(sink));

    deliverLive(client, last + 1);

    EXPECT_EQ(ReplayerRecovery::MAX_RETAINED_FRAMES, client.recovery.retainedFrameCount()) << "dropped, not retained";
    EXPECT_EQ(1u, overflowReports(sink));
}

TEST(ReplayerRecoveryGapRecovery, FramesOfTheLargestPayloadAreRetainedUpToTheByteCapAndTheNextIsDropped)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    ScopedLoggerSink sink;
    const std::size_t fit = ReplayerRecovery::MAX_RETAINED_BYTES / LARGEST_FRAME_LENGTH;
    const auto last = static_cast<std::int64_t>(2 + fit); // 2 is the hole
    for (std::int64_t clusterSeqNo = 3; clusterSeqNo <= last; ++clusterSeqNo)
    {
        deliverLivePadded(client, clusterSeqNo, LARGEST_FRAME_LENGTH);
    }
    ASSERT_EQ(fit, client.recovery.retainedFrameCount());
    ASSERT_EQ(0u, overflowReports(sink));

    deliverLivePadded(client, last + 1, LARGEST_FRAME_LENGTH);

    EXPECT_EQ(fit, client.recovery.retainedFrameCount()) << "dropped, not retained";
    EXPECT_EQ(1u, overflowReports(sink));
}

// Retained frames the replay has meanwhile covered are discarded on drain rather than double-delivered —
// the ranges legitimately overlap, since a replay restarts behind the hole while the queue keeps arriving.
TEST(ReplayerRecoveryGapRecovery, RetainedFramesAlreadyCoveredByTheReplayAreNotRedelivered)
{
    std::vector<std::int64_t> delivered;
    Client client{ [&](const SequencedEvent& event) { delivered.push_back(event.clusterSeqNo); } };

    deliverLive(client, 1);
    deliverLive(client, 3); // gap -> retained
    deliverLive(client, 4); // retained

    deliverReplay(client, 1); // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2);
    deliverReplay(client, 3); // the replay covers a frame the queue already retained
    deliverReplay(client, 4); // and another

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2, 3, 4 }), delivered) << "each clusterSeqNo dispatched exactly once";
}

// A hole in REPLAYED history was the one invariant violation that produced no log and no counter:
// the frame is dropped, the walk rides on, and recovery quietly never converges.
// Nothing unsafe follows — contiguity still holds — so this reports rather than aborts, once per
// episode, since the walk retries against the same recording every 500ms.
TEST(ReplayerRecoveryGapRecovery, GapInReplayedHistoryIsReportedOncePerEpisode)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverReplay(client, 1);
    ASSERT_EQ(1, delivered);

    ScopedLoggerSink sink;    // installed after the baseline, so it captures only the hole below
    deliverReplay(client, 7); // the recording does not cover 2..6

    EXPECT_EQ(1, delivered) << "the frame past the hole must not be dispatched";
    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Severity::Warn, sink.events[0].severity);
    EXPECT_EQ(diag::eventCode::QueueGap, sink.events[0].code);

    deliverReplay(client, 8);
    EXPECT_EQ(1u, sink.events.size()) << "the walk retries against the same recording — report the episode, not "
                                         "every frame";

    // A later, distinct episode must be reported again rather than swallowed by the latch.
    deliverReplay(client, 2);
    ASSERT_EQ(2, delivered);
    deliverReplay(client, 9);
    EXPECT_EQ(2u, sink.events.size()) << "the latch clears once history goes contiguous again";
}

TEST(ReplayerRecoveryGapRecovery, AReplayReachingItsBoundPastAHoleResumesInsteadOfReportingCaughtUp)
{
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 7); // the recording does not cover 2..6
    completeReplay(client);

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "frames 2..6 were never dispatched";
    EXPECT_TRUE(client.recovery.isAwaitingReplay()) << "the hole is resumed, not left";
    const int sends = client.requestsSent;
    advancePastTimers(client); // the resend timer sends it, at its pace: a hole every replay repeats is not a hot loop
    EXPECT_EQ(sends + 1, client.requestsSent);

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1); // anchored on the last dispatched frame
    deliverReplay(client, 2);
    deliverReplay(client, 3);
    completeReplay(client);

    EXPECT_EQ(3, delivered);
    EXPECT_TRUE(client.recovery.isCaughtUp());
}

TEST(ReplayerRecoveryGapRecovery, DroppedQueueFramesAreReportedOncePerEpisode)
{
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    deliverLive(client, 3); // gap -> resume anchored on frame 1; 3 is retained

    ScopedLoggerSink sink; // installed after the setup gap, so it captures only the overflow lines
    deliverLiveTooBigToRetain(client, 4);
    deliverLiveTooBigToRetain(client, 5);
    ASSERT_EQ(1u, overflowReports(sink)) << "one episode is one line, not one per dropped frame";

    // The resume is what covers the dropped frames, so the episode ends when it is requested.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    completeReplay(client);
    ASSERT_TRUE(client.recovery.isAwaitingReplay());

    deliverLiveTooBigToRetain(client, 9);

    EXPECT_EQ(2u, overflowReports(sink)) << "a later overflow is a distinct episode and names itself";
}

// The resume's safety check: its first frame must be the frame the position was anchored on. Positions
// follow from the frames and the recording changes only with the node's driver, so a mismatch means the
// recording is not what this client's positions say, and riding it would dispatch an arbitrary frame.
TEST(ReplayerRecoveryGapRecovery, ResumeOpeningOnTheWrongFrameIsFatal)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    Client client{ [&](const SequencedEvent&) { ++delivered; } };

    deliverLive(client, 1);
    deliverLive(client, 5); // gap -> resume anchored on frame 1
    ASSERT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.recovery.replaySessionId());

    deliverReplay(client, 77); // that position is some other frame entirely

    ASSERT_TRUE(client.recovery.fault().has_value());
    EXPECT_NE(std::string::npos, client.recovery.fault()->find("expected 1")) << *client.recovery.fault();
    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "nothing more is asked for";
    EXPECT_EQ(-1, client.recovery.replaySessionId()) << "and it lets go of the replay it was riding";
    EXPECT_EQ(1, delivered) << "the frame it opened on must not be dispatched";
    EXPECT_FALSE(client.recovery.isCaughtUp());
    ASSERT_EQ(2u, sink.events.size()) << "the gap, then the mismatch";
    EXPECT_EQ(diag::Severity::Fault, sink.events[1].severity);
}

// The resume anchor is consumed by the first frame of ONE replay episode — a retry of
// that same logical resume (image closed short of its bound, or stalled) starts a NEW episode and must
// re-arm the check via requestResume(), not resend the bare fromPosition via requestReplay() directly.
// Without it, the retried replay's first frame rides with no anchor to validate against: a frame that
// happens to look contiguous (clusterSeqNo == last + 1) is silently dispatched as if it were the frame the
// resume asked for, instead of being caught as a mismatch.
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
    ASSERT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "still a resume retry, not a walk step";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    // The retried resume's first frame is NOT the frame it was anchored on, but it looks contiguous.
    deliverReplay(client, 2);

    EXPECT_EQ(1, delivered) << "must not silently accept an unanchored frame merely because it looks contiguous";
    EXPECT_TRUE(client.recovery.fault().has_value()) << "the mismatch is fatal";
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
    ASSERT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "still a resume retry, not a walk step";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 2); // looks contiguous but is not the anchored frame

    EXPECT_EQ(1, delivered) << "must not silently accept an unanchored frame merely because it looks contiguous";
    EXPECT_TRUE(client.recovery.fault().has_value()) << "the mismatch is fatal";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_FALSE(client.recovery.isCaughtUp());
}

// For a walk "nothing to replay" means caught up. For a resume the Replayer answers it only when it refuses
// the position; a recording that has not reached it yet is answered ReplayPending instead. Taking it at
// face value would close the open hole by fiat.
TEST(ReplayerRecoveryGapRecovery, NoReplayNeededForAResumeIsFatal)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.recovery.isCaughtUp());
    deliverLive(client, 6); // gap -> resume
    ASSERT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));

    ASSERT_TRUE(client.recovery.fault().has_value());
    EXPECT_NE(std::string::npos, client.recovery.fault()->find("refused a resume")) << *client.recovery.fault();
    EXPECT_FALSE(client.recovery.isAwaitingReplay());
    EXPECT_FALSE(client.recovery.isCaughtUp()) << "the hole above clusterSeqNo=2 is still open";
}

// A resume ends at the bound it was given — so it must declare itself caught up and close its image, which
// frees the Replayer's slot.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundCatchesUpAndRequestsNothingMore)
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
    completeReplay(client);

    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "no follow-up request";
    EXPECT_EQ(-1, client.recovery.replaySessionId());
    EXPECT_TRUE(client.recovery.isCaughtUp()) << "we hold everything the recording had when the request was served";
    EXPECT_EQ(2, caughtUpNotifications) << "re-fired, so consumers re-arm on re-convergence";
}

// The companion defect (review #5): a resume can reach its bound while a retained-ahead frame still
// sits behind a hole the replay never covered — onReplaySegmentComplete used to declare caught up on the
// bound alone, with no check that m_lastClusterSeqNo had actually reached the frontier the retained FIFO
// knows about. That reading is silently wrong: consumers gate real decisions on isCaughtUp() and nothing
// would rediscover the hole until some later, unrelated queue frame happened to expose it.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundWithARetainedHoleStillOpenResumesAgain)
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

    completeReplay(client); // reached the bound it was given, but the hole above 2 is still open

    EXPECT_FALSE(client.recovery.isCaughtUp())
        << "must not declare caught up with a retained frame still behind a hole";
    EXPECT_EQ(1, caughtUpNotifications) << "no false re-convergence notification";
    EXPECT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition())
        << "resumes at the last dispatched frame instead of trusting the bound";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
    EXPECT_EQ(-1, client.recovery.replaySessionId());
}

// The overflow half of the same guard. When retainFrame had to DROP queue frames, the
// frontier is short by frames that are gone from the queue for good, and every retained frame draining
// cleanly says nothing about them. drainRetained used to clear the overflow on its first dispatch —
// before this check could read it — so precisely the run that drained successfully was the one that
// declared itself caught up over the hole.
TEST(ReplayerRecoveryGapRecovery, ResumeReachingItsBoundAfterDroppedQueueFramesResumesAgain)
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

    completeReplay(client);

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "frame 4 was dropped — the drained frontier is not the real one";
    EXPECT_EQ(1, caughtUpNotifications) << "no false re-convergence notification";
    EXPECT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition())
        << "resumes now rather than leaving the hole for a later queue gap";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
}

// ── The end of a walk ───────────────────────────────────────────────────────────────────────
// A walk ends at its bound, and reaching it is not by itself permission to declare caught up: the frontier
// is what the retained-ahead FIFO knows, not what the replay covered.

TEST(ReplayerRecoveryGapRecovery, AWalkReachingItsBoundWithARetainedHoleStillOpenResumesInstead)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    // A walk bounded at clusterSeqNo 2 — while the queue runs ahead and frame 5 is retained behind the hole at 3,4.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverLive(client, 5); // ahead of the hole -> retained, not dropped

    completeReplay(client);

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "must not declare caught up with a retained frame behind a hole";
    EXPECT_EQ(0, caughtUpNotifications) << "no false convergence notification — this opens the accept gate";
    EXPECT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "resumes rather than trusting the bound";
    EXPECT_TRUE(client.recovery.isAwaitingReplay());
}

// The negative control for the guard above: a walk whose retained frames all drained must still finish.
TEST(ReplayerRecoveryGapRecovery, AWalkReachingItsBoundWithEveryRetainedFrameDrainedCatchesUp)
{
    Client client{ [](const SequencedEvent&) {} };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverLive(client, 3);   // ahead of the hole at 2 -> retained
    deliverReplay(client, 2); // closes it, so 3 drains straight over

    completeReplay(client);

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "nothing is outstanding — the guard must not fire here";
    EXPECT_FALSE(client.recovery.isAwaitingReplay()) << "not a spurious resume";
}

// The same guard at the bound, driven through to convergence: the resume must both happen AND be able to
// finish. The overflow is cleared where that resume is requested, which is the only clear point that does
// both — clearing it on drain forgets the drop and catches up over the hole, never clearing it leaves a
// client that can never declare itself caught up again.
TEST(ReplayerRecoveryGapRecovery, AWalkReachingItsBoundAfterDroppedQueueFramesResumesThenCatchesUp)
{
    int caughtUpNotifications = 0;
    Client client{ [](const SequencedEvent&) {}, [&] { ++caughtUpNotifications; } };

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    deliverLiveTooBigToRetain(client, 4); // the queue runs ahead of the walk, and this one is dropped

    completeReplay(client);

    EXPECT_FALSE(client.recovery.isCaughtUp()) << "the walk reached its bound, but a queue frame above it was dropped";
    EXPECT_EQ(0, caughtUpNotifications) << "no false convergence notification — this opens the accept gate";
    ASSERT_NE(REPLAYER_FROM_START, client.recovery.requestFromPosition()) << "resumes rather than trusting the bound";

    // The resume replays what the drop lost, and this time nothing is dropped.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.recovery.requestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/900));
    deliverReplay(client, 2); // the anchor
    deliverReplay(client, 3);
    deliverReplay(client, 4);
    completeReplay(client);

    EXPECT_TRUE(client.recovery.isCaughtUp()) << "a resume that dropped nothing must be able to finish";
    EXPECT_EQ(1, caughtUpNotifications);
}

// ── Convergence alarm ─────────────────────────────────────────────────────────────────────────────────────
// Measured by progress, not elapsed time: a cold start has no time bound. Locked here: an in-order dispatch
// counts as progress, catching up without dispatching does too, the report carries the state telling the
// causes apart, and each episode reports and clears exactly once.

constexpr std::int64_t PAST_DEADLINE_MS = CLOCK_MS + 30'000; // RECOVERY_PROGRESS_TIMEOUT_MS

TEST(ReplayerRecoveryConvergence, RecoveryDeliveringNothingIsReportedOncePerEpisode)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    EXPECT_FALSE(checkProgressAt(client, CLOCK_MS)) << "the first observation only anchors the clock";
    EXPECT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));

    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::component::ReplayerStreamReceiver, sink.events[0].component);
    EXPECT_EQ(diag::Severity::Fault, sink.events[0].severity);
    EXPECT_EQ(diag::eventCode::RecoveryStalled, sink.events[0].code);

    // The condition persists for as long as the archive is broken; the caller logs, so it must not
    // repeat every duty cycle.
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS + 60'000));
    EXPECT_EQ(1u, sink.events.size());
}

TEST(ReplayerRecoveryConvergence, AWalkThatIsStillDeliveringIsNeverReportedHoweverLongItRuns)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };

    // A cold start replaying a long log: slow, but converging one frame at a time. Measuring
    // elapsed time instead of progress is exactly what would fence this.
    for (std::int64_t gseq = 1; gseq <= 20; ++gseq)
    {
        deliverReplay(client, gseq);
        EXPECT_FALSE(checkProgressAt(client, CLOCK_MS + gseq * 60'000)) << "at clusterSeqNo " << gseq;
    }
}

TEST(ReplayerRecoveryConvergence, AGapAfterAHealthyRunStartsItsOwnEpisodeRatherThanInheritingOne)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS)); // an episode anchored during cold start
    deliverLive(client, 1);                          // ... which then converges
    ASSERT_TRUE(client.recovery.isCaughtUp());

    deliverLive(client, 5); // much later, a queue gap: !isCaughtUp() again
    ASSERT_FALSE(client.recovery.isCaughtUp());

    // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk
    // it triggers had any chance to converge.
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS));
    ASSERT_EQ(1u, sink.events.size()) << "the queue-gap warn only — no convergence report";
    EXPECT_EQ(diag::eventCode::QueueGap, sink.events[0].code);
}

TEST(ReplayerRecoveryConvergence, ACaughtUpClientThatSimplyGoesQuietIsNotReported)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverLive(client, 1);
    ASSERT_TRUE(client.recovery.isCaughtUp());

    // A queue that goes silent is the queue-stall fence's business, and it is not a convergence
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

    ScopedLoggerSink sink; // installed after the gap, so it captures only the report below
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS));
    ASSERT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));

    ASSERT_EQ(1u, sink.events.size());
    const std::string text(sink.events[0].text.data(), sink.events[0].textLen);
    // Without these an operator cannot tell an unanswering node from one whose recording cannot cover the hole.
    EXPECT_NE(std::string::npos, text.find("lastClusterSeqNo=1")) << text;
    EXPECT_NE(std::string::npos, text.find("awaitingReplay=1")) << text;
}

TEST(ReplayerRecoveryConvergence, TheStallGaugeClearsOnceOnTheDispatchThatEndsAReportedEpisode)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    deliverReplay(client, 1);
    EXPECT_TRUE(client.stalledGauge.empty()) << "progress with nothing reported has no edge to fall from";

    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS));
    ASSERT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));
    deliverReplay(client, 2); // the reported episode ends here
    deliverReplay(client, 3); // ... and only here

    EXPECT_EQ((std::vector<bool>{ true, false }), client.stalledGauge);
}

TEST(ReplayerRecoveryConvergence, ALaterEpisodeIsReportedAgainRatherThanSwallowedByTheFirst)
{
    ScopedLoggerSink sink;
    Client client{ [](const SequencedEvent&) {} };
    ASSERT_FALSE(checkProgressAt(client, CLOCK_MS));
    ASSERT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS));
    deliverReplay(client, 1); // one frame got through: the walk is slow, not stuck

    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS + 1'000)) << "anchors a fresh episode";
    EXPECT_FALSE(checkProgressAt(client, PAST_DEADLINE_MS + 30'000)) << "rather than inheriting the old clock";
    EXPECT_TRUE(checkProgressAt(client, PAST_DEADLINE_MS + 31'000)) << "a second, distinct episode reports too";
    EXPECT_EQ(2u, sink.events.size());
}

// ── Delivery ─────────────────────────────────────────────────────────────────────────────────────

std::vector<std::uint8_t> encodeLeadershipChanged(const std::int64_t clusterSeqNo)
{
    std::vector<std::uint8_t> buf(256, 0);
    frm::LeadershipChanged frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    frame.header()
        .sourceId(-1)
        .connectionId(-1)
        .sessionId(-1)
        .systemEventType(protocol::LEADERSHIP_CHANGED)
        .clusterSeqNo(clusterSeqNo)
        .timestamp(clusterSeqNo * 1000)
        .groupSeqNo(clusterSeqNo);
    frame.newLeaderMemberId(2).leadershipTermId(3);
    buf.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return buf;
}

void deliverLeadershipChanged(ReplayerRecovery& recovery, const std::int64_t clusterSeqNo)
{
    auto buf = encodeLeadershipChanged(clusterSeqNo);
    recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), clusterSeqNo * 1024, /*receiveNs=*/0,
                     /*fromReplay=*/false);
}

TEST(ReplayerRecoveryDelivery, ALeadershipChangeReachesOnSequencedWhenNoLeadershipCallbackIsGiven)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&dispatched](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
    deliverLeadershipChanged(client.recovery, 1);

    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), dispatched) << "dropped rather than delivered";
}

// A bare ReplayerRecoveryActions: this case needs the leadership callback the Client fixture leaves empty.
struct NoActions final : ReplayerRecoveryActions
{
    void sendReplayRequest(std::int64_t, std::int64_t, std::int64_t) override
    {}
    void openReplay(std::int64_t) override
    {}
    void closeReplay() override
    {}
    void recoveryStalled(bool) override
    {}
    std::int64_t clientId() override
    {
        return CLIENT_ID;
    }
    std::int64_t nowMs() override
    {
        return CLOCK_MS;
    }
};

TEST(ReplayerRecoveryDelivery, ALeadershipChangeWithACallbackGoesToItAlone)
{
    std::vector<std::int64_t> dispatched;
    std::vector<std::int64_t> changes;
    NoActions actions;
    ReplayerRecovery recovery{ actions,
                               [&dispatched](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); },
                               {},
                               {},
                               [&changes](std::int32_t, std::int64_t, std::int64_t clusterSeqNo) {
                                   changes.push_back(clusterSeqNo);
                               } };
    deliverLeadershipChanged(recovery, 1);

    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), changes);
    EXPECT_TRUE(dispatched.empty()) << "delivered twice";
}

TEST(ReplayerRecoveryDelivery, OnSequencedIsRequired)
{
    NoActions actions;
    EXPECT_THROW(ReplayerRecovery(actions, {}), std::invalid_argument);
}

// ── Restoring a snapshot ─────────────────────────────────────────────────────────────────────────

namespace rf = restore_frames;

// The snapshot's cut. Frame n starts at n * 1024, as in the Java twin.
constexpr std::int64_t CUT = 10;

struct RestoreRecorder final : SnapshotRestoreHandler
{
    bool supportsFormatVersion(const std::uint32_t formatVersion) override
    {
        return formatVersion == 1;
    }

    void onSnapshotHeader(const protocol::SnapshotHeader& header) override
    {
        restored.push_back("header " + std::to_string(header.leadershipTermId) + " " +
                           std::to_string(header.leaderMemberId));
    }

    void onSnapshotRecord(const std::span<const std::uint8_t> record, const std::int32_t recordIndex,
                          const std::uint32_t formatVersion) override
    {
        EXPECT_EQ(1U, formatVersion);
        restored.push_back(std::to_string(recordIndex) + " " + std::to_string(rf::valueOf(record.data())));
    }

    std::vector<std::string> restored;
};

// The snapshot every round here holds: the header, term 4 and leader 2, then one record.
const rf::Bytes SNAPSHOT_HEADER = rf::header(4, 2);
const rf::Bytes SNAPSHOT_RECORD = rf::record(7);

// A client restoring from a store of its own, started once the test has written its files.
struct Restoring
{
    Restoring() = default;

    explicit Restoring(ReplayerRecovery::OnLeadershipChanged onLeadershipChanged) :
      client{ [this](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); },
              {},
              std::move(onLeadershipChanged) }
    {}

    void start()
    {
        client.recovery.restoreFrom(store, handler);
        client.recovery.start();
    }

    // This instance's confirmed file of a round, cut at CUT: the header and one record.
    void writeSnapshot(const std::int64_t round)
    {
        rf::write(store, round, CUT, CUT * 1024, 1, { SNAPSHOT_HEADER, SNAPSHOT_RECORD });
    }

    std::filesystem::path file(const std::int64_t round) const
    {
        return directory.path() / (std::to_string(round) + ".snapshot");
    }

    helpers::TempDirectory directory;
    SnapshotStore store{ directory.path() };
    std::vector<std::int64_t> dispatched;
    RestoreRecorder handler;
    Client client{ [this](const SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };
};

void attachRestoreReplay(Client& client, const std::int64_t replaySessionId)
{
    deliverControl(client, encodeReplaying(CLIENT_ID, client.recovery.requestId(), replaySessionId, 64 * 1024));
}

void deliverReplayFrame(Client& client, rf::Bytes frame, const std::int64_t clusterSeqNo)
{
    client.recovery.onFrame(reinterpret_cast<char*>(frame.data()), frame.size(), clusterSeqNo * 1024, /*receiveNs=*/0,
                            /*fromReplay=*/true);
}

// The replay of the cut opens on the round's SnapshotStarted, as the file recorded it.
void replayTheCut(Client& client)
{
    attachRestoreReplay(client, 21);
    deliverReplayFrame(client, rf::started(CUT, 2), CUT);
}

rf::Bytes readFile(const std::filesystem::path& path)
{
    std::ifstream in(path, std::ios::binary);
    return rf::Bytes(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void writeFile(const std::filesystem::path& path, const rf::Bytes& bytes)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

TEST(ReplayerRecoveryRestore, ARestoringColdStartReplaysFromItsNewestSnapshotsCutFirstHoldingTheLiveQueue)
{
    Restoring r;
    rf::write(r.store, 1, CUT - 5, (CUT - 5) * 1024, 1, { SNAPSHOT_HEADER, SNAPSHOT_RECORD });
    r.writeSnapshot(2);
    r.start();

    EXPECT_EQ(1, r.client.requestsSent);
    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(CUT * 1024, r.client.recovery.requestFromPosition()) << "the newest round's cut";
    EXPECT_EQ(CUT, r.client.lastCutClusterSeqNo) << "the request names the cut, the position being a hint";
    EXPECT_FALSE(r.client.recovery.isRestoring()) << "no record before the cut is seen";
    EXPECT_TRUE(r.client.recovery.isRecovering());

    deliverLive(r.client, 40); // mid-stream, and no baseline: held, not fatal
    EXPECT_TRUE(r.dispatched.empty());
}

TEST(ReplayerRecoveryRestore, AnUnansweredReplayOfTheCutIsResentAndAPendingAnswerHoldsIt)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    advancePastTimers(r.client);
    EXPECT_EQ(2, r.client.requestsSent);
    EXPECT_EQ(CUT * 1024, r.client.recovery.requestFromPosition());
    EXPECT_EQ(CUT, r.client.lastCutClusterSeqNo) << "the resend names the cut too";

    deliverControl(r.client, encodeReplayPending(CLIENT_ID, r.client.recovery.requestId()));
    r.client.clockMs += 100;
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_EQ(2, r.client.requestsSent);
}

TEST(ReplayerRecoveryRestore, WithNoLocalSnapshotTheColdStartWalksTheRecordingFromItsStart)
{
    Restoring r;
    r.start();

    EXPECT_EQ(1, r.client.requestsSent);
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(REPLAYER_FROM_START, r.client.recovery.requestFromPosition());
    EXPECT_EQ(REPLAYER_NO_CUT, r.client.lastCutClusterSeqNo) << "a walk names no cut";
}

TEST(ReplayerRecoveryRestore, ASnapshotWhoseCutTheRecordingHoldsIsRestoredFromItsFileThenResumesAnchoredAtItsCut)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    attachRestoreReplay(r.client, 21);
    deliverReplayFrame(r.client, rf::started(CUT, 2), CUT);
    EXPECT_TRUE(r.client.recovery.isRestoring());
    EXPECT_EQ(-1, r.client.recovery.replaySessionId()) << "the records come from the file, not the replay";
    EXPECT_TRUE(r.dispatched.empty()) << "the cut is not dispatched";

    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_TRUE(r.handler.restored.empty()) << "the records are checked against the trailer before any is passed on";
    EXPECT_TRUE(r.client.recovery.isRestoring());

    r.client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ((std::vector<std::string>{ "header 4 2", "0 7" }), r.handler.restored);
    EXPECT_TRUE(r.dispatched.empty());
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ(CUT, r.client.recovery.lastClusterSeqNo());
    EXPECT_EQ(2, r.client.recovery.currentLeaderMemberId()) << "the header's leader";
    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(CUT * 1024, r.client.recovery.requestFromPosition());

    attachRestoreReplay(r.client, 22);
    deliverReplayFrame(r.client, rf::started(CUT, 2), CUT); // the anchor, already dispatched
    deliverReplayFrame(r.client, encodeHeartbeat(11), 11);
    deliverReplayFrame(r.client, encodeHeartbeat(12), 12);
    EXPECT_EQ((std::vector<std::int64_t>{ 11, 12 }), r.dispatched);
}

TEST(ReplayerRecoveryRestore, ACutFoundElsewhereInTheRecordingThanItsFilesPositionIsResumedAtWhereItWasFound)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    attachRestoreReplay(r.client, 21);
    deliverReplayFrame(r.client, rf::started(CUT, 2), CUT + 7); // the file's cutPosition is a hint only
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ((CUT + 7) * 1024, r.client.recovery.requestFromPosition()) << "not the file's";
}

TEST(ReplayerRecoveryRestore, AfterARestoreTheQueuesContinuityResumesAtTheCutFramesGroupSeqNo)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    attachRestoreReplay(r.client, 21);
    deliverReplayFrame(r.client, rf::started(CUT, 4, 2, CUT * 1000), 4); // the queue's fourth frame
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);

    attachRestoreReplay(r.client, 22);
    deliverReplayFrame(r.client, rf::started(CUT, 4, 2, CUT * 1000), 4);
    deliverReplayFrame(r.client, encodeHeartbeat(CUT + 3, 5), 5);
    EXPECT_EQ((std::vector<std::int64_t>{ CUT + 3 }), r.dispatched) << "groupSeqNo 5 follows the cut's 4";
}

TEST(ReplayerRecoveryRestore, AfterARestoreTheLeadershipHandlerIsToldTheSnapshotsLeaderAtTheCut)
{
    std::vector<std::string> changes;
    Restoring r{ [&changes](const std::int32_t leaderMemberId, const std::int64_t leadershipTermId,
                            const std::int64_t clusterSeqNo) {
        changes.push_back(std::to_string(leaderMemberId) + " " + std::to_string(leadershipTermId) + " " +
                          std::to_string(clusterSeqNo));
    } };
    r.writeSnapshot(2);
    r.start();
    replayTheCut(r.client);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_TRUE(changes.empty()) << "not before the records are restored";

    r.client.recovery.doTimers(/*requestPublicationPending=*/false);

    EXPECT_EQ((std::vector<std::string>{ "2 4 " + std::to_string(CUT) }), changes) << "the header's leader and term";
    EXPECT_TRUE(r.dispatched.empty()) << "it reaches the handler alone";
}

TEST(ReplayerRecoveryRestore, AReplayOfTheCutThatStopsAdvancingIsRequestedAgainAtTheCut)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    attachRestoreReplay(r.client, 21);
    advancePastTimers(r.client);

    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(CUT * 1024, r.client.recovery.requestFromPosition());
    attachRestoreReplay(r.client, 22);
    deliverReplayFrame(r.client, rf::started(CUT, 2), CUT);
    EXPECT_TRUE(r.client.recovery.isRestoring());
}

TEST(ReplayerRecoveryRestore,
     ARestoreReadsABoundedNumberOfRecordsPerDutyCycleCountedAsItsWorkHoldingTheQueueUntilItsLast)
{
    Restoring r;
    std::vector<rf::Bytes> records{ SNAPSHOT_HEADER };
    for (int i = 1; i <= ReplayerRecovery::MAX_RESTORE_RECORDS_PER_CYCLE; ++i)
    {
        records.push_back(rf::record(i));
    }
    rf::write(r.store, 2, CUT, CUT * 1024, 1, records);
    r.start();
    EXPECT_EQ(0, r.client.recovery.doTimers(/*requestPublicationPending=*/false)) << "waiting for the cut is no work";
    replayTheCut(r.client);

    EXPECT_EQ(ReplayerRecovery::MAX_RESTORE_RECORDS_PER_CYCLE,
              r.client.recovery.doTimers(/*requestPublicationPending=*/false));
    EXPECT_EQ(1, r.client.recovery.doTimers(/*requestPublicationPending=*/false));
    EXPECT_TRUE(r.handler.restored.empty()) << "the check is paced the same";
    EXPECT_TRUE(r.client.recovery.isRestoring());

    EXPECT_EQ(ReplayerRecovery::MAX_RESTORE_RECORDS_PER_CYCLE,
              r.client.recovery.doTimers(/*requestPublicationPending=*/false));
    EXPECT_TRUE(r.client.recovery.isRestoring());
    EXPECT_EQ(static_cast<std::size_t>(ReplayerRecovery::MAX_RESTORE_RECORDS_PER_CYCLE), r.handler.restored.size());
    deliverLive(r.client, 40);
    EXPECT_TRUE(r.dispatched.empty());

    EXPECT_EQ(1, r.client.recovery.doTimers(/*requestPublicationPending=*/false));
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ(records.size(), r.handler.restored.size());
    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(0, r.client.recovery.doTimers(/*requestPublicationPending=*/false)) << "nothing once restored";
}

TEST(ReplayerRecoveryRestore, ASnapshotWhoseCutTheRecordingDoesNotHoldGivesWayToTheNextOlderOneAndTheLastToAWalk)
{
    Restoring r;
    for (std::int64_t round = 3; round >= 1; --round)
    {
        r.writeSnapshot(round); // newest first: a confirmation deletes the older files
    }
    const rf::Bytes intact = readFile(r.file(3));
    writeFile(r.file(4), rf::Bytes(intact.begin(), intact.begin() + 40));
    r.start();

    EXPECT_EQ(1, r.client.requestsSent) << "round 4 is cut short: no replay for it";
    attachRestoreReplay(r.client, 21);
    deliverReplayFrame(r.client, rf::started(CUT, 3, 1), CUT); // another log's frame at round 3's cut
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ(2, r.client.requestsSent) << "round 2's cut, next";
    attachRestoreReplay(r.client, 22);
    deliverReplayFrame(r.client, encodeHeartbeat(CUT + 1), CUT + 1); // round 2's cut is not where this recording has it
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ(3, r.client.requestsSent);
    deliverControl(r.client, encodeReplaying(CLIENT_ID, r.client.recovery.requestId(), REPLAYER_NO_REPLAY_NEEDED, 0));
    EXPECT_FALSE(r.client.recovery.fault()) << "round 1's cut is no frame of this recording";
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_TRUE(r.client.recovery.isAwaitingReplay());
    EXPECT_EQ(REPLAYER_FROM_START, r.client.recovery.requestFromPosition());
    EXPECT_TRUE(r.handler.restored.empty()) << "nothing restored from a file the recording does not hold";
}

TEST(ReplayerRecoveryRestore, AfterARestoreAResumeOpeningAnywhereButTheCutIsFatal)
{
    Restoring r;
    r.writeSnapshot(2);
    r.start();
    replayTheCut(r.client);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false); // the records' check
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    attachRestoreReplay(r.client, 22);

    deliverReplayFrame(r.client, encodeHeartbeat(12), 12); // not the anchor

    ASSERT_TRUE(r.client.recovery.fault().has_value());
    EXPECT_NE(std::string::npos, r.client.recovery.fault()->find("expected " + std::to_string(CUT)))
        << *r.client.recovery.fault();
    EXPECT_TRUE(r.dispatched.empty()) << "nothing after a wrong cut may be dispatched";
}

TEST(ReplayerRecoveryRestore, ASnapshotThatCannotBeRestoredStopsRecoveryItsFormatOrItsHeader)
{
    {
        Restoring r;
        rf::write(r.store, 2, CUT, CUT * 1024, 9, { SNAPSHOT_HEADER, SNAPSHOT_RECORD });
        r.start();
        ASSERT_TRUE(r.client.recovery.restoreFailure());
        EXPECT_NE(std::string::npos, r.client.recovery.restoreFailure()->find("formatVersion 9"));
        EXPECT_FALSE(r.client.recovery.isRecovering());
        EXPECT_EQ(0, r.client.requestsSent);
        deliverLive(r.client, 40); // no baseline, and no longer an error either
        EXPECT_TRUE(r.dispatched.empty());
    }
    {
        Restoring r;
        rf::Bytes header = SNAPSHOT_HEADER;
        header[0] = 9;
        rf::write(r.store, 2, CUT, CUT * 1024, 1, { header, SNAPSHOT_RECORD });
        r.start();
        replayTheCut(r.client);
        r.client.recovery.doTimers(/*requestPublicationPending=*/false); // the records' check
        r.client.recovery.doTimers(/*requestPublicationPending=*/false);
        ASSERT_TRUE(r.client.recovery.restoreFailure());
        EXPECT_NE(std::string::npos, r.client.recovery.restoreFailure()->find("header of version 9"));
        EXPECT_FALSE(r.client.recovery.isRecovering());
    }
    Client client{ [](const SequencedEvent&) {} };
    EXPECT_FALSE(client.recovery.restoreFailure());
}

TEST(ReplayerRecoveryRestore, ASnapshotWhoseRecordsDoNotMatchItsTrailerGivesWayToTheNextOlderOneBeforeTheHandlerSeesAny)
{
    Restoring r;
    rf::write(r.store, 1, CUT - 5, (CUT - 5) * 1024, 1, { SNAPSHOT_HEADER, rf::record(5) });
    r.writeSnapshot(2);
    rf::Bytes damaged = readFile(r.file(2));
    damaged[2 + SNAPSHOT_HEADER.size() + 2] ^= 1; // the record's first byte; the trailer still matches
    writeFile(r.file(2), damaged);
    r.start();
    replayTheCut(r.client);

    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_FALSE(r.client.recovery.restoreFailure());
    EXPECT_TRUE(r.handler.restored.empty()) << "nothing of the damaged file reaches the handler";
    EXPECT_FALSE(r.client.recovery.isRestoring());
    EXPECT_EQ(2, r.client.requestsSent) << "round 1's cut, next";
    EXPECT_EQ((CUT - 5) * 1024, r.client.recovery.requestFromPosition());

    attachRestoreReplay(r.client, 22);
    deliverReplayFrame(r.client, rf::started(CUT - 5, 1), CUT - 5);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    r.client.recovery.doTimers(/*requestPublicationPending=*/false);
    EXPECT_EQ((std::vector<std::string>{ "header 4 2", "0 5" }), r.handler.restored);
    EXPECT_EQ(CUT - 5, r.client.recovery.lastClusterSeqNo());
    EXPECT_TRUE(std::filesystem::exists(r.file(2))) << "the damaged file stays until its round is confirmed again";
}

// A lifecycle frame reaches onSequenced when no lifecycle callback was given. This side alone has the
// onConnected/onDisconnected seam — the Java twin's receiver has no such callbacks and always delivers
// these on onSequenced — so this case has no Java counterpart by construction rather than by omission.
// app/Session takes them here: confirmed ingress must see every frame the producer placed, its own
// ConnectionOpened included.
TEST(ReplayerRecoveryLifecycle, AConnectionFrameReachesOnSequencedWhenNoLifecycleCallbackIsGiven)
{
    std::vector<std::int64_t> dispatched;
    Client client{ [&dispatched](const protocol::SequencedEvent& event) { dispatched.push_back(event.clusterSeqNo); } };

    auto buf = encodeHeartbeat(/*clusterSeqNo=*/1);
    // The systemEventType is what diverts a frame, not its template, so overriding it on the heartbeat
    // frame the other cases use is the whole difference.
    frm::MessageHeader header(reinterpret_cast<char*>(buf.data()), 0, buf.size(),
                              frm::MessageHeader::sbeSchemaVersion());
    frm::SequencedSystemHeader systemHeader(reinterpret_cast<char*>(buf.data()), frm::MessageHeader::encodedLength(),
                                            buf.size(), frm::MessageHeader::sbeSchemaVersion());
    systemHeader.systemEventType(protocol::CONNECTION_OPENED);
    client.recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), /*framePosition=*/0, /*receiveNs=*/0,
                            /*fromReplay=*/false);

    ASSERT_EQ(1u, dispatched.size()) << "dropped rather than delivered";
    EXPECT_EQ(1, dispatched.front());
}

} // namespace
} // namespace org::limitless::seqeron::replayer::client::detail
