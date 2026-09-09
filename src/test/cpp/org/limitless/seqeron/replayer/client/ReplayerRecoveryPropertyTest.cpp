// ReplayerRecovery against a MODEL of the counterparty it talks to, rather than against scripted stimuli,
// and the twin of the Java ReplayerRecoveryPropertyTest — keep the two in step, as with the named suites.
//
// ReplayerRecoveryTest.cpp names one situation per case and asserts the decision taken in it, which is what
// pins the protocol down, but it can only reach the interleavings someone thought to write. Here the node's
// archive, its tap and its Replayer are simulated, a seeded generator drives faults through them, and the
// two properties recovery exists to provide are asserted over whatever comes out.
//
// Safety   — every dispatched frame is the next globalSeqNo, always. No gap, no duplicate, no reordering,
//            at any point in any run. This is the invariant the whole walk/resume/retain machinery is for,
//            and it is checked on every dispatch, so a violation names the frame that broke it.
// Liveness — once the faults stop, recovery converges: caught up, at the tip, having dispatched every frame
//            ever published. A state machine can hold the safety property by dispatching nothing, so
//            without this one the suite would pass on a client that gave up.
//
// The e2e harnesses reach these paths too, but a chaos run produces single-digit gap episodes in minutes,
// all of one shape — one dropped frame on a live tap. A run here is a few hundred, mixing drops with lost
// requests, refusals, truncated images, stalled replays, recording rotations and tap redeliveries, in under
// a second.
//
// Seeds are fixed and listed, not drawn from the clock: a failing run must be re-runnable, and a suite that
// fails on a different case each time is not a regression signal. Add seeds to widen the search; the
// failing one is in the test name.

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/replayer/client/ReplayerRecovery.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"
#include "org_limitless_seqeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"
#include "org_limitless_seqeron_sbe_replay/ReplayUnavailable.h"
#include "org_limitless_seqeron_sbe_replay/Replaying.h"

namespace org::limitless::seqeron::replayer::client {
namespace {

namespace frm = org::limitless::seqeron::sbe::frame;
namespace diag = org::limitless::seqeron::util;

constexpr std::int32_t CLIENT_ID = 4;

// Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter.
constexpr std::int64_t CLOCK_MS = 3 * 60 * 60 * 1000;

// One frame's span in the recording's position space. Opaque to the client — only ordering matters.
constexpr std::int64_t STRIDE = 64;

constexpr int CHAOS_STEPS = 600;

// Generous: convergence takes a bounded number of steps, and a run that needs them all still passes.
constexpr int QUIESCE_STEPS = 20'000;

// The walk terminator: nothing left to replay AND no recording named.
constexpr std::int64_t CHAIN_EXHAUSTED = -1;

// Every gap logs, and a run makes hundreds — kept out of the test output rather than counted. RAII so a run
// that fails out early still restores the default sink.
struct SilentLoggerSink final : diag::LoggerSink
{
    SilentLoggerSink()
    {
        diag::Logger::install(*this);
    }

    ~SilentLoggerSink() override
    {
        diag::Logger::reset();
    }

    void record(const diag::LoggerEvent&) override
    {}
};

// Where frame globalSeqNo starts. Frame 1 at 0, so the position after frame n is n*STRIDE.
std::int64_t positionOf(const std::int64_t globalSeqNo)
{
    return (globalSeqNo - 1) * STRIDE;
}

// One frame carrying a ClusterHeartbeat — the cheapest well-formed frame there is, and the same one the
// named suite uses.
std::vector<std::uint8_t> encodeHeartbeat(const std::int64_t globalSeqNo)
{
    std::vector<std::uint8_t> buf(256, 0);
    frm::ClusterHeartbeat frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    frame.header()
        .sourceId(1)
        .connectionId(0)
        .sessionId(0)
        .systemEventType(sequencer::CLUSTER_HEARTBEAT)
        .globalSeqNo(globalSeqNo)
        .timestamp(globalSeqNo * 1000);
    buf.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return buf;
}

std::vector<std::uint8_t> encodeReplaying(const std::int64_t requestId, const std::int64_t replaySessionId,
                                          const std::int64_t catchUpPosition, const std::int64_t recordingId)
{
    std::vector<std::uint8_t> buf(64, 0);
    rpl::Replaying enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(CLIENT_ID)
        .requestId(requestId)
        .replaySessionId(replaySessionId)
        .catchUpPosition(catchUpPosition)
        .recordingId(recordingId);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayPending(const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    rpl::ReplayPending enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(CLIENT_ID).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayUnavailable(const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    rpl::ReplayUnavailable enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(CLIENT_ID).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

// One recording in the chain. last < first while it holds nothing yet.
struct Segment
{
    std::int64_t recordingId;
    std::int64_t first;
    std::int64_t last;
};

// One seeded run: the node's archive, its tap and its Replayer, driving one ReplayerRecovery.
//
// Implements ReplayerRecoveryActions itself — the client's every outbound act is a request arriving at this
// Replayer, so recording them and serving them are the same object.
struct Simulation final : ReplayerRecoveryActions
{
    explicit Simulation(const std::uint64_t seed) :
      rng(seed),
      tag("seed=" + std::to_string(seed)),
      recovery(CLIENT_ID, *this, [this](const SequencedEvent& event) { onSequenced(event); })
    {}

    void execute()
    {
        // History before the client starts. A cold start over an EMPTY archive that then drops the very
        // first tap frame is the one designed abort — the first frame observed must be globalSeqNo 1 — and
        // not a recovery failure, so the model does not construct it.
        segments.push_back({ nextRecordingId++, 1, 0 });
        for (int i = 0; i < 3; ++i)
        {
            publish();
        }

        recovery.start();

        for (int step = 0; step < CHAOS_STEPS && !broken; ++step)
        {
            chaosStep();
        }

        // Quiescence: faults off, but the tap keeps running. A hole the chaos phase left open is only ever
        // discovered by the NEXT tap frame, so convergence has to be given one.
        chaos = false;
        for (int i = 0; i < 3; ++i)
        {
            deliverTap(publish());
        }
        for (int step = 0; step < QUIESCE_STEPS && !broken && !converged(); ++step)
        {
            if (pendingRequestId >= 0)
            {
                serveRequest();
            }
            else if (replayEndSeqNo >= 0)
            {
                deliverReplayFrames(4);
            }
            else
            {
                tick();
            }
        }

        EXPECT_TRUE(recovery.isCaughtUp()) << tag << ": never re-converged after the faults stopped";
        EXPECT_EQ(tip, recovery.lastGlobalSeqNo()) << tag << ": converged short of the tip";
        EXPECT_EQ(tip + 1, expectedNext) << tag << ": caught up without having dispatched every frame";
    }

  private:
    bool converged() const
    {
        return recovery.isCaughtUp() && recovery.lastGlobalSeqNo() == tip;
    }

    void chaosStep()
    {
        switch (roll(10))
        {
            case 0:
            case 1:
            case 2:
            case 3: {
                const std::int64_t globalSeqNo = publish();
                // A tap drop is the fault the whole resume path exists for, so it is the common one.
                if (!chance(20))
                {
                    deliverTap(globalSeqNo);
                }
                break;
            }
            case 4:
            case 5:
                serveRequest();
                break;
            case 6:
            case 7:
                deliverReplayFrames(1 + roll(3));
                break;
            case 8:
                tick();
                break;
            default:
                injectFault();
                break;
        }
    }

    void injectFault()
    {
        switch (roll(4))
        {
            case 0:
                if (replayEndSeqNo >= 0)
                {
                    // The Replayer stopped this replay under us — its image closes SHORT of the bound.
                    recovery.onReplayImageClosed(positionOf(replayCursor));
                }
                break;
            case 1:
                clockMs += 6'000; // past REPLAY_STALL_TIMEOUT_MS as well as the resend interval
                recovery.doTimers(/*requestPublicationPending=*/false);
                break;
            case 2:
                // The same tap frame offered twice. Both de-dupes have to hold: the contiguity one when it
                // sits at or below the baseline, and the retained FIFO's when it is ahead of a hole.
                if (lastTapped > 0)
                {
                    auto buf = encodeHeartbeat(lastTapped);
                    recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), positionOf(lastTapped),
                                     /*receiveNs=*/0, /*fromReplay=*/false);
                }
                break;
            default:
                segments.push_back({ nextRecordingId++, tip + 1, tip });
                break;
        }
    }

    // ── the tap ───────────────────────────────────────────────────────────────────────────────────────

    std::int64_t publish()
    {
        segments.back().last = ++tip;
        return tip;
    }

    void deliverTap(const std::int64_t globalSeqNo)
    {
        lastTapped = globalSeqNo;
        auto buf = encodeHeartbeat(globalSeqNo);
        recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), positionOf(globalSeqNo),
                         /*receiveNs=*/0, /*fromReplay=*/false);
    }

    // ── the Replayer ──────────────────────────────────────────────────────────────────────────────────

    void serveRequest()
    {
        if (pendingRequestId < 0)
        {
            return;
        }
        const std::int64_t requestId = pendingRequestId;
        const std::int32_t segmentIndex = pendingSegmentIndex;
        const std::int64_t fromPosition = pendingFromPosition;
        pendingRequestId = -1;
        if (requestId != recovery.requestId())
        {
            return; // superseded by a request this run dropped — the Replayer would serve the newer one
        }

        if (chaos && chance(10))
        {
            deliverControl(encodeReplayPending(requestId));
            return;
        }
        if (chaos && chance(5))
        {
            deliverControl(encodeReplayUnavailable(requestId));
            return;
        }

        if (segmentIndex < 0) // a resume, anchored on a position rather than a segment
        {
            const Segment& active = segments.back();
            const std::int64_t anchor = fromPosition / STRIDE + 1;
            if (anchor < active.first || anchor > active.last)
            {
                // The position no longer sits in the active recording — it rotated under the client.
                deliverControl(encodeReplaying(requestId, REPLAYER_NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED));
                return;
            }
            serveReplay(requestId, anchor, active.last, active.recordingId);
            return;
        }
        if (static_cast<std::size_t>(segmentIndex) >= segments.size())
        {
            deliverControl(encodeReplaying(requestId, REPLAYER_NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED));
            return;
        }
        const Segment& segment = segments[static_cast<std::size_t>(segmentIndex)];
        if (segment.last < segment.first)
        {
            // Empty, not exhausted: the recording is named, which is what tells the two apart.
            deliverControl(encodeReplaying(requestId, REPLAYER_NO_REPLAY_NEEDED, 0, segment.recordingId));
            return;
        }
        serveReplay(requestId, segment.first, segment.last, segment.recordingId);
    }

    // Bound at the tip the recording holds NOW — frames published later are the tap's problem, not this
    // replay's, which is exactly how a bounded replay of an active recording behaves.
    void serveReplay(const std::int64_t requestId, const std::int64_t fromSeqNo, const std::int64_t toSeqNo,
                     const std::int64_t recordingId)
    {
        replayCursor = fromSeqNo;
        replayEndSeqNo = toSeqNo;
        deliverControl(encodeReplaying(requestId, nextSessionId++, positionOf(toSeqNo + 1), recordingId));
    }

    // Feeds up to count frames off the open replay, then reports where it has reached. Both the frames and
    // the position report can make the client abandon the replay (an anchor mismatch, the bound being
    // reached), which closeReplay() records — hence the re-checks.
    void deliverReplayFrames(const int count)
    {
        for (int i = 0; i < count && replayEndSeqNo >= 0 && replayCursor <= replayEndSeqNo; ++i)
        {
            const std::int64_t globalSeqNo = replayCursor++;
            auto buf = encodeHeartbeat(globalSeqNo);
            recovery.onFrame(reinterpret_cast<char*>(buf.data()), buf.size(), positionOf(globalSeqNo),
                             /*receiveNs=*/0, /*fromReplay=*/true);
        }
        if (replayEndSeqNo >= 0)
        {
            recovery.onReplayPosition(positionOf(replayCursor));
        }
    }

    void deliverControl(std::vector<std::uint8_t> body)
    {
        recovery.onControl(reinterpret_cast<char*>(body.data()), body.size());
    }

    void tick()
    {
        clockMs += 100 + roll(900); // straddles RESEND_INTERVAL_MS
        recovery.doTimers(/*requestPublicationPending=*/false);
        recovery.checkRecoveryProgress();
    }

    // ── ReplayerRecoveryActions: the client's outbound side ───────────────────────────────────────────

    void sendReplayRequest(const std::int64_t requestId, const std::int32_t segmentIndex,
                           const std::int64_t fromPosition) override
    {
        if (chaos && chance(15))
        {
            return; // the offer did not land; only the resend timer recovers this
        }
        pendingRequestId = requestId;
        pendingSegmentIndex = segmentIndex;
        pendingFromPosition = fromPosition;
    }

    bool sendReplayComplete() override
    {
        return !chaos || chance(70);
    }

    bool sendReplayHeartbeat() override
    {
        return !chaos || chance(70);
    }

    void openReplay(std::int64_t) override
    {}

    void closeReplay() override
    {
        replayEndSeqNo = -1;
    }

    void recoveryStalled(bool) override
    {}

    std::int64_t nowMs() override
    {
        return clockMs;
    }

    // ── the safety property ───────────────────────────────────────────────────────────────────────────

    void onSequenced(const SequencedEvent& event)
    {
        if (event.globalSeqNo != expectedNext || event.globalSeqNo > tip)
        {
            broken = true; // stops the run rather than reporting the same break once per remaining frame
            ADD_FAILURE() << tag << ": dispatched globalSeqNo " << event.globalSeqNo << ", expected " << expectedNext
                          << " (tip " << tip << ")";
        }
        expectedNext = event.globalSeqNo + 1;
    }

    int roll(const int bound)
    {
        return static_cast<int>(rng() % static_cast<std::uint64_t>(bound));
    }

    bool chance(const int percent)
    {
        return roll(100) < percent;
    }

    std::mt19937_64 rng;
    const std::string tag;

    // the node's archive: the recording chain, complete by construction
    std::vector<Segment> segments;
    std::int64_t nextRecordingId = 0;
    std::int64_t tip = 0;

    // the Replayer's view of this one client
    std::int64_t pendingRequestId = -1;
    std::int32_t pendingSegmentIndex = 0;
    std::int64_t pendingFromPosition = 0;
    std::int64_t nextSessionId = 1;

    // next globalSeqNo the open replay will deliver; replayEndSeqNo < 0 means none is open
    std::int64_t replayCursor = 0;
    std::int64_t replayEndSeqNo = -1;

    std::int64_t lastTapped = 0; // last globalSeqNo the tap delivered — what a redelivery re-offers
    std::int64_t clockMs = CLOCK_MS;
    bool chaos = true;
    bool broken = false;

    std::int64_t expectedNext = 1; // the safety property, one frame at a time

    ReplayerRecovery recovery; // last: every member above is already live when it binds *this
};

class ReplayerRecoveryProperty : public testing::TestWithParam<std::uint64_t>
{};

TEST_P(ReplayerRecoveryProperty, StaysGapFreeAndConvergesUnderRandomFaults)
{
    const SilentLoggerSink silence;
    Simulation simulation(GetParam());
    simulation.execute();
}

INSTANTIATE_TEST_SUITE_P(Seeds, ReplayerRecoveryProperty,
                         testing::Values(1ULL, 2ULL, 3ULL, 5ULL, 8ULL, 13ULL, 21ULL, 34ULL, 55ULL, 89ULL, 144ULL,
                                         233ULL, 377ULL, 610ULL, 987ULL, 1597ULL));

} // namespace
} // namespace org::limitless::seqeron::replayer::client
