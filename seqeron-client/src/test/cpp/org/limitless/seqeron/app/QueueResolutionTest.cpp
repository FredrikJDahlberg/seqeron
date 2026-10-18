// Which queue an instance reads (spec R-6), resolved against a scripted Replayer. Case for case with
// QueueResolutionTest.java and QueueResolutionTest.cs, in the same order.

#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/detail/QueueResolution.hpp"

namespace org::limitless::seqeron::app::detail {
namespace {

constexpr std::int64_t CLIENT_ID = 4;
constexpr std::int64_t CLOCK_MS = 3 * 60 * 60 * 1000;

struct Replayer final : QueueResolution::Actions
{
    void sendQueueRequest(const std::int64_t requestId) override
    {
        sent.push_back(requestId);
    }

    std::int64_t clientId() override
    {
        return CLIENT_ID;
    }

    std::int64_t nowMs() override
    {
        return clockMs;
    }

    std::vector<std::int64_t> sent;
    std::int64_t clockMs = CLOCK_MS;
};

struct Fixture
{
    void resolved(const std::int64_t clientId, const std::int64_t requestId, const std::int32_t queueId)
    {
        std::array<char, 64> buffer{};
        sbe::replay::QueueResolved encoder;
        encoder.wrapAndApplyHeader(buffer.data(), 0, buffer.size());
        encoder.clientId(clientId).requestId(requestId).queueId(queueId);
        resolution.onControl(buffer.data(), sbe::replay::MessageHeader::encodedLength() + encoder.encodedLength());
    }

    void pending(const std::int64_t requestId)
    {
        std::array<char, 64> buffer{};
        sbe::replay::ReplayPending encoder;
        encoder.wrapAndApplyHeader(buffer.data(), 0, buffer.size());
        encoder.clientId(CLIENT_ID).requestId(requestId);
        resolution.onControl(buffer.data(), sbe::replay::MessageHeader::encodedLength() + encoder.encodedLength());
    }

    Replayer replayer;
    QueueResolution resolution{ replayer };
};

TEST(QueueResolution, NothingIsSentBeforeStart)
{
    Fixture f;
    f.replayer.clockMs += 10'000;
    f.resolution.doTimers(false);

    EXPECT_TRUE(f.replayer.sent.empty());
    EXPECT_EQ(QueueResolution::UNRESOLVED, f.resolution.groupId());
}

TEST(QueueResolution, StartSendsTheFirstRequest)
{
    Fixture f;
    f.resolution.start();

    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), f.replayer.sent);
    EXPECT_EQ(QueueResolution::UNRESOLVED, f.resolution.groupId());
}

TEST(QueueResolution, TheAnswerToTheCurrentRequestResolvesTheQueue)
{
    Fixture f;
    f.resolution.start();
    f.resolved(CLIENT_ID, 1, 3);

    EXPECT_EQ(3, f.resolution.groupId());
}

TEST(QueueResolution, AnAnswerForAnotherClientOrAnEarlierRequestIsIgnored)
{
    Fixture f;
    f.resolution.start();
    f.replayer.clockMs += QueueResolution::RESEND_INTERVAL_MS + 1;
    f.resolution.doTimers(false);

    f.resolved(CLIENT_ID + 1, 2, 3);
    f.resolved(CLIENT_ID, 1, 3);

    EXPECT_EQ(QueueResolution::UNRESOLVED, f.resolution.groupId());
}

TEST(QueueResolution, AnUnansweredRequestIsResentWithANewRequestId)
{
    Fixture f;
    f.resolution.start();
    f.replayer.clockMs += QueueResolution::RESEND_INTERVAL_MS;
    f.resolution.doTimers(false);
    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), f.replayer.sent) << "not before the interval has passed";

    f.replayer.clockMs += 1;
    f.resolution.doTimers(false);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2 }), f.replayer.sent);
    EXPECT_EQ(2, f.resolution.requestId());
}

TEST(QueueResolution, APendingAnswerHoldsTheResendOff)
{
    Fixture f;
    f.resolution.start();
    f.replayer.clockMs += QueueResolution::RESEND_INTERVAL_MS;
    f.pending(1); // the row is not on the log yet

    f.replayer.clockMs += 1;
    f.resolution.doTimers(false);

    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), f.replayer.sent);
}

TEST(QueueResolution, AnUnconnectedPublicationIsRetriedEveryCycle)
{
    Fixture f;
    f.resolution.start();
    f.resolution.doTimers(true);

    EXPECT_EQ((std::vector<std::int64_t>{ 1, 2 }), f.replayer.sent);
}

TEST(QueueResolution, NothingIsSentOnceResolved)
{
    Fixture f;
    f.resolution.start();
    f.resolved(CLIENT_ID, 1, 3);
    f.replayer.clockMs += 10'000;
    f.resolution.doTimers(true);
    f.resolved(CLIENT_ID, 1, 5);

    EXPECT_EQ((std::vector<std::int64_t>{ 1 }), f.replayer.sent);
    EXPECT_EQ(3, f.resolution.groupId()) << "a source's group never changes";
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
