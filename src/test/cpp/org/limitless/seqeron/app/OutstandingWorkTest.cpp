// Unit tests for the outstanding-work state machine, one situation per case. No Aeron runtime. Case for
// case with OutstandingWorkTest.java; OutstandingWorkPropertyTest drives the interleavings.

#include <cstdint>
#include <functional>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/OutstandingWork.hpp"

namespace org::limitless::seqeron::app {
namespace {

using Keys = std::vector<std::int64_t>;

// Records every request it takes, refusing once slots run out — a leader with no free worker.
struct Recorder
{
    Keys dispatched;
    int slots = 1'000'000;

    bool operator()(std::int64_t key, int&)
    {
        if (slots <= 0)
        {
            return false;
        }
        --slots;
        dispatched.push_back(key);
        return true;
    }
};

TEST(OutstandingWork, RequestIsTrackedAndIdempotent)
{
    OutstandingWork<int> work;
    work.onRequest(1, 10);
    work.onRequest(1, 11);

    EXPECT_EQ(1u, work.size());
    EXPECT_FALSE(work.isDispatched(1));
}

TEST(OutstandingWork, LeaderDispatchesEachRequestOnce)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    work.onRequest(2, 0);

    Recorder first;
    EXPECT_EQ(2, work.dispatchUndispatched(std::ref(first)));
    EXPECT_EQ((Keys{ 1, 2 }), first.dispatched);
    EXPECT_TRUE(work.isDispatched(1));
    EXPECT_TRUE(work.isDispatched(2));

    Recorder again;
    EXPECT_EQ(0, work.dispatchUndispatched(std::ref(again))) << "both are awaiting their replies";
    EXPECT_TRUE(again.dispatched.empty());
}

TEST(OutstandingWork, PromotedFollowerDispatchesTrackedRequests)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    work.onRequest(2, 0);
    work.onRequest(3, 0);
    EXPECT_FALSE(work.isDispatched(1)) << "never dispatched while a follower";

    Recorder promoted;
    EXPECT_EQ(3, work.dispatchUndispatched(std::ref(promoted))) << "including requests sequenced before it led";
}

TEST(OutstandingWork, ReplyRemovesRequest)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    Recorder first;
    work.dispatchUndispatched(std::ref(first));

    work.onReply(1);

    EXPECT_EQ(0u, work.size());
    EXPECT_FALSE(work.isDispatched(1));
    Recorder again;
    EXPECT_EQ(0, work.dispatchUndispatched(std::ref(again)));
}

TEST(OutstandingWork, LeadershipLossReDispatches)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    Recorder first;
    work.dispatchUndispatched(std::ref(first)); // its reply is lost in flight

    work.onNotLeader();
    EXPECT_FALSE(work.isDispatched(1));

    Recorder next;
    EXPECT_EQ(1, work.dispatchUndispatched(std::ref(next)));
    EXPECT_EQ((Keys{ 1 }), next.dispatched);
}

TEST(OutstandingWork, ReplyNotEmittedReDispatches)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    Recorder first;
    work.dispatchUndispatched(std::ref(first));

    work.onReplyNotEmitted(1);
    EXPECT_FALSE(work.isDispatched(1));
    EXPECT_EQ(1u, work.size()) << "still outstanding: only a sequenced reply discharges it";

    Recorder second;
    EXPECT_EQ(1, work.dispatchUndispatched(std::ref(second))) << "no leadership change needed";

    work.onReply(1);
    EXPECT_EQ(0u, work.size());
    Recorder third;
    EXPECT_EQ(0, work.dispatchUndispatched(std::ref(third)));
}

TEST(OutstandingWork, CapacityStopsTheSweep)
{
    OutstandingWork<int> work;
    work.onRequest(1, 0);
    work.onRequest(2, 0);
    work.onRequest(3, 0);

    Recorder first;
    first.slots = 2;
    EXPECT_EQ(2, work.dispatchUndispatched(std::ref(first)));

    Recorder rest;
    EXPECT_EQ(1, work.dispatchUndispatched(std::ref(rest)));

    EXPECT_EQ((Keys{ 1, 2 }), first.dispatched);
    EXPECT_EQ((Keys{ 3 }), rest.dispatched) << "each dispatched exactly once";
}

TEST(OutstandingWork, ReplyForUnknownKeyIsHarmless)
{
    OutstandingWork<int> work;
    work.onReply(42);

    EXPECT_EQ(0u, work.size());
}

TEST(OutstandingWork, DispatchFollowsRequestOrder)
{
    OutstandingWork<int> work;
    work.onRequest(5, 0);
    work.onRequest(3, 0);
    work.onRequest(9, 0);
    work.onRequest(5, 1);

    Recorder recorder;
    work.dispatchUndispatched(std::ref(recorder));

    EXPECT_EQ((Keys{ 5, 3, 9 }), recorder.dispatched) << "the order requests were sequenced, not key order";
}

} // namespace
} // namespace org::limitless::seqeron::app
