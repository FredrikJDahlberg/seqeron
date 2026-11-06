// RetainedCommands (spec H-17) alone, then against a model of the host with a seeded generator interleaving offers,
// the host deciding its queue, gate edges and reports. Safety: every command is placed exactly once and in the order
// it was offered, across any number of edges; the arena never overflows.
//
// Seeds are fixed; the failing one is in the test name.

#include <cstdint>
#include <cstring>
#include <deque>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/helpers/SplitMix64.hpp"
#include "org/limitless/seqeron/hosted/detail/RetainedCommands.hpp"

namespace org::limitless::seqeron::hosted::detail {
namespace {

std::vector<char> message(const std::int64_t id, const std::size_t length = 16)
{
    std::vector<char> bytes(length, 0);
    std::memcpy(bytes.data(), &id, sizeof id);
    return bytes;
}

std::int64_t idOf(const char* data)
{
    std::int64_t id = 0;
    std::memcpy(&id, data, sizeof id);
    return id;
}

void retain(RetainedCommands& ring, const std::int64_t seq, const std::int64_t epoch, const std::int64_t id,
            const std::size_t length = 16)
{
    const std::vector<char> bytes = message(id, length);
    ASSERT_TRUE(ring.canRetain(bytes.size()));
    ring.retain(seq, epoch, bytes.data(), bytes.size());
}

class RetainedCommandsTest : public ::testing::Test
{
  protected:
    RetainedCommands ring;

    void SetUp() override
    {
        ring.reset(256, 16);
    }
};

TEST_F(RetainedCommandsTest, ResolvedFreesWhatWasPlaced)
{
    retain(ring, 1, 0, 100);
    retain(ring, 2, 0, 101);
    ring.resolved(1);
    EXPECT_EQ(1U, ring.count());
    ring.resolved(2);
    EXPECT_EQ(0U, ring.count());
    EXPECT_EQ(0U, ring.bytesInUse());
}

TEST_F(RetainedCommandsTest, ADroppedCommandStaysUntilItIsResent)
{
    retain(ring, 1, 0, 100);
    retain(ring, 2, 0, 101);
    retain(ring, 3, 0, 102);
    ring.dropped(2, 3);
    ring.resolved(3);
    EXPECT_TRUE(ring.hasFlagged());
    EXPECT_EQ(2U, ring.count());
    EXPECT_EQ(101, idOf(ring.flagged().data));
    ring.resent(4, 1);
    EXPECT_EQ(102, idOf(ring.flagged().data));
    ring.resent(5, 1);
    EXPECT_FALSE(ring.hasFlagged());
    EXPECT_EQ(2U, ring.count());
    ring.resolved(5);
    EXPECT_EQ(0U, ring.count());
}

TEST_F(RetainedCommandsTest, ResendKeepsTheOrderBehindAFlaggedHead)
{
    retain(ring, 1, 0, 100);
    retain(ring, 2, 0, 101);
    ring.dropped(1, 1);
    ring.resolved(2);
    // 101 is placed and freed behind the flagged head, which keeps the arena until it is resent.
    EXPECT_EQ(2U, ring.count());
    ring.resent(3, 1);
    EXPECT_EQ(1U, ring.count());
    EXPECT_FALSE(ring.hasFlagged());
    ring.resolved(3);
    EXPECT_EQ(0U, ring.count());
}

TEST_F(RetainedCommandsTest, AResentCommandDroppedAgainIsStillResentBeforeTheOnesOfferedAfterIt)
{
    retain(ring, 1, 0, 100);
    retain(ring, 2, 0, 101);
    ring.dropped(1, 2);
    ring.resolved(2);
    ring.resent(3, 1); // 100 again, now behind 101 in the arena
    ring.dropped(3, 3);
    ring.resolved(3);
    EXPECT_EQ(100, idOf(ring.flagged().data));
    ring.resent(4, 2);
    EXPECT_EQ(101, idOf(ring.flagged().data));
}

TEST_F(RetainedCommandsTest, ANewPublishWaitsForAFlaggedOrAnUndecidedOlderCommand)
{
    EXPECT_FALSE(ring.blocksPublish(0));
    retain(ring, 1, 0, 100);
    EXPECT_FALSE(ring.blocksPublish(0));
    EXPECT_TRUE(ring.blocksPublish(1));
    EXPECT_TRUE(ring.hasUndecidedBefore(1));
    ring.dropped(1, 1);
    ring.resolved(1);
    EXPECT_TRUE(ring.blocksPublish(1));
    EXPECT_FALSE(ring.hasUndecidedBefore(1));
}

TEST_F(RetainedCommandsTest, AResendWaitsForALiveCommandBehindTheFlaggedOnes)
{
    retain(ring, 1, 0, 100);
    retain(ring, 2, 0, 101);
    ring.dropped(1, 1);
    EXPECT_TRUE(ring.hasUndecidedBefore(1));
    ring.dropped(2, 2);
    EXPECT_FALSE(ring.hasUndecidedBefore(1));
}

TEST_F(RetainedCommandsTest, TheArenaWrapsAndRefusesWhatDoesNotFit)
{
    for (std::int64_t i = 0; i < 4; i++)
    {
        retain(ring, i + 1, 0, i, 64);
    }
    EXPECT_FALSE(ring.canRetain(1));
    ring.resolved(2);
    EXPECT_TRUE(ring.canRetain(128));
    EXPECT_FALSE(ring.canRetain(129));
    retain(ring, 5, 0, 4, 64);
    retain(ring, 6, 0, 5, 64);
    EXPECT_EQ(4U, ring.count());
    ring.resolved(6);
    EXPECT_EQ(0U, ring.count());
    EXPECT_TRUE(ring.canRetain(256));
}

TEST_F(RetainedCommandsTest, AFullTableRefusesBeforeTheArenaIsFull)
{
    ring.reset(1024, 3);
    retain(ring, 1, 0, 1);
    retain(ring, 2, 0, 2);
    retain(ring, 3, 0, 3);
    EXPECT_FALSE(ring.canRetain(1));
}

// The host's side of H-17: a FIFO that places a command stamped with the current epoch and drops any other, reporting
// each run it drops and then how far it has decided.
class HostModel
{
  public:
    struct Command
    {
        std::int64_t seq;
        std::int64_t epoch;
        std::int64_t id;
    };

    std::deque<Command> queue;
    std::vector<std::int64_t> placed;
    std::int64_t epoch = 0;

    // Decides up to `limit` commands from the head; the reports go to the ring in stream order.
    void decide(RetainedCommands& ring, const int limit)
    {
        std::int64_t runFirst = 0;
        std::int64_t runLast = 0;
        std::int64_t through = 0;
        for (int i = 0; i < limit && !queue.empty(); i++)
        {
            const Command command = queue.front();
            queue.pop_front();
            if (command.epoch != epoch)
            {
                if (runFirst != 0 && command.seq == runLast + 1)
                {
                    runLast = command.seq;
                }
                else
                {
                    if (runFirst != 0)
                    {
                        ring.dropped(runFirst, runLast);
                    }
                    runFirst = command.seq;
                    runLast = command.seq;
                }
            }
            else
            {
                if (runFirst != 0)
                {
                    ring.dropped(runFirst, runLast);
                    runFirst = 0;
                }
                placed.push_back(command.id);
            }
            through = command.seq;
        }
        if (runFirst != 0)
        {
            ring.dropped(runFirst, runLast);
        }
        if (through != 0)
        {
            ring.resolved(through);
        }
    }
};

class RetainedCommandsPropertyTest : public ::testing::TestWithParam<std::uint64_t>
{};

TEST_P(RetainedCommandsPropertyTest, EveryCommandIsPlacedOnceAndInOrderAcrossEdges)
{
    helpers::SplitMix64 random(GetParam());
    RetainedCommands ring;
    ring.reset(2048, 128);
    HostModel host;
    std::int64_t seq = 0;
    std::int64_t offered = 0;
    constexpr std::int64_t COMMANDS = 3000;

    const auto resend = [&] {
        if (!ring.hasFlagged() || ring.hasUndecidedBefore(host.epoch))
        {
            return;
        }
        const RetainedCommands::Flagged flagged = ring.flagged();
        if (!ring.canRetain(flagged.length))
        {
            return;
        }
        const std::int64_t id = idOf(flagged.data);
        ring.resent(++seq, host.epoch);
        host.queue.push_back({ seq, host.epoch, id });
    };

    for (int step = 0; step < 200'000 && (offered < COMMANDS || !host.queue.empty() || ring.count() != 0); step++)
    {
        const int roll = random.roll(100);
        if (roll < 40 && offered < COMMANDS)
        {
            const std::size_t length = 16 + static_cast<std::size_t>(random.roll(48));
            if (!ring.blocksPublish(host.epoch) && ring.canRetain(length))
            {
                const std::vector<char> bytes = message(offered, length);
                ring.retain(++seq, host.epoch, bytes.data(), bytes.size());
                host.queue.push_back({ seq, host.epoch, offered });
                offered++;
            }
        }
        else if (roll < 75)
        {
            host.decide(ring, 1 + random.roll(8));
        }
        else if (roll < 80 && offered < COMMANDS)
        {
            host.epoch++;
        }
        else
        {
            resend();
        }
        ASSERT_LE(ring.bytesInUse(), 2048U);
    }

    ASSERT_EQ(COMMANDS, static_cast<std::int64_t>(host.placed.size()));
    for (std::int64_t i = 0; i < COMMANDS; i++)
    {
        ASSERT_EQ(i, host.placed[static_cast<std::size_t>(i)]) << "at " << i;
    }
    EXPECT_EQ(0U, ring.count());
}

INSTANTIATE_TEST_SUITE_P(Seeds, RetainedCommandsPropertyTest,
                         ::testing::Values(1ULL, 2ULL, 3ULL, 7ULL, 42ULL, 1234ULL, 99991ULL, 0xC0FFEEULL),
                         [](const ::testing::TestParamInfo<std::uint64_t>& info) {
                             return "seed" + std::to_string(info.param);
                         });

} // namespace
} // namespace org::limitless::seqeron::hosted::detail
