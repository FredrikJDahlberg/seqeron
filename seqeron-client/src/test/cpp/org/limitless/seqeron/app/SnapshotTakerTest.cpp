// A façade's side of snapshot rounds (doc/snapshot.md §4), driven through its Actions. Case for case with the
// Java SnapshotTakerTest.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "org/limitless/seqeron/app/detail/SnapshotTaker.hpp"

namespace org::limitless::seqeron::app::detail {
namespace {

const protocol::SnapshotHeader HEADER{ 3, 1, std::nullopt };

// Encodes `records` records of 1000 bytes, each filled with its index, then answers 0.
struct State final : SnapshotListener
{
    std::uint32_t formatVersion() const override
    {
        return 5;
    }

    std::int32_t onSnapshot(const std::span<std::uint8_t> buffer, const std::int32_t recordIndex) override
    {
        if (recordIndex == 0)
        {
            ++serialized;
        }
        if (recordIndex == records)
        {
            return 0;
        }
        if (badLength != 0)
        {
            return badLength;
        }
        std::fill_n(buffer.begin(), 1000, static_cast<std::uint8_t>(recordIndex));
        return 1000;
    }

    void onRestore(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
    {
        restored.push_back(std::to_string(recordIndex) + ":" + std::to_string(record.size()) + ":" +
                           std::to_string(record[0]));
    }

    std::int32_t records = 2;
    int serialized = 0;
    std::int32_t badLength = 0;
    std::vector<std::string> restored;
};

// Records what was placed; declines once `budget` frames have gone.
struct Frames
{
    protocol::Publish publishChunk(const std::int64_t round, const std::int32_t chunkIndex,
                                   const std::span<const std::uint8_t> record)
    {
        if (budget == 0)
        {
            return protocol::Publish::Declined;
        }
        --budget;
        EXPECT_EQ(static_cast<std::int32_t>(chunks.size()), chunkIndex) << "chunks go out in order, each once";
        chunks.emplace_back(record.begin(), record.end());
        chunkRounds.push_back(round);
        return protocol::Publish::Published;
    }

    protocol::Publish publishEnd(const std::int64_t round, const std::int32_t chunkCount, const std::uint64_t length,
                                 const std::uint32_t crc32c, const std::uint32_t formatVersion)
    {
        if (budget == 0)
        {
            return protocol::Publish::Declined;
        }
        --budget;
        ends.push_back({ round, chunkCount, static_cast<std::int64_t>(length), crc32c, formatVersion });
        return protocol::Publish::Published;
    }

    struct End
    {
        std::int64_t round;
        std::int32_t chunkCount;
        std::int64_t length;
        std::uint32_t crc32c;
        std::uint32_t formatVersion;
    };

    std::vector<std::vector<std::uint8_t>> chunks;
    std::vector<std::int64_t> chunkRounds;
    std::vector<End> ends;
    int budget = 1 << 30;
};

std::vector<std::uint8_t> filled(const std::uint8_t value)
{
    return std::vector<std::uint8_t>(1000, value);
}

TEST(SnapshotTaker, WithoutAListenerOrWithTheRowOffAReplicaSerializesAndSubmitsNothing)
{
    State state;
    Frames frames;
    SnapshotTaker noListener{ nullptr };
    noListener.participating(true);
    EXPECT_FALSE(noListener.isParticipating());

    SnapshotTaker rowOff{ &state };
    rowOff.participating(false);
    rowOff.onSnapshotStarted(1, HEADER, true);
    EXPECT_EQ(0, state.serialized);
    EXPECT_EQ(0, rowOff.submit(frames));
    EXPECT_TRUE(rowOff.onSnapshotEnd(1, 3, 2018, 0)) << "nothing serialized, nothing to disagree with";
}

TEST(SnapshotTaker, ThePublisherSubmitsTheHeaderItsRecordsInOrderThenAnEndItsOwnFramesValidate)
{
    State state;
    Frames frames;
    SnapshotTaker taker{ &state };
    taker.participating(true);
    taker.onSnapshotStarted(7, HEADER, true);
    EXPECT_EQ(1, state.serialized);
    EXPECT_EQ(4, taker.submit(frames)) << "two records, the header and the end";
    EXPECT_FALSE(taker.isPublishing());

    ASSERT_EQ(3u, frames.chunks.size());
    EXPECT_EQ(HEADER, protocol::SnapshotHeader::decode(frames.chunks[0].data(), frames.chunks[0].size()));
    EXPECT_EQ(filled(1), frames.chunks[2]);
    ASSERT_EQ(1u, frames.ends.size());
    const Frames::End& end = frames.ends[0];
    EXPECT_EQ(7, end.round);
    EXPECT_EQ(3, end.chunkCount);
    EXPECT_EQ(2018, end.length);
    EXPECT_EQ(5u, end.formatVersion) << "the listener's formatVersion";

    protocol::SnapshotValidator validator;
    validator.reset(7);
    for (std::size_t i = 0; i < frames.chunks.size(); ++i)
    {
        validator.onChunk(7, static_cast<std::int32_t>(i), frames.chunks[i].data(), frames.chunks[i].size());
    }
    EXPECT_EQ(protocol::SnapshotValidator::State::Complete, validator.onEnd(7, end.chunkCount, end.length, end.crc32c));
    EXPECT_TRUE(taker.onSnapshotEnd(7, end.chunkCount, end.length, end.crc32c)) << "its own end agrees with it";
}

TEST(SnapshotTaker, ADeclinedFrameIsPlacedAgainNextCycleAndNoCyclePlacesMoreThanItsBudget)
{
    State state;
    state.records = 40;
    Frames frames;
    SnapshotTaker taker{ &state };
    taker.participating(true);
    taker.onSnapshotStarted(2, HEADER, true);
    frames.budget = 5;
    EXPECT_EQ(5, taker.submit(frames));
    frames.budget = 1 << 30;
    EXPECT_EQ(SnapshotTaker::MAX_CHUNKS_PER_CYCLE, taker.submit(frames));
    while (taker.isPublishing())
    {
        taker.submit(frames);
    }
    EXPECT_EQ(41u, frames.chunks.size()) << "the header and forty records, none lost or repeated";
    EXPECT_EQ(1u, frames.ends.size());
}

TEST(SnapshotTaker, AReplicaThatDoesNotPublishSubmitsNothingAndComparesTheSourcesEndWithItsOwn)
{
    State state;
    Frames frames;
    SnapshotTaker publisher{ &state };
    publisher.participating(true);
    publisher.onSnapshotStarted(4, HEADER, true);
    publisher.submit(frames);
    const Frames::End end = frames.ends[0];

    SnapshotTaker follower{ &state };
    follower.participating(true);
    follower.onSnapshotStarted(4, HEADER, false);
    Frames none;
    EXPECT_EQ(0, follower.submit(none));
    EXPECT_TRUE(follower.onSnapshotEnd(4, end.chunkCount, end.length, end.crc32c));

    SnapshotTaker diverged{ &state };
    diverged.participating(true);
    state.records = 3;
    diverged.onSnapshotStarted(4, HEADER, false);
    EXPECT_FALSE(diverged.onSnapshotEnd(4, end.chunkCount, end.length, end.crc32c))
        << "different state, different records";

    SnapshotTaker otherRound{ &state };
    otherRound.participating(true);
    otherRound.onSnapshotStarted(5, HEADER, false);
    EXPECT_TRUE(otherRound.onSnapshotEnd(4, 99, 0, 0)) << "a late end of an earlier round is no evidence";
}

TEST(SnapshotTaker, APublisherThatLosesTheRoleStopsForGoodWithNoEnd)
{
    State state;
    state.records = 40;
    Frames frames;
    SnapshotTaker taker{ &state };
    taker.participating(true);
    taker.onSnapshotStarted(6, HEADER, true);
    taker.submit(frames);
    taker.stopPublishing();
    EXPECT_EQ(0, taker.submit(frames));
    EXPECT_EQ(static_cast<std::size_t>(SnapshotTaker::MAX_CHUNKS_PER_CYCLE), frames.chunks.size());
    EXPECT_TRUE(frames.ends.empty());
}

TEST(SnapshotTaker, AListenerAnsweringALengthNoRecordCanHaveDropsTheRoundWithNothingSubmitted)
{
    for (const std::int32_t length : { -1, 1303 })
    {
        State state;
        state.badLength = length;
        Frames frames;
        SnapshotTaker taker{ &state };
        taker.participating(true);
        taker.onSnapshotStarted(3, HEADER, true);
        EXPECT_FALSE(taker.isPublishing());
        EXPECT_EQ(0, taker.submit(frames));
        EXPECT_TRUE(taker.onSnapshotEnd(3, 3, 2018, 0)) << "nothing serialized, nothing to disagree with";
    }
}

TEST(SnapshotTaker, ANewRoundSupersedesOneStillBeingSubmittedTheOldGetsNoEndTheNewStartsAtZero)
{
    State state;
    state.records = 40;
    Frames frames;
    SnapshotTaker taker{ &state };
    taker.participating(true);
    taker.onSnapshotStarted(8, HEADER, true);
    taker.submit(frames);
    state.records = 1;
    taker.onSnapshotStarted(9, HEADER, true);
    Frames next;
    while (taker.isPublishing())
    {
        taker.submit(next);
    }
    EXPECT_EQ((std::vector<std::int64_t>{ 9, 9 }), next.chunkRounds);
    ASSERT_EQ(1u, next.ends.size());
    EXPECT_EQ(9, next.ends[0].round);
    EXPECT_TRUE(frames.ends.empty()) << "round 8 never ended";
}

TEST(SnapshotTaker, ARestoreReadsOnlyThisBuildsFormatMakesTheSourceTakePartAndHandsItsRecordsOn)
{
    State state;
    SnapshotTaker taker{ &state };
    EXPECT_TRUE(taker.supportsFormatVersion(5));
    EXPECT_FALSE(taker.supportsFormatVersion(6));

    taker.onSnapshotHeader(HEADER);
    EXPECT_TRUE(taker.isParticipating()) << "its topology row lies before the cut";
    const std::vector<std::uint8_t> record = filled(1);
    taker.onSnapshotRecord(record, 0);
    EXPECT_EQ((std::vector<std::string>{ "0:1000:1" }), state.restored);
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
