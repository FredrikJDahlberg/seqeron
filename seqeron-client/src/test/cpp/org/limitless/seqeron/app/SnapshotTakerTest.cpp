// A façade's side of snapshot rounds (doc/snapshot.md §4), driven through its Actions. Case for case with the
// Java SnapshotTakerTest.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "org/limitless/seqeron/app/detail/SnapshotTaker.hpp"
#include "org/limitless/seqeron/helpers/TempDirectory.hpp"

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

// Records the ends placed; declines once `budget` have gone.
struct Frames
{
    protocol::Publish publishEnd(const std::int64_t round, const std::int32_t recordCount, const std::uint64_t length,
                                 const std::uint32_t crc32c, const std::uint32_t formatVersion)
    {
        if (budget == 0)
        {
            return protocol::Publish::Declined;
        }
        --budget;
        ends.push_back({ round, recordCount, static_cast<std::int64_t>(length), crc32c, formatVersion });
        return protocol::Publish::Published;
    }

    struct End
    {
        std::int64_t round;
        std::int32_t recordCount;
        std::int64_t length;
        std::uint32_t crc32c;
        std::uint32_t formatVersion;
    };

    std::vector<End> ends;
    int budget = 1 << 30;
};

// Instances, each with a directory of its own under one test directory.
struct Instances
{
    // A participating instance; its store is the one stores.back() holds.
    SnapshotTaker& participating()
    {
        SnapshotTaker& taker = instance();
        taker.participating(true);
        return taker;
    }

    SnapshotTaker& instance()
    {
        stores.push_back(
            std::make_unique<replayer::client::SnapshotStore>(directory.path() / std::to_string(stores.size())));
        takers.push_back(std::make_unique<SnapshotTaker>(&state, stores.back().get()));
        return *takers.back();
    }

    replayer::client::SnapshotStore& lastStore()
    {
        return *stores.back();
    }

    helpers::TempDirectory directory;
    State state;
    Frames frames;
    std::vector<std::unique_ptr<replayer::client::SnapshotStore>> stores;
    std::vector<std::unique_ptr<SnapshotTaker>> takers;
};

std::vector<std::uint8_t> filled(const int value)
{
    return std::vector<std::uint8_t>(1000, static_cast<std::uint8_t>(value));
}

std::vector<std::vector<std::uint8_t>> readAll(replayer::client::SnapshotStore::Reader& reader)
{
    std::vector<std::vector<std::uint8_t>> records;
    std::span<const std::uint8_t> record;
    std::int32_t length;
    while ((length = reader.next(record)) >= 0)
    {
        records.emplace_back(record.begin(), record.end());
    }
    EXPECT_EQ(replayer::client::SnapshotStore::Reader::END, length);
    return records;
}

TEST(SnapshotTaker, WithoutAListenerOrWithTheRowOffAReplicaSerializesWritesAndSubmitsNothing)
{
    Instances t;
    SnapshotTaker noListener{ nullptr, nullptr };
    noListener.participating(true);
    EXPECT_FALSE(noListener.isParticipating());

    SnapshotTaker& rowOff = t.instance();
    rowOff.participating(false);
    rowOff.onSnapshotStarted(1, HEADER, true);
    EXPECT_EQ(0, t.state.serialized);
    EXPECT_EQ(-1, t.lastStore().latestRound());
    EXPECT_EQ(0, rowOff.submit(t.frames));
    EXPECT_TRUE(rowOff.onSnapshotEnd(1, 3, 2018, 0)) << "nothing serialized, nothing to disagree with";
}

TEST(SnapshotTaker, TheRoundsFileHoldsTheHeaderAndTheRecordsThePublisherSubmitsTheEndItMatches)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(7, HEADER, true);
    EXPECT_EQ(1, t.state.serialized);
    EXPECT_TRUE(taker.isPublishing());
    EXPECT_EQ(1, taker.submit(t.frames)) << "the end, once";
    EXPECT_FALSE(taker.isPublishing());
    EXPECT_EQ(0, taker.submit(t.frames));

    ASSERT_EQ(1u, t.frames.ends.size());
    const Frames::End end = t.frames.ends[0];
    EXPECT_EQ(7, end.round);
    EXPECT_EQ(3, end.recordCount);
    EXPECT_EQ(2018, end.length);
    EXPECT_EQ(5U, end.formatVersion) << "the listener's formatVersion";

    std::optional<replayer::client::SnapshotStore::Reader> reader = t.lastStore().open(7);
    ASSERT_TRUE(reader.has_value());
    EXPECT_EQ(3, reader->recordCount());
    EXPECT_EQ(static_cast<std::uint64_t>(end.length), reader->length());
    EXPECT_EQ(end.crc32c, reader->crc32c());
    const auto records = readAll(*reader);
    ASSERT_EQ(3u, records.size());
    EXPECT_EQ(HEADER, protocol::SnapshotHeader::decode(records[0].data(), records[0].size()));
    EXPECT_EQ(filled(0), records[1]);
    EXPECT_EQ(filled(1), records[2]);
    EXPECT_TRUE(taker.onSnapshotEnd(7, end.recordCount, end.length, end.crc32c)) << "its own end agrees with it";
}

TEST(SnapshotTaker, ADeclinedEndIsPlacedAgainOnTheNextCycle)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(2, HEADER, true);
    t.frames.budget = 0;
    EXPECT_EQ(0, taker.submit(t.frames));
    EXPECT_TRUE(taker.isPublishing());
    t.frames.budget = 1 << 30;
    EXPECT_EQ(1, taker.submit(t.frames));
    EXPECT_EQ(1u, t.frames.ends.size());
}

TEST(SnapshotTaker, AReplicaThatDoesNotPublishWritesItsFileSubmitsNothingAndComparesTheSourcesEnd)
{
    Instances t;
    SnapshotTaker& publisher = t.participating();
    publisher.onSnapshotStarted(4, HEADER, true);
    publisher.submit(t.frames);
    const Frames::End end = t.frames.ends[0];

    SnapshotTaker& follower = t.participating();
    follower.onSnapshotStarted(4, HEADER, false);
    EXPECT_EQ(4, t.lastStore().latestRound());
    Frames none;
    EXPECT_EQ(0, follower.submit(none));
    EXPECT_TRUE(follower.onSnapshotEnd(4, end.recordCount, end.length, end.crc32c));

    SnapshotTaker& diverged = t.participating();
    t.state.records = 3;
    diverged.onSnapshotStarted(4, HEADER, false);
    EXPECT_FALSE(diverged.onSnapshotEnd(4, end.recordCount, end.length, end.crc32c))
        << "different state, different records";

    SnapshotTaker& otherRound = t.participating();
    otherRound.onSnapshotStarted(5, HEADER, false);
    EXPECT_TRUE(otherRound.onSnapshotEnd(4, 99, 0, 0)) << "a late end of an earlier round is no evidence";
}

TEST(SnapshotTaker, AnEndThatMatchesMakesItsRoundTheOldestFileKeptOneThatDoesNotDeletesNothing)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(1, HEADER, false);
    taker.onSnapshotStarted(2, HEADER, true);
    taker.submit(t.frames);
    taker.onSnapshotStarted(3, HEADER, false);
    replayer::client::SnapshotStore& store = t.lastStore();
    EXPECT_EQ(3, store.latestRound());

    EXPECT_FALSE(taker.onSnapshotEnd(3, 99, 0, 0));
    EXPECT_EQ(1, store.latestRound(2)) << "a diverged instance keeps what it had";

    taker.onSnapshotStarted(4, HEADER, false);
    const Frames::End end = t.frames.ends[0];
    EXPECT_TRUE(taker.onSnapshotEnd(4, end.recordCount, end.length, end.crc32c));
    EXPECT_EQ(4, store.latestRound());
    EXPECT_EQ(-1, store.latestRound(4)) << "rounds 1 to 3 are gone";
}

TEST(SnapshotTaker, APublisherThatLosesTheRoleStopsForGoodWithNoEnd)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(6, HEADER, true);
    taker.stopPublishing();
    EXPECT_EQ(0, taker.submit(t.frames));
    EXPECT_TRUE(t.frames.ends.empty());
    EXPECT_EQ(6, t.lastStore().latestRound()) << "its file stays";
}

TEST(SnapshotTaker, AListenerAnsweringALengthNoRecordCanHaveDropsTheRoundWithNoFileAndNoEnd)
{
    for (const std::int32_t length : { -1, 1303 })
    {
        Instances t;
        t.state.badLength = length;
        SnapshotTaker& taker = t.participating();
        taker.onSnapshotStarted(3, HEADER, true);
        EXPECT_FALSE(taker.isPublishing());
        EXPECT_EQ(0, taker.submit(t.frames));
        EXPECT_EQ(-1, t.lastStore().latestRound());
        EXPECT_TRUE(taker.onSnapshotEnd(3, 3, 2018, 0)) << "nothing serialized, nothing to disagree with";
    }
}

TEST(SnapshotTaker, ANewRoundSupersedesOneWhoseEndIsNotYetPlacedOnlyTheNewOneEnds)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(8, HEADER, true);
    taker.onSnapshotStarted(9, HEADER, true);
    EXPECT_EQ(1, taker.submit(t.frames));
    ASSERT_EQ(1u, t.frames.ends.size());
    EXPECT_EQ(9, t.frames.ends[0].round);
    EXPECT_TRUE(taker.onSnapshotEnd(8, 99, 0, 0)) << "nothing held for round 8 any more";
}

TEST(SnapshotTaker, ARoundStartedAfterTheRowTurnedOffStillEndsTheOneBeingSubmitted)
{
    Instances t;
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(8, HEADER, true);
    taker.participating(false);
    taker.onSnapshotStarted(9, HEADER, true);
    EXPECT_FALSE(taker.isPublishing());
    EXPECT_EQ(0, taker.submit(t.frames));
    EXPECT_TRUE(t.frames.ends.empty()) << "round 8 never ended";
    EXPECT_TRUE(taker.onSnapshotEnd(8, 99, 0, 0)) << "nothing held for round 8 any more";
}

TEST(SnapshotTaker, AHeaderLongerThanARecordDropsTheRound)
{
    Instances t;
    protocol::SnapshotGatewayState rows{ 9, 10, 42, {} };
    rows.rows.assign(35, protocol::SnapshotGatewayRow{ 10, 0, "GW" });
    SnapshotTaker& taker = t.participating();
    taker.onSnapshotStarted(3, protocol::SnapshotHeader{ 7, 2, rows }, true);
    EXPECT_EQ(0, t.state.serialized) << "35 rows outgrow a record";
    EXPECT_FALSE(taker.isPublishing());
    EXPECT_FALSE(t.lastStore().open(3).has_value());
}

TEST(SnapshotTaker, ARestoreReadsOnlyThisBuildsFormatMakesTheSourceTakePartAndHandsItsRecordsOn)
{
    Instances t;
    SnapshotTaker& taker = t.instance();
    EXPECT_TRUE(taker.supportsFormatVersion(5));
    EXPECT_FALSE(taker.supportsFormatVersion(6));

    taker.onSnapshotHeader(HEADER);
    EXPECT_TRUE(taker.isParticipating()) << "its topology row lies before the cut";
    const std::vector<std::uint8_t> record = filled(1);
    taker.onSnapshotRecord(record, 0);
    EXPECT_EQ((std::vector<std::string>{ "0:1000:1" }), t.state.restored);
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
