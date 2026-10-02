// One instance's records of a round: the façade's header as record 0, then the listener's. Case for case with
// the Java SnapshotRecordsTest, whose golden CRC this shares.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "org/limitless/seqeron/app/detail/SnapshotRecords.hpp"

namespace org::limitless::seqeron::app::detail {
namespace {

const protocol::SnapshotHeader HEADER{ 7, 2, std::nullopt };

std::vector<std::uint8_t> body()
{
    std::vector<std::uint8_t> bytes(3000);
    for (std::size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<std::uint8_t>(i * 31 + 7);
    }
    return bytes;
}

std::vector<std::vector<std::uint8_t>> records(SnapshotRecords& writer)
{
    std::vector<std::vector<std::uint8_t>> all;
    while (const auto record = writer.nextRecord())
    {
        all.emplace_back(record->begin(), record->end());
    }
    return all;
}

TEST(SnapshotRecords, RecordsFollowTheHeader)
{
    SnapshotRecords writer;
    ASSERT_TRUE(writer.reset(HEADER, true));
    EXPECT_EQ(1, writer.chunkCount());
    EXPECT_EQ(protocol::SnapshotHeader::APPLICATION_LENGTH, writer.length());

    const std::vector<std::uint8_t> bytes = body();
    writer.append(bytes.data(), 1000);
    writer.append(bytes.data() + 1000, 1302);
    writer.append(bytes.data() + 2302, 698);
    EXPECT_EQ(4, writer.chunkCount());
    EXPECT_EQ(3018U, writer.length());
    EXPECT_EQ(0x6c24f56bU, writer.crc32c()) << "the same value the Java SnapshotRecordsTest asserts";

    const auto all = records(writer);
    ASSERT_EQ(4U, all.size());
    EXPECT_EQ(HEADER, protocol::SnapshotHeader::decode(all[0].data(), all[0].size()));
    EXPECT_EQ(1000U, all[1].size());
    EXPECT_EQ(1302U, all[2].size());
    EXPECT_EQ(698U, all[3].size());
}

TEST(SnapshotRecords, OversizedHeaderIsRefused)
{
    protocol::SnapshotGatewayState state{ 9, 10, 42, {} };
    state.rows.assign(35, protocol::SnapshotGatewayRow{ 10, 0, "GW" });
    SnapshotRecords writer;
    EXPECT_FALSE(writer.reset(protocol::SnapshotHeader{ 7, 2, state }, true)) << "35 rows outgrow a record";
}

TEST(SnapshotRecords, NotRetainedKeepsOnlyTheTotals)
{
    SnapshotRecords kept;
    SnapshotRecords counted;
    kept.reset(HEADER, true);
    counted.reset(HEADER, false);
    const std::vector<std::uint8_t> bytes = body();
    for (SnapshotRecords* writer : { &kept, &counted })
    {
        writer->append(bytes.data(), 1302);
        writer->append(bytes.data() + 1302, 1302);
    }
    EXPECT_EQ(kept.chunkCount(), counted.chunkCount());
    EXPECT_EQ(kept.length(), counted.length());
    EXPECT_EQ(kept.crc32c(), counted.crc32c());
    EXPECT_TRUE(records(counted).empty());
}

TEST(SnapshotRecords, RecordsSpanSegments)
{
    SnapshotRecords writer;
    writer.reset(HEADER, true);
    std::vector<std::vector<std::uint8_t>> appended;
    std::uint64_t total = protocol::SnapshotHeader::APPLICATION_LENGTH;
    for (std::size_t i = 0; total < 3 * SnapshotRecords::SEGMENT_LENGTH; ++i)
    {
        std::vector<std::uint8_t> record(i % 7 == 0 ? 1302 : (i * 37) % 1303);
        for (std::size_t j = 0; j < record.size(); ++j)
        {
            record[j] = static_cast<std::uint8_t>(i + j);
        }
        writer.append(record.data(), record.size());
        total += record.size();
        appended.push_back(std::move(record));
    }

    const auto all = records(writer);
    ASSERT_EQ(appended.size() + 1, all.size());
    protocol::SnapshotValidator validator;
    validator.reset(1);
    for (std::size_t i = 0; i < all.size(); ++i)
    {
        if (i > 0)
        {
            EXPECT_EQ(appended[i - 1], all[i]) << "record " << i;
        }
        validator.onChunk(1, static_cast<std::int32_t>(i), all[i].data(), all[i].size());
    }
    EXPECT_EQ(protocol::SnapshotValidator::State::Complete,
              validator.onEnd(1, writer.chunkCount(), static_cast<std::int64_t>(writer.length()), writer.crc32c()));

    writer.rewind();
    EXPECT_EQ(all.size(), records(writer).size()) << "rewind reads them again";
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
