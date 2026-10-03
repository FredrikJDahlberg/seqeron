// An instance's own snapshot files (doc/snapshot.md §4). Case for case with the Java SnapshotStoreTest, whose
// golden trailer this shares, so a file one language writes is one the other reads.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "org/limitless/seqeron/helpers/TempDirectory.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"

namespace org::limitless::seqeron::replayer::client {
namespace {

using Bytes = std::vector<std::uint8_t>;

// Round 3's trailer over the application header and the 3000-byte body as three records, formatVersion 1.
const std::string GOLDEN_TRAILER = "0300000000000000"
                                   "ca0b000000000000"
                                   "04000000"
                                   "6bf5246c"
                                   "01000000"
                                   "534e4150";

// The application header, then the 3000-byte body as records of 1000, 1302 and 698 bytes.
std::vector<Bytes> records()
{
    Bytes header(protocol::SnapshotHeader::APPLICATION_LENGTH);
    protocol::SnapshotHeader{ 7, 2, std::nullopt }.encode(header.data());
    Bytes body(3000);
    for (std::size_t i = 0; i < body.size(); ++i)
    {
        body[i] = static_cast<std::uint8_t>(i * 31 + 7);
    }
    return { header, Bytes(body.begin(), body.begin() + 1000), Bytes(body.begin() + 1000, body.begin() + 2302),
             Bytes(body.begin() + 2302, body.end()) };
}

void write(SnapshotStore& store, const std::int64_t round, const std::vector<Bytes>& records)
{
    std::uint64_t length = 0;
    std::uint32_t crc = 0;
    store.begin(round);
    for (const Bytes& record : records)
    {
        store.append(record);
        length += record.size();
        crc = protocol::crc32c(record.data(), record.size(), crc);
    }
    store.commit(static_cast<std::int32_t>(records.size()), length, crc, 1);
}

Bytes readFile(const std::filesystem::path& path)
{
    std::ifstream in(path, std::ios::binary);
    return Bytes(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void writeFile(const std::filesystem::path& path, const Bytes& bytes)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

std::string hex(const Bytes::const_iterator first, const Bytes::const_iterator last)
{
    static constexpr char DIGITS[] = "0123456789abcdef";
    std::string out;
    for (auto at = first; at != last; ++at)
    {
        out += DIGITS[*at >> 4];
        out += DIGITS[*at & 0xF];
    }
    return out;
}

// Each record's length in order, then what ends the read.
std::vector<std::int32_t> readAll(std::optional<SnapshotStore::Reader> reader)
{
    std::vector<std::int32_t> lengths;
    std::span<const std::uint8_t> record;
    std::int32_t length;
    do
    {
        length = reader->next(record);
        lengths.push_back(length);
    } while (length >= 0);
    return lengths;
}

TEST(SnapshotStore, ACommittedRoundReadsBackRecordByRecordItsTrailerTheSharedGoldenBytes)
{
    helpers::TempDirectory directory;
    SnapshotStore store(directory.path());
    const std::vector<Bytes> all = records();
    write(store, 3, all);

    const Bytes file = readFile(directory.path() / "3.snapshot");
    ASSERT_EQ(3018 + 2 * 4 + SnapshotStore::TRAILER_LENGTH, file.size());
    EXPECT_EQ(GOLDEN_TRAILER, hex(file.end() - SnapshotStore::TRAILER_LENGTH, file.end()));

    std::optional<SnapshotStore::Reader> reader = store.open(3);
    ASSERT_TRUE(reader.has_value());
    EXPECT_EQ(4, reader->recordCount());
    EXPECT_EQ(3018U, reader->length());
    EXPECT_EQ(0x6c24f56bU, reader->crc32c());
    EXPECT_EQ(1U, reader->formatVersion());
    std::span<const std::uint8_t> record;
    for (const Bytes& expected : all)
    {
        ASSERT_EQ(static_cast<std::int32_t>(expected.size()), reader->next(record));
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), record.begin(), record.end()));
    }
    EXPECT_EQ(SnapshotStore::Reader::END, reader->next(record));
}

TEST(SnapshotStore, OnlyACommittedWriteCountsAndTheNewestRoundBelowABoundIsFound)
{
    helpers::TempDirectory directory;
    SnapshotStore store(directory.path());
    const Bytes eight(8);
    store.begin(5);
    store.append(eight);
    store.abandon();
    EXPECT_EQ(-1, store.latestRound()) << "an abandoned write leaves no file";

    write(store, 2, records());
    write(store, 4, records());
    store.begin(6); // never committed, as a crash leaves it
    store.append(eight);

    EXPECT_EQ(4, store.latestRound());
    EXPECT_EQ(2, store.latestRound(4));
    EXPECT_EQ(-1, store.latestRound(2));
    EXPECT_FALSE(store.open(6).has_value());
    EXPECT_FALSE(store.open(7).has_value()) << "no such round";
}

TEST(SnapshotStore, AFileCutShortOrOfAnotherRoundDoesNotOpenRecordsThatFailItsTrailerAreDamaged)
{
    helpers::TempDirectory directory;
    SnapshotStore store(directory.path());
    write(store, 2, records());
    const std::filesystem::path file = directory.path() / "2.snapshot";
    const Bytes intact = readFile(file);
    const std::size_t record1 = 2 + protocol::SnapshotHeader::APPLICATION_LENGTH;
    constexpr auto DAMAGED = SnapshotStore::Reader::DAMAGED;

    std::filesystem::copy_file(file, directory.path() / "3.snapshot");
    EXPECT_FALSE(store.open(3).has_value()) << "its trailer names round 2";

    writeFile(file, Bytes(intact.begin(), intact.end() - 1));
    EXPECT_FALSE(store.open(2).has_value()) << "cut short: no trailer";

    Bytes flipped = intact;
    flipped[record1 + 2 + 5] ^= 1; // a body byte of record 1
    writeFile(file, flipped);
    EXPECT_EQ((std::vector<std::int32_t>{ 18, 1000, 1302, 698, DAMAGED }), readAll(store.open(2)));

    Bytes oversized = intact;
    oversized[record1 + 1] = 0xFF; // record 1's length, past the records the trailer bounds
    writeFile(file, oversized);
    EXPECT_EQ((std::vector<std::int32_t>{ 18, DAMAGED }), readAll(store.open(2)));

    Bytes miscounted = intact;
    miscounted[intact.size() - SnapshotStore::TRAILER_LENGTH + 16] = 5; // a recordCount the records do not reach
    writeFile(file, miscounted);
    EXPECT_EQ((std::vector<std::int32_t>{ 18, 1000, 1302, 698, DAMAGED }), readAll(store.open(2)));
}

TEST(SnapshotStore, DeletingBeforeARoundKeepsItAndEveryNewerOneAndClearsAWriteACrashLeftBehind)
{
    helpers::TempDirectory directory;
    {
        SnapshotStore crashed(directory.path());
        write(crashed, 1, records());
        write(crashed, 2, records());
        write(crashed, 3, records());
        crashed.begin(4);
    }

    SnapshotStore(directory.path()).deleteBefore(2);

    std::vector<std::string> names;
    for (const auto& entry : std::filesystem::directory_iterator(directory.path()))
    {
        names.push_back(entry.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    EXPECT_EQ((std::vector<std::string>{ "2.snapshot", "3.snapshot" }), names);
}

} // namespace
} // namespace org::limitless::seqeron::replayer::client
