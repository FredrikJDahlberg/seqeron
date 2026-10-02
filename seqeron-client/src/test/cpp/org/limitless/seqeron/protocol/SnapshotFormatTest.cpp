// The snapshot byte format (doc/snapshot.md §2, §5, §6). The golden vectors are the Java
// SnapshotFormatTest's, computed by a third implementation, so the two languages agree with each other and
// not merely with themselves. Case for case with the Java twin, in the same order.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::protocol {
namespace {

using State = SnapshotValidator::State;

std::vector<std::uint8_t> hex(const std::string& text)
{
    std::vector<std::uint8_t> bytes;
    for (std::size_t i = 0; i < text.size(); i += 2)
    {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(text.substr(i, 2), nullptr, 16)));
    }
    return bytes;
}

std::string zeros(const std::size_t count)
{
    return std::string(count * 2, '0');
}

const std::string APPLICATION_HEADER = "010012000000070000000000000002000000";
const std::string GATEWAY_HEADER = "01006a000000070000000000000002000000090000000a0000002a0000000200" +
                                   std::string("0a0000000047572d41") + zeros(28) + "0b0000000147572d42" + zeros(28);
constexpr std::uint32_t APPLICATION_SNAPSHOT_CRC = 0x6c24f56bU;

std::vector<std::uint8_t> body()
{
    std::vector<std::uint8_t> bytes(3000);
    for (std::size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<std::uint8_t>(i * 31 + 7);
    }
    return bytes;
}

SnapshotHeader gatewayHeader()
{
    return SnapshotHeader{ 7, 2, SnapshotGatewayState{ 9, 10, 42, { { 10, 0, "GW-A" }, { 11, 1, "GW-B" } } } };
}

std::vector<std::uint8_t> encode(const SnapshotHeader& header)
{
    std::vector<std::uint8_t> bytes(header.encodedLength());
    EXPECT_EQ(bytes.size(), header.encode(bytes.data()));
    return bytes;
}

std::vector<std::uint8_t> snapshot()
{
    std::vector<std::uint8_t> bytes = encode(SnapshotHeader{ 7, 2, std::nullopt });
    const std::vector<std::uint8_t> tail = body();
    bytes.insert(bytes.end(), tail.begin(), tail.end());
    return bytes;
}

std::size_t recordLength(const std::vector<std::uint8_t>& bytes, const std::int32_t index)
{
    const std::size_t offset = static_cast<std::size_t>(index) * MAX_SNAPSHOT_RECORD_LENGTH;
    return std::min<std::size_t>(MAX_SNAPSHOT_RECORD_LENGTH, bytes.size() - offset);
}

// Feeds the 1302-byte records named by order, the last one short, then the matching end.
State feed(SnapshotValidator& validator, const std::vector<std::uint8_t>& bytes, const std::int64_t round,
           const std::vector<std::int32_t>& order)
{
    validator.reset(round);
    for (const std::int32_t index : order)
    {
        validator.onChunk(round, index, bytes.data() + index * MAX_SNAPSHOT_RECORD_LENGTH, recordLength(bytes, index));
    }
    return validator.onEnd(round, 3, static_cast<std::int64_t>(bytes.size()), crc32c(bytes.data(), bytes.size()));
}

State chunksThenEnd(const std::vector<std::uint8_t>& bytes, const std::int32_t chunkCount, const std::int64_t length,
                    const std::uint32_t crc)
{
    SnapshotValidator validator;
    validator.reset(4);
    for (std::int32_t index = 0; index < 3; ++index)
    {
        validator.onChunk(4, index, bytes.data() + index * MAX_SNAPSHOT_RECORD_LENGTH, recordLength(bytes, index));
    }
    return validator.onEnd(4, chunkCount, length, crc);
}

TEST(SnapshotFormat, CrcAndRecordSize)
{
    const std::string check = "123456789";
    EXPECT_EQ(0xE3069283U, crc32c(reinterpret_cast<const std::uint8_t*>(check.data()), check.size()));
    EXPECT_EQ(1302, MAX_SNAPSHOT_RECORD_LENGTH);
}

TEST(SnapshotFormat, HeadersEncodeToTheGoldenBytes)
{
    const SnapshotHeader application{ 7, 2, std::nullopt };
    EXPECT_EQ(hex(APPLICATION_HEADER), encode(application));
    EXPECT_EQ(hex(GATEWAY_HEADER), encode(gatewayHeader()));

    for (const SnapshotHeader& header : { application, gatewayHeader() })
    {
        const std::vector<std::uint8_t> bytes = encode(header);
        EXPECT_EQ(header, SnapshotHeader::decode(bytes.data(), bytes.size()));
    }
}

TEST(SnapshotFormat, ForeignOrShortHeadersDoNotDecode)
{
    std::vector<std::uint8_t> gateway = encode(gatewayHeader());
    EXPECT_FALSE(SnapshotHeader::decode(gateway.data(), gateway.size() - 1)) << "truncated";
    EXPECT_FALSE(SnapshotHeader::decode(gateway.data(), 17)) << "shorter than any header";

    gateway[0] = 2;
    EXPECT_EQ(2, SnapshotHeader::version(gateway.data(), gateway.size()));
    EXPECT_FALSE(SnapshotHeader::decode(gateway.data(), gateway.size())) << "a later headerVersion";

    std::vector<std::uint8_t> rows = encode(gatewayHeader());
    rows[30] = 3;
    EXPECT_FALSE(SnapshotHeader::decode(rows.data(), rows.size())) << "rowCount disagrees with length";
}

TEST(SnapshotFormat, ChunksInOrderAreValid)
{
    const std::vector<std::uint8_t> bytes = snapshot();
    EXPECT_EQ(APPLICATION_SNAPSHOT_CRC, crc32c(bytes.data(), bytes.size()));

    SnapshotValidator validator;
    EXPECT_EQ(State::Complete, feed(validator, bytes, 4, { 0, 1, 2 }));
    EXPECT_EQ(bytes.size(), validator.length());
}

TEST(SnapshotFormat, MisorderedChunksInvalidate)
{
    const std::vector<std::uint8_t> bytes = snapshot();
    SnapshotValidator first;
    EXPECT_EQ(State::Invalid, feed(first, bytes, 4, { 0, 2, 1 }));
    SnapshotValidator second;
    EXPECT_EQ(State::Invalid, feed(second, bytes, 4, { 0, 1, 1, 2 }));
    SnapshotValidator third;
    EXPECT_EQ(State::Invalid, feed(third, bytes, 4, { 0, 1 }));

    SnapshotValidator oversized;
    oversized.reset(4);
    EXPECT_EQ(State::Invalid, oversized.onChunk(4, 0, bytes.data(), 1303));

    SnapshotValidator validator;
    feed(validator, bytes, 4, { 1 });
    EXPECT_EQ(State::Invalid, validator.onChunk(4, 0, bytes.data(), 1302));
}

TEST(SnapshotFormat, EndMustAgreeWithTheChunks)
{
    const std::vector<std::uint8_t> bytes = snapshot();
    const std::uint32_t crc = crc32c(bytes.data(), bytes.size());
    const auto length = static_cast<std::int64_t>(bytes.size());
    EXPECT_EQ(State::Invalid, chunksThenEnd(bytes, 4, length, crc));
    EXPECT_EQ(State::Invalid, chunksThenEnd(bytes, 3, length + 1, crc));
    EXPECT_EQ(State::Invalid, chunksThenEnd(bytes, 3, length, crc ^ 1U));
    EXPECT_EQ(State::Complete, chunksThenEnd(bytes, 3, length, crc));

    SnapshotValidator validator;
    validator.reset(4);
    EXPECT_EQ(State::Collecting, validator.onChunk(3, 5, bytes.data(), 10)) << "a late chunk";
    EXPECT_EQ(State::Collecting, validator.onEnd(5, 0, 0, 0)) << "a later round's end";
    EXPECT_EQ(0U, validator.length());
}

} // namespace
} // namespace org::limitless::seqeron::protocol
