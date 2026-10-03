// The snapshot byte format (doc/snapshot.md §2, §6). The golden vectors are the Java
// SnapshotFormatTest's, computed by a third implementation, so the two languages agree with each other and
// not merely with themselves. Case for case with the Java twin, in the same order.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::protocol {
namespace {

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

TEST(SnapshotFormat, CrcAndRecordSize)
{
    const std::string check = "123456789";
    EXPECT_EQ(0xE3069283U, crc32c(reinterpret_cast<const std::uint8_t*>(check.data()), check.size()));
    const std::vector<std::uint8_t> bytes = snapshot();
    EXPECT_EQ(APPLICATION_SNAPSHOT_CRC, crc32c(bytes.data(), bytes.size()));
    EXPECT_EQ(1302, MAX_SNAPSHOT_RECORD_LENGTH);
}

TEST(SnapshotFormat, TheCrcInstructionIsUsedOnX86AndArm)
{
#if defined(__x86_64__) || defined(__aarch64__)
    EXPECT_TRUE(detail::hasCrc32cInstruction()) << "crc32c would fall back to the table";
#else
    GTEST_SKIP() << "no CRC-32C instruction path on this architecture";
#endif
}

TEST(SnapshotFormat, TheCrcInstructionMatchesTheTableAtEveryLengthAlignmentAndSeed)
{
    if (!detail::hasCrc32cInstruction())
    {
        GTEST_SKIP() << "this CPU has no CRC-32C instruction";
    }
    std::vector<std::uint8_t> bytes(3 * MAX_SNAPSHOT_RECORD_LENGTH + 8);
    std::uint32_t lcg = 12345;
    for (std::uint8_t& byte : bytes)
    {
        lcg = lcg * 1103515245U + 12345U;
        byte = static_cast<std::uint8_t>(lcg >> 24);
    }
    std::vector<std::size_t> lengths;
    for (std::size_t length = 0; length <= 64; ++length)
    {
        lengths.push_back(length);
    }
    for (const std::size_t length : { MAX_SNAPSHOT_RECORD_LENGTH - 1, MAX_SNAPSHOT_RECORD_LENGTH + 0,
                                      MAX_SNAPSHOT_RECORD_LENGTH + 1, 3 * MAX_SNAPSHOT_RECORD_LENGTH + 0 })
    {
        lengths.push_back(length);
    }
    for (std::size_t offset = 0; offset < 8; ++offset)
    {
        for (const std::size_t length : lengths)
        {
            for (const std::uint32_t seed : { 0U, 0xE3069283U })
            {
                EXPECT_EQ(detail::crc32cTable(bytes.data() + offset, length, seed),
                          detail::crc32cHardware(bytes.data() + offset, length, seed))
                    << "offset " << offset << ", length " << length << ", seed " << seed;
            }
        }
    }
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

} // namespace
} // namespace org::limitless::seqeron::protocol
