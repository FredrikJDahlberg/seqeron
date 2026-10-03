#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#if defined(__x86_64__)
#include <nmmintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#if defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#endif

// The snapshot format (doc/snapshot.md §2, §6): a sequence of records, the façade's header first, and the
// CRC-32C over their bytes in order. The Java twins are protocol/SnapshotFormat and SnapshotHeader; keep them in
// step.

namespace org::limitless::seqeron::protocol {

// The most bytes one record holds: the room in the buffer a listener encodes it into.
inline constexpr std::uint16_t MAX_SNAPSHOT_RECORD_LENGTH = 1302;

namespace detail {

inline constexpr std::array<std::uint32_t, 256> CRC32C_TABLE = [] {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i)
    {
        std::uint32_t c = i;
        for (int bit = 0; bit < 8; ++bit)
        {
            c = (c >> 1) ^ ((c & 1U) != 0 ? 0x82F63B78U : 0U);
        }
        table[i] = c;
    }
    return table;
}();

inline void putLe(std::uint8_t* out, std::uint64_t value, const std::size_t bytes)
{
    for (std::size_t i = 0; i < bytes; ++i)
    {
        out[i] = static_cast<std::uint8_t>(value);
        value >>= 8;
    }
}

inline std::uint64_t getLe(const std::uint8_t* in, const std::size_t bytes)
{
    std::uint64_t value = 0;
    for (std::size_t i = bytes; i > 0; --i)
    {
        value = (value << 8) | in[i - 1];
    }
    return value;
}

/**
 * Computes the CRC-32C a byte at a time through CRC32C_TABLE: what crc32c falls back to on a CPU without the
 * instruction, and the reference the hardware path is tested against.
 *
 * @param data   the bytes
 * @param length how many
 * @param crc    the CRC of the bytes before these, or 0 to start
 * @return the CRC of everything so far
 */
inline std::uint32_t crc32cTable(const std::uint8_t* data, const std::size_t length, const std::uint32_t crc)
{
    std::uint32_t c = ~crc;
    for (std::size_t i = 0; i < length; ++i)
    {
        c = CRC32C_TABLE[(c ^ data[i]) & 0xFFU] ^ (c >> 8);
    }
    return ~c;
}

// The CRC-32C instruction, compiled for its own function by a target attribute so a consumer needs no compiler
// flag, and called only once hasCrc32cInstruction() has said the CPU has it.
#if defined(__x86_64__)

inline bool hasCrc32cInstruction()
{
    return __builtin_cpu_supports("sse4.2");
}

/**
 * Computes the CRC-32C with SSE4.2's crc32 instruction, eight bytes at a time.
 *
 * @param data   the bytes
 * @param length how many
 * @param crc    the CRC of the bytes before these, or 0 to start
 * @return the CRC of everything so far
 */
__attribute__((target("sse4.2"))) inline std::uint32_t crc32cHardware(const std::uint8_t* data, std::size_t length,
                                                                      const std::uint32_t crc)
{
    std::uint64_t c = ~crc;
    for (; length >= sizeof(std::uint64_t); length -= sizeof(std::uint64_t), data += sizeof(std::uint64_t))
    {
        std::uint64_t word;
        std::memcpy(&word, data, sizeof(word));
        c = _mm_crc32_u64(c, word);
    }
    auto c32 = static_cast<std::uint32_t>(c);
    for (; length > 0; --length, ++data)
    {
        c32 = _mm_crc32_u8(c32, *data);
    }
    return ~c32;
}

#elif defined(__aarch64__)

inline bool hasCrc32cInstruction()
{
#if defined(__ARM_FEATURE_CRC32) || defined(__APPLE__)
    return true;
#elif defined(__linux__)
    return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
#else
    return false;
#endif
}

/**
 * Computes the CRC-32C with ARMv8's crc32c instructions, eight bytes at a time.
 *
 * @param data   the bytes
 * @param length how many
 * @param crc    the CRC of the bytes before these, or 0 to start
 * @return the CRC of everything so far
 */
__attribute__((target("+crc"))) inline std::uint32_t crc32cHardware(const std::uint8_t* data, std::size_t length,
                                                                    const std::uint32_t crc)
{
    std::uint32_t c = ~crc;
    for (; length >= sizeof(std::uint64_t); length -= sizeof(std::uint64_t), data += sizeof(std::uint64_t))
    {
        std::uint64_t word;
        std::memcpy(&word, data, sizeof(word));
        c = __crc32cd(c, word);
    }
    for (; length > 0; --length, ++data)
    {
        c = __crc32cb(c, *data);
    }
    return ~c;
}

#else

inline bool hasCrc32cInstruction()
{
    return false;
}

inline std::uint32_t crc32cHardware(const std::uint8_t* data, const std::size_t length, const std::uint32_t crc)
{
    return crc32cTable(data, length, crc);
}

#endif

} // namespace detail

/**
 * Computes the CRC-32C of a span of bytes, as SnapshotEnd::crc32c carries it, or continues one: with the CPU's
 * CRC-32C instruction where it has one, through a table where it does not.
 *
 * @param data   the bytes
 * @param length how many
 * @param crc    the CRC of the bytes before these, or 0 to start
 * @return the CRC of everything so far
 */
inline std::uint32_t crc32c(const std::uint8_t* data, const std::size_t length, const std::uint32_t crc = 0)
{
    static const bool hardware = detail::hasCrc32cInstruction();
    return hardware ? detail::crc32cHardware(data, length, crc) : detail::crc32cTable(data, length, crc);
}

// One instance's list row, in a gateway's snapshot header.
struct SnapshotGatewayRow
{
    std::int32_t gatewayId;
    std::uint8_t preferenceRank;
    std::string gatewayName; ///< at most 32 US-ASCII characters

    bool operator==(const SnapshotGatewayRow&) const = default;
};

// A gateway's continuation of the snapshot header.
struct SnapshotGatewayState
{
    std::int32_t gatewaySourceId;
    std::int32_t activeGatewayId;         ///< the current GatewayActive's, or SnapshotHeader::NO_GATEWAY
    std::int32_t highestConnectionId;     ///< the highest in the source's history
    std::vector<SnapshotGatewayRow> rows; ///< every instance of the gateway, in list order

    bool operator==(const SnapshotGatewayState&) const = default;
};

/**
 * The header a façade writes ahead of the application's body: what it derives from frames before the
 * round's cut. Little-endian and packed; the layout is in the Java twin, SnapshotHeader. Every instance of a
 * source writes the same header (A-6).
 */
struct SnapshotHeader
{
    static constexpr std::uint16_t VERSION = 1;
    static constexpr std::size_t APPLICATION_LENGTH = 18;
    static constexpr std::int32_t NO_GATEWAY = -1;
    static constexpr std::size_t GATEWAY_FIXED_LENGTH = 14;
    static constexpr std::size_t GATEWAY_NAME_LENGTH = 32;
    static constexpr std::size_t GATEWAY_ROW_LENGTH = 5 + GATEWAY_NAME_LENGTH;

    std::int64_t leadershipTermId;
    std::int32_t leaderMemberId;
    std::optional<SnapshotGatewayState> gateway; ///< empty for an application's header

    bool operator==(const SnapshotHeader&) const = default;

    // Bytes encode writes; the value of headerLength.
    std::size_t encodedLength() const
    {
        return gateway ? APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH + GATEWAY_ROW_LENGTH * gateway->rows.size()
                       : APPLICATION_LENGTH;
    }

    /**
     * Writes the header.
     *
     * @param out where to write; at least encodedLength() bytes
     * @return the bytes written
     */
    std::size_t encode(std::uint8_t* out) const
    {
        const std::size_t length = encodedLength();
        detail::putLe(out, VERSION, 2);
        detail::putLe(out + 2, length, 4);
        detail::putLe(out + 6, static_cast<std::uint64_t>(leadershipTermId), 8);
        detail::putLe(out + 14, static_cast<std::uint32_t>(leaderMemberId), 4);
        if (gateway)
        {
            detail::putLe(out + 18, static_cast<std::uint32_t>(gateway->gatewaySourceId), 4);
            detail::putLe(out + 22, static_cast<std::uint32_t>(gateway->activeGatewayId), 4);
            detail::putLe(out + 26, static_cast<std::uint32_t>(gateway->highestConnectionId), 4);
            detail::putLe(out + 30, gateway->rows.size(), 2);
            std::uint8_t* row = out + APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH;
            for (const SnapshotGatewayRow& each : gateway->rows)
            {
                detail::putLe(row, static_cast<std::uint32_t>(each.gatewayId), 4);
                row[4] = each.preferenceRank;
                const std::size_t nameLength = std::min(each.gatewayName.size(), GATEWAY_NAME_LENGTH);
                std::copy_n(each.gatewayName.data(), nameLength, row + 5);
                std::fill_n(row + 5 + nameLength, GATEWAY_NAME_LENGTH - nameLength, std::uint8_t{ 0 });
                row += GATEWAY_ROW_LENGTH;
            }
        }
        return length;
    }

    /**
     * Reads the headerVersion a snapshot starts with.
     *
     * @param in     the snapshot's first byte
     * @param length the snapshot's bytes
     * @return the version, or -1 if length cannot hold one
     */
    static std::int32_t version(const std::uint8_t* in, const std::size_t length)
    {
        return length < 2 ? -1 : static_cast<std::int32_t>(detail::getLe(in, 2));
    }

    /**
     * Reads a header written by encode.
     *
     * @param in     the snapshot's first byte
     * @param length the snapshot's bytes
     * @return the header, or empty if it is not a VERSION header or does not fit length
     */
    static std::optional<SnapshotHeader> decode(const std::uint8_t* in, const std::size_t length)
    {
        if (length < APPLICATION_LENGTH || version(in, length) != VERSION)
        {
            return std::nullopt;
        }
        const std::uint64_t headerLength = detail::getLe(in + 2, 4);
        SnapshotHeader header{ static_cast<std::int64_t>(detail::getLe(in + 6, 8)),
                               static_cast<std::int32_t>(detail::getLe(in + 14, 4)), std::nullopt };
        if (headerLength == APPLICATION_LENGTH)
        {
            return header;
        }
        if (headerLength > length || headerLength < APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH)
        {
            return std::nullopt;
        }
        const std::size_t rowCount = detail::getLe(in + 30, 2);
        if (headerLength != APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH + GATEWAY_ROW_LENGTH * rowCount)
        {
            return std::nullopt;
        }
        SnapshotGatewayState state{ static_cast<std::int32_t>(detail::getLe(in + 18, 4)),
                                    static_cast<std::int32_t>(detail::getLe(in + 22, 4)),
                                    static_cast<std::int32_t>(detail::getLe(in + 26, 4)),
                                    {} };
        const std::uint8_t* row = in + APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH;
        for (std::size_t i = 0; i < rowCount; ++i)
        {
            const auto* name = reinterpret_cast<const char*>(row + 5);
            const std::size_t nameLength = std::find(name, name + GATEWAY_NAME_LENGTH, '\0') - name;
            state.rows.push_back(SnapshotGatewayRow{ static_cast<std::int32_t>(detail::getLe(row, 4)), row[4],
                                                     std::string(name, nameLength) });
            row += GATEWAY_ROW_LENGTH;
        }
        header.gateway = std::move(state);
        return header;
    }
};

} // namespace org::limitless::seqeron::protocol
