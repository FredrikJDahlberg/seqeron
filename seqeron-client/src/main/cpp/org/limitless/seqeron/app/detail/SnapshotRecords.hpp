#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::app::detail {

// One instance's snapshot of a round (doc/snapshot.md §4): the records serialized at the cut, each at most
// MAX_SNAPSHOT_RECORD_LENGTH bytes and each carried by one SnapshotChunk, the façade's header first. Only the
// publisher keeps the records, packed into fixed-size segments; every other instance keeps only the count,
// length and CRC it compares against the SnapshotEnd. The Java twin is app/SnapshotRecords.java.
class SnapshotRecords
{
  public:
    // Bytes per storage segment; a record and its 2-byte length never straddle two.
    static constexpr std::size_t SEGMENT_LENGTH = std::size_t{ 1 } << 20;

    /**
     * Appends one record.
     *
     * @param record the record
     * @param count  its length, at most MAX_SNAPSHOT_RECORD_LENGTH
     */
    void append(const void* record, const std::size_t count)
    {
        const auto* bytes = static_cast<const std::uint8_t*>(record);
        m_crc = protocol::crc32c(bytes, count, m_crc);
        if (m_retain)
        {
            if (m_writeOffset + RECORD_PREFIX_LENGTH + count > SEGMENT_LENGTH)
            {
                if (m_writeOffset + RECORD_PREFIX_LENGTH <= SEGMENT_LENGTH)
                {
                    protocol::detail::putLe(m_segments.back().get() + m_writeOffset, END_OF_SEGMENT,
                                            RECORD_PREFIX_LENGTH);
                }
                m_segments.push_back(std::make_unique<std::uint8_t[]>(SEGMENT_LENGTH));
                m_writeOffset = 0;
            }
            std::uint8_t* at = m_segments.back().get() + m_writeOffset;
            protocol::detail::putLe(at, count, RECORD_PREFIX_LENGTH);
            std::copy_n(bytes, count, at + RECORD_PREFIX_LENGTH);
            m_writeOffset += RECORD_PREFIX_LENGTH + count;
        }
        ++m_chunkCount;
        m_length += count;
    }

    /**
     * Starts a new snapshot, dropping the last one, with the façade's header as record 0.
     *
     * @param header the façade's header
     * @param retain whether to keep the records, as only the publisher does
     * @return false if the header is longer than a record, as a gateway of more than 34 instances' is
     */
    bool reset(const protocol::SnapshotHeader& header, const bool retain)
    {
        m_retain = retain;
        m_segments.clear();
        m_crc = 0;
        m_chunkCount = 0;
        m_length = 0;
        m_writeOffset = SEGMENT_LENGTH;
        rewind();
        if (header.encodedLength() > protocol::MAX_SNAPSHOT_RECORD_LENGTH)
        {
            return false;
        }
        std::array<std::uint8_t, protocol::MAX_SNAPSHOT_RECORD_LENGTH> record{};
        append(record.data(), header.encode(record.data()));
        return true;
    }

    // Drops the kept records, once submitted or abandoned; the totals stay.
    void release()
    {
        m_segments.clear();
        m_writeOffset = SEGMENT_LENGTH;
        rewind();
    }

    // Records appended so far, the header included: the chunkCount.
    std::int32_t chunkCount() const
    {
        return m_chunkCount;
    }

    // Their bytes: the length.
    std::uint64_t length() const
    {
        return m_length;
    }

    // Their CRC-32C: the crc32c.
    std::uint32_t crc32c() const
    {
        return m_crc;
    }

    // Starts nextRecord over from record 0.
    void rewind()
    {
        m_readSegment = 0;
        m_readOffset = 0;
    }

    // The next kept record, or empty after the last.
    std::optional<std::span<const std::uint8_t>> nextRecord()
    {
        if (m_readSegment >= m_segments.size() ||
            (m_readSegment == m_segments.size() - 1 && m_readOffset == m_writeOffset))
        {
            return std::nullopt;
        }
        if (m_readOffset + RECORD_PREFIX_LENGTH > SEGMENT_LENGTH ||
            protocol::detail::getLe(m_segments[m_readSegment].get() + m_readOffset, RECORD_PREFIX_LENGTH) ==
                END_OF_SEGMENT)
        {
            ++m_readSegment;
            m_readOffset = 0;
        }
        const std::uint8_t* at = m_segments[m_readSegment].get() + m_readOffset;
        const std::size_t count = protocol::detail::getLe(at, RECORD_PREFIX_LENGTH);
        m_readOffset += RECORD_PREFIX_LENGTH + count;
        return std::span<const std::uint8_t>(at + RECORD_PREFIX_LENGTH, count);
    }

  private:
    static constexpr std::size_t RECORD_PREFIX_LENGTH = 2;
    // In place of a length: the rest of this segment is unused. No record is this long.
    static constexpr std::uint64_t END_OF_SEGMENT = 0xFFFF;

    std::vector<std::unique_ptr<std::uint8_t[]>> m_segments;
    bool m_retain = false;
    std::int32_t m_chunkCount = 0;
    std::uint64_t m_length = 0;
    std::uint32_t m_crc = 0;
    std::size_t m_writeOffset = SEGMENT_LENGTH;
    std::size_t m_readSegment = 0;
    std::size_t m_readOffset = 0;
};

} // namespace org::limitless::seqeron::app::detail
