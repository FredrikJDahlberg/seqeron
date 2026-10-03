#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"

namespace org::limitless::seqeron::replayer::client {

/**
 * This instance's own snapshots, one file per round in a directory no other instance writes (doc/snapshot.md §4).
 * <round>.snapshot holds the records, each a little-endian uint16 length and its bytes, record 0 the façade's
 * header, then a trailer whose layout is in the Java twin, ending in MAGIC. A file is written as <round>.tmp and
 * renamed once complete, without an fsync: one an OS crash tore fails its trailer or its records, and a restore
 * checks both. A write that fails leaves no file and is logged. Not thread-safe. The Java twin is
 * replayer/client/SnapshotStore.java; keep the two in step.
 */
class SnapshotStore
{
  public:
    // Bytes after the records.
    static constexpr std::size_t TRAILER_LENGTH = 32;

    // "SNAP", last in the file, so a file cut short has none.
    static constexpr std::uint32_t MAGIC = 0x50414E53;

    // One round's file, read record by record, its trailer known before the first.
    class Reader
    {
      public:
        // After the last record, which every record before it matched the trailer to.
        static constexpr std::int32_t END = -1;

        // The records do not match the trailer, or could not be read. Latched.
        static constexpr std::int32_t DAMAGED = -2;

        // The trailer's recordCount.
        [[nodiscard]] std::int32_t recordCount() const
        {
            return m_recordCount;
        }

        // The trailer's length.
        [[nodiscard]] std::uint64_t length() const
        {
            return m_length;
        }

        // The trailer's crc32c.
        [[nodiscard]] std::uint32_t crc32c() const
        {
            return m_crc32c;
        }

        // The trailer's formatVersion.
        [[nodiscard]] std::uint32_t formatVersion() const
        {
            return m_formatVersion;
        }

        /**
         * Reads the next record.
         *
         * @param record set to the record, valid until the next call
         * @return its length, END or DAMAGED
         */
        std::int32_t next(std::span<const std::uint8_t>& record)
        {
            if (m_damaged)
            {
                return DAMAGED;
            }
            if (m_consumed == m_recordsLength)
            {
                m_damaged = m_count != m_recordCount || m_bytes != m_length || m_crc != m_crc32c;
                return m_damaged ? DAMAGED : END;
            }
            std::array<std::uint8_t, RECORD_PREFIX_LENGTH> prefix{};
            if (m_consumed + RECORD_PREFIX_LENGTH > m_recordsLength || !read(prefix.data(), RECORD_PREFIX_LENGTH))
            {
                return damage();
            }
            const auto recordLength = static_cast<std::size_t>(protocol::detail::getLe(prefix.data(), 2));
            if (recordLength > protocol::MAX_SNAPSHOT_RECORD_LENGTH ||
                m_consumed + RECORD_PREFIX_LENGTH + recordLength > m_recordsLength ||
                !read(m_record.data(), recordLength))
            {
                return damage();
            }
            m_crc = protocol::crc32c(m_record.data(), recordLength, m_crc);
            m_consumed += RECORD_PREFIX_LENGTH + recordLength;
            m_bytes += recordLength;
            ++m_count;
            record = std::span<const std::uint8_t>(m_record.data(), recordLength);
            return static_cast<std::int32_t>(recordLength);
        }

      private:
        friend class SnapshotStore;

        Reader(std::ifstream in, const std::uint64_t recordsLength, const std::uint8_t* trailer) :
          m_in(std::move(in)),
          m_recordsLength(recordsLength),
          m_recordCount(static_cast<std::int32_t>(protocol::detail::getLe(trailer + 16, 4))),
          m_length(protocol::detail::getLe(trailer + 8, 8)),
          m_crc32c(static_cast<std::uint32_t>(protocol::detail::getLe(trailer + 20, 4))),
          m_formatVersion(static_cast<std::uint32_t>(protocol::detail::getLe(trailer + 24, 4)))
        {}

        bool read(std::uint8_t* out, const std::size_t count)
        {
            return static_cast<bool>(m_in.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(count)));
        }

        std::int32_t damage()
        {
            m_damaged = true;
            return DAMAGED;
        }

        std::ifstream m_in;
        std::uint64_t m_recordsLength;
        std::int32_t m_recordCount;
        std::uint64_t m_length;
        std::uint32_t m_crc32c;
        std::uint32_t m_formatVersion;
        std::array<std::uint8_t, protocol::MAX_SNAPSHOT_RECORD_LENGTH> m_record{};
        std::uint64_t m_consumed = 0;
        std::uint64_t m_bytes = 0;
        std::int32_t m_count = 0;
        std::uint32_t m_crc = 0;
        bool m_damaged = false;
    };

    /**
     * Opens this instance's directory, creating it if missing.
     *
     * @param directory this instance's own
     * @throws std::filesystem::filesystem_error if it cannot be created
     */
    explicit SnapshotStore(std::filesystem::path directory) : m_directory(std::move(directory))
    {
        std::filesystem::create_directories(m_directory);
    }

    SnapshotStore(const SnapshotStore&) = delete;
    SnapshotStore& operator=(const SnapshotStore&) = delete;

    /**
     * Starts writing a round's file, dropping any write still open.
     *
     * @param round its round
     */
    void begin(const std::int64_t round)
    {
        abandon();
        m_writingRound = round;
        m_out.open(temporaryFile(round), std::ios::binary | std::ios::trunc);
        m_writing = true;
        if (!m_out)
        {
            failWrite("cannot create the file");
        }
    }

    /**
     * Appends one record to the file being written.
     *
     * @param record the record, at most MAX_SNAPSHOT_RECORD_LENGTH bytes
     */
    void append(const std::span<const std::uint8_t> record)
    {
        if (!m_writing)
        {
            return;
        }
        std::array<std::uint8_t, RECORD_PREFIX_LENGTH> prefix{};
        protocol::detail::putLe(prefix.data(), record.size(), RECORD_PREFIX_LENGTH);
        m_out.write(reinterpret_cast<const char*>(prefix.data()), RECORD_PREFIX_LENGTH);
        m_out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size()));
        if (!m_out)
        {
            failWrite("cannot write a record");
        }
    }

    /**
     * Completes the file being written: its trailer, then its name.
     *
     * @param recordCount   records appended
     * @param length        their bytes
     * @param crc32c        their CRC-32C
     * @param formatVersion the listener's
     */
    void commit(const std::int32_t recordCount, const std::uint64_t length, const std::uint32_t crc32c,
                const std::uint32_t formatVersion)
    {
        if (!m_writing)
        {
            return;
        }
        std::array<std::uint8_t, TRAILER_LENGTH> trailer{};
        protocol::detail::putLe(trailer.data(), static_cast<std::uint64_t>(m_writingRound), 8);
        protocol::detail::putLe(trailer.data() + 8, length, 8);
        protocol::detail::putLe(trailer.data() + 16, static_cast<std::uint32_t>(recordCount), 4);
        protocol::detail::putLe(trailer.data() + 20, crc32c, 4);
        protocol::detail::putLe(trailer.data() + 24, formatVersion, 4);
        protocol::detail::putLe(trailer.data() + 28, MAGIC, 4);
        m_out.write(reinterpret_cast<const char*>(trailer.data()), TRAILER_LENGTH);
        m_out.close();
        if (!m_out)
        {
            failWrite("cannot write the trailer");
            return;
        }
        m_writing = false;
        std::error_code error;
        std::filesystem::rename(temporaryFile(m_writingRound), file(m_writingRound), error);
        if (error)
        {
            m_writing = true;
            failWrite(error.message().c_str());
        }
    }

    // Drops the write in progress, if any.
    void abandon()
    {
        if (!m_writing)
        {
            return;
        }
        m_out.close();
        m_out.clear();
        m_writing = false;
        std::error_code ignored; // a leftover goes with the next deleteBefore
        std::filesystem::remove(temporaryFile(m_writingRound), ignored);
    }

    /**
     * Deletes the file of every round before a round, and any write a crash left behind.
     *
     * @param round the oldest round to keep
     */
    void deleteBefore(const std::int64_t round)
    {
        std::error_code error;
        for (const auto& entry : std::filesystem::directory_iterator(m_directory, error))
        {
            const std::filesystem::path& path = entry.path();
            const std::int64_t fileRound = roundOf(path);
            if ((path.extension() == TEMPORARY_SUFFIX && !m_writing) || (fileRound >= 0 && fileRound < round))
            {
                std::filesystem::remove(path, error);
            }
        }
        if (error)
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                               "cannot delete snapshots before round %lld in %s: %s", static_cast<long long>(round),
                               m_directory.c_str(), error.message().c_str());
        }
    }

    /**
     * Finds the newest round below a bound with a complete file.
     *
     * @param belowRound the bound; the default finds the newest of all
     * @return that round, or -1
     */
    [[nodiscard]] std::int64_t latestRound(
        const std::int64_t belowRound = std::numeric_limits<std::int64_t>::max()) const
    {
        std::int64_t latest = -1;
        std::error_code error;
        for (const auto& entry : std::filesystem::directory_iterator(m_directory, error))
        {
            const std::int64_t round = roundOf(entry.path());
            if (round < belowRound && round > latest)
            {
                latest = round;
            }
        }
        if (error)
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                               "cannot list snapshots in %s: %s", m_directory.c_str(), error.message().c_str());
        }
        return latest;
    }

    /**
     * Opens a round's file to read its records.
     *
     * @param round its round
     * @return empty if it has none, or its trailer is cut short or names another round
     */
    [[nodiscard]] std::optional<Reader> open(const std::int64_t round) const
    {
        std::ifstream in(file(round), std::ios::binary | std::ios::ate);
        if (!in)
        {
            return std::nullopt;
        }
        const auto size = static_cast<std::uint64_t>(in.tellg());
        std::array<std::uint8_t, TRAILER_LENGTH> trailer{};
        if (size < TRAILER_LENGTH || !in.seekg(static_cast<std::streamoff>(size - TRAILER_LENGTH)) ||
            !in.read(reinterpret_cast<char*>(trailer.data()), TRAILER_LENGTH) ||
            protocol::detail::getLe(trailer.data() + 28, 4) != MAGIC ||
            static_cast<std::int64_t>(protocol::detail::getLe(trailer.data(), 8)) != round || !in.seekg(0))
        {
            return std::nullopt;
        }
        return Reader(std::move(in), size - TRAILER_LENGTH, trailer.data());
    }

  private:
    static constexpr std::size_t RECORD_PREFIX_LENGTH = 2;
    static constexpr const char* SUFFIX = ".snapshot";
    static constexpr const char* TEMPORARY_SUFFIX = ".tmp";

    void failWrite(const char* reason)
    {
        util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                           "cannot write round %lld's snapshot in %s: %s", static_cast<long long>(m_writingRound),
                           m_directory.c_str(), reason);
        abandon();
    }

    [[nodiscard]] std::filesystem::path file(const std::int64_t round) const
    {
        return m_directory / (std::to_string(round) + SUFFIX);
    }

    [[nodiscard]] std::filesystem::path temporaryFile(const std::int64_t round) const
    {
        return m_directory / (std::to_string(round) + TEMPORARY_SUFFIX);
    }

    // The round a complete file's name holds, or -1 for any other name.
    static std::int64_t roundOf(const std::filesystem::path& path)
    {
        if (path.extension() != SUFFIX)
        {
            return -1;
        }
        const std::string stem = path.stem().string();
        if (stem.empty() || stem.find_first_not_of("0123456789") != std::string::npos)
        {
            return -1;
        }
        try
        {
            return std::stoll(stem);
        }
        catch (const std::out_of_range&)
        {
            return -1;
        }
    }

    std::filesystem::path m_directory;
    std::ofstream m_out;
    std::int64_t m_writingRound = 0;
    bool m_writing = false; // m_out holds the round's .tmp file
};

} // namespace org::limitless::seqeron::replayer::client
