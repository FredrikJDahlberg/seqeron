#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"

namespace org::limitless::seqeron::replayer::client {

/**
 * This instance's own snapshots, in a directory no other instance writes (doc/snapshot.md §4.1). A round is written
 * as `<round>.tmp`: the records, each a little-endian uint16 length and its bytes, record 0 the façade's header, then a
 * trailer whose layout is in the Java twin, ending in MAGIC. Once the log confirms the round, a thread of its own
 * forces the file to disk, renames it `<round>.snapshot`, forces the name and deletes every older file but the newest
 * confirmed one, so the caller does not wait for the disk: a `.snapshot` file is confirmed and durable, and the store
 * keeps two, the older to restore from when the newer is damaged. A write that fails leaves no file and is logged. Not
 * thread-safe: one thread calls it, and only its file work runs on the other. The Java twin is
 * replayer/client/SnapshotStore.java; keep the two in step.
 */
class SnapshotStore
{
    // Each file stream's buffer: a multiple of every SSD page size, and few system calls per file.
    static constexpr std::size_t IO_BUFFER_LENGTH = 64 * 1024;

  public:
    // Bytes after the records.
    static constexpr std::size_t TRAILER_LENGTH = 56;

    // "SNP2", last in the file, so a file cut short has none.
    static constexpr std::uint32_t MAGIC = 0x32504E53;

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

        // The trailer's cutClusterSeqNo.
        [[nodiscard]] std::int64_t cutClusterSeqNo() const
        {
            return m_cutClusterSeqNo;
        }

        // The trailer's cutPosition.
        [[nodiscard]] std::int64_t cutPosition() const
        {
            return m_cutPosition;
        }

        // The trailer's cutTimestamp.
        [[nodiscard]] std::int64_t cutTimestampNs() const
        {
            return m_cutTimestampNs;
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
                !read(m_file->record.data(), recordLength))
            {
                return damage();
            }
            m_crc = protocol::crc32c(m_file->record.data(), recordLength, m_crc);
            m_consumed += RECORD_PREFIX_LENGTH + recordLength;
            m_bytes += recordLength;
            ++m_count;
            record = std::span<const std::uint8_t>(m_file->record.data(), recordLength);
            return static_cast<std::int32_t>(recordLength);
        }

      private:
        friend class SnapshotStore;

        // The open file, its read buffer and the record buffer, on the heap and moved as one: the stream reads
        // through the buffer, which is declared first so that it outlives it.
        struct File
        {
            std::array<char, IO_BUFFER_LENGTH> buffer;
            std::ifstream in;
            std::array<std::uint8_t, protocol::MAX_SNAPSHOT_RECORD_LENGTH> record;
        };

        Reader(std::unique_ptr<File> file, const std::uint64_t recordsLength, const std::uint8_t* trailer) :
          m_file(std::move(file)),
          m_recordsLength(recordsLength),
          m_recordCount(static_cast<std::int32_t>(protocol::detail::getLe(trailer + 16, 4))),
          m_length(protocol::detail::getLe(trailer + 8, 8)),
          m_crc32c(static_cast<std::uint32_t>(protocol::detail::getLe(trailer + 20, 4))),
          m_formatVersion(static_cast<std::uint32_t>(protocol::detail::getLe(trailer + 24, 4))),
          m_cutClusterSeqNo(static_cast<std::int64_t>(protocol::detail::getLe(trailer + 28, 8))),
          m_cutPosition(static_cast<std::int64_t>(protocol::detail::getLe(trailer + 36, 8))),
          m_cutTimestampNs(static_cast<std::int64_t>(protocol::detail::getLe(trailer + 44, 8)))
        {}

        bool read(std::uint8_t* out, const std::size_t count)
        {
            return static_cast<bool>(
                m_file->in.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(count)));
        }

        std::int32_t damage()
        {
            m_damaged = true;
            return DAMAGED;
        }

        std::unique_ptr<File> m_file;
        std::uint64_t m_recordsLength;
        std::int32_t m_recordCount;
        std::uint64_t m_length;
        std::uint32_t m_crc32c;
        std::uint32_t m_formatVersion;
        std::int64_t m_cutClusterSeqNo;
        std::int64_t m_cutPosition;
        std::int64_t m_cutTimestampNs;
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
     * Starts writing a round's file, dropping any write still open, once the rounds before it are confirmed.
     *
     * @param round          its round
     * @param cutClusterSeqNo its SnapshotStarted's clusterSeqNo
     * @param cutPosition    where that frame starts in the recording
     * @param cutTimestampNs that frame's consensus timestamp
     */
    void begin(const std::int64_t round, const std::int64_t cutClusterSeqNo, const std::int64_t cutPosition,
               const std::int64_t cutTimestampNs)
    {
        awaitWrites();
        abandon();
        m_writingRound = round;
        m_cutClusterSeqNo = cutClusterSeqNo;
        m_cutPosition = cutPosition;
        m_cutTimestampNs = cutTimestampNs;
        m_out.rdbuf()->pubsetbuf(m_outBuffer.data(), static_cast<std::streamsize>(m_outBuffer.size()));
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
     * Completes the file being written with its trailer, still under its temporary name.
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
        protocol::detail::putLe(trailer.data() + 28, static_cast<std::uint64_t>(m_cutClusterSeqNo), 8);
        protocol::detail::putLe(trailer.data() + 36, static_cast<std::uint64_t>(m_cutPosition), 8);
        protocol::detail::putLe(trailer.data() + 44, static_cast<std::uint64_t>(m_cutTimestampNs), 8);
        protocol::detail::putLe(trailer.data() + 52, MAGIC, 4);
        m_out.write(reinterpret_cast<const char*>(trailer.data()), TRAILER_LENGTH);
        m_out.close();
        if (!m_out)
        {
            failWrite("cannot write the trailer");
            return;
        }
        m_writing = false;
        m_committedRound = m_writingRound;
    }

    // Drops the write in progress, if any.
    void abandon()
    {
        m_committedRound = -1;
        if (!m_writing)
        {
            return;
        }
        m_out.close();
        m_out.clear();
        m_writing = false;
        std::error_code ignored; // a leftover goes with the next confirmation
        std::filesystem::remove(temporaryFile(m_writingRound), ignored);
    }

    /**
     * The log confirms a round: off this thread, its file is made durable under its name, and every older file but the
     * newest confirmed one, and any write of one a crash left behind, is deleted. Nothing if its file was not
     * completed. durableRound names it once it is durable.
     *
     * @param round the round last committed
     */
    void confirm(const std::int64_t round)
    {
        if (round != m_committedRound)
        {
            return;
        }
        m_committedRound = -1;
        afterWrites([this, round] { confirmFile(round); });
    }

    // Waits until the rounds confirmed so far are durable, or failed, and the files they replace deleted.
    void awaitWrites()
    {
        if (m_writes.valid())
        {
            m_writes.wait();
        }
    }

    // The newest round confirmed whose file is durable under its name; -1 for none. Any thread.
    [[nodiscard]] std::int64_t durableRound() const
    {
        return m_durableRound.load(std::memory_order_acquire);
    }

    /**
     * Finds the newest round below a bound with a confirmed file.
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
            const std::int64_t round = roundOf(entry.path(), SUFFIX);
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
        auto opened = std::make_unique<Reader::File>();
        std::ifstream& in = opened->in;
        in.rdbuf()->pubsetbuf(opened->buffer.data(), static_cast<std::streamsize>(opened->buffer.size()));
        in.open(file(round), std::ios::binary | std::ios::ate);
        if (!in)
        {
            return std::nullopt;
        }
        const auto size = static_cast<std::uint64_t>(in.tellg());
        std::array<std::uint8_t, TRAILER_LENGTH> trailer{};
        if (size < TRAILER_LENGTH || !in.seekg(static_cast<std::streamoff>(size - TRAILER_LENGTH)) ||
            !in.read(reinterpret_cast<char*>(trailer.data()), TRAILER_LENGTH) ||
            protocol::detail::getLe(trailer.data() + 52, 4) != MAGIC ||
            static_cast<std::int64_t>(protocol::detail::getLe(trailer.data(), 8)) != round || !in.seekg(0))
        {
            return std::nullopt;
        }
        return Reader(std::move(opened), size - TRAILER_LENGTH, trailer.data());
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

    // Runs a step on a thread of its own once the step before it is done.
    void afterWrites(std::function<void()> step)
    {
        m_writes = std::async(std::launch::async, [previous = std::move(m_writes), step = std::move(step)]() mutable {
            if (previous.valid())
            {
                previous.wait();
            }
            step();
        });
    }

    // Forces a committed round's file to disk, then its name, then deletes what it replaces.
    void confirmFile(const std::int64_t round)
    {
        std::error_code error = sync(temporaryFile(round), O_WRONLY);
        if (!error)
        {
            std::filesystem::rename(temporaryFile(round), file(round), error);
        }
        if (!error)
        {
            error = sync(m_directory, O_RDONLY);
        }
        if (error)
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                               "cannot make round %lld's snapshot durable in %s: %s", static_cast<long long>(round),
                               m_directory.c_str(), error.message().c_str());
            return;
        }
        m_durableRound.store(round, std::memory_order_release);
        const std::int64_t kept = latestRound(round);
        for (const auto& entry : std::filesystem::directory_iterator(m_directory, error))
        {
            const std::filesystem::path& path = entry.path();
            const std::int64_t confirmedRound = roundOf(path, SUFFIX);
            const std::int64_t temporaryRound = roundOf(path, TEMPORARY_SUFFIX);
            if ((confirmedRound >= 0 && confirmedRound < kept) || (temporaryRound >= 0 && temporaryRound < round))
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

    // Forces a file's data, or a directory's names, to disk.
    static std::error_code sync(const std::filesystem::path& path, const int flags)
    {
        const int fd = ::open(path.c_str(), flags);
        if (fd < 0)
        {
            return { errno, std::generic_category() };
        }
        const std::error_code error =
            ::fsync(fd) == 0 ? std::error_code() : std::error_code(errno, std::generic_category());
        ::close(fd);
        return error;
    }

    [[nodiscard]] std::filesystem::path file(const std::int64_t round) const
    {
        return m_directory / (std::to_string(round) + SUFFIX);
    }

    [[nodiscard]] std::filesystem::path temporaryFile(const std::int64_t round) const
    {
        return m_directory / (std::to_string(round) + TEMPORARY_SUFFIX);
    }

    // The round a name ending in a suffix holds, or -1 for any other name.
    static std::int64_t roundOf(const std::filesystem::path& path, const char* suffix)
    {
        if (path.extension() != suffix)
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
    // m_out writes through it, so it is declared first and outlives it.
    std::vector<char> m_outBuffer = std::vector<char>(IO_BUFFER_LENGTH);
    std::ofstream m_out;
    std::int64_t m_writingRound = 0;
    std::int64_t m_cutClusterSeqNo = 0;
    std::int64_t m_cutPosition = 0;
    std::int64_t m_cutTimestampNs = 0;
    bool m_writing = false; // m_out holds the round's .tmp file
    // The round whose file was last completed, which confirm may name; -1 for none.
    std::int64_t m_committedRound = -1;
    // The newest round whose file is durable under its name, which the writer sets; -1 for none.
    std::atomic<std::int64_t> m_durableRound = -1;
    // What runs off the caller's thread, one step after another. Last, so its destructor waits before the rest go.
    std::future<void> m_writes;
};

} // namespace org::limitless::seqeron::replayer::client
