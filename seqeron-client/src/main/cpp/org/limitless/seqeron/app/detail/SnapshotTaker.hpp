#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>

#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/app/detail/SnapshotRecords.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotRestoreHandler.hpp"

namespace org::limitless::seqeron::app::detail {

/**
 * A façade's side of snapshot rounds, with no Aeron in it (doc/snapshot.md §4): serialize at the cut, submit the
 * records as chunks if this instance may publish, then the end, and compare the source's sequenced SnapshotEnd
 * with what this instance serialized (A-7). The façade feeds it the frames and drives submit from its duty cycle;
 * every outcome is the façade's to act on.
 *
 * No takeover: an instance that is not the publisher at the cut submits nothing for that round, and a publisher
 * that loses the role stops for good.
 *
 * It is also what a restore hands the snapshot's records to (§7), passing the source's own to the listener. The
 * Java twin is app/SnapshotTaker.java; keep the two in step.
 *
 * Actions, how an instance places the frames of a round it publishes, provides:
 *   protocol::Publish publishChunk(std::int64_t round, std::int32_t chunkIndex, std::span<const std::uint8_t> record)
 *   protocol::Publish publishEnd(std::int64_t round, std::int32_t chunkCount, std::uint64_t length,
 *                                std::uint32_t crc32c, std::uint32_t formatVersion)
 */
class SnapshotTaker final : public replayer::client::SnapshotRestoreHandler
{
  public:
    // Chunks one duty cycle submits at most, so a large snapshot does not starve the tap.
    static constexpr int MAX_CHUNKS_PER_CYCLE = 16;

    /**
     * Creates a taker in no round.
     *
     * @param listener what serializes the state, or nullptr if this build takes part in no round; must outlive
     *                 this object
     */
    explicit SnapshotTaker(SnapshotListener* const listener) : m_listener{ listener }
    {}

    SnapshotTaker(const SnapshotTaker&) = delete;
    SnapshotTaker& operator=(const SnapshotTaker&) = delete;

    /**
     * Sets whether the source's topology row takes part. Without a listener it cannot, whatever the row says.
     *
     * @param snapshot the row's snapshot
     */
    void participating(const bool snapshot)
    {
        m_participating = snapshot && m_listener != nullptr;
    }

    [[nodiscard]] bool isParticipating() const
    {
        return m_participating;
    }

    /**
     * Takes a dispatched SnapshotStarted: supersedes any round still open and serializes this one, pulling
     * records from the listener until it returns 0. A length outside 0 … 1302 is the listener's bug; the round
     * is dropped, as it is on every instance of the same build.
     *
     * @param round      its round
     * @param header     the façade's header as of the cut
     * @param mayPublish whether this instance is the one that publishes at the cut
     */
    void onSnapshotStarted(const std::int64_t round, const protocol::SnapshotHeader& header, const bool mayPublish)
    {
        if (!m_participating)
        {
            return;
        }
        m_round = round;
        m_publishing = false;
        m_serialized = m_records.reset(header, mayPublish);
        for (std::int32_t recordIndex = 0; m_serialized; ++recordIndex)
        {
            const std::int32_t length = m_listener->onSnapshot(std::span<std::uint8_t>(m_encoding), recordIndex);
            if (length == 0)
            {
                break;
            }
            if (length < 0 || length > protocol::MAX_SNAPSHOT_RECORD_LENGTH)
            {
                m_serialized = false;
                m_records.release();
            }
            else
            {
                m_records.append(m_encoding.data(), static_cast<std::size_t>(length));
            }
        }
        if (!m_serialized)
        {
            return;
        }
        m_publishing = mayPublish;
        m_nextChunkIndex = 0;
        m_fetched = false;
    }

    /**
     * Takes the source's own dispatched SnapshotEnd.
     *
     * @return false if it is for the round this instance serialized and disagrees with it: this instance has
     *         diverged from the log (A-7)
     */
    bool onSnapshotEnd(const std::int64_t round, const std::int32_t chunkCount, const std::int64_t length,
                       const std::uint32_t crc32c)
    {
        if (!m_serialized || round != m_round)
        {
            return true;
        }
        m_serialized = false;
        m_publishing = false;
        m_records.release();
        return chunkCount == m_records.chunkCount() && static_cast<std::uint64_t>(length) == m_records.length() &&
               crc32c == m_records.crc32c();
    }

    // This instance may no longer publish this round: another took the role. It does not resume.
    void stopPublishing()
    {
        if (m_publishing)
        {
            m_publishing = false;
            m_records.release();
        }
    }

    [[nodiscard]] bool isPublishing() const
    {
        return m_publishing;
    }

    // The build reads only the format it writes (§9).
    bool supportsFormatVersion(const std::uint32_t formatVersion) override
    {
        return formatVersion == m_listener->formatVersion();
    }

    // The source took part at the cut, and its topology row lies before it, never to be dispatched here.
    void onSnapshotHeader(const protocol::SnapshotHeader&) override
    {
        participating(true);
    }

    void onSnapshotRecord(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
    {
        m_listener->onRestore(record, recordIndex);
    }

    /**
     * Places what it can of the round being published: chunks in order, then the end, at most
     * MAX_CHUNKS_PER_CYCLE a call. A declined frame is placed again on the next call.
     *
     * @param actions places the frames
     * @return frames placed
     */
    template<typename Actions>
    int submit(Actions& actions)
    {
        int placed = 0;
        while (m_publishing && placed < MAX_CHUNKS_PER_CYCLE)
        {
            if (!m_fetched)
            {
                m_record = m_records.nextRecord();
                m_fetched = true;
            }
            const protocol::Publish outcome =
                m_record ? actions.publishChunk(m_round, m_nextChunkIndex, *m_record)
                         : actions.publishEnd(m_round, m_records.chunkCount(), m_records.length(), m_records.crc32c(),
                                              m_listener->formatVersion());
            if (outcome != protocol::Publish::Published)
            {
                return placed;
            }
            ++placed;
            if (!m_record)
            {
                m_publishing = false;
                m_records.release();
            }
            else
            {
                ++m_nextChunkIndex;
                m_fetched = false;
            }
        }
        return placed;
    }

  private:
    SnapshotListener* const m_listener;
    SnapshotRecords m_records;
    std::array<std::uint8_t, protocol::MAX_SNAPSHOT_RECORD_LENGTH> m_encoding{};
    bool m_participating = false;
    std::int64_t m_round = -1;
    bool m_serialized = false;
    bool m_publishing = false;
    std::int32_t m_nextChunkIndex = 0;
    bool m_fetched = false;                                // m_record holds the next frame to place
    std::optional<std::span<const std::uint8_t>> m_record; // empty: the end is next
};

} // namespace org::limitless::seqeron::app::detail
