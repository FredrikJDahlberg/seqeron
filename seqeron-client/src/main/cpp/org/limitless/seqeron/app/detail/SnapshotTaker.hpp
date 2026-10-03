#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotRestoreHandler.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"

namespace org::limitless::seqeron::app::detail {

/**
 * A façade's side of snapshot rounds, with no Aeron in it (doc/snapshot.md §4): serialize at the cut into this
 * instance's own file, submit the round's SnapshotEnd if this instance may publish, and compare the source's
 * sequenced SnapshotEnd with what this instance serialized (A-7). The façade feeds it the frames and drives submit
 * from its duty cycle; every outcome is the façade's to act on.
 *
 * No takeover: an instance that is not the publisher at the cut submits nothing for that round, and a publisher
 * that loses the role stops for good.
 *
 * It is also what a restore hands the snapshot's records to (§7), passing the source's own to the listener.
 *
 * A passive instance holds no state until it is activated (§4): it takes part in no round, a restore hands its
 * listener nothing, and the façade dispatches it no payload while holdsState() is false. The Java twin is
 * app/SnapshotTaker.java; keep the two in step.
 *
 * Actions, how an instance places the end of a round it publishes, provides:
 *   protocol::Publish publishEnd(std::int64_t round, std::int32_t recordCount, std::uint64_t length,
 *                                std::uint32_t crc32c, std::uint32_t formatVersion)
 */
class SnapshotTaker final : public replayer::client::SnapshotRestoreHandler
{
  public:
    /**
     * Creates a taker in no round.
     *
     * @param listener  what serializes the state, or nullptr if this build takes part in no round; must outlive
     *                  this object
     * @param store     where this instance keeps its snapshots; nullptr exactly when listener is, and must outlive
     *                  this object
     * @param passive   whether this instance holds no state until it is activated
     * @param keepAlive keeps the cluster session alive while a round is serialized; self-throttling
     */
    SnapshotTaker(SnapshotListener* const listener, replayer::client::SnapshotStore* const store, const bool passive,
                  std::function<void()> keepAlive) :
      m_listener{ listener },
      m_store{ store },
      m_keepAlive{ std::move(keepAlive) },
      m_passive{ passive }
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

    // Whether this instance holds the source's state: false while it is passive.
    [[nodiscard]] bool holdsState() const
    {
        return !m_passive;
    }

    /**
     * Ends passivity once this instance is activated and caught up on the election.
     *
     * @param activated whether the pair's designation names this instance
     * @param caughtUp  whether this instance has caught up on the tap
     * @return true exactly then: the façade restarts its recovery, which restores the state this instance now holds
     */
    bool activate(const bool activated, const bool caughtUp)
    {
        if (!m_passive || !activated || !caughtUp)
        {
            return false;
        }
        m_passive = false;
        return true;
    }

    /**
     * Takes a dispatched SnapshotStarted: supersedes any round still open and serializes this one into its file,
     * pulling records from the listener until it returns 0. A length outside 0 … 65535 is the listener's bug; the
     * round is dropped, as it is on every instance of the same build, and so is one whose header outgrows a
     * record. One that no longer takes part still abandons the round it held. The cluster session is kept alive
     * after each record, so the round may take as long as its state needs.
     *
     * @param round      its round
     * @param header     the façade's header as of the cut
     * @param mayPublish whether this instance is the one that publishes at the cut
     */
    void onSnapshotStarted(const std::int64_t round, const protocol::SnapshotHeader& header, const bool mayPublish)
    {
        m_publishing = false;
        m_serialized = false;
        if (m_passive || !m_participating || header.encodedLength() > protocol::MAX_SNAPSHOT_RECORD_LENGTH)
        {
            return;
        }
        m_round = round;
        m_crc = 0;
        m_recordCount = 0;
        m_length = 0;
        m_store->begin(round);
        append(header.encode(m_encoding.data()));
        for (std::int32_t recordIndex = 0;; ++recordIndex)
        {
            const std::int32_t length = m_listener->onSnapshot(std::span<std::uint8_t>(m_encoding), recordIndex);
            if (length == 0)
            {
                break;
            }
            if (length < 0 || length > protocol::MAX_SNAPSHOT_RECORD_LENGTH)
            {
                m_store->abandon();
                return;
            }
            append(static_cast<std::size_t>(length));
        }
        m_store->commit(m_recordCount, m_length, m_crc, m_listener->formatVersion());
        m_serialized = true;
        m_publishing = mayPublish;
    }

    /**
     * Takes the source's own dispatched SnapshotEnd. One that matches makes this round's file the oldest this
     * instance keeps.
     *
     * @return false if it is for the round this instance serialized and disagrees with it: this instance has
     *         diverged from the log (A-7)
     */
    bool onSnapshotEnd(const std::int64_t round, const std::int32_t recordCount, const std::int64_t length,
                       const std::uint32_t crc32c)
    {
        if (!m_serialized || round != m_round)
        {
            return true;
        }
        m_serialized = false;
        m_publishing = false;
        if (recordCount != m_recordCount || static_cast<std::uint64_t>(length) != m_length || crc32c != m_crc)
        {
            return false;
        }
        m_store->deleteBefore(round);
        return true;
    }

    // This instance may no longer publish this round: another took the role. It does not resume.
    void stopPublishing()
    {
        m_publishing = false;
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
        if (!m_passive)
        {
            participating(true);
        }
    }

    void onSnapshotRecord(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
    {
        if (!m_passive)
        {
            m_listener->onRestore(record, recordIndex);
        }
    }

    /**
     * Places the end of the round being published. A declined end is placed again on the next call.
     *
     * @param actions places the end
     * @return frames placed
     */
    template<typename Actions>
    int submit(Actions& actions)
    {
        if (!m_publishing || actions.publishEnd(m_round, m_recordCount, m_length, m_crc, m_listener->formatVersion()) !=
                                 protocol::Publish::Published)
        {
            return 0;
        }
        m_publishing = false;
        return 1;
    }

  private:
    // Takes the record m_encoding holds.
    void append(const std::size_t length)
    {
        m_crc = protocol::crc32c(m_encoding.data(), length, m_crc);
        ++m_recordCount;
        m_length += length;
        m_store->append(std::span<const std::uint8_t>(m_encoding.data(), length));
        m_keepAlive();
    }

    SnapshotListener* const m_listener;
    replayer::client::SnapshotStore* const m_store;
    std::function<void()> m_keepAlive;
    std::vector<std::uint8_t> m_encoding = std::vector<std::uint8_t>(protocol::MAX_SNAPSHOT_RECORD_LENGTH);
    bool m_participating = false;
    bool m_passive;
    std::int64_t m_round = -1;
    bool m_serialized = false;
    bool m_publishing = false;
    std::int32_t m_recordCount = 0;
    std::uint64_t m_length = 0;
    std::uint32_t m_crc = 0;
};

} // namespace org::limitless::seqeron::app::detail
