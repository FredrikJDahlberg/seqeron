#pragma once

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <utility>

#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org_limitless_seqeron_sbe_frame/SnapshotEnd.h"

namespace org::limitless::seqeron::app::detail {

/**
 * The frame of a round a façade publishes, its SnapshotEnd (doc/snapshot.md §4), under its sourceId and belonging
 * to no connection: SnapshotTaker's Actions. The Java twin is app/SnapshotFrames.java; keep the two in step.
 *
 * @tparam Session the detail::Session that places the frames
 */
template<typename Session>
class SnapshotFrames
{
  public:
    /**
     * Creates the frames of one façade.
     *
     * @param session  places the end; must outlive this object
     * @param sourceId the façade's sourceId, read at each frame: a gateway's resolves from its row
     */
    SnapshotFrames(Session& session, std::function<std::int32_t()> sourceId) :
      m_session{ session },
      m_sourceId{ std::move(sourceId) }
    {}

    protocol::Publish publishEnd(const std::int64_t round, const std::int32_t recordCount, const std::uint64_t length,
                                 const std::uint32_t crc32c, const std::uint32_t formatVersion)
    {
        return placed(m_session.template publishSystem<sbe::frame::SnapshotEnd>(
            m_sourceId(), NO_CONNECTION, protocol::SNAPSHOT_END, [&](sbe::frame::SnapshotEnd& end) {
                end.round(round)
                    .recordCount(recordCount)
                    .length(static_cast<std::int64_t>(length))
                    .crc32c(crc32c)
                    .formatVersion(formatVersion);
            }));
    }

  private:
    static constexpr std::int32_t NO_CONNECTION = -1;

    // A refused end is this class's own bug, never a condition to wait out.
    static protocol::Publish placed(const protocol::Publish outcome)
    {
        if (outcome == protocol::Publish::Refused)
        {
            throw std::logic_error("a SnapshotEnd the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return outcome;
    }

    Session& m_session;
    std::function<std::int32_t()> m_sourceId;
};

} // namespace org::limitless::seqeron::app::detail
