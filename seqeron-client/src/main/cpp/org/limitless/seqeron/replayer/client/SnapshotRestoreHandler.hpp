#pragma once

#include <cstdint>
#include <span>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::replayer::client {

/**
 * Takes a source's latest snapshot as it is replayed, before any frame after its cut is dispatched
 * (doc/snapshot.md §7). A restore that starts over, after a replay lost under it, begins again with
 * onSnapshotHeader. The Java twin is replayer/client/SnapshotRestoreHandler.java.
 */
class SnapshotRestoreHandler
{
  public:
    /**
     * Whether this build reads records of a format; a snapshot it does not read stops the restore.
     *
     * @param formatVersion the snapshot's SnapshotEnd::formatVersion
     * @return true if this build reads it
     */
    virtual bool supportsFormatVersion(std::uint32_t formatVersion) = 0;

    /**
     * Takes record 0, the façade's: the first call of a restore, and of each time it starts over.
     *
     * @param header the decoded header
     */
    virtual void onSnapshotHeader(const protocol::SnapshotHeader& header) = 0;

    /**
     * Takes one of the source's own records, in order.
     *
     * @param record      the record, valid only during this call
     * @param recordIndex 0 for the record after the header
     */
    virtual void onSnapshotRecord(std::span<const std::uint8_t> record, std::int32_t recordIndex) = 0;

  protected:
    ~SnapshotRestoreHandler() = default;
};

} // namespace org::limitless::seqeron::replayer::client
