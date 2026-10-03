#pragma once

#include <cstdint>
#include <span>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::replayer::client {

/**
 * Takes a source's snapshot as a restore reads it from this instance's own file, before any frame after its cut is
 * dispatched (doc/snapshot.md §7). Each restore, a restart's included, begins with onSnapshotHeader. The Java twin
 * is replayer/client/SnapshotRestoreHandler.java.
 */
class SnapshotRestoreHandler
{
  public:
    /**
     * Whether this build reads records of a format; a snapshot it does not read stops the restore.
     *
     * @param formatVersion the round's SnapshotEnd::formatVersion
     * @return true if this build reads it
     */
    virtual bool supportsFormatVersion(std::uint32_t formatVersion) = 0;

    /**
     * Takes record 0, the façade's: the first call of each restore.
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
