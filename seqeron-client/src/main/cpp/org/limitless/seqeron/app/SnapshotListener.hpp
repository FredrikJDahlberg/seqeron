#pragma once

#include <cstdint>
#include <span>

namespace org::limitless::seqeron::app {

/**
 * What a source that takes part in snapshot rounds serializes, and restores from (doc/snapshot.md §4, §7).
 * Given to a façade's Config; a source whose topology row says snapshot="true" takes part only if it has one,
 * and every instance of a source runs the same build, so either all of them do or none does. An instance
 * given one restores its source's latest snapshot on start, if there is one. The Java twin is
 * app/SnapshotListener.java.
 */
class SnapshotListener
{
  public:
    /**
     * Names the record format onSnapshot writes, carried in SnapshotEnd::formatVersion and opaque to seqeron.
     *
     * @return the format's version; a restore stops on one its build does not write
     */
    virtual std::uint32_t formatVersion() const = 0;

    /**
     * Encodes the next record of this instance's state as of the round's cut — every frame up to the
     * SnapshotStarted dispatched, none after. The façade calls it repeatedly, before it dispatches the next
     * frame, until it returns 0; it does so on every instance of the source, live or replayed, and every one
     * must produce the same records for the same state (A-6): no hash-map iteration order, no local time, no
     * node identity. Must not throw.
     *
     * @param buffer      where to encode the record, from its start, at most its 65535 bytes; valid only during
     *                    this call
     * @param recordIndex 0 on a round's first call, which is where an iteration over the state starts over
     * @return the record's length, or 0 when the snapshot is complete
     */
    virtual std::int32_t onSnapshot(std::span<std::uint8_t> buffer, std::int32_t recordIndex) = 0;

    /**
     * Decodes the next record of the snapshot this instance restores from, in the order onSnapshot encoded
     * them, before any frame after its cut is dispatched. A snapshot of no records makes no call.
     *
     * @param record      the record, valid only during this call
     * @param recordIndex 0 on the first record, which is where the state is cleared: a restore that starts
     *                    over begins again at 0
     */
    virtual void onRestore(std::span<const std::uint8_t> record, std::int32_t recordIndex) = 0;

  protected:
    ~SnapshotListener() = default;
};

} // namespace org::limitless::seqeron::app
