#pragma once

// A snapshot as a restore meets it (doc/snapshot.md §5, §7): the instance's own file, the round's frames, and the
// Replayer's answer confirming it. The twin of the Java tests' RestoreFrames.

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotEnd.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotStarted.h"
#include "org_limitless_seqeron_sbe_replay/SnapshotLocation.h"

namespace org::limitless::seqeron::replayer::client::restore_frames {

using Bytes = std::vector<std::uint8_t>;

// What a SnapshotEnd says of a snapshot's records.
struct Digest
{
    std::int32_t recordCount;
    std::int64_t length;
    std::uint32_t crc32c;

    static Digest of(const std::vector<Bytes>& records)
    {
        Digest digest{ static_cast<std::int32_t>(records.size()), 0, 0 };
        for (const Bytes& record : records)
        {
            digest.length += static_cast<std::int64_t>(record.size());
            digest.crc32c = protocol::crc32c(record.data(), record.size(), digest.crc32c);
        }
        return digest;
    }
};

inline Bytes started(const std::int64_t globalSeqNo, const std::int64_t round)
{
    Bytes out(128, 0);
    sbe::frame::SnapshotStarted frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    frame.header()
        .sourceId(-1)
        .connectionId(-1)
        .sessionId(-1)
        .systemEventType(protocol::SNAPSHOT_STARTED)
        .globalSeqNo(globalSeqNo)
        .timestamp(globalSeqNo * 1000);
    frame.round(round);
    out.resize(sbe::frame::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

inline Bytes system(const std::int64_t globalSeqNo, const std::int32_t sourceId, const std::uint16_t systemEventType,
                    const char* body, const std::uint16_t bodyLength)
{
    Bytes out(256, 0);
    sbe::frame::SequencedSystem frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    frame.header()
        .sourceId(sourceId)
        .connectionId(-1)
        .sessionId(5)
        .systemEventType(systemEventType)
        .globalSeqNo(globalSeqNo)
        .timestamp(globalSeqNo * 1000);
    frame.putBody(body, bodyLength);
    out.resize(sbe::frame::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

// The end the records match.
inline Bytes end(const std::int64_t globalSeqNo, const std::int32_t sourceId, const std::int64_t round,
                 const std::uint32_t formatVersion, const std::vector<Bytes>& records)
{
    const Digest digest = Digest::of(records);
    std::array<char, 64> body{};
    sbe::frame::SnapshotEnd encoder;
    encoder.wrapForEncode(body.data(), 0, body.size());
    encoder.round(round)
        .recordCount(digest.recordCount)
        .length(digest.length)
        .crc32c(digest.crc32c)
        .formatVersion(formatVersion);
    return system(globalSeqNo, sourceId, protocol::SNAPSHOT_END, body.data(),
                  static_cast<std::uint16_t>(encoder.encodedLength()));
}

// Writes records as a round's file, as the instance did when it serialized them.
inline void write(SnapshotStore& store, const std::int64_t round, const std::uint32_t formatVersion,
                  const std::vector<Bytes>& records)
{
    const Digest digest = Digest::of(records);
    store.begin(round);
    for (const Bytes& record : records)
    {
        store.append(record);
    }
    store.commit(digest.recordCount, static_cast<std::uint64_t>(digest.length), digest.crc32c, formatVersion);
}

// An application's header record.
inline Bytes header(const std::int64_t leadershipTermId, const std::int32_t leaderMemberId)
{
    const protocol::SnapshotHeader header{ leadershipTermId, leaderMemberId, std::nullopt };
    Bytes out(header.encodedLength());
    header.encode(out.data());
    return out;
}

// A record holding one int64, in the byte order the Java twin's putLong writes.
inline Bytes record(const std::int64_t value)
{
    Bytes out(sizeof(value));
    protocol::detail::putLe(out.data(), static_cast<std::uint64_t>(value), sizeof(value));
    return out;
}

inline std::int64_t valueOf(const std::uint8_t* record)
{
    return static_cast<std::int64_t>(protocol::detail::getLe(record, sizeof(std::int64_t)));
}

// The Replayer's answer: a round's cut and sequenced end, or none for round -1.
inline Bytes location(const std::int32_t clientId, const std::int64_t requestId, const std::int64_t round,
                      const std::int64_t asOfGlobalSeqNo, const std::int64_t asOfPosition,
                      const std::uint32_t formatVersion, const Digest& end)
{
    Bytes out(128, 0);
    sbe::replay::SnapshotLocation encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    encoder.clientId(clientId)
        .requestId(requestId)
        .round(round)
        .asOfGlobalSeqNo(asOfGlobalSeqNo)
        .asOfPosition(asOfPosition)
        .formatVersion(formatVersion)
        .recordCount(end.recordCount)
        .length(end.length)
        .crc32c(end.crc32c);
    out.resize(sbe::replay::MessageHeader::encodedLength() + encoder.encodedLength());
    return out;
}

} // namespace org::limitless::seqeron::replayer::client::restore_frames
