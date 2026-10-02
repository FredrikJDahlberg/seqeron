#pragma once

// A snapshot's frames as a restore replays them, and the Replayer's answer locating it (doc/snapshot.md §5, §7).
// The twin of the Java tests' RestoreFrames.

#include <array>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotChunk.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotEnd.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotStarted.h"
#include "org_limitless_seqeron_sbe_replay/SnapshotLocation.h"

namespace org::limitless::seqeron::replayer::client::restore_frames {

using Bytes = std::vector<std::uint8_t>;

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
    Bytes out(protocol::MAX_SNAPSHOT_RECORD_LENGTH + 128, 0);
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

inline Bytes chunk(const std::int64_t globalSeqNo, const std::int32_t sourceId, const std::int64_t round,
                   const std::int32_t chunkIndex, const Bytes& record)
{
    std::array<char, 2048> body{};
    sbe::frame::SnapshotChunk encoder;
    encoder.wrapForEncode(body.data(), 0, body.size());
    encoder.round(round)
        .chunkIndex(chunkIndex)
        .putData(reinterpret_cast<const char*>(record.data()), static_cast<std::uint16_t>(record.size()));
    return system(globalSeqNo, sourceId, protocol::SNAPSHOT_CHUNK, body.data(),
                  static_cast<std::uint16_t>(encoder.encodedLength()));
}

inline Bytes end(const std::int64_t globalSeqNo, const std::int32_t sourceId, const std::int64_t round,
                 const std::int32_t chunkCount, const std::int64_t length, const std::uint32_t crc32c,
                 const std::uint32_t formatVersion)
{
    std::array<char, 64> body{};
    sbe::frame::SnapshotEnd encoder;
    encoder.wrapForEncode(body.data(), 0, body.size());
    encoder.round(round).chunkCount(chunkCount).length(length).crc32c(crc32c).formatVersion(formatVersion);
    return system(globalSeqNo, sourceId, protocol::SNAPSHOT_END, body.data(),
                  static_cast<std::uint16_t>(encoder.encodedLength()));
}

// The end the records validate against.
inline Bytes end(const std::int64_t globalSeqNo, const std::int32_t sourceId, const std::int64_t round,
                 const std::uint32_t formatVersion, const std::initializer_list<Bytes> records)
{
    std::int64_t length = 0;
    std::uint32_t crc = 0;
    for (const Bytes& record : records)
    {
        length += static_cast<std::int64_t>(record.size());
        crc = protocol::crc32c(record.data(), record.size(), crc);
    }
    return end(globalSeqNo, sourceId, round, static_cast<std::int32_t>(records.size()), length, crc, formatVersion);
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

// The Replayer's answer; round -1 for none.
inline Bytes location(const std::int32_t clientId, const std::int64_t requestId, const std::int64_t round,
                      const std::int64_t asOfGlobalSeqNo, const std::int64_t asOfPosition,
                      const std::int64_t endPosition, const std::uint32_t formatVersion)
{
    Bytes out(128, 0);
    sbe::replay::SnapshotLocation encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    encoder.clientId(clientId)
        .requestId(requestId)
        .round(round)
        .asOfGlobalSeqNo(asOfGlobalSeqNo)
        .asOfPosition(asOfPosition)
        .endPosition(endPosition)
        .formatVersion(formatVersion);
    out.resize(sbe::replay::MessageHeader::encodedLength() + encoder.encodedLength());
    return out;
}

} // namespace org::limitless::seqeron::replayer::client::restore_frames
