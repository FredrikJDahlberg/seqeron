#pragma once

// The replay protocol's addresses, shared by the Replayer and its co-located clients. The Java twin is
// org.limitless.seqeron.protocol.ReplayProtocol.

#include <cstdint>

namespace org::limitless::seqeron::protocol {

// The whole protocol is node-local: a client and its Replayer share one media driver.
inline constexpr const char* REPLAYER_IPC_CHANNEL = "aeron:ipc?alias=seqeron-replay";

// Replayer -> client: the replays themselves, one Aeron session each.
inline constexpr std::int32_t REPLAYER_REPLAY_STREAM_ID = 201;
// Client -> Replayer: ReplayRequest and QueueRequest.
inline constexpr std::int32_t REPLAYER_REQUEST_STREAM_ID = 202;
// Replayer -> client: Replaying, ReplayPending and QueueResolved.
inline constexpr std::int32_t REPLAYER_CONTROL_STREAM_ID = 203;

// ReplayRequest.fromPosition asking for the active recording from its start.
inline constexpr std::int64_t REPLAYER_FROM_START = -1;
// Replaying.replaySessionId when there is nothing to replay from where the request asked.
inline constexpr std::int64_t REPLAYER_NO_REPLAY_NEEDED = -1;
// QueueRequest.sourceId naming no application.
inline constexpr std::int32_t REPLAYER_NO_SOURCE_ID = -1;
// ReplayRequest.cutClusterSeqNo of a request that is not a snapshot restore's.
inline constexpr std::int64_t REPLAYER_NO_CUT = -1;

} // namespace org::limitless::seqeron::protocol
