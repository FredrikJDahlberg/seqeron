#pragma once

// The replay protocol's addresses, shared by the Replayer and its co-located clients. The Java twin is
// org.limitless.seqeron.protocol.ReplayProtocol.

#include <cstdint>

namespace org::limitless::seqeron::protocol {

inline constexpr const char* REPLAYER_IPC_CHANNEL = "aeron:ipc";
inline constexpr std::int32_t REPLAYER_REPLAY_STREAM_ID = 201;
inline constexpr std::int32_t REPLAYER_REQUEST_STREAM_ID = 202;
inline constexpr std::int32_t REPLAYER_CONTROL_STREAM_ID = 203;

// Replaying.replaySessionId sentinel: "nothing to replay, you are at the tip — follow the live tap".
inline constexpr std::int64_t REPLAYER_NO_REPLAY_NEEDED = -1;

} // namespace org::limitless::seqeron::protocol
