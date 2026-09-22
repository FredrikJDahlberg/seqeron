#pragma once

#include <cstddef>
#include <cstdint>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

// The defaults both façades' Config takes; the Java twins are the DEFAULT_* constants on Gateway and
// ColocatedApplication.

namespace org::limitless::seqeron::app {

// The deployment policy every producer had been copying: the tap may be silent for 20 heartbeat periods.
inline constexpr std::int64_t DEFAULT_TAP_STALL_TIMEOUT_MS = 20 * protocol::CLUSTER_HEARTBEAT_INTERVAL_MS;

// Longer than the tap's, since a re-walk is slower than the live stream it is catching up to.
inline constexpr std::int64_t DEFAULT_RECOVERY_STALL_TIMEOUT_MS = 3 * DEFAULT_TAP_STALL_TIMEOUT_MS;

// Frames in flight between a publish and the tap; far above what one round trip holds.
inline constexpr std::size_t DEFAULT_PENDING_CAPACITY = 1024;

// How long ingress is tried on this member's own aeron:ipc: short, as a follower never answers.
inline constexpr std::int64_t DEFAULT_IPC_CONNECT_TIMEOUT_MS = 500;

// The lag at which the tap is called stale. The same span as the tap-silence timeout: a tap that is a
// whole stall window behind is as good as silent to anything reading it.
inline constexpr std::int64_t DEFAULT_TAP_LAG_THRESHOLD_MS = DEFAULT_TAP_STALL_TIMEOUT_MS;

} // namespace org::limitless::seqeron::app
