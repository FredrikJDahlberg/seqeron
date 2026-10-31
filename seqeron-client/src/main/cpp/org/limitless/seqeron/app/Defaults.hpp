#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

// The defaults both façades' Config takes; the Java twins are the DEFAULT_* constants on Gateway and
// Application.

namespace org::limitless::seqeron::app {

// The deployment policy every producer had been copying: the queue may be silent for 20 heartbeat periods.
inline constexpr std::int64_t DEFAULT_QUEUE_STALL_TIMEOUT_MS = 20 * protocol::CLUSTER_HEARTBEAT_INTERVAL_MS;

// Longer than the queue's, since a re-walk is slower than the live stream it is catching up to.
inline constexpr std::int64_t DEFAULT_RECOVERY_STALL_TIMEOUT_MS = 3 * DEFAULT_QUEUE_STALL_TIMEOUT_MS;

// Frames in flight between a publish and the queue; far above what one round trip holds.
inline constexpr std::size_t DEFAULT_PENDING_CAPACITY = 1024;

// How long ingress is tried on this member's own aeron:ipc: short, as a follower never answers.
inline constexpr std::int64_t DEFAULT_IPC_CONNECT_TIMEOUT_MS = 500;

// An ephemeral port: one cluster session, and no port of its own to allocate.
inline const std::string DEFAULT_EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

/**
 * The policy knobs a producer rarely changes, kept off its Config so that holds only identity and wiring.
 *
 * pendingCapacity is the messages confirmed ingress tracks, beyond which publish returns Declined; a Consumer sends
 * nothing and ignores it. queueStallTimeoutMs fences, with QUEUE_STALLED, an instance that acts when no
 * ClusterHeartbeat arrives for that long, and recoveryStallTimeoutMs does so with RECOVERY_STALLED when recovery
 * dispatches nothing for that long; any other instance logs.
 */
struct Tuning
{
    std::size_t pendingCapacity = DEFAULT_PENDING_CAPACITY;
    std::int64_t queueStallTimeoutMs = DEFAULT_QUEUE_STALL_TIMEOUT_MS;
    std::int64_t recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;
};

} // namespace org::limitless::seqeron::app
