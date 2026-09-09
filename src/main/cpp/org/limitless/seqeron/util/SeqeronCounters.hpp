#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

// C++ half of org.limitless.seqeron.metrics.SeqeronCounters — the operator counters seqeron's own
// processes publish into their node's media-driver counters file, where MetricsExporter reads them.
//
// The type ids and the key layout below MUST match the Java class, exactly as FEEDER_STREAM_ID is
// matched across the two sides: the exporter maps a counter to a metric name BY TYPE ID and reads
// memberId/clientId straight out of the key, so a mismatch here silently publishes a counter nothing
// scrapes, or scrapes one under the wrong name.
//
// App counters carry TWO ints, not one. A node runs several co-located C++ replicas (FixGateway,
// OrderExecServer, BasicDataServer) and each publishes the same type id, so memberId alone would give
// them identical Prometheus label sets — one series per node, silently overwritten. The replayer
// clientId disambiguates them, and it is already the per-node-unique id ReplayClientIdCollisions
// polices.
namespace org::limitless::seqeron::util {

// ── Co-located C++ application replicas (5200-5299) ──────────────────────────────────────────
inline constexpr std::int32_t APP_TYPE_ID_MIN = 5200;

// 1 while this replica's recovery has been reported unconvergent (see RecoveryProgressPolicy), else 0.
inline constexpr std::int32_t APP_RECOVERY_STALLED_TYPE_ID = 5200;

inline constexpr std::size_t KEY_MEMBER_ID_OFFSET = 0;
inline constexpr std::size_t KEY_CLIENT_ID_OFFSET = 4;
inline constexpr std::size_t APP_KEY_LENGTH = 8;

// Allocates an app counter keyed on {memberId, clientId}. Returns the registration id to resolve with
// Aeron::findCounter — the add is async, so the counter is not usable on the calling line (callers
// resolve it on a later duty cycle, exactly as they do publications and subscriptions).
inline std::int64_t addAppCounter(const std::shared_ptr<aeron::Aeron>& aeron, const std::int32_t typeId,
                                  const std::string& label, const std::int32_t memberId, const std::int32_t clientId)
{
    std::uint8_t key[APP_KEY_LENGTH] = {};
    std::memcpy(key + KEY_MEMBER_ID_OFFSET, &memberId, sizeof(memberId));
    std::memcpy(key + KEY_CLIENT_ID_OFFSET, &clientId, sizeof(clientId));
    return aeron->addCounter(typeId, key, APP_KEY_LENGTH, label);
}

} // namespace org::limitless::seqeron::util
