#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

// C++ half of org.limitless.seqeron.metrics.SeqeronCounters. The type ids and key layout MUST match the
// Java class: the exporter names a counter by type id and reads memberId/clientId out of the key, so a
// mismatch publishes a counter nothing scrapes. App counters are keyed on {memberId, clientId}, since a
// node's several replicas publish the same type id.
namespace org::limitless::seqeron::util {

// ── Co-located C++ application replicas (5200-5299) ──────────────────────────────────────────
inline constexpr std::int32_t APP_TYPE_ID_MIN = 5200;

// 1 while this replica's recovery has been reported unconvergent (see ReplayerRecovery::checkRecoveryProgress), else 0.
inline constexpr std::int32_t APP_RECOVERY_STALLED_TYPE_ID = 5200;

inline constexpr std::size_t KEY_MEMBER_ID_OFFSET = 0;
inline constexpr std::size_t KEY_CLIENT_ID_OFFSET = 4;
inline constexpr std::size_t APP_KEY_LENGTH = 8;

// Allocates an app counter keyed on {memberId, clientId}. Returns the registration id for
// Aeron::findCounter: the add is async, so callers resolve it on a later duty cycle.
inline std::int64_t addAppCounter(const std::shared_ptr<aeron::Aeron>& aeron, const std::int32_t typeId,
                                  const std::string& label, const std::int32_t memberId, const std::int32_t clientId)
{
    std::uint8_t key[APP_KEY_LENGTH] = {};
    std::memcpy(key + KEY_MEMBER_ID_OFFSET, &memberId, sizeof(memberId));
    std::memcpy(key + KEY_CLIENT_ID_OFFSET, &clientId, sizeof(clientId));
    return aeron->addCounter(typeId, key, APP_KEY_LENGTH, label);
}

} // namespace org::limitless::seqeron::util
