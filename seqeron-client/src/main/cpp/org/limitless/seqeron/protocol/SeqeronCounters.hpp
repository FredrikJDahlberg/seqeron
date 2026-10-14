#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

// C++ half of org.limitless.seqeron.protocol.SeqeronCounters, and of SeqeronCounters.cs. The type ids and key
// layout MUST match the Java class: the exporter names a counter by type id and reads memberId/clientId out of
// the key, so a mismatch publishes a counter nothing scrapes. App counters are keyed on {memberId, clientId},
// since a node's several replicas publish the same type id. Aeron reserves 0-999.
namespace org::limitless::seqeron::protocol {

// ── SequencerService (5000-5099) ─────────────────────────────────────────────────────────────
inline constexpr std::int32_t SEQUENCER_TYPE_ID_MIN = 5000;
inline constexpr std::int32_t SEQUENCER_TYPE_ID_MAX = 5099;

// The last globalSeqNo emitted on this node's tap.
inline constexpr std::int32_t SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID = 5000;
// Count of times the tap-emit back-pressure alert threshold has fired.
inline constexpr std::int32_t SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID = 5001;
// Count of malformed ingress messages the sequencer skipped.
inline constexpr std::int32_t SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID = 5002;
// Count of leadership changes this node has observed and sequenced.
inline constexpr std::int32_t SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID = 5003;
// memberId of the leader last recorded by this node's sequencer.
inline constexpr std::int32_t SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID = 5004;
// Consensus timestamp of the last 1 Hz ClusterHeartbeat emitted.
inline constexpr std::int32_t SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID = 5005;
// Count of standby-promotion GatewayActive frames emitted on a gateway session close.
inline constexpr std::int32_t SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID = 5006;
// 1 once the bootstrap GatewayActive has been emitted in this cluster's log, else 0.
inline constexpr std::int32_t SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID = 5007;
// 1 while tap back-pressure has lasted past the stall threshold with no recording progress, else 0.
inline constexpr std::int32_t SEQUENCER_TAP_STALLED_TYPE_ID = 5008;
// 5009 is retired, and never reused.
// Count of ingress messages successfully sequenced.
inline constexpr std::int32_t SEQUENCER_INGRESS_MESSAGES_TYPE_ID = 5010;
// Count of gateway session closes that found no standby to promote.
inline constexpr std::int32_t SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID = 5011;

// ── ReplayerService (5100-5199) ──────────────────────────────────────────────────────────────
inline constexpr std::int32_t REPLAYER_TYPE_ID_MIN = 5100;
inline constexpr std::int32_t REPLAYER_TYPE_ID_MAX = 5199;

// 1 once the co-located tap recording is visible and replay requests are being served, else 0.
inline constexpr std::int32_t REPLAYER_READY_TYPE_ID = 5101;
// Current count of in-flight replays.
inline constexpr std::int32_t REPLAYER_ACTIVE_SLOTS_TYPE_ID = 5102;
// Current count of replay requests waiting for a free slot.
inline constexpr std::int32_t REPLAYER_PENDING_REQUESTS_TYPE_ID = 5103;
// Count of replays started since this node came up.
inline constexpr std::int32_t REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID = 5104;
// Count of control replies dropped because an app stopped draining the control stream.
inline constexpr std::int32_t REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID = 5107;
// Count of replay requests held pending only because the tap recording trailed the tap while a replay ran.
inline constexpr std::int32_t REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID = 5110;
// Count of local-archive calls that threw; a sustained rate means the archive is not serving replays.
inline constexpr std::int32_t REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID = 5111;

// ── Co-located application replicas (5200-5299) ──────────────────────────────────────────────
// Core reserves 5200, 5298 and 5299; a consumer allocates its own in 5201-5297 (doc/ops.md, "Counter type ids").
inline constexpr std::int32_t APP_TYPE_ID_MIN = 5200;
inline constexpr std::int32_t APP_TYPE_ID_MAX = 5299;

// 1 while this replica's recovery has been reported unconvergent (see ReplayerRecovery::checkRecoveryProgress), else 0.
inline constexpr std::int32_t APP_RECOVERY_STALLED_TYPE_ID = 5200;

// The last snapshot round a participating client confirmed (doc/snapshot.md §4), 0 for none yet.
inline constexpr std::int32_t APP_SNAPSHOT_ROUND_TYPE_ID = 5298;

// Connections a gateway instance holds open: taken and not yet closed, and 0 while it does not serve.
inline constexpr std::int32_t GATEWAY_CONNECTIONS_TYPE_ID = 5299;

// The type ids seqeron owns, for a typeId-range scan.
inline constexpr std::int32_t MIN_TYPE_ID = SEQUENCER_TYPE_ID_MIN;
inline constexpr std::int32_t MAX_TYPE_ID = APP_TYPE_ID_MAX;

// Key layout: an operator counter's key is the memberId; an app counter's is memberId, then clientId.
inline constexpr std::size_t KEY_MEMBER_ID_OFFSET = 0;
inline constexpr std::size_t KEY_CLIENT_ID_OFFSET = 4;
inline constexpr std::size_t KEY_LENGTH = 4;
inline constexpr std::size_t APP_KEY_LENGTH = 8;

/**
 * Allocates an app counter keyed on {memberId, clientId}.
 *
 * @param aeron    the client to allocate it on
 * @param typeId   the counter's type id (doc/ops.md, "Counter type ids")
 * @param label    the counter's label
 * @param memberId the node the app runs on, the key's first half
 * @param clientId the app's client id, the key's second half
 * @return the registration id for Aeron::findCounter: the add is async, so callers resolve it on a later
 *         duty cycle
 */
inline std::int64_t addAppCounter(const std::shared_ptr<aeron::Aeron>& aeron, const std::int32_t typeId,
                                  const std::string& label, const std::int32_t memberId, const std::int32_t clientId)
{
    std::uint8_t key[APP_KEY_LENGTH] = {};
    std::memcpy(key + KEY_MEMBER_ID_OFFSET, &memberId, sizeof(memberId));
    std::memcpy(key + KEY_CLIENT_ID_OFFSET, &clientId, sizeof(clientId));
    return aeron->addCounter(typeId, key, APP_KEY_LENGTH, label);
}

} // namespace org::limitless::seqeron::protocol
