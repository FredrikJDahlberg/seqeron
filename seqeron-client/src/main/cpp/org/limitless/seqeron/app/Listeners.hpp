#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/GroupDirectory.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"

namespace org::limitless::seqeron::app {

/**
 * What an Application's Listener provides. onGroupsResolved(directory), onCaughtUp(clusterSeqNo) and
 * onClusterHeartbeat(clusterTimeNs, receiveTimeNs) are optional: the replica calls them only if the Listener has them.
 */
template<typename L>
concept ApplicationListener = requires(L& listener, const GroupDirectory& directory, const Payload& payload,
                                       bool leading, std::int64_t clusterSeqNo, std::int64_t clusterTimeNs,
                                       std::int64_t receiveTimeNs, ClusterError fence, const std::string& detail) {
    // The gate crossed an edge. Every leadership change shuts an open gate, and the work may have gone with the
    // election: Config::onNotLeader is where OutstandingWork::onNotLeader() belongs. False also comes before
    // onFenced when this replica fences with its gate open.
    listener.onLeadershipChanged(leading);
    listener.onSequenced(payload);
    listener.onFenced(fence, detail);
};

/**
 * What a Consumer's Listener provides. onCaughtUp(clusterSeqNo) and onClusterHeartbeat(clusterTimeNs, receiveTimeNs)
 * are optional: the consumer calls them only if the Listener has them.
 */
template<typename L>
concept ConsumerListener =
    requires(L& listener, const Payload& payload, ClusterError fence, const std::string& detail) {
        // One application payload off the queue, in clusterSeqNo order.
        listener.onSequenced(payload);
        // Latched, once: this consumer may no longer be trusted to see the queue. Exiting is the usual way.
        listener.onFenced(fence, detail);
    };

/**
 * What a Gateway's Listener provides: the edge. onGroupsResolved(directory), onConnectionOpened(connectionId, data,
 * length), onConnectionClosed(connectionId), onCaughtUp(clusterSeqNo) and onClusterHeartbeat(clusterTimeNs,
 * receiveTimeNs) are optional: the gateway calls them only if the Listener has them.
 */
template<typename L>
concept GatewayListener =
    requires(L& listener, const GroupDirectory& directory, const Payload& payload, std::int32_t connectionId,
             const char* data, std::size_t length, std::int64_t clusterSeqNo, std::int64_t clusterTimeNs,
             std::int64_t receiveTimeNs, ClusterError fence, const std::string& detail) {
        // Open the edge; false is retried on the next doWork().
        { listener.onActivated(connectionId) } -> std::convertible_to<bool>;
        // Close it and drop every connection it let in: on a sibling's designation, and before onFenced when this
        // instance fences while serving.
        listener.onStandby();
        listener.onSequenced(payload);
        listener.onFenced(fence, detail);
    };

} // namespace org::limitless::seqeron::app
