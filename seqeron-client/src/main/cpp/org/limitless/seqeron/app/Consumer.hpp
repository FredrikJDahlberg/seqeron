#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/detail/QueueStallFence.hpp"
#include "org/limitless/seqeron/app/detail/RecoveryStallFence.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"

namespace org::limitless::seqeron::app {

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
 * A read-only client: it reads one group's queue and publishes nothing, so it opens no cluster session, takes no
 * part in an election and needs no topology row. Audit, risk or a downstream feed. It replays local history through
 * the co-located Replayer, switches to the live queue once caught up, and delivers every application message once,
 * in clusterSeqNo order, as a Payload — the same view the producers' façades give.
 *
 * Fenced, like them, when it can no longer be trusted to see the queue: ClusterError::RecoveryStalled and
 * ClusterError::QueueStalled. It takes no snapshots and so always restarts from the start of its queue, which a
 * cluster with a <snapshots> policy no longer holds; a consumer there is an Application or Gateway with a
 * SnapshotListener that writes no records.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls doWork() each iteration.
 * The Java twin is app/Consumer.java; keep the two in step. ReplayerStreamReceiver is the lower-level path this sits
 * on.
 */
template<typename Listener>
class Consumer
{
  public:
    // Everything one consumer needs to follow its queue.
    struct Config
    {
        // This consumer's client id, its counters' label — required, and unique among the clients on its node: two
        // sharing one share their counters' series.
        std::optional<std::int32_t> clientId;
        // The node whose queue this consumer follows, or a gateway host's node id.
        std::int32_t memberId = 0;
        // The group whose queue this consumer reads — required; the first topology's is 1.
        std::optional<std::int32_t> groupId;
        // The stall policy; pendingCapacity is unused, as a consumer sends nothing.
        Tuning tuning;
    };

    /**
     * Creates a consumer that has not started.
     *
     * @param config   the consumer's identity and the group it reads
     * @param listener the consumer's handlers; must outlive it
     * @throws std::invalid_argument if config has no clientId or no groupId
     */
    Consumer(Config config, Listener& listener) :
      m_config{ require(std::move(config)) },
      m_listener{ listener },
      m_recoveryStall{ m_config.tuning.recoveryStallTimeoutMs },
      m_queueStall{ m_config.tuning.queueStallTimeoutMs },
      m_receiver{ *m_config.clientId, [this](const protocol::SequencedEvent& event) { onSequenced(event); } }
    {
        // Here rather than on the template parameter, where a listener that owns its Consumer is incomplete.
        static_assert(ConsumerListener<Listener>);
    }

    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;

    // Starts following the group's queue on this node; the Aeron client is the caller's.
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_receiver.start(std::move(aeron), m_config.memberId, *m_config.groupId);
    }

    // One duty-cycle iteration. Nothing once fenced. Returns units of work done, for the caller's idle strategy.
    int doWork()
    {
        if (m_fenced)
        {
            return 0;
        }
        const int work = m_receiver.poll();
        checkCaughtUp();
        if (!m_fenced)
        {
            checkFences();
        }
        return work;
    }

    [[nodiscard]] bool isCaughtUp() const
    {
        return m_receiver.isCaughtUp();
    }

    [[nodiscard]] std::int64_t lastClusterSeqNo() const
    {
        return m_receiver.lastClusterSeqNo();
    }

    // Closes the queue and replay subscriptions. The Aeron client is the caller's and is left open.
    void close()
    {
        m_receiver.close();
    }

  private:
    static Config require(Config config)
    {
        if (!config.clientId)
        {
            throw std::invalid_argument("clientId is required");
        }
        if (!config.groupId)
        {
            throw std::invalid_argument("groupId is required");
        }
        return config;
    }

    static std::int64_t monotonicMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    void onSequenced(const protocol::SequencedEvent& event)
    {
        if (event.system)
        {
            if (event.systemEventType == protocol::CLUSTER_HEARTBEAT)
            {
                m_queueStall.onClusterHeartbeat(monotonicMs());
                if constexpr (requires {
                                  m_listener.onClusterHeartbeat(event.clusterTimestampNs, event.receiveTimeNs);
                              })
                {
                    m_listener.onClusterHeartbeat(event.clusterTimestampNs, event.receiveTimeNs);
                }
            }
            return;
        }
        m_listener.onSequenced(detail::makePayload(event));
    }

    // Polled rather than taken off the receiver's callback: neither fence may wait on the next frame to land.
    void checkCaughtUp()
    {
        const bool now = m_receiver.isCaughtUp();
        if (now == m_caughtUp)
        {
            return;
        }
        m_caughtUp = now;
        if (now)
        {
            m_recoveryStall.onCaughtUp();
            m_queueStall.onCaughtUp(monotonicMs());
            if constexpr (requires { m_listener.onCaughtUp(m_receiver.lastClusterSeqNo()); })
            {
                m_listener.onCaughtUp(m_receiver.lastClusterSeqNo());
            }
        }
    }

    void checkFences()
    {
        const std::int64_t nowMs = monotonicMs();
        if (!m_receiver.isCaughtUp())
        {
            if (m_recoveryStall.onNotCaughtUp(nowMs, m_receiver.lastClusterSeqNo()))
            {
                fence(ClusterError::RecoveryStalled,
                      "recovery has dispatched nothing for >" + std::to_string(m_config.tuning.recoveryStallTimeoutMs) +
                          "ms (clusterSeqNo stuck at " + std::to_string(m_receiver.lastClusterSeqNo()) + ")");
            }
        }
        else if (m_queueStall.isStalled(nowMs))
        {
            fence(ClusterError::QueueStalled,
                  "no ClusterHeartbeat for >" + std::to_string(m_config.tuning.queueStallTimeoutMs) + "ms");
        }
    }

    // Releases the subscriptions first, so a fenced consumer holds no Replayer slot while its listener hears why.
    void fence(const ClusterError reason, const std::string& detail)
    {
        m_fenced = true;
        m_receiver.close();
        m_listener.onFenced(reason, detail);
    }

    Config m_config;
    Listener& m_listener;
    detail::RecoveryStallFence m_recoveryStall;
    detail::QueueStallFence m_queueStall;
    replayer::client::ReplayerStreamReceiver m_receiver;
    bool m_caughtUp = false;
    bool m_fenced = false;
};

} // namespace org::limitless::seqeron::app
