#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/detail/RecoveryStallFence.hpp"
#include "org/limitless/seqeron/app/detail/TapStallFence.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/sequencer/client/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"
#include "org/limitless/seqeron/sequencer/client/PendingSends.hpp"

namespace org::limitless::seqeron::app::detail {

/**
 * What every seqeron client does the same way: the cluster session it submits on, the co-located tap it
 * follows, confirmed ingress across a failover, and the fences that say when it may no longer act — wired
 * together into one doWork() whose ordering is not the caller's to get right.
 *
 * Not API. Gateway and Application are the façades over it; it holds no election and no leader
 * gate of its own, so both sit on the same core. The Java twin is app/Session.java; keep the two in step.
 *
 * Dispatch provides:
 *   void onSystem(const protocol::SequencedEvent& event) // LeadershipChanged never arrives here
 *   void onLeadershipChanged()                           // confirmed ingress has already taken the new term
 *   void onPayload(const Payload& payload)
 *   void onCaughtUp(std::int64_t globalSeqNo)            // every transition to caught-up, the first included
 *   void onClusterHeartbeat(std::int64_t clusterTimeNs, std::int64_t receiveTimeNs) // the cluster clock's tick
 *   void onFenced(ClusterError fence, const std::string& detail) // once, latched
 */
template<typename Dispatch>
class Session
{
  public:
    Session(const std::int32_t clientId, const std::size_t pendingCapacity, const std::int64_t tapStallTimeoutMs,
            const std::int64_t recoveryStallTimeoutMs, Dispatch& dispatch) :
      m_dispatch{ dispatch },
      m_pending{ pendingCapacity },
      m_recoveryStall{ recoveryStallTimeoutMs },
      m_tapStall{ tapStallTimeoutMs },
      m_recoveryStallTimeoutMs{ recoveryStallTimeoutMs },
      m_tapStallTimeoutMs{ tapStallTimeoutMs },
      m_receiver{ clientId,
                  [this](const protocol::SequencedEvent& event) { onSequenced(event); },
                  {},
                  {},
                  [this](std::int32_t, std::int64_t leadershipTermId, std::int64_t) {
                      onLeadershipChanged(leadershipTermId);
                  } }
    {
        m_sender.setIngressHold(&m_pending);
    }

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Opens the cluster session over UDP and starts following this node's tap.
    void start(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId, const std::string& egressChannel)
    {
        m_sender.connect(aeron, egressChannel);
        m_receiver.start(std::move(aeron), memberId);
    }

    // The same for a client sharing this member's media driver: ingress over its own aeron:ipc while that
    // member leads, the UDP endpoint set when it does not.
    void startColocated(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId,
                        const std::int64_t ipcConnectTimeoutMs, const std::string& egressChannel)
    {
        m_sender.connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel);
        m_receiver.start(std::move(aeron), memberId);
    }

    // One duty cycle, in the order the parts require: the tap first, so a fence is judged on what this cycle
    // saw; the resend last, so nothing new goes out ahead of it. Returns units of work done.
    int doWork()
    {
        if (m_fenced)
        {
            return 0;
        }
        int work = m_receiver.poll();
        checkCaughtUp();
        checkFences();
        if (m_fenced)
        {
            return work;
        }
        // Nothing is addressed to a producer on egress: everything it publishes comes back on the tap. Unlike
        // the Java twin this counts no work, since the C++ pollEgress reports no fragment count.
        m_sender.pollEgress([](const std::uint8_t*, std::int32_t) {});
        m_sender.keepAlive();
        work += static_cast<int>(m_pending.resendMissing(m_sender));
        return work;
    }

    // The same for a payload a caller encoded into a buffer of its own; the Java twin's only publish.
    [[nodiscard]] protocol::Publish publishPayload(const std::int32_t sourceId, const std::int32_t connectionId,
                                                   const std::uint16_t payloadId, const std::uint8_t* payload,
                                                   const std::uint16_t payloadLength)
    {
        if (m_fenced)
        {
            return protocol::Publish::Declined;
        }
        return sequencer::client::publishPayload(m_sender, &m_pending, sourceId, connectionId, payloadId, payload,
                                                 payloadLength);
    }

    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publishPayload(const std::int32_t sourceId, const std::int32_t connectionId,
                                                   const std::uint16_t payloadId, Fill&& fill)
    {
        if (m_fenced)
        {
            return protocol::Publish::Declined;
        }
        return sequencer::client::publishPayload<Encoder>(m_sender, &m_pending, sourceId, connectionId, payloadId,
                                                          std::forward<Fill>(fill));
    }

    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publishSystem(const std::int32_t sourceId, const std::int32_t connectionId,
                                                  const std::uint16_t systemEventType, Fill&& fill)
    {
        if (m_fenced)
        {
            return protocol::Publish::Declined;
        }
        return sequencer::client::publishSystem<Encoder>(m_sender, &m_pending, sourceId, connectionId, systemEventType,
                                                         std::forward<Fill>(fill));
    }

    // Tells the cluster this client is alive, for a facade whose consumer spins inside a callback and would
    // otherwise starve the one in doWork(). Self-throttling, so calling it per spin costs nothing.
    void keepAlive()
    {
        if (!m_fenced)
        {
            m_sender.keepAlive();
        }
    }

    // Send nothing new while this holds: an older term's frames are still unseen or unresent.
    [[nodiscard]] bool isHolding() const
    {
        return m_pending.isHolding();
    }

    [[nodiscard]] bool isCaughtUp() const
    {
        return m_receiver.isCaughtUp();
    }

    [[nodiscard]] std::int64_t lastGlobalSeqNo() const
    {
        return m_receiver.lastGlobalSeqNo();
    }

    [[nodiscard]] std::int32_t currentLeaderMemberId() const
    {
        return m_receiver.currentLeaderMemberId();
    }

    // Closes the cluster session. The receiver releases its streams when this object goes.
    void close()
    {
        m_sender.close();
    }

  private:
    // The fences are deadlines, so they are measured on a clock no wall-clock step can move.
    static std::int64_t monotonicMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
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
        if (!now)
        {
            return;
        }
        m_recoveryStall.onCaughtUp();
        m_tapStall.onCaughtUp(monotonicMs());
        m_dispatch.onCaughtUp(m_receiver.lastGlobalSeqNo());
    }

    void checkFences()
    {
        // isConnected() as well as isSessionLost(): the sender also gives its session up, with no event at
        // all, when a new leader does not arrive before its timeout.
        if (m_sender.isSessionLost() || !m_sender.isConnected())
        {
            fence(ClusterError::ClusterSessionLost, m_sender.isSessionLost() ? "session lost" : "closed");
            return;
        }
        if (m_pending.isFaulted())
        {
            fence(ClusterError::IngressConfirmFaulted,
                  "an own frame came back differing from the oldest pending one, so what reached the log can no "
                  "longer be counted");
            return;
        }
        const std::int64_t nowMs = monotonicMs();
        if (!m_receiver.isCaughtUp())
        {
            if (m_recoveryStall.onNotCaughtUp(nowMs, m_receiver.lastGlobalSeqNo()))
            {
                fence(ClusterError::RecoveryStalled,
                      "recovery has dispatched nothing for >" + std::to_string(m_recoveryStallTimeoutMs) +
                          "ms (globalSeqNo stuck at " + std::to_string(m_receiver.lastGlobalSeqNo()) + ")");
            }
            return;
        }
        if (m_tapStall.isStalled(nowMs))
        {
            fence(ClusterError::TapStalled, "no ClusterHeartbeat for >" + std::to_string(m_tapStallTimeoutMs) + "ms");
        }
    }

    void fence(const ClusterError reason, const std::string& detail)
    {
        m_fenced = true;
        m_dispatch.onFenced(reason, detail);
    }

    // Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on.
    void onLeadershipChanged(const std::int64_t leadershipTermId)
    {
        m_pending.onLeadershipChanged(leadershipTermId);
        m_dispatch.onLeadershipChanged();
    }

    void onSequenced(const protocol::SequencedEvent& event)
    {
        m_pending.onSequenced(event);
        if (event.system)
        {
            if (event.systemEventType == protocol::CLUSTER_HEARTBEAT)
            {
                m_tapStall.onClusterHeartbeat(monotonicMs());
                m_dispatch.onClusterHeartbeat(event.clusterTimestampNs, event.receiveTimeNs);
            }
            m_dispatch.onSystem(event);
            return;
        }
        m_dispatch.onPayload(Payload{ event });
    }

    Dispatch& m_dispatch;
    sequencer::client::PendingSends m_pending;
    sequencer::client::ClusterStreamSender m_sender;
    RecoveryStallFence m_recoveryStall;
    TapStallFence m_tapStall;
    std::int64_t m_recoveryStallTimeoutMs;
    std::int64_t m_tapStallTimeoutMs;
    replayer::client::ReplayerStreamReceiver m_receiver;

    bool m_caughtUp = false;
    bool m_fenced = false;
};

} // namespace org::limitless::seqeron::app::detail
