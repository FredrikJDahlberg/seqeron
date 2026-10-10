#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
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
 *   bool mayReconnect() const // whether a session the cluster closed may be replaced rather than fenced
 *   bool isActing() const     // whether this instance acts for its producer: a stall fences only one that does
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

    /**
     * Restores a source's newest snapshot in a store that the log confirms before following the tap; call before
     * starting.
     *
     * @param sourceId the source whose snapshot to restore
     * @param store    this instance's own snapshots; must outlive this object
     * @param handler  takes the snapshot's records; must outlive this object
     */
    void restoreFrom(const std::int32_t sourceId, replayer::client::SnapshotStore& store,
                     replayer::client::SnapshotRestoreHandler& handler)
    {
        m_receiver.restoreFrom(sourceId, store, handler);
    }

    // Follows the tap from its start again, restoring the snapshot given to restoreFrom first.
    void restart()
    {
        m_recoveryStall.onRestart(); // the restore pass dispatches nothing, and is no stall
        m_receiver.restart();
    }

    // Opens the cluster session over UDP and starts following this node's tap.
    void start(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId, const std::string& egressChannel,
               const std::string& ingressEndpoints)
    {
        m_sender.connect(aeron, egressChannel, ingressEndpoints);
        m_receiver.start(std::move(aeron), memberId);
    }

    // The same for a client sharing this member's media driver: ingress over its own aeron:ipc while that
    // member leads, the UDP endpoint set when it does not.
    void startColocated(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId,
                        const std::int64_t ipcConnectTimeoutMs, const std::string& egressChannel,
                        const std::string& ingressEndpoints)
    {
        m_sender.connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints);
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

    [[nodiscard]] bool isFenced() const
    {
        return m_fenced;
    }

    [[nodiscard]] std::int64_t lastGlobalSeqNo() const
    {
        return m_receiver.lastGlobalSeqNo();
    }

    [[nodiscard]] std::int32_t currentLeaderMemberId() const
    {
        return m_receiver.currentLeaderMemberId();
    }

    // The term of the last LeadershipChanged applied; -1 before the first.
    [[nodiscard]] std::int64_t leadershipTermId() const
    {
        return m_leadershipTermId;
    }

    /**
     * Latches the first fence; a façade raises its own, such as a diverged snapshot, through this too.
     *
     * @param reason why this producer may no longer act
     * @param detail what the log line says
     */
    void fence(const ClusterError reason, const std::string& detail)
    {
        if (m_fenced)
        {
            return;
        }
        m_fenced = true;
        if (m_sender.isReconnecting())
        {
            m_sender.close(); // nothing advances the attempt once fenced
        }
        m_dispatch.onFenced(reason, detail);
    }

    /**
     * Forgets every frame not yet seen on the tap, for a façade that knows none of them will be sequenced.
     *
     * @param why what the log line says
     */
    void discardUnconfirmed(const std::string& why)
    {
        const std::size_t dropped = m_pending.discardUnconfirmed();
        if (dropped > 0)
        {
            util::Logger::info(util::component::Cluster, "%zu unconfirmed frames are not resent: %s", dropped,
                               why.c_str());
        }
    }

    // Whether a cluster session is open: the sender gives its session up, with no event at all, when a new
    // leader does not arrive before its timeout, hence isConnected() as well as the latch.
    [[nodiscard]] bool hasSession() const
    {
        return !m_sender.isSessionLost() && m_sender.isConnected();
    }

    // Closes the cluster session. The receiver releases its streams when this object goes.
    void close()
    {
        m_sender.close();
    }

  private:
    // How often an attempt to replace a lost session begins while the façade allows it.
    static constexpr std::int64_t RECONNECT_INTERVAL_MS = 1'000;

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
        if (!hasSession())
        {
            const char* why = m_sender.isSessionLost() ? "session lost" : "closed";
            if (!m_dispatch.mayReconnect())
            {
                fence(ClusterError::ClusterSessionLost, why);
                return;
            }
            replaceSession(why);
            if (m_fenced)
            {
                return;
            }
        }
        if (m_pending.isFaulted())
        {
            fence(ClusterError::IngressConfirmFaulted,
                  "an own frame came back differing from the oldest pending one, so what reached the log can no "
                  "longer be counted");
            return;
        }
        if (m_receiver.restoreFailure())
        {
            fence(ClusterError::SnapshotUnrestorable, *m_receiver.restoreFailure());
            return;
        }
        const std::int64_t nowMs = monotonicMs();
        if (!m_receiver.isCaughtUp())
        {
            if (m_recoveryStall.onNotCaughtUp(nowMs, m_receiver.lastGlobalSeqNo()))
            {
                stalled(ClusterError::RecoveryStalled, util::eventCode::RecoveryStalled,
                        "recovery has dispatched nothing for >" + std::to_string(m_recoveryStallTimeoutMs) +
                            "ms (globalSeqNo stuck at " + std::to_string(m_receiver.lastGlobalSeqNo()) + ")");
                return;
            }
        }
        else if (m_tapStall.isStalled(nowMs))
        {
            stalled(ClusterError::TapStalled, util::eventCode::TapStalled,
                    "no ClusterHeartbeat for >" + std::to_string(m_tapStallTimeoutMs) + "ms");
            return;
        }
        if (m_stallAlarm)
        {
            util::Logger::info(util::component::Cluster, "%s cleared", clusterErrorName(*m_stallAlarm));
            m_stallAlarm.reset();
        }
    }

    // Fences an instance that acts; one that does not raises an alarm once and keeps following the tap.
    void stalled(const ClusterError stall, const util::EventCode& code, const std::string& detail)
    {
        if (m_dispatch.isActing())
        {
            fence(stall, detail);
            return;
        }
        if (m_stallAlarm != stall)
        {
            m_stallAlarm = stall;
            util::Logger::error(util::component::Cluster, code, "%s, not fenced while this instance does not act: %s",
                                clusterErrorName(stall), detail.c_str());
        }
    }

    // Opens a session in place of the lost one, an attempt at most once a second and each advanced a step per
    // cycle, until one opens or the tap could have gone silent for as long: past that the cluster is not coming
    // back for this process, and the session's loss is a fence. An attempt under way then is let finish, since one
    // whose request an election swallowed spends its whole timeout.
    void replaceSession(const char* why)
    {
        const std::int64_t nowMs = monotonicMs();
        if (m_sessionLostSinceMs < 0)
        {
            m_sessionLostSinceMs = nowMs;
            m_nextReconnectMs = nowMs;
        }
        if (nowMs - m_sessionLostSinceMs > m_tapStallTimeoutMs && !m_sender.isReconnecting())
        {
            fence(ClusterError::ClusterSessionLost,
                  std::string{ why } + "; no session replaced it within " + std::to_string(m_tapStallTimeoutMs) + "ms");
            return;
        }
        if (!m_sender.isReconnecting())
        {
            if (nowMs < m_nextReconnectMs)
            {
                return;
            }
            m_nextReconnectMs = nowMs + RECONNECT_INTERVAL_MS;
        }
        if (!m_sender.reconnect())
        {
            return;
        }
        m_sessionLostSinceMs = -1;
        discardUnconfirmed(std::string{ "the session they went out on was lost (" } + why + ")");
    }

    // Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on.
    void onLeadershipChanged(const std::int64_t leadershipTermId)
    {
        m_pending.onLeadershipChanged(leadershipTermId);
        m_leadershipTermId = leadershipTermId;
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
    std::int64_t m_leadershipTermId = -1;
    // Monotonic ms when the session was found lost; -1 while one is open.
    std::int64_t m_sessionLostSinceMs = -1;
    std::int64_t m_nextReconnectMs = 0;
    // The stall an instance that does not act has been alarmed for.
    std::optional<ClusterError> m_stallAlarm;
};

} // namespace org::limitless::seqeron::app::detail
