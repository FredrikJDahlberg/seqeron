#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/GroupDirectory.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/detail/GroupResolution.hpp"
#include "org/limitless/seqeron/app/detail/QueueResolution.hpp"
#include "org/limitless/seqeron/app/detail/QueueStallFence.hpp"
#include "org/limitless/seqeron/app/detail/RecoveryStallFence.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/ReplayProtocol.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/sequencer/client/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"
#include "org/limitless/seqeron/sequencer/client/PendingSends.hpp"

#include "org_limitless_seqeron_sbe_replay/QueueRequest.h"

namespace org::limitless::seqeron::app::detail {

/**
 * What every seqeron client does the same way: the cluster session it submits on, the co-located queue it
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
 *   void onCaughtUp(std::int64_t clusterSeqNo)           // every transition to caught-up, the first included
 *   void onClusterHeartbeat(std::int64_t clusterTimeNs, std::int64_t receiveTimeNs) // the cluster clock's tick
 *   void onGroupsResolved(const GroupDirectory& directory) // once, at the first catch-up: before any publish
 *   void onFenced(ClusterError fence, const std::string& detail) // once, latched
 *   bool mayReconnect() const // whether a session the cluster closed may be replaced rather than fenced
 *   bool isActing() const     // whether this instance acts for its producer: a stall fences only one that does
 */
template<typename Dispatch>
class Session
{
  public:
    /**
     * @param sourceId        the application's sourceId its row names, or protocol::REPLAYER_NO_SOURCE_ID for a
     *                        gateway instance
     * @param gatewayName     the gateway instance's row name, or empty for an application
     * @param addressedGroups the names of the other groups the instance publishes to
     */
    Session(const std::int32_t clientId, const std::size_t pendingCapacity, const std::int64_t queueStallTimeoutMs,
            const std::int64_t recoveryStallTimeoutMs, const std::int32_t sourceId, std::string gatewayName,
            std::vector<std::string> addressedGroups, Dispatch& dispatch) :
      m_dispatch{ dispatch },
      m_groups{ std::move(addressedGroups) },
      m_groupsRestore{ *this },
      m_pending{ pendingCapacity },
      m_recoveryStall{ recoveryStallTimeoutMs },
      m_queueStall{ queueStallTimeoutMs },
      m_recoveryStallTimeoutMs{ recoveryStallTimeoutMs },
      m_queueStallTimeoutMs{ queueStallTimeoutMs },
      m_receiver{ clientId,
                  [this](const protocol::SequencedEvent& event) { onSequenced(event); },
                  {},
                  {},
                  [this](std::int32_t, std::int64_t leadershipTermId, std::int64_t) {
                      onLeadershipChanged(leadershipTermId);
                  } },
      m_queueSourceId{ sourceId },
      m_queueGatewayName{ std::move(gatewayName) },
      m_queueResolutionActions{ *this },
      m_queueResolution{ m_queueResolutionActions }
    {
        m_sender.setIngressHold(&m_pending);
    }

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    /**
     * Restores the newest snapshot in a store before following the queue; call before starting.
     *
     * @param store   this instance's own snapshots; must outlive this object
     * @param handler takes the snapshot's records; must outlive this object
     */
    void restoreFrom(replayer::client::SnapshotStore& store, replayer::client::SnapshotRestoreHandler& handler)
    {
        m_groupsRestore.m_handler = &handler;
        m_receiver.restoreFrom(store, m_groupsRestore);
    }

    // Opens the cluster session over UDP and asks this node's Replayer which queue to follow.
    void start(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId, const std::string& egressChannel,
               const std::string& ingressEndpoints)
    {
        m_sender.connect(aeron, egressChannel, ingressEndpoints);
        resolveQueue(std::move(aeron), memberId);
    }

    // The same for a client sharing this member's media driver: ingress over its own aeron:ipc while that
    // member leads, the UDP endpoint set when it does not.
    void startColocated(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId,
                        const std::int64_t ipcConnectTimeoutMs, const std::string& egressChannel,
                        const std::string& ingressEndpoints)
    {
        m_sender.connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints);
        resolveQueue(std::move(aeron), memberId);
    }

    // One duty cycle, in the order the parts require: the queue first, so a fence is judged on what this cycle
    // saw; the resend last, so nothing new goes out ahead of it. Returns units of work done.
    int doWork()
    {
        if (m_fenced)
        {
            return 0;
        }
        int work =
            m_queueResolution.groupId() == QueueResolution::UNRESOLVED ? pollQueueResolution() : m_receiver.poll();
        checkCaughtUp();
        if (!m_fenced)
        {
            checkFences();
        }
        if (m_fenced)
        {
            // Here rather than at the fence, which may be raised from inside that poll. Nothing polls the receiver
            // again, and a replay left open would hold one of the member's Replayer slots.
            m_receiver.close();
            closeQueueResolution();
            return work;
        }
        // Nothing is addressed to a producer on egress: everything it publishes comes back on the queue. Unlike
        // the Java twin this counts no work, since the C++ pollEgress reports no fragment count.
        m_sender.pollEgress([](const std::uint8_t*, std::int32_t) {});
        m_sender.keepAlive();
        work += static_cast<int>(m_pending.resendMissing(m_sender));
        return work;
    }

    /**
     * Encodes and submits a payload; declined until the groups are resolved, since nothing may be published before
     * (A-9).
     *
     * @param groupMask the groups addressed besides the own one, 0 for none
     * @return Refused for a mask naming a group the table lacks
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publishPayload(const std::int32_t sourceId, const std::int32_t connectionId,
                                                   const std::uint16_t groupMask, const std::uint16_t payloadId,
                                                   Fill&& fill)
    {
        const std::int32_t frameMask = frameMaskFor(groupMask);
        if (frameMask < 0)
        {
            return frameMask == GroupResolution::NOT_ADDRESSABLE ? protocol::Publish::Refused
                                                                 : protocol::Publish::Declined;
        }
        return sequencer::client::publishPayload<Encoder>(m_sender, &m_pending, sourceId, connectionId, payloadId,
                                                          static_cast<std::uint16_t>(frameMask),
                                                          std::forward<Fill>(fill));
    }

    // The same for a payload a caller encoded into a buffer of its own; the Java twin's only publish.
    [[nodiscard]] protocol::Publish publishPayload(const std::int32_t sourceId, const std::int32_t connectionId,
                                                   const std::uint16_t groupMask, const std::uint16_t payloadId,
                                                   const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        const std::int32_t frameMask = frameMaskFor(groupMask);
        if (frameMask < 0)
        {
            return frameMask == GroupResolution::NOT_ADDRESSABLE ? protocol::Publish::Refused
                                                                 : protocol::Publish::Declined;
        }
        return sequencer::client::publishPayload(m_sender, &m_pending, sourceId, connectionId, payloadId,
                                                 static_cast<std::uint16_t>(frameMask), payload, payloadLength);
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

    [[nodiscard]] std::int64_t lastClusterSeqNo() const
    {
        return m_receiver.lastClusterSeqNo();
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

    // The group table so far, for a snapshot header.
    [[nodiscard]] const std::vector<protocol::SnapshotGroup>& groupTable() const
    {
        return m_groups.table();
    }

    /**
     * Latches the first fence and releases the cluster session, so a designated instance's sibling is promoted now
     * rather than when the application exits; a façade raises its own, such as a diverged snapshot, through this too.
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
        m_sender.close();
        m_dispatch.onFenced(reason, detail);
    }

    /**
     * Forgets every frame not yet seen on the queue, for a façade that knows none of them will be sequenced.
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
    // leader does not arrive before its timeout, hence isConnected() as well as the latch. And the queue, whose
    // SessionClosed is the cluster's own word.
    [[nodiscard]] bool hasSession() const
    {
        return !m_sender.isSessionLost() && m_sender.isConnected() && !m_pending.isClosed(m_sender.clusterSessionId());
    }

    // Closes the cluster session. The receiver releases its streams when this object goes.
    void close()
    {
        m_sender.close();
    }

  private:
    // How often an attempt to replace a lost session begins while the façade allows it.
    static constexpr std::int64_t RECONNECT_INTERVAL_MS = 1'000;

    // What frameMaskFor returns while the groups are unresolved or the session fenced.
    static constexpr std::int32_t DECLINED = -2;

    static constexpr int QUEUE_CONTROL_FRAGMENT_LIMIT = 16;

    // Takes a restore's group table before the façade's handler takes the header.
    class GroupsRestore final : public replayer::client::SnapshotRestoreHandler
    {
      public:
        explicit GroupsRestore(Session& session) : m_session{ session }
        {}

        bool supportsFormatVersion(const std::uint32_t formatVersion) override
        {
            return m_handler->supportsFormatVersion(formatVersion);
        }

        void onSnapshotHeader(const protocol::SnapshotHeader& header) override
        {
            m_session.m_groups.onSnapshotHeader(header.groups);
            m_handler->onSnapshotHeader(header);
        }

        void onSnapshotRecord(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
        {
            m_handler->onSnapshotRecord(record, recordIndex);
        }

        replayer::client::SnapshotRestoreHandler* m_handler = nullptr;

      private:
        Session& m_session;
    };

    // The mask a frame carries, GroupResolution::NOT_ADDRESSABLE, or DECLINED.
    [[nodiscard]] std::int32_t frameMaskFor(const std::uint16_t groupMask) const
    {
        return m_fenced || m_groups.directory() == nullptr ? DECLINED : m_groups.frameMask(groupMask);
    }

    // The Aeron half of QueueResolution.
    class QueueResolutionActions final : public QueueResolution::Actions
    {
      public:
        explicit QueueResolutionActions(Session& session) : m_session{ session }
        {}

        void sendQueueRequest(const std::int64_t requestId) override
        {
            const auto& publication = m_session.m_queueRequestPub;
            if (!publication || !publication->isConnected())
            {
                return; // Replayer not up yet; the resend timer retries
            }
            alignas(16) std::array<std::uint8_t, 64> buffer{};
            sbe::replay::QueueRequest request;
            request.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
            request.clientId(m_session.m_queueClientId).requestId(requestId).sourceId(m_session.m_queueSourceId);
            request.putGatewayName(m_session.m_queueGatewayName);
            const auto length = static_cast<aeron::util::index_t>(sbe::replay::MessageHeader::encodedLength() +
                                                                  request.encodedLength());
            aeron::concurrent::AtomicBuffer atomic(buffer.data(), buffer.size());
            publication->offer(atomic, 0, length);
        }

        std::int64_t clientId() override
        {
            return m_session.m_queueClientId;
        }

        std::int64_t nowMs() override
        {
            return monotonicMs();
        }

      private:
        Session& m_session;
    };

    // The receiver starts once the Replayer has said which queue this instance reads (spec R-6).
    void resolveQueue(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId)
    {
        m_aeron = std::move(aeron);
        m_memberId = memberId;
        m_queueClientId = m_aeron->nextCorrelationId();
        m_queueControlSubRegId =
            m_aeron->addSubscription(replayer::client::REPLAYER_CONTROL_CHANNEL, protocol::REPLAYER_CONTROL_STREAM_ID);
        m_queueRequestPubRegId =
            m_aeron->addPublication(protocol::REPLAYER_IPC_CHANNEL, protocol::REPLAYER_REQUEST_STREAM_ID);
        m_queueControlPoll = [this](const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                                    const aeron::util::index_t length, const aeron::Header&) {
            m_queueResolution.onControl(reinterpret_cast<char*>(buffer.buffer()) + offset,
                                        static_cast<std::uint64_t>(length));
        };
        m_queueResolution.start();
    }

    // Waits for the Replayer's answer, unfenced, then starts the receiver on that queue.
    int pollQueueResolution()
    {
        if (!m_aeron || m_queueControlSubRegId < 0)
        {
            return 0;
        }
        if (!m_queueControlSub)
        {
            m_queueControlSub = m_aeron->findSubscription(m_queueControlSubRegId);
        }
        if (!m_queueRequestPub)
        {
            m_queueRequestPub = m_aeron->findPublication(m_queueRequestPubRegId);
        }
        int work = m_queueControlSub ? m_queueControlSub->poll(m_queueControlPoll, QUEUE_CONTROL_FRAGMENT_LIMIT) : 0;
        m_queueResolution.doTimers(!m_queueRequestPub || !m_queueRequestPub->isConnected());
        const std::int32_t groupId = m_queueResolution.groupId();
        if (groupId != QueueResolution::UNRESOLVED)
        {
            m_receiver.start(m_aeron, m_memberId, groupId);
            closeQueueResolution();
            ++work;
        }
        return work;
    }

    void closeQueueResolution()
    {
        if (m_aeron && m_queueControlSubRegId >= 0)
        {
            // An add already answered would otherwise stay open with nothing holding it. A registration found
            // once is forgotten, so only what is not held yet is found.
            if (!m_queueControlSub)
            {
                m_queueControlSub = m_aeron->findSubscription(m_queueControlSubRegId);
            }
            if (!m_queueRequestPub)
            {
                m_queueRequestPub = m_aeron->findPublication(m_queueRequestPubRegId);
            }
        }
        m_queueControlSub.reset();
        m_queueRequestPub.reset();
        m_queueControlSubRegId = m_queueRequestPubRegId = -1;
    }

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
        m_queueStall.onCaughtUp(monotonicMs());
        if (m_groups.directory() == nullptr)
        {
            if (const std::optional<std::string> unknown = m_groups.resolve(m_queueResolution.groupId()))
            {
                fence(ClusterError::GroupUnknown, "group " + *unknown + " is not in the log's group table");
                return;
            }
            m_dispatch.onGroupsResolved(*m_groups.directory());
        }
        m_dispatch.onCaughtUp(m_receiver.lastClusterSeqNo());
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
            if (m_recoveryStall.onNotCaughtUp(nowMs, m_receiver.lastClusterSeqNo()))
            {
                stalled(ClusterError::RecoveryStalled, util::eventCode::RecoveryStalled,
                        "recovery has dispatched nothing for >" + std::to_string(m_recoveryStallTimeoutMs) +
                            "ms (clusterSeqNo stuck at " + std::to_string(m_receiver.lastClusterSeqNo()) + ")");
                return;
            }
        }
        else if (m_queueStall.isStalled(nowMs))
        {
            stalled(ClusterError::QueueStalled, util::eventCode::QueueStalled,
                    "no ClusterHeartbeat for >" + std::to_string(m_queueStallTimeoutMs) + "ms");
            return;
        }
        if (m_stallAlarm)
        {
            util::Logger::info(util::component::Cluster, "%s cleared", clusterErrorName(*m_stallAlarm));
            m_stallAlarm.reset();
        }
    }

    // Fences an instance that acts; one that does not raises an alarm once and keeps following the queue.
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
    // cycle, until one opens or the queue could have gone silent for as long: past that the cluster is not coming
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
        if (nowMs - m_sessionLostSinceMs > m_queueStallTimeoutMs && !m_sender.isReconnecting())
        {
            fence(ClusterError::ClusterSessionLost, std::string{ why } + "; no session replaced it within " +
                                                        std::to_string(m_queueStallTimeoutMs) + "ms");
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
        if (m_pending.size() > 0)
        {
            util::Logger::info(util::component::Cluster,
                               "%zu unconfirmed frames hold new ones until the queue settles them: the session they "
                               "went out on was lost (%s)",
                               m_pending.size(), why);
        }
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
            if (event.systemEventType == protocol::GROUPS_REGISTERED)
            {
                m_groups.onGroupsRegistered(event.payload, event.payloadLength);
            }
            else if (event.systemEventType == protocol::CLUSTER_HEARTBEAT)
            {
                m_queueStall.onClusterHeartbeat(monotonicMs());
                m_dispatch.onClusterHeartbeat(event.clusterTimestampNs, event.receiveTimeNs);
            }
            m_dispatch.onSystem(event);
            return;
        }
        m_dispatch.onPayload(Payload{ event });
    }

    Dispatch& m_dispatch;
    GroupResolution m_groups;
    GroupsRestore m_groupsRestore;
    sequencer::client::PendingSends m_pending;
    sequencer::client::ClusterStreamSender m_sender;
    RecoveryStallFence m_recoveryStall;
    QueueStallFence m_queueStall;
    std::int64_t m_recoveryStallTimeoutMs;
    std::int64_t m_queueStallTimeoutMs;
    replayer::client::ReplayerStreamReceiver m_receiver;

    // What the QueueRequest names: the instance's topology row (spec R-6).
    const std::int32_t m_queueSourceId;
    const std::string m_queueGatewayName;
    // The queue's resolution, until the receiver starts on it.
    QueueResolutionActions m_queueResolutionActions;
    QueueResolution m_queueResolution;
    std::shared_ptr<aeron::Aeron> m_aeron;
    std::int32_t m_memberId = 0;
    std::int64_t m_queueClientId = -1;
    std::int64_t m_queueControlSubRegId = -1;
    std::int64_t m_queueRequestPubRegId = -1;
    std::shared_ptr<aeron::Subscription> m_queueControlSub;
    std::shared_ptr<aeron::Publication> m_queueRequestPub;
    aeron::fragment_handler_t m_queueControlPoll;

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
