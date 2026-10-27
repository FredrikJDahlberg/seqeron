#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/GroupDirectory.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/app/detail/LeaderGate.hpp"
#include "org/limitless/seqeron/app/detail/Session.hpp"
#include "org/limitless/seqeron/app/detail/SnapshotFrames.hpp"
#include "org/limitless/seqeron/app/detail/SnapshotTaker.hpp"
#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SeqeronCounters.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"

#include "org_limitless_seqeron_sbe_frame/ApplicationRegistered.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotEnd.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotStarted.h"

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
 * One replica of a co-located application — the kind of producer nothing elects. One runs per node, the
 * topology's `<applications>` section names it, and LeadershipChanged already picks the replica that submits:
 * this one publishes only while its own node leads. What that takes is behind it — the cluster session over
 * the node's own aeron:ipc, the queue it follows, the leader gate, confirmed ingress across a failover, and
 * the fences that say it may no longer act.
 *
 * Off the cluster (Config::offCluster), on a gateway host, it is the one instance rather than one of three:
 * its gate opens once caught up, whoever leads, and it submits over UDP, never over a member's aeron:ipc. Nothing stops
 * a second instance publishing too; an application that needs a standby off the cluster is a Gateway.
 *
 * What is left to the consumer is its own work: the payloads it reads, the state it keeps, the payloads it
 * submits. Every replica reads the same ordered stream and so holds the same state, which is what makes
 * OutstandingWork — fed that same stream — the way work survives the gate closing under it.
 *
 * Snapshots (doc/snapshot.md §4): given a SnapshotListener and a topology row with snapshot="true", every replica
 * serializes its state at each round's cut into its own directory, and the one whose gate is open then submits the
 * round's SnapshotEnd. A replica whose snapshot differs from the one sequenced is fenced with
 * ClusterError::SnapshotDiverged. On start, a replica given a listener restores the newest snapshot of its own that
 * the log confirms and resumes after its cut, or replays from clusterSeqNo 1 without one; one it cannot restore is
 * fenced with ClusterError::SnapshotUnrestorable.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls doWork() each
 * iteration. The Java twin is app/Application.java; keep the two in step. Publish and reply each take an
 * SBE encoder and a Fill, or already-encoded bytes as the Java twin does. ApplicationListener above is what
 * the Listener provides.
 */
template<typename Listener>
class Application
{
  public:
    // A payload of this application's own belongs to no connection.
    static constexpr std::int32_t NO_CONNECTION = -1;

    // Everything one replica needs to join its deployment; the twin of the Java builder.
    struct Config
    {
        // This application's sourceId, the one its <applications> row declares — required, because spec §5
        // is one id space, and never -1, which ingress refuses.
        std::int32_t sourceId = -1;
        // The names of the groups this application publishes to besides its own; one the log's group table lacks
        // fences it with ClusterError::GroupUnknown.
        std::vector<std::string> addressedGroups;
        // This replica's client id, its counters' label — required, and unique among the clients on its node: two
        // sharing one share their counters' series.
        std::optional<std::int32_t> clientId;
        // The node this replica runs on: whose queue it follows, and whose leadership opens its gate.
        std::int32_t memberId = 0;
        // Whether this node is a gateway host, running no member, rather than a member: the gate then opens
        // whoever leads, and ingress is UDP alone. Run one instance, as nothing elects between two. Not with an
        // ipcConnectTimeoutMs, which has no IPC ingress to time.
        bool offCluster = false;
        // This client's own egress endpoint; two media drivers on one host cannot both bind a port.
        std::string egressChannel = DEFAULT_EGRESS_CHANNEL;
        // The members to reach when this node is not leading, "memberId=host:port,...".
        std::string ingressEndpoints = protocol::ingressEndpointsCsv();
        // The stall and capacity policy.
        Tuning tuning;
        // How long to wait for IPC ingress to the local member while it leads before using UDP; empty means
        // DEFAULT_IPC_CONNECT_TIMEOUT_MS. Not with offCluster.
        std::optional<std::int64_t> ipcConnectTimeoutMs;
        // Takes this replica into snapshot rounds; without it it takes part in none, whatever its topology row says,
        // and recovers from clusterSeqNo 1.
        std::optional<Snapshots> snapshots;
        // Called whenever the gate closes, before onLeadershipChanged(false): the place for OutstandingWork's
        // onNotLeader(). Optional; without it the listener does so.
        std::function<void()> onNotLeader;
    };

    /**
     * Creates a replica that has not started.
     *
     * @param config   the replica's identity and deployment policy
     * @param listener the application; must outlive the replica
     * @throws std::invalid_argument if config has no clientId, offCluster with an ipcConnectTimeoutMs, or snapshots
     * with an empty directory
     */
    Application(Config config, Listener& listener) :
      m_config{ validated(std::move(config)) },
      m_listener{ listener },
      m_gate{ m_config.memberId, m_config.offCluster },
      m_store{ snapshotStore(m_config) },
      m_snapshots{ m_config.snapshots ? &m_config.snapshots->listener : nullptr, m_store.get(),
                   [this] { m_session.keepAlive(); } },
      m_dispatch{ *this },
      m_session{ *m_config.clientId,
                 m_config.tuning.pendingCapacity,
                 m_config.tuning.queueStallTimeoutMs,
                 m_config.tuning.recoveryStallTimeoutMs,
                 m_config.sourceId,
                 "",
                 m_config.addressedGroups,
                 m_dispatch },
      m_snapshotFrames{ m_session, [this] { return m_config.sourceId; } }
    {
        // Here rather than on the template parameter, where a listener that owns its Application is incomplete.
        static_assert(ApplicationListener<Listener>);
        if (m_store)
        {
            m_session.restoreFrom(*m_store, m_snapshots);
        }
    }

    Application(const Application&) = delete;
    Application& operator=(const Application&) = delete;

    /**
     * Opens the cluster session on this node and starts following its queue; the gate stays shut until caught
     * up.
     *
     * @param aeron the client, on this node's Aeron directory
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = aeron;
        if (m_snapshots.isListening())
        {
            m_snapshotRoundCounterRegId = protocol::addAppCounter(
                m_aeron, protocol::APP_SNAPSHOT_ROUND_TYPE_ID,
                "seqeron.app.snapshotRound source=" + std::to_string(m_config.sourceId) +
                    " member=" + std::to_string(m_config.memberId) + " client=" + std::to_string(*m_config.clientId),
                m_config.memberId, *m_config.clientId);
        }
        if (m_config.offCluster)
        {
            m_session.start(std::move(aeron), m_config.memberId, m_config.egressChannel, m_config.ingressEndpoints);
        }
        else
        {
            m_session.startColocated(std::move(aeron), m_config.memberId,
                                     m_config.ipcConnectTimeoutMs.value_or(DEFAULT_IPC_CONNECT_TIMEOUT_MS),
                                     m_config.egressChannel, m_config.ingressEndpoints);
        }
    }

    // One duty-cycle iteration: the cluster session and the queue, then the gate over what they left. Nothing once
    // fenced. Returns units of work done, for the caller's idle strategy.
    int doWork()
    {
        int work = m_session.doWork();
        updateSnapshotRoundCounter();
        if (m_session.isFenced())
        {
            return work;
        }
        // Shut while a lost session is replaced: a reply sent meanwhile may not land, so the next opening redispatches.
        const detail::LeaderGate::Transition transition =
            m_gate.update(m_session.isCaughtUp() && m_session.hasSession(), m_session.currentLeaderMemberId());
        if (transition != detail::LeaderGate::Transition::None)
        {
            gateEdge(transition == detail::LeaderGate::Transition::Opened);
            ++work;
        }
        work += m_snapshots.signal(m_snapshotSignals);
        if (m_gate.isOpen())
        {
            work += m_snapshots.submit(m_snapshotFrames);
        }
        return work;
    }

    // A closing gate discharges the tracked work's dispatches before the listener hears of it. The duty cycle's;
    // public as a test seam, not API.
    void gateEdge(const bool leading)
    {
        if (!leading && m_config.onNotLeader)
        {
            m_config.onNotLeader();
        }
        m_listener.onLeadershipChanged(leading);
    }

    // Whether leader-only work may reach ingress right now: the gate is open, and ingress is not held.
    [[nodiscard]] bool canPublish() const
    {
        return m_gate.isOpen() && !m_session.isHolding();
    }

    // Whether this replica may do leader-only work at all; canPublish() is what a publish needs.
    [[nodiscard]] bool isLeading() const noexcept
    {
        return m_gate.isOpen();
    }

    /**
     * Encodes and submits one payload of this application's own to its own group, stamped with its sourceId and
     * belonging to no connection.
     *
     * @tparam Encoder  the payload's SBE encoder
     * @param payloadId the payload's protocol
     * @param fill      called with the encoder, its messageHeader already applied, to stamp the fields
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publish(const std::uint16_t payloadId, Fill&& fill)
    {
        return submit<Encoder>(m_config.sourceId, NO_CONNECTION, 0, payloadId, std::forward<Fill>(fill));
    }

    /**
     * The same to the groups groupMask names as well as its own.
     *
     * @tparam Encoder  the payload's SBE encoder
     * @param groupMask bits from the GroupDirectory; one the table lacks is Refused
     * @param payloadId the payload's protocol
     * @param fill      called with the encoder, its messageHeader already applied, to stamp the fields
     * @return as publish(payloadId, fill)
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publish(const std::uint16_t groupMask, const std::uint16_t payloadId, Fill&& fill)
    {
        return submit<Encoder>(m_config.sourceId, NO_CONNECTION, groupMask, payloadId, std::forward<Fill>(fill));
    }

    /**
     * Submits one payload of this application's own that the caller encoded itself, a payload with no schema
     * at all (§13.2) included. The Java twin takes only this form, Java's SBE codecs sharing no interface,
     * so a port keeps the two in step here as Gateway does.
     *
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    [[nodiscard]] protocol::Publish publish(const std::uint16_t payloadId, const std::uint8_t* payload,
                                            const std::uint16_t payloadLength)
    {
        return submit(m_config.sourceId, NO_CONNECTION, 0, payloadId, payload, payloadLength);
    }

    /**
     * The same to the groups groupMask names as well as its own.
     *
     * @param groupMask     bits from the GroupDirectory; one the table lacks is Refused
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     * @return as publish(payloadId, payload, payloadLength)
     */
    [[nodiscard]] protocol::Publish publish(const std::uint16_t groupMask, const std::uint16_t payloadId,
                                            const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        return submit(m_config.sourceId, NO_CONNECTION, groupMask, payloadId, payload, payloadLength);
    }

    /**
     * Encodes and submits a reply on behalf of the producer that asked for it. It carries the requester's
     * sourceId, connectionId and groupMask, which is how the gateway that took the request routes the answer back
     * out, and how every group that saw the request sees the answer. Keep the RequestRef (Payload::requestRef())
     * rather than the request itself — a Payload is valid only during its callback, and a reply is usually
     * dispatched later.
     *
     * @tparam Encoder  the reply's SBE encoder
     * @param request   the request being answered
     * @param payloadId the reply's protocol
     * @param fill              called with the encoder, its messageHeader already applied, to stamp the fields
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish reply(const RequestRef& request, const std::uint16_t payloadId, Fill&& fill)
    {
        return submit<Encoder>(request.sourceId, request.connectionId, request.groupMask, payloadId,
                               std::forward<Fill>(fill));
    }

    /**
     * Submits a reply the caller encoded itself, on behalf of the producer that asked for it.
     *
     * @param request       the request being answered
     * @param payloadId     the reply's protocol
     * @param payload       the reply's first byte, its own messageHeader included if it has one
     * @param payloadLength the reply's length
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    [[nodiscard]] protocol::Publish reply(const RequestRef& request, const std::uint16_t payloadId,
                                          const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        return submit(request.sourceId, request.connectionId, request.groupMask, payloadId, payload, payloadLength);
    }

    // This replica's own sourceId, the one its topology row gives it.
    [[nodiscard]] std::int32_t sourceId() const noexcept
    {
        return m_config.sourceId;
    }

    [[nodiscard]] bool isCaughtUp() const
    {
        return m_session.isCaughtUp();
    }

    [[nodiscard]] std::int64_t lastClusterSeqNo() const
    {
        return m_session.lastClusterSeqNo();
    }

    // Closes the cluster session and the queue. The Aeron client is the caller's and is left open.
    void close()
    {
        m_session.close();
        m_snapshotRoundCounter.reset();
        m_snapshotRoundCounterRegId = -1;
    }

  private:
    void updateSnapshotRoundCounter()
    {
        if (!m_snapshotRoundCounter && m_snapshotRoundCounterRegId >= 0)
        {
            m_snapshotRoundCounter = m_aeron->findCounter(m_snapshotRoundCounterRegId);
        }
        if (m_snapshotRoundCounter)
        {
            m_snapshotRoundCounter->set(m_snapshots.confirmedRound());
        }
    }

    // A shut gate declines rather than submits: only the leading replica's copy of the work is the one sent.
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish submit(const std::int32_t frameSourceId, const std::int32_t connectionId,
                                           const std::uint16_t groupMask, const std::uint16_t payloadId, Fill&& fill)
    {
        if (!m_gate.isOpen())
        {
            return protocol::Publish::Declined;
        }
        return m_session.template publishPayload<Encoder>(frameSourceId, connectionId, groupMask, payloadId,
                                                          std::forward<Fill>(fill));
    }

    [[nodiscard]] protocol::Publish submit(const std::int32_t frameSourceId, const std::int32_t connectionId,
                                           const std::uint16_t groupMask, const std::uint16_t payloadId,
                                           const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        if (!m_gate.isOpen())
        {
            return protocol::Publish::Declined;
        }
        return m_session.publishPayload(frameSourceId, connectionId, groupMask, payloadId, payload, payloadLength);
    }

    // What comes off the queue, and the one frame the gate is driven by.
    class SessionDispatch
    {
      public:
        explicit SessionDispatch(Application& app) : m_app{ app }
        {}

        // Of seqeron's own vocabulary, this replica reads its topology row and the snapshot rounds; the
        // leadership the gate turns on arrives below rather than here.
        void onSystem(const protocol::SequencedEvent& event)
        {
            switch (event.systemEventType)
            {
                case protocol::APPLICATION_REGISTERED: {
                    auto row = protocol::decodeSystem<sbe::frame::ApplicationRegistered>(event);
                    if (row.applicationSourceId() == m_app.m_config.sourceId)
                    {
                        m_app.m_snapshots.participating(row.snapshot() == 1);
                    }
                    break;
                }
                case protocol::SNAPSHOT_STARTED: {
                    auto started = protocol::decodeSystem<sbe::frame::SnapshotStarted>(event);
                    m_app.m_snapshots.onSnapshotStarted(
                        started.round(), event.clusterSeqNo, event.position, event.clusterTimestampNs,
                        protocol::SnapshotHeader{ m_app.m_session.leadershipTermId(),
                                                  m_app.m_session.currentLeaderMemberId(), m_app.m_session.groupTable(),
                                                  std::nullopt },
                        m_app.m_gate.isOpen());
                    break;
                }
                case protocol::SNAPSHOT_END: {
                    if (event.sourceId != m_app.m_config.sourceId)
                    {
                        break;
                    }
                    auto end = protocol::decodeSystem<sbe::frame::SnapshotEnd>(event);
                    if (!m_app.m_snapshots.onSnapshotEnd(end.round(), end.recordCount(), end.length(), end.crc32c()))
                    {
                        m_app.m_session.fence(ClusterError::SnapshotDiverged,
                                              "round " + std::to_string(end.round()) +
                                                  "'s sequenced snapshot at clusterSeqNo " +
                                                  std::to_string(event.clusterSeqNo) + " differs from this replica's");
                    }
                    break;
                }
                default:
                    break; // nothing else says anything to a producer nothing elects
            }
        }

        void onSnapshotRound(const std::int64_t round)
        {
            m_app.m_snapshots.onSnapshotRound(round);
        }

        // A term won by another member ends this replica's part in the round it is publishing.
        void onLeadershipChanged()
        {
            m_app.m_gate.onLeadershipChanged();
            if (!m_app.m_config.offCluster && m_app.m_session.currentLeaderMemberId() != m_app.m_config.memberId)
            {
                m_app.m_snapshots.stopPublishing();
            }
        }

        void onPayload(const Payload& payload)
        {
            m_app.m_listener.onSequenced(payload);
        }

        void onGroupsResolved(const GroupDirectory& directory)
        {
            if constexpr (requires { m_app.m_listener.onGroupsResolved(directory); })
            {
                m_app.m_listener.onGroupsResolved(directory);
            }
        }

        void onCaughtUp(const std::int64_t clusterSeqNo)
        {
            if constexpr (requires { m_app.m_listener.onCaughtUp(clusterSeqNo); })
            {
                m_app.m_listener.onCaughtUp(clusterSeqNo);
            }
        }

        // The cluster clock's tick (spec §7), once a second. It is the one time source that keeps
        // advancing while every producer is silent, which is exactly when a watchdog must still fire, and
        // it is identical on every node — so a timer driven by it decides the same thing everywhere.
        void onClusterHeartbeat(const std::int64_t clusterTimeNs, const std::int64_t receiveTimeNs)
        {
            if constexpr (requires { m_app.m_listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs); })
            {
                m_app.m_listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
            }
        }

        // An open gate closes first, so the listener has given up leader-only work by the time it hears why.
        void onFenced(const ClusterError fence, const std::string& detail)
        {
            if (m_app.m_gate.onFenced())
            {
                m_app.gateEdge(false);
            }
            m_app.m_listener.onFenced(fence, detail);
        }

        // Nothing elects a replica, and its work stays outstanding on the log until replied to.
        [[nodiscard]] bool mayReconnect() const
        {
            return true;
        }

        // The leader's replica, caught up or not: no other holds the leader-only work.
        [[nodiscard]] bool isActing() const
        {
            return m_app.m_config.offCluster || m_app.m_session.currentLeaderMemberId() == m_app.m_config.memberId;
        }

      private:
        Application& m_app;
    };

    static Config validated(Config config)
    {
        if (!config.clientId)
        {
            throw std::invalid_argument("clientId is required");
        }
        if (config.offCluster && config.ipcConnectTimeoutMs)
        {
            throw std::invalid_argument("ipcConnectTimeoutMs has no meaning with offCluster");
        }
        return config;
    }

    // The store of a replica given a listener, which requires its directory.
    static std::unique_ptr<replayer::client::SnapshotStore> snapshotStore(const Config& config)
    {
        if (!config.snapshots)
        {
            return nullptr;
        }
        if (config.snapshots->directory.empty())
        {
            throw std::invalid_argument("snapshots.directory is required");
        }
        return std::make_unique<replayer::client::SnapshotStore>(config.snapshots->directory);
    }

    Config m_config;
    Listener& m_listener;
    detail::LeaderGate m_gate;
    std::unique_ptr<replayer::client::SnapshotStore> m_store;
    detail::SnapshotTaker m_snapshots;
    SessionDispatch m_dispatch;
    detail::Session<SessionDispatch> m_session;
    detail::SnapshotFrames<detail::Session<SessionDispatch>> m_snapshotFrames;
    // How this replica tells its node's Replayer where a round stands.
    struct SnapshotSignals
    {
        Application& m_app;

        bool joined(const std::int64_t round)
        {
            return m_app.m_session.sendSnapshotJoined(m_app.m_config.sourceId, round);
        }

        bool confirmed(const std::int64_t round)
        {
            return m_app.m_session.sendSnapshotConfirmed(m_app.m_config.sourceId, round);
        }
    } m_snapshotSignals{ *this };

    // The last round this replica confirmed, null until its async add resolves, and never without a listener.
    std::shared_ptr<aeron::Aeron> m_aeron;
    std::int64_t m_snapshotRoundCounterRegId = -1;
    std::shared_ptr<aeron::Counter> m_snapshotRoundCounter;
};

} // namespace org::limitless::seqeron::app
