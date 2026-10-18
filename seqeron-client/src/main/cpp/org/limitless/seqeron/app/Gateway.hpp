#pragma once

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/app/detail/GatewayLifecycle.hpp"
#include "org/limitless/seqeron/app/detail/Session.hpp"
#include "org/limitless/seqeron/app/detail/SnapshotFrames.hpp"
#include "org/limitless/seqeron/app/detail/SnapshotTaker.hpp"
#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SeqeronCounters.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotRestoreHandler.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"
#include "org_limitless_seqeron_sbe_frame/ConnectionClosed.h"
#include "org_limitless_seqeron_sbe_frame/ConnectionOpened.h"
#include "org_limitless_seqeron_sbe_frame/GatewayActive.h"
#include "org_limitless_seqeron_sbe_frame/GatewayRegistered.h"
#include "org_limitless_seqeron_sbe_frame/GatewayStarted.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotEnd.h"
#include "org_limitless_seqeron_sbe_frame/SnapshotStarted.h"

namespace org::limitless::seqeron::app {

/** What a Gateway's Listener provides: the edge. */
template<typename L>
concept GatewayListener = requires(L& listener, const Payload& payload, std::int32_t connectionId, const char* data,
                                   std::size_t length, std::int64_t clusterSeqNo, std::int64_t clusterTimeNs,
                                   std::int64_t receiveTimeNs, ClusterError fence, const std::string& detail) {
    // Open the edge; false is retried on the next doWork().
    { listener.onActivated(connectionId) } -> std::convertible_to<bool>;
    // Close it and drop every connection it let in: on a sibling's designation, and before onFenced when this
    // instance fences while serving.
    listener.onStandby();
    listener.onSequenced(payload);
    listener.onConnectionOpened(connectionId, data, length);
    listener.onConnectionClosed(connectionId);
    listener.onCaughtUp(clusterSeqNo);
    listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
    listener.onFenced(fence, detail);
};

/**
 * One instance of an elected active/standby producer pair. Everything this tier defines about being a
 * gateway is behind it: the list row that names the instance, the designation that makes it serve, the
 * GatewayStarted that binds its session, the connection id space it resumes from its predecessor, the
 * connection lifecycle frames, confirmed ingress across a failover, and the fences that stand it down.
 *
 * What is left to the consumer is its edge — a socket, a dialler, a codec. It opens that edge in
 * onActivated, closes it in onStandby, and otherwise exchanges payloads: nothing of the frame layer or of
 * seqeron's system vocabulary appears in its code.
 *
 * Snapshots (doc/snapshot.md §4): given a SnapshotListener and topology rows with snapshot="true", every instance
 * serializes its state at each round's cut into its own directory, under a header carrying the pair's election
 * state, and the active one then submits the round's SnapshotEnd. An instance whose snapshot differs from the one
 * sequenced is fenced with ClusterError::SnapshotDiverged. On start, an instance given a listener restores the
 * newest snapshot of its own that the log confirms and resumes after its cut, or replays from clusterSeqNo 1 without
 * one; one it cannot restore is fenced with ClusterError::SnapshotUnrestorable.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls doWork() each
 * iteration. The Java twin is app/Gateway.java; keep the two in step. The Listener is the edge;
 * GatewayListener above is what it provides.
 */
template<typename Listener>
class Gateway
{
  public:
    // A frame that belongs to the gateway rather than to one of its connections, and what openConnection
    // answers when there is none to give.
    static constexpr std::int32_t NO_CONNECTION = -1;

    // What sourceId() and gatewayId() answer until a GatewayRegistered row names this instance. Spelled
    // out rather than aliased: LifecycleActions is not declared yet here. The static_assert below keeps it
    // in step with GatewayLifecycle's own.
    static constexpr std::int32_t UNRESOLVED = -1;

    // Everything one instance needs to join its pair; the twin of the Java builder.
    struct Config
    {
        // The GatewayRegistered row name this instance joins on.
        std::string gatewayName;
        // This replica's client id, its counters' label; unique among the co-located apps on its node.
        std::int32_t clientId = 0;
        // The node whose queue this instance follows.
        std::int32_t memberId = 0;
        // This client's own egress endpoint; two media drivers on one host cannot both bind a port.
        std::string egressChannel;
        // The members to reach, "memberId=host:port,...".
        std::string ingressEndpoints = protocol::ingressEndpointsCsv();
        std::size_t pendingCapacity = DEFAULT_PENDING_CAPACITY;
        std::int64_t queueStallTimeoutMs = DEFAULT_QUEUE_STALL_TIMEOUT_MS;
        std::int64_t recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;
        // The logical gateway's sourceId, the one its rows declare. Required with a snapshotListener, whose snapshot
        // counter it labels.
        std::int32_t sourceId = UNRESOLVED;
        // What serializes this instance's state for snapshot rounds and restores it on start; without one it takes
        // part in none, whatever its rows say, and recovers from clusterSeqNo 1. Must outlive the gateway.
        SnapshotListener* snapshotListener = nullptr;
        // Where this instance keeps its snapshots, one file per round; required with a snapshotListener. Its own: no
        // other instance, its pair's included, may write it. A restart restores from what it holds, so it must
        // outlive the process.
        std::filesystem::path snapshotDirectory;
    };

    /**
     * Creates an instance that has not started.
     *
     * @param config   the instance's identity and deployment policy
     * @param listener the edge; must outlive the gateway
     * @throws std::invalid_argument if config has a snapshotListener and no sourceId or snapshotDirectory
     */
    Gateway(Config config, Listener& listener) :
      m_config{ std::move(config) },
      m_listener{ listener },
      m_actions{ *this },
      m_lifecycle{ m_config.gatewayName, m_actions },
      m_store{ snapshotStore(m_config) },
      m_snapshots{ m_config.snapshotListener, m_store.get(), [this] { m_session.keepAlive(); } },
      m_restore{ *this },
      m_dispatch{ *this },
      m_session{ m_config.clientId, m_config.pendingCapacity, m_config.queueStallTimeoutMs,
                 m_config.recoveryStallTimeoutMs, m_dispatch },
      m_snapshotFrames{ m_session, [this] { return m_lifecycle.gatewaySourceId(); } }
    {
        // Here rather than on the template parameter, where a listener that owns its Gateway is incomplete.
        static_assert(GatewayListener<Listener>);
        if (m_store)
        {
            m_session.restoreFrom(*m_store, m_restore);
        }
    }

    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    /**
     * Opens the cluster session and starts following this node's queue; the gate stays shut until designated.
     *
     * @param aeron the client, on the co-located member's Aeron directory
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = aeron;
        m_connectionsCounterRegId =
            protocol::addAppCounter(m_aeron, protocol::GATEWAY_CONNECTIONS_TYPE_ID,
                                    "seqeron.gateway.connections member=" + std::to_string(m_config.memberId) +
                                        " client=" + std::to_string(m_config.clientId),
                                    m_config.memberId, m_config.clientId);
        if (m_snapshots.isListening())
        {
            m_snapshotRoundCounterRegId = protocol::addAppCounter(
                m_aeron, protocol::APP_SNAPSHOT_ROUND_TYPE_ID,
                "seqeron.app.snapshotRound source=" + std::to_string(m_config.sourceId) +
                    " member=" + std::to_string(m_config.memberId) + " client=" + std::to_string(m_config.clientId),
                m_config.memberId, m_config.clientId);
        }
        m_session.start(std::move(aeron), m_config.memberId, m_config.egressChannel, m_config.ingressEndpoints);
    }

    // One duty-cycle iteration: the cluster session and the queue, then whatever the connection lifecycle, the
    // election and a snapshot round still owe. Nothing once fenced, so a designation dispatched in the cycle that
    // fenced opens no gate. Returns units of work done, for the caller's idle strategy.
    int doWork()
    {
        int work = m_session.doWork();
        updateCounters();
        if (m_session.isFenced())
        {
            return work;
        }
        work += drainLifecycle();
        work += m_session.isCaughtUp() ? m_lifecycle.advance() : 0;
        work += m_snapshots.submit(m_snapshotFrames);
        return work;
    }

    // The edge closed without being asked to — a dial that failed, a counterparty that hung up. The
    // designation stands, so doWork() opens it again through onActivated, and without a second
    // GatewayStarted. An acceptor whose listen socket stays bound never calls this; an initiator does.
    void gateClosed()
    {
        m_lifecycle.onGateClosed();
        m_connections.clear();
    }

    // Tells the cluster this instance is alive. doWork() already does it once a cycle; call this as well from
    // inside a Dispatch callback that spins, since doWork() cannot run again until that callback returns and
    // the cluster drops a session that goes quiet for sessionTimeoutMs. Self-throttling.
    void keepAlive()
    {
        m_session.keepAlive();
    }

    // Whether a connection may be taken right now: this instance is serving, and ingress is not held behind
    // a failover's resend. Check it before accepting or dialling — a connection taken while ingress is held
    // could not have its ConnectionOpened placed.
    [[nodiscard]] bool canAccept() const
    {
        return m_lifecycle.isServing() && !m_session.isHolding();
    }

    /**
     * Takes one connection into the cluster's view of this gateway: allocates its id from the resume point
     * and adds its ConnectionOpened to the backlog that doWork() places and retries.
     *
     * @param connectionData the ConnectionOpened's connectionData, copied; what a standby rebuilds the
     *                       connection's state from
     * @param length         its length
     * @return the connection's id, or NO_CONNECTION if this instance is not serving
     */
    std::int32_t openConnection(const char* connectionData, const std::size_t length)
    {
        if (!m_lifecycle.onSessionAcquired())
        {
            return NO_CONNECTION;
        }
        const std::int32_t connectionId = m_nextConnectionId++;
        m_lifecycleBacklog.push_back(
            { connectionId, protocol::CONNECTION_OPENED, std::vector<char>(connectionData, connectionData + length) });
        m_unopened.insert(connectionId);
        m_connections.insert(connectionId);
        return connectionId;
    }

    /**
     * Takes one connection whose only identity is its id; see openConnection(const char*, std::size_t).
     *
     * @return the connection's id, or NO_CONNECTION if this instance is not serving
     */
    std::int32_t openConnection()
    {
        return openConnection(nullptr, 0);
    }

    /**
     * Releases a connection that has gone, adding its ConnectionClosed to the backlog that doWork() places and
     * retries. One the cluster never heard of is dropped rather than announced.
     *
     * @param connectionId the id openConnection returned
     */
    void closeConnection(const std::int32_t connectionId)
    {
        if (!m_lifecycle.isServing())
        {
            // Stood down: the connections went with the edge, and a successor's GatewayStarted releases them.
            return;
        }
        m_connections.erase(connectionId);
        if (m_unopened.erase(connectionId) > 0)
        {
            std::erase_if(m_lifecycleBacklog,
                          [connectionId](const Lifecycle& entry) { return entry.connectionId == connectionId; });
            return;
        }
        m_lifecycleBacklog.push_back({ connectionId, protocol::CONNECTION_CLOSED, {} });
    }

    /**
     * Encodes and submits one application payload on a connection, stamped with this gateway's sourceId.
     *
     * @tparam Encoder     the payload's SBE encoder
     * @param connectionId the connection it belongs to, or NO_CONNECTION for the gateway itself
     * @param payloadId    the payload's protocol
     * @param fill         called with the encoder, its messageHeader already applied, to stamp the fields
     * @return Published; Declined while ingress is held or back-pressured or the connection's
     *         ConnectionOpened has not landed yet — retry it; Refused, permanently
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish publish(const std::int32_t connectionId, const std::uint16_t payloadId, Fill&& fill)
    {
        if (m_unopened.contains(connectionId))
        {
            return protocol::Publish::Declined;
        }
        return m_session.template publishPayload<Encoder>(m_lifecycle.gatewaySourceId(), connectionId, payloadId,
                                                          std::forward<Fill>(fill));
    }

    /**
     * Submits one payload the caller encoded itself, stamped with this gateway's sourceId. The Java twin
     * takes only this form — Java's SBE codecs share no interface — so a port keeps the two in step here.
     *
     * @param connectionId  the connection it belongs to, or NO_CONNECTION for the gateway itself
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     * @return Published; Declined while ingress is held or back-pressured or the connection's
     *         ConnectionOpened has not landed yet — retry it; Refused, permanently
     */
    [[nodiscard]] protocol::Publish publish(const std::int32_t connectionId, const std::uint16_t payloadId,
                                            const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        if (m_unopened.contains(connectionId))
        {
            return protocol::Publish::Declined;
        }
        return m_session.publishPayload(m_lifecycle.gatewaySourceId(), connectionId, payloadId, payload, payloadLength);
    }

    // Whether the last GatewayActive for this pair named this instance.
    [[nodiscard]] bool isActivated() const noexcept
    {
        return m_lifecycle.isActivated();
    }

    // Whether the edge is open — onActivated has returned true and nothing has stood it down.
    [[nodiscard]] bool isServing() const noexcept
    {
        return m_lifecycle.isServing();
    }

    // This logical gateway's sourceId, shared with its standby; UNRESOLVED until a row names it.
    [[nodiscard]] std::int32_t sourceId() const noexcept
    {
        return m_lifecycle.gatewaySourceId();
    }

    // This instance's list row, or UNRESOLVED until a row names it.
    [[nodiscard]] std::int32_t gatewayId() const noexcept
    {
        return m_lifecycle.gatewayId();
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
        m_connectionsCounter.reset();
        m_connectionsCounterRegId = -1;
        m_snapshotRoundCounter.reset();
        m_snapshotRoundCounterRegId = -1;
    }

  private:
    void updateCounters()
    {
        if (!m_connectionsCounter && m_connectionsCounterRegId >= 0)
        {
            m_connectionsCounter = m_aeron->findCounter(m_connectionsCounterRegId);
        }
        if (m_connectionsCounter)
        {
            m_connectionsCounter->set(static_cast<std::int64_t>(m_connections.size()));
        }
        if (!m_snapshotRoundCounter && m_snapshotRoundCounterRegId >= 0)
        {
            m_snapshotRoundCounter = m_aeron->findCounter(m_snapshotRoundCounterRegId);
        }
        if (m_snapshotRoundCounter)
        {
            m_snapshotRoundCounter->set(m_snapshots.confirmedRound());
        }
    }

    // One connection lifecycle frame waiting to be placed.
    struct Lifecycle
    {
        std::int32_t connectionId;
        std::uint16_t systemEventType;
        std::vector<char> connectionData;
    };

    // Places what the connection lifecycle owes, oldest first, stopping at the first frame that is declined.
    int drainLifecycle()
    {
        int work = 0;
        while (!m_lifecycleBacklog.empty())
        {
            const Lifecycle& next = m_lifecycleBacklog.front();
            const auto result =
                next.systemEventType == protocol::CONNECTION_OPENED
                    ? m_session.template publishSystem<sbe::frame::ConnectionOpened>(
                          m_lifecycle.gatewaySourceId(), next.connectionId, next.systemEventType,
                          [&next](sbe::frame::ConnectionOpened& encoder) {
                              encoder.putConnectionData(next.connectionData.data(),
                                                        static_cast<std::uint16_t>(next.connectionData.size()));
                          })
                    : m_session.template publishSystem<sbe::frame::ConnectionClosed>(
                          m_lifecycle.gatewaySourceId(), next.connectionId, next.systemEventType,
                          [](sbe::frame::ConnectionClosed&) {});
            if (!published(result))
            {
                break;
            }
            m_unopened.erase(next.connectionId);
            m_lifecycleBacklog.pop_front();
            work++;
        }
        return work;
    }

    // A ConnectionOpened's opaque tail, or nothing. §7.1 lets connectionData be absent, and a producer
    // that takes the option encodes no var-data header at all — so a payload too short to hold one is that
    // case, not a short read.
    void dispatchConnectionOpened(const protocol::SequencedEvent& event)
    {
        if (event.payloadLength < sbe::frame::ConnectionOpened::connectionDataHeaderLength())
        {
            m_listener.onConnectionOpened(event.connectionId, nullptr, 0);
            return;
        }
        auto opened = protocol::decodeSystem<sbe::frame::ConnectionOpened>(event);
        // Length first: connectionData() advances sbePosition past the var-data, and reading the length
        // after that reads off the end of the payload.
        const std::uint16_t length = opened.connectionDataLength();
        m_listener.onConnectionOpened(event.connectionId, opened.connectionData(), length);
    }

    // The resume point, read off every frame this logical gateway's history holds, whichever instance
    // issued it.
    void observeConnectionId(const std::int32_t sourceId, const std::int32_t connectionId)
    {
        if (m_lifecycle.gatewaySourceId() != detail::GatewayLifecycle<LifecycleActions>::UNRESOLVED &&
            sourceId == m_lifecycle.gatewaySourceId() && connectionId > m_highestConnectionId)
        {
            m_highestConnectionId = connectionId;
        }
    }

    // What this instance derives from the frames before a cut, for the round it starts (§6).
    protocol::SnapshotHeader snapshotHeader() const
    {
        return { m_session.leadershipTermId(), m_session.currentLeaderMemberId(),
                 protocol::SnapshotGatewayState{ m_lifecycle.gatewaySourceId(), m_lifecycle.activeGatewayId(),
                                                 m_highestConnectionId, m_lifecycle.pairRows() } };
    }

    // A refused frame is this class's own bug, never a condition to wait out.
    static bool published(const protocol::Publish result)
    {
        if (result == protocol::Publish::Refused)
        {
            throw std::logic_error("a frame the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return result == protocol::Publish::Published;
    }

    // The election's side effects.
    class LifecycleActions
    {
      public:
        explicit LifecycleActions(Gateway& gateway) : m_gateway{ gateway }
        {}

        bool publishGatewayStarted(const std::int32_t gatewayId)
        {
            m_gateway.m_nextConnectionId = m_gateway.m_highestConnectionId + 1;
            const std::int32_t firstConnectionId = m_gateway.m_nextConnectionId;
            return published(m_gateway.m_session.template publishSystem<sbe::frame::GatewayStarted>(
                m_gateway.m_lifecycle.gatewaySourceId(), NO_CONNECTION, protocol::GATEWAY_STARTED,
                [gatewayId, firstConnectionId](sbe::frame::GatewayStarted& encoder) {
                    encoder.gatewayId(gatewayId).firstConnectionId(firstConnectionId);
                }));
        }

        bool openGate()
        {
            return m_gateway.m_listener.onActivated(m_gateway.m_nextConnectionId);
        }

        void closeGate()
        {
            // The connections go with the edge, and a successor's GatewayStarted is what releases them.
            m_gateway.m_lifecycleBacklog.clear();
            m_gateway.m_unopened.clear();
            m_gateway.m_connections.clear();
            m_gateway.m_listener.onStandby();
        }

      private:
        Gateway& m_gateway;
    };

    // What a restore hands over in place of the frames before the cut: the header's election state and connection
    // id resume point, then the application's records.
    class Restore final : public replayer::client::SnapshotRestoreHandler
    {
      public:
        explicit Restore(Gateway& gateway) : m_gateway{ gateway }
        {}

        bool supportsFormatVersion(const std::uint32_t formatVersion) override
        {
            return m_gateway.m_snapshots.supportsFormatVersion(formatVersion);
        }

        void onSnapshotHeader(const protocol::SnapshotHeader& header) override
        {
            if (header.gateway)
            {
                m_gateway.m_lifecycle.onSnapshotHeader(*header.gateway);
                m_gateway.m_highestConnectionId = header.gateway->highestConnectionId;
            }
            m_gateway.m_snapshots.onSnapshotHeader(header);
        }

        void onSnapshotRecord(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
        {
            m_gateway.m_snapshots.onSnapshotRecord(record, recordIndex);
        }

      private:
        Gateway& m_gateway;
    };

    // What comes off the queue, split into what the election reads and what the consumer does.
    class SessionDispatch
    {
      public:
        explicit SessionDispatch(Gateway& gateway) : m_gateway{ gateway }
        {}

        // A submitted system payload carries no messageHeader, so its block length and version come from this
        // build's own constants (doc/seqeron-protocol-spec.md §7, V-3) — which is what decodeSystem supplies, for a
        // synthesized frame as much as a submitted one.
        void onSystem(const protocol::SequencedEvent& event)
        {
            m_gateway.observeConnectionId(event.sourceId, event.connectionId);
            switch (event.systemEventType)
            {
                // This logical gateway's connection lifecycle, whichever instance issued it. A consumer that
                // keeps per-connection state rebuilds it from these while it replays, and releases it on the
                // close — a client that drops its socket without logging out produces no payload at all.
                case protocol::CONNECTION_OPENED:
                    if (event.sourceId == m_gateway.m_lifecycle.gatewaySourceId())
                    {
                        m_gateway.dispatchConnectionOpened(event);
                    }
                    break;
                case protocol::CONNECTION_CLOSED:
                    if (event.sourceId == m_gateway.m_lifecycle.gatewaySourceId())
                    {
                        m_gateway.m_listener.onConnectionClosed(event.connectionId);
                    }
                    break;
                case protocol::GATEWAY_REGISTERED: {
                    auto row = protocol::decodeSystem<sbe::frame::GatewayRegistered>(event);
                    m_gateway.m_lifecycle.onGatewayRegistered(row.gatewayId(), row.gatewaySourceId(),
                                                              row.getGatewayNameAsString(), row.preferenceRank());
                    if (row.gatewayId() == m_gateway.m_lifecycle.gatewayId())
                    {
                        m_gateway.m_snapshots.participating(row.snapshot() == 1);
                    }
                    break;
                }
                case protocol::GATEWAY_ACTIVE: {
                    auto active = protocol::decodeSystem<sbe::frame::GatewayActive>(event);
                    m_gateway.m_lifecycle.onGatewayActive(active.gatewayId());
                    if (!m_gateway.m_lifecycle.isActivated())
                    {
                        m_gateway.m_snapshots.stopPublishing();
                        // S-6 refuses whatever this instance's session sends from here on, resends included.
                        m_gateway.m_session.discardUnconfirmed("gateway instance " +
                                                               std::to_string(active.gatewayId()) + " is designated");
                    }
                    break;
                }
                case protocol::SNAPSHOT_STARTED: {
                    auto started = protocol::decodeSystem<sbe::frame::SnapshotStarted>(event);
                    m_gateway.m_snapshots.onSnapshotStarted(started.round(), event.clusterSeqNo, event.position,
                                                            event.clusterTimestampNs, m_gateway.snapshotHeader(),
                                                            m_gateway.m_lifecycle.isAnnounced());
                    break;
                }
                case protocol::SNAPSHOT_END: {
                    if (event.sourceId != m_gateway.m_lifecycle.gatewaySourceId())
                    {
                        break;
                    }
                    auto end = protocol::decodeSystem<sbe::frame::SnapshotEnd>(event);
                    if (!m_gateway.m_snapshots.onSnapshotEnd(end.round(), end.recordCount(), end.length(),
                                                             end.crc32c()))
                    {
                        m_gateway.m_session.fence(
                            ClusterError::SnapshotDiverged,
                            "round " + std::to_string(end.round()) + "'s sequenced snapshot at clusterSeqNo " +
                                std::to_string(event.clusterSeqNo) + " differs from this instance's");
                    }
                    break;
                }
                default:
                    break;
            }
        }

        void onLeadershipChanged()
        {
            // A gateway is elected by the cluster rather than gated on its node leading: nothing to do.
        }

        void onPayload(const Payload& payload)
        {
            m_gateway.observeConnectionId(payload.sourceId(), payload.connectionId());
            m_gateway.m_listener.onSequenced(payload);
        }

        void onCaughtUp(const std::int64_t clusterSeqNo)
        {
            m_gateway.m_lifecycle.onCaughtUp();
            m_gateway.m_listener.onCaughtUp(clusterSeqNo);
        }

        // The cluster clock's tick (spec §7), once a second. It is the one time source that keeps
        // advancing while every producer is silent, which is exactly when a watchdog must still fire, and
        // it is identical on every node — so a timer driven by it decides the same thing everywhere.
        void onClusterHeartbeat(const std::int64_t clusterTimeNs, const std::int64_t receiveTimeNs)
        {
            m_gateway.m_listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        // A serving edge closes first, so the listener has stood down by the time it hears why.
        void onFenced(const ClusterError fence, const std::string& detail)
        {
            m_gateway.m_lifecycle.onFenced();
            m_gateway.m_listener.onFenced(fence, detail);
        }

        // Not once this activation's GatewayStarted went out: closing the session it bound promotes the sibling, and
        // no frame on the queue says whether that promotion is still to come, so a replacement could act on a stale
        // view. One designated after its session was lost bound nothing, and announces on the new one.
        [[nodiscard]] bool mayReconnect() const
        {
            return !m_gateway.m_lifecycle.isAnnounced();
        }

        // The designated instance: a standby serves no one.
        [[nodiscard]] bool isActing() const
        {
            return m_gateway.m_lifecycle.isActivated();
        }

      private:
        Gateway& m_gateway;
    };

    // The store of an instance given a listener, which requires its sourceId and directory.
    static std::unique_ptr<replayer::client::SnapshotStore> snapshotStore(const Config& config)
    {
        if (config.snapshotListener == nullptr)
        {
            return nullptr;
        }
        if (config.sourceId == UNRESOLVED)
        {
            throw std::invalid_argument("sourceId is required with a snapshotListener");
        }
        if (config.snapshotDirectory.empty())
        {
            throw std::invalid_argument("snapshotDirectory is required with a snapshotListener");
        }
        return std::make_unique<replayer::client::SnapshotStore>(config.snapshotDirectory);
    }

    Config m_config;
    Listener& m_listener;
    LifecycleActions m_actions;
    detail::GatewayLifecycle<LifecycleActions> m_lifecycle;
    std::unique_ptr<replayer::client::SnapshotStore> m_store;
    detail::SnapshotTaker m_snapshots;
    Restore m_restore;
    SessionDispatch m_dispatch;
    detail::Session<SessionDispatch> m_session;
    detail::SnapshotFrames<detail::Session<SessionDispatch>> m_snapshotFrames;

    // Connection lifecycle frames still to be placed, in the order they were asked for.
    std::deque<Lifecycle> m_lifecycleBacklog;

    // Connections whose ConnectionOpened has not landed: nothing may be published on one yet.
    std::unordered_set<std::int32_t> m_unopened;

    // Connections this instance holds open, taken and not yet closed while serving; the connections gauge, null
    // until its async add resolves.
    std::unordered_set<std::int32_t> m_connections;
    std::shared_ptr<aeron::Aeron> m_aeron;
    std::int64_t m_connectionsCounterRegId = -1;
    std::shared_ptr<aeron::Counter> m_connectionsCounter;
    // The last round this instance confirmed, null until its async add resolves, and never without a listener.
    std::int64_t m_snapshotRoundCounterRegId = -1;
    std::shared_ptr<aeron::Counter> m_snapshotRoundCounter;

    // The highest connectionId this logical gateway's history holds; the resume point for §7's row.
    std::int32_t m_highestConnectionId = NO_CONNECTION;
    std::int32_t m_nextConnectionId = 0;

    static_assert(UNRESOLVED == detail::GatewayLifecycle<LifecycleActions>::UNRESOLVED);
};

} // namespace org::limitless::seqeron::app
