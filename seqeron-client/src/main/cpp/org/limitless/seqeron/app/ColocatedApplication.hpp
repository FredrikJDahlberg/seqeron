#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Defaults.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/detail/LeaderGate.hpp"
#include "org/limitless/seqeron/app/detail/Session.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"

namespace org::limitless::seqeron::app {

/** What a ColocatedApplication's Listener provides. */
template<typename L>
concept ColocatedApplicationListener =
    requires(L& listener, const Payload& payload, bool leading, std::int64_t globalSeqNo, std::int64_t clusterTimeNs,
             std::int64_t receiveTimeNs, ClusterError fence, const std::string& detail) {
        // The gate crossed an edge. False is where OutstandingWork::onNotLeader() belongs: every leadership
        // change shuts an open gate, and the work may have gone with the election.
        listener.onLeadershipChanged(leading);
        listener.onSequenced(payload);
        listener.onCaughtUp(globalSeqNo);
        listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        listener.onFenced(fence, detail);
    };

/**
 * One replica of a co-located application — the kind of producer nothing elects. One runs per node, the
 * topology's `<applications>` section names it, and LeadershipChanged already picks the replica that submits:
 * this one publishes only while its own node leads. What that takes is behind it — the cluster session over
 * the node's own aeron:ipc, the tap it follows, the leader gate, confirmed ingress across a failover, and
 * the fences that say it may no longer act.
 *
 * What is left to the consumer is its own work: the payloads it reads, the state it keeps, the payloads it
 * submits. Every replica reads the same ordered stream and so holds the same state, which is what makes
 * OutstandingWork — fed that same stream — the way work survives the gate closing under it.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls doWork() each
 * iteration. The Java twin is app/ColocatedApplication.java; keep the two in step. Where Java's builder
 * takes an ingressEndpoints string, this side has none: ClusterStreamSender dials member 0 on localhost and
 * follows the cluster's redirect; publish and reply each take an SBE encoder and a Fill, or already-encoded
 * bytes as the Java twin does. ColocatedApplicationListener above is what the Listener provides.
 */
template<ColocatedApplicationListener Listener>
class ColocatedApplication
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
        // This replica's Replayer client id, unique among the co-located apps on its node.
        std::int32_t clientId = 0;
        // The node this replica runs on: whose tap it follows, and whose leadership opens its gate.
        std::int32_t memberId = 0;
        // This client's own egress endpoint; two media drivers on one host cannot both bind a port.
        std::string egressChannel;
        std::size_t pendingCapacity = DEFAULT_PENDING_CAPACITY;
        std::int64_t tapStallTimeoutMs = DEFAULT_TAP_STALL_TIMEOUT_MS;
        std::int64_t recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;
        std::int64_t tapLagThresholdMs = DEFAULT_TAP_LAG_THRESHOLD_MS;
        std::int64_t ipcConnectTimeoutMs = DEFAULT_IPC_CONNECT_TIMEOUT_MS;
    };

    /**
     * Creates a replica that has not started.
     *
     * @param config   the replica's identity and deployment policy
     * @param listener the application; must outlive the replica
     */
    ColocatedApplication(Config config, Listener& listener) :
      m_config{ std::move(config) },
      m_listener{ listener },
      m_gate{ m_config.memberId },
      m_dispatch{ *this },
      m_session{ m_config.clientId,          m_config.pendingCapacity,
                 m_config.tapStallTimeoutMs, m_config.recoveryStallTimeoutMs,
                 m_config.tapLagThresholdMs, m_dispatch }
    {}

    ColocatedApplication(const ColocatedApplication&) = delete;
    ColocatedApplication& operator=(const ColocatedApplication&) = delete;

    /**
     * Opens the cluster session on this node and starts following its tap; the gate stays shut until caught
     * up.
     *
     * @param aeron the client, on this member's Aeron directory
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_session.startColocated(std::move(aeron), m_config.memberId, m_config.ipcConnectTimeoutMs,
                                 m_config.egressChannel);
    }

    // One duty-cycle iteration: the cluster session and the tap, then the gate over what they left. Returns
    // units of work done, for the caller's idle strategy.
    int doWork()
    {
        const int work = m_session.doWork();
        const detail::LeaderGate::Transition transition =
            m_gate.update(m_session.isCaughtUp(), m_session.currentLeaderMemberId());
        if (transition == detail::LeaderGate::Transition::None)
        {
            return work;
        }
        m_listener.onLeadershipChanged(transition == detail::LeaderGate::Transition::Opened);
        return work + 1;
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
     * Encodes and submits one payload of this application's own, stamped with its sourceId and belonging to
     * no connection.
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
        return submit<Encoder>(m_config.sourceId, NO_CONNECTION, payloadId, std::forward<Fill>(fill));
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
        return submit(m_config.sourceId, NO_CONNECTION, payloadId, payload, payloadLength);
    }

    /**
     * Encodes and submits a reply on behalf of the producer that asked for it. It carries the requester's
     * sourceId and connectionId, which is how the gateway that took the request routes the answer back out.
     * Keep those two off the request rather than the request itself — a Payload is valid only during its
     * callback, and a reply is usually dispatched later.
     *
     * @tparam Encoder          the reply's SBE encoder
     * @param requesterSourceId the request's sourceId
     * @param connectionId      the request's connectionId
     * @param payloadId         the reply's protocol
     * @param fill              called with the encoder, its messageHeader already applied, to stamp the fields
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish reply(const std::int32_t requesterSourceId, const std::int32_t connectionId,
                                          const std::uint16_t payloadId, Fill&& fill)
    {
        return submit<Encoder>(requesterSourceId, connectionId, payloadId, std::forward<Fill>(fill));
    }

    /**
     * Submits a reply the caller encoded itself, on behalf of the producer that asked for it.
     *
     * @param requesterSourceId the request's sourceId
     * @param connectionId      the request's connectionId
     * @param payloadId         the reply's protocol
     * @param payload           the reply's first byte, its own messageHeader included if it has one
     * @param payloadLength     the reply's length
     * @return Published; Declined while the gate is shut, ingress is held or the transport is back-pressured
     *         — retry it; Refused, permanently
     */
    [[nodiscard]] protocol::Publish reply(const std::int32_t requesterSourceId, const std::int32_t connectionId,
                                          const std::uint16_t payloadId, const std::uint8_t* payload,
                                          const std::uint16_t payloadLength)
    {
        return submit(requesterSourceId, connectionId, payloadId, payload, payloadLength);
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

    [[nodiscard]] std::int64_t lastGlobalSeqNo() const
    {
        return m_session.lastGlobalSeqNo();
    }

    // How far behind the leader this node's tap is running. Observation only — nothing here raises a
    // fence; a consumer that wants to report staleness, or log its edges, polls it.
    [[nodiscard]] const TapLagMonitor& tapLag() const noexcept
    {
        return m_session.tapLag();
    }

    // Closes the cluster session and the tap. The Aeron client is the caller's and is left open.
    void close()
    {
        m_session.close();
    }

  private:
    // A shut gate declines rather than submits: only the leading replica's copy of the work is the one sent.
    template<typename Encoder, typename Fill>
    [[nodiscard]] protocol::Publish submit(const std::int32_t frameSourceId, const std::int32_t connectionId,
                                           const std::uint16_t payloadId, Fill&& fill)
    {
        if (!m_gate.isOpen())
        {
            return protocol::Publish::Declined;
        }
        return m_session.template publishPayload<Encoder>(frameSourceId, connectionId, payloadId,
                                                          std::forward<Fill>(fill));
    }

    [[nodiscard]] protocol::Publish submit(const std::int32_t frameSourceId, const std::int32_t connectionId,
                                           const std::uint16_t payloadId, const std::uint8_t* payload,
                                           const std::uint16_t payloadLength)
    {
        if (!m_gate.isOpen())
        {
            return protocol::Publish::Declined;
        }
        return m_session.publishPayload(frameSourceId, connectionId, payloadId, payload, payloadLength);
    }

    // What comes off the tap, and the one frame the gate is driven by.
    class SessionDispatch
    {
      public:
        explicit SessionDispatch(ColocatedApplication& app) : m_app{ app }
        {}

        void onSystem(const protocol::SequencedEvent&)
        {
            // Seqeron's own vocabulary says nothing to a producer nothing elects; the leadership the gate
            // turns on arrives below rather than here.
        }

        void onLeadershipChanged()
        {
            m_app.m_gate.onLeadershipChanged();
        }

        void onPayload(const Payload& payload)
        {
            m_app.m_listener.onSequenced(payload);
        }

        void onCaughtUp(const std::int64_t globalSeqNo)
        {
            m_app.m_listener.onCaughtUp(globalSeqNo);
        }

        // The cluster clock's tick (spec §7), once a second. It is the one time source that keeps
        // advancing while every producer is silent, which is exactly when a watchdog must still fire, and
        // it is identical on every node — so a timer driven by it decides the same thing everywhere.
        void onClusterHeartbeat(const std::int64_t clusterTimeNs, const std::int64_t receiveTimeNs)
        {
            m_app.m_listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        void onFenced(const ClusterError fence, const std::string& detail)
        {
            m_app.m_listener.onFenced(fence, detail);
        }

      private:
        ColocatedApplication& m_app;
    };

    Config m_config;
    Listener& m_listener;
    detail::LeaderGate m_gate;
    SessionDispatch m_dispatch;
    detail::Session<SessionDispatch> m_session;
};

} // namespace org::limitless::seqeron::app
