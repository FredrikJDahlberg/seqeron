#pragma once

#include <cstdint>
#include <string_view>

#include "org/limitless/seqeron/app/Gateway.hpp"
#include "org/limitless/seqeron/hosted/detail/ClientLink.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"

#include "org_limitless_seqeron_sbe_hosted/Activated.h"
#include "org_limitless_seqeron_sbe_hosted/CloseConnection.h"
#include "org_limitless_seqeron_sbe_hosted/Connection.h"
#include "org_limitless_seqeron_sbe_hosted/GateClosed.h"
#include "org_limitless_seqeron_sbe_hosted/OpenConnection.h"
#include "org_limitless_seqeron_sbe_hosted/Publish.h"
#include "org_limitless_seqeron_sbe_hosted/Role.h"

namespace org::limitless::seqeron::hosted {

/**
 * The client stub of a hosted Gateway (doc/hosted-facade-draft.md §17): the surface of app::Gateway, with the façade
 * itself, the pair and the election running in a host process that this talks to over schema 218. Its listener is the
 * direct Gateway's, and onActivated returns what the host waits for (§17.5).
 *
 * What differs: connection ids are allocated here, from the host's last Role (H-4); a Published command can be lost
 * at a gate edge (H-3, H-4), and one on a connection whose ConnectionOpened is not yet placed is Published, not
 * Declined; the queries mirror the host's last Gate (H-16).
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread.
 */
template<typename Listener>
class Gateway : public detail::ClientLink<Gateway<Listener>, Listener>
{
    using Base = detail::ClientLink<Gateway<Listener>, Listener>;
    friend Base;

  public:
    // A frame that belongs to the gateway rather than to one of its connections, and what openConnection answers
    // when there is none to give.
    static constexpr std::int32_t NO_CONNECTION = -1;

    struct Config
    {
        // The logical gateway's sourceId, the one its rows declare and the host runs; the attach is refused otherwise.
        std::int32_t sourceId = -1;
        // Answers the host's SnapshotCut and restores its RestoreRecords; without it every cut is abandoned.
        app::SnapshotListener* snapshots = nullptr;
        // The host's heartbeat counter may be silent this long before the stub gives it up (H-9, section 17.6).
        std::int64_t hostTimeoutMs = 3000;
        std::int64_t attachTimeoutMs = 90'000;
        // Names the host this attaches to, the one its proxy.hostName gives; the attach is refused otherwise.
        std::string hostName = protocol::PROXY_DEFAULT_HOST_NAME;
    };

    Gateway(Config config, Listener& listener) :
      Base{ listener,         wire::Kind::GATEWAY,  config.sourceId,
            config.snapshots, config.hostTimeoutMs, config.attachTimeoutMs,
            config.hostName },
      m_config{ config }
    {
        static_assert(app::GatewayListener<Listener>);
    }

    /** Whether a connection may be taken right now: the host's last Gate, or this stub's own Activated(true). */
    [[nodiscard]] bool canAccept() const noexcept
    {
        return m_serving && this->m_canPublish;
    }

    /** Whether the edge is open: onActivated returned true and no Gate or Role has closed it since. */
    [[nodiscard]] bool isServing() const noexcept
    {
        return m_serving;
    }

    [[nodiscard]] std::int32_t sourceId() const noexcept
    {
        return m_config.sourceId;
    }

    /**
     * Takes one connection into the cluster's view of this gateway, under the next id from the last Role (H-4).
     *
     * @param connectionData what onConnectionOpened will carry, or null
     * @param length         its length
     * @return the connection's id, or NO_CONNECTION while not serving, with the window full, or with stream 304
     *         back-pressured: retry it while canAccept()
     */
    std::int32_t openConnection(const char* connectionData, const std::size_t length)
    {
        if (!canAccept() || length > protocol::MAX_PAYLOAD_LENGTH)
        {
            return NO_CONNECTION;
        }
        const std::int32_t id = m_nextConnectionId;
        const bool offered =
            this->template offerWindowed<wire::OpenConnection>(length, [&](wire::OpenConnection& open) {
                open.connectionId(id).putData(connectionData, static_cast<std::uint32_t>(length));
            });
        if (!offered)
        {
            return NO_CONNECTION;
        }
        m_nextConnectionId++;
        return id;
    }

    /** Takes one connection whose only identity is its id; see openConnection(const char*, std::size_t). */
    std::int32_t openConnection()
    {
        return openConnection(nullptr, 0);
    }

    /**
     * That connection has gone. Returns nothing: the offer is retried within the call, as it cannot be declined (H-5).
     *
     * @param connectionId the id openConnection returned
     */
    void closeConnection(const std::int32_t connectionId)
    {
        if (!m_serving)
        {
            return;
        }
        this->template sendUndeclinable<wire::CloseConnection>(
            [&](wire::CloseConnection& close) { close.epoch(this->m_epoch).connectionId(connectionId); });
    }

    /**
     * The edge closed without being asked to: the host opens it again through onActivated, if this instance is still
     * the one designated. Retried within the call.
     */
    void gateClosed()
    {
        if (!m_serving)
        {
            return;
        }
        this->template sendUndeclinable<wire::GateClosed>(
            [&](wire::GateClosed& closed) { closed.epoch(this->m_epoch); });
        m_serving = false;
    }

    /**
     * Offers one payload on a connection to this gateway's own group (H-4). The result is an offer's, not the
     * cluster's: Published means the host may place it, and a gate edge in flight drops it without notice (H-3).
     *
     * @param connectionId  the connection it belongs to, or NO_CONNECTION for the gateway itself
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     * @return Published; Declined while the gate is shut, the window is full or stream 304 is back-pressured, to
     *         retry; Refused, permanently
     */
    [[nodiscard]] protocol::Publish publish(const std::int32_t connectionId, const std::uint16_t payloadId,
                                            const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        return publish(connectionId, 0, payloadId, payload, payloadLength);
    }

    /**
     * The same to the groups groupMask names as well as its own.
     *
     * @param connectionId  the connection it belongs to, or NO_CONNECTION for the gateway itself
     * @param groupMask     bits from the GroupDirectory; one the table lacks is Refused
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     * @return as publish(connectionId, payloadId, payload, payloadLength)
     */
    [[nodiscard]] protocol::Publish publish(const std::int32_t connectionId, const std::uint16_t groupMask,
                                            const std::uint16_t payloadId, const std::uint8_t* payload,
                                            const std::uint16_t payloadLength)
    {
        return this->template offerPayload<wire::Publish>(
            groupMask, payloadId, payloadLength, [&](wire::Publish& command) {
                command.connectionId(connectionId)
                    .groupMask(groupMask)
                    .payloadId(payloadId)
                    .putBody(reinterpret_cast<const char*>(payload), payloadLength);
            });
    }

  private:
    // The hooks of ClientLink.
    void onGate(const bool leading)
    {
        m_serving = leading;
    }

    void onOther(const std::uint16_t templateId, char* base, const aeron::util::index_t length,
                 const std::uint16_t blockLength, const std::uint16_t version)
    {
        switch (templateId)
        {
            case wire::Role::sbeTemplateId():
                onRole(wire::Role(base, detail::HEADER, length, blockLength, version));
                break;
            case wire::Connection::sbeTemplateId():
                onConnection(wire::Connection(base, detail::HEADER, length, blockLength, version));
                break;
            default:
                break;
        }
    }

    // §17.5: answer onActivated with Activated and, once opened, serve at once under the epoch one above the last
    // Gate's, the one the host's open edge takes (H-3). A Role that repeats, because the dial failed, asks again.
    void onRole(wire::Role role)
    {
        if (role.active() != 0)
        {
            const std::int32_t first = static_cast<std::int32_t>(role.firstConnectionId());
            const bool opened = this->m_listener.onActivated(first);
            this->template sendUndeclinable<wire::Activated>(
                [&](wire::Activated& activated) { activated.opened(opened ? 1 : 0); });
            if (opened)
            {
                m_nextConnectionId = first;
                m_serving = true;
                this->m_canPublish = true;
                this->m_epoch++;
            }
            return;
        }
        this->m_listener.onStandby();
        m_serving = false;
        this->m_canPublish = false;
    }

    void onConnection(wire::Connection connection)
    {
        const auto id = static_cast<std::int32_t>(connection.connectionId());
        if (connection.opened() != 0)
        {
            if constexpr (requires(Listener& l, const char* data, std::size_t size) {
                              l.onConnectionOpened(std::int32_t{}, data, size);
                          })
            {
                const std::string_view data = connection.getDataAsStringView();
                this->m_listener.onConnectionOpened(id, data.data(), data.size());
            }
        }
        else if constexpr (requires { this->m_listener.onConnectionClosed(id); })
        {
            this->m_listener.onConnectionClosed(id);
        }
    }

    Config m_config;
    std::int32_t m_nextConnectionId = 0;
    bool m_serving = false;
};

} // namespace org::limitless::seqeron::hosted
