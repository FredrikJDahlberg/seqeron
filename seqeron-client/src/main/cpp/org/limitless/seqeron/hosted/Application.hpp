#pragma once

#include <cstdint>

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/hosted/detail/Attachment.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"

#include "org_limitless_seqeron_sbe_hosted/Publish.h"
#include "org_limitless_seqeron_sbe_hosted/Reply.h"

namespace org::limitless::seqeron::hosted {

/**
 * The client stub of a hosted Application (doc/hosted-facade-draft.md §17): the surface of app::Application, with the
 * façade itself running in a host process that this talks to over schema 218. Its listener is the direct
 * Application's, and a Payload reaches it as it would there.
 *
 * What differs: a Published command can be lost at a gate edge (H-3, H-4), the queries mirror the host's last Gate
 * (H-16), and a snapshot cut is answered from the listener the Config names. The Gateway's is hosted::Gateway.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread.
 */
template<typename Listener>
class Application : public detail::Attachment<Application<Listener>, Listener>
{
    using Base = detail::Attachment<Application<Listener>, Listener>;
    friend Base;

  public:
    struct Config
    {
        // The source the host runs, the one its <applications> row declares; never -1.
        std::int32_t sourceId = -1;
        // Answers the host's SnapshotCut and restores its RestoreRecords; without it every cut is abandoned.
        app::SnapshotListener* snapshots = nullptr;
        // The host's heartbeat counter may be silent this long before the stub gives it up (H-9, section 17.6).
        std::int64_t hostTimeoutMs = 3000;
        std::int64_t attachTimeoutMs = 90'000;
    };

    Application(Config config, Listener& listener) :
      Base{ listener,         wire::Kind::APPLICATION, config.sourceId,
            config.snapshots, config.hostTimeoutMs,    config.attachTimeoutMs },
      m_config{ config }
    {
        static_assert(app::ApplicationListener<Listener>);
    }

    /** What the last Gate said (H-16); lags the host by a hop. */
    [[nodiscard]] bool canPublish() const noexcept
    {
        return this->m_canPublish;
    }

    [[nodiscard]] bool isLeading() const noexcept
    {
        return m_leading;
    }

    /**
     * Offers one payload of this application's own to its own group (H-4). The result is an offer's, not the
     * cluster's: Published means the host may place it, and a gate edge in flight drops it without notice (H-3).
     *
     * @return Published; Declined while the gate is shut, the window is full or stream 304 is back-pressured, to
     *         retry; Refused, permanently, for the local checks of a direct Application
     */
    [[nodiscard]] protocol::Publish publish(const std::uint16_t payloadId, const std::uint8_t* payload,
                                            const std::uint16_t payloadLength)
    {
        return publish(0, payloadId, payload, payloadLength);
    }

    /**
     * The same to the groups groupMask names as well as its own.
     *
     * @param groupMask     bits from the GroupDirectory; one the table lacks is Refused
     * @param payloadId     the payload's protocol
     * @param payload       the payload's first byte, its own messageHeader included if it has one
     * @param payloadLength the payload's length
     */
    [[nodiscard]] protocol::Publish publish(const std::uint16_t groupMask, const std::uint16_t payloadId,
                                            const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        if (m_config.sourceId == -1)
        {
            return protocol::Publish::Refused;
        }
        return this->template offerPayload<wire::Publish>(
            groupMask, payloadId, payloadLength, [&](wire::Publish& command) {
                command.connectionId(-1).groupMask(groupMask).payloadId(payloadId).putBody(
                    reinterpret_cast<const char*>(payload), payloadLength);
            });
    }

    /**
     * The same on behalf of the producer that asked for it (H-11): the reply carries the request's sourceId,
     * connectionId and groupMask.
     *
     * @param request       the request's RequestRef, kept from its Payload
     * @param payloadId     the reply's protocol
     * @param payload       the reply's first byte, its own messageHeader included if it has one
     * @param payloadLength the reply's length
     */
    [[nodiscard]] protocol::Publish reply(const app::RequestRef& request, const std::uint16_t payloadId,
                                          const std::uint8_t* payload, const std::uint16_t payloadLength)
    {
        if (m_config.sourceId == -1)
        {
            return protocol::Publish::Refused;
        }
        return this->template offerPayload<wire::Reply>(
            request.groupMask, payloadId, payloadLength, [&](wire::Reply& command) {
                command.sourceId(request.sourceId)
                    .connectionId(request.connectionId)
                    .groupMask(request.groupMask)
                    .payloadId(payloadId)
                    .putBody(reinterpret_cast<const char*>(payload), payloadLength);
            });
    }

  private:
    // The hooks of Attachment.
    void onGate(const bool leading)
    {
        if (leading != m_leading)
        {
            m_leading = leading;
            this->m_listener.onLeadershipChanged(leading);
        }
    }

    void onOther(std::uint16_t, char*, aeron::util::index_t, std::uint16_t, std::uint16_t)
    {}

    Config m_config;
    bool m_leading = false;
};

} // namespace org::limitless::seqeron::hosted
