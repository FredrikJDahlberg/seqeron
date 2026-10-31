#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "FragmentAssembler.h"

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

#include "org_limitless_seqeron_sbe_hosted/Attach.h"
#include "org_limitless_seqeron_sbe_hosted/Attached.h"
#include "org_limitless_seqeron_sbe_hosted/CaughtUp.h"
#include "org_limitless_seqeron_sbe_hosted/Detach.h"
#include "org_limitless_seqeron_sbe_hosted/Fenced.h"
#include "org_limitless_seqeron_sbe_hosted/Gate.h"
#include "org_limitless_seqeron_sbe_hosted/MessageHeader.h"
#include "org_limitless_seqeron_sbe_hosted/Payload.h"
#include "org_limitless_seqeron_sbe_hosted/Publish.h"
#include "org_limitless_seqeron_sbe_hosted/RestoreRecord.h"
#include "org_limitless_seqeron_sbe_hosted/SnapshotCut.h"
#include "org_limitless_seqeron_sbe_hosted/SnapshotEnd.h"
#include "org_limitless_seqeron_sbe_hosted/SnapshotRecord.h"

namespace org::limitless::seqeron::hosted {

namespace wire = sbe::hosted;

/** Why a stub stopped (doc/hosted-facade-draft.md §17.6): its own faults, which are not a fence. */
enum class Failure : std::uint8_t
{
    None,
    AttachTimedOut,
    AttachRefused,
    HostLost,
};

namespace detail {

inline constexpr const char* HOST_NAME = "hosted";
inline constexpr std::uint16_t PROTOCOL_VERSION = 1;
inline constexpr std::uint16_t SCHEMA_ID = 218;
inline constexpr const char* ATTACH_REQUEST_CHANNEL = "aeron:ipc?term-length=64k";
inline constexpr const char* ATTACH_REPLY_CHANNEL = "aeron:ipc?tether=false";
inline constexpr const char* EVENTS_CHANNEL = "aeron:ipc?session-id=";
inline constexpr const char* COMMANDS_CHANNEL = "aeron:ipc?term-length=1m";
inline constexpr std::int32_t ATTACH_REQUEST_STREAM = 301;
inline constexpr std::int32_t ATTACH_REPLY_STREAM = 302;
inline constexpr std::int32_t EVENTS_STREAM = 303;
inline constexpr std::int32_t COMMANDS_STREAM = 304;
inline constexpr std::int32_t CLIENT_HEARTBEAT_TYPE_ID = 5300;
inline constexpr std::int32_t CLIENT_APPLIED_TYPE_ID = 5301;
inline constexpr std::size_t HEADER = 8;
inline constexpr std::int64_t ATTACH_RETRY_NS = 500'000'000LL;
inline constexpr std::int64_t SEND_BOUND_NS = 10'000'000'000LL;
inline constexpr std::uint16_t MAX_RECORD = 65535;

inline std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

} // namespace detail

/**
 * The client stub of a hosted Application (doc/hosted-facade-draft.md §17): the surface of app::Application, with the
 * façade itself running in a host process that this talks to over schema 218. Its listener is the direct
 * Application's, and a Payload reaches it as it would there.
 *
 * What differs: a Published command can be lost at a gate edge (H-3, H-4), the queries mirror the host's last Gate
 * (H-16), and a snapshot cut is answered from the listener the Config names. Group masks, Reply and the Gateway
 * messages are not built.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread.
 */
template<typename Listener>
class Application
{
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

    Application(Config config, Listener& listener) : m_config{ config }, m_listener{ listener }
    {
        static_assert(app::ApplicationListener<Listener>);
        m_out.resize(128 * 1024);
    }

    Application(const Application&) = delete;
    Application& operator=(const Application&) = delete;

    /**
     * Allocates the counters and the command publication, and starts sending Attach (H-8). The gate stays shut until
     * the host's first Gate.
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        m_clientId = m_aeron->nextCorrelationId();
        m_heartbeat = counter(detail::CLIENT_HEARTBEAT_TYPE_ID, "seqeron.client.heartbeat hosted");
        m_applied = counter(detail::CLIENT_APPLIED_TYPE_ID, "seqeron.client.applied hosted");
        const std::int64_t commandsId =
            m_aeron->addExclusivePublication(detail::COMMANDS_CHANNEL, detail::COMMANDS_STREAM);
        const std::int64_t requestsId =
            m_aeron->addPublication(detail::ATTACH_REQUEST_CHANNEL, detail::ATTACH_REQUEST_STREAM);
        const std::int64_t repliesId =
            m_aeron->addSubscription(detail::ATTACH_REPLY_CHANNEL, detail::ATTACH_REPLY_STREAM);
        while (!(m_commands = m_aeron->findExclusivePublication(commandsId)))
        {
            std::this_thread::yield();
        }
        while (!(m_requests = m_aeron->findPublication(requestsId)))
        {
            std::this_thread::yield();
        }
        while (!(m_replies = m_aeron->findSubscription(repliesId)))
        {
            std::this_thread::yield();
        }
        m_attachDeadlineNs = detail::nowNs() + m_config.attachTimeoutMs * 1'000'000LL;
    }

    /** One duty cycle: the heartbeat, the attach while it is unanswered, then the host's events. */
    int doWork()
    {
        if (m_failure != Failure::None)
        {
            return 0;
        }
        beat();
        if (!m_attached)
        {
            return attach();
        }
        const int work = m_events->poll(m_assembler.handler(), 16);
        if (hostLost())
        {
            m_failure = Failure::HostLost;
        }
        return work;
    }

    /** What the last Gate said (H-16); lags the host by a hop. */
    [[nodiscard]] bool canPublish() const noexcept
    {
        return m_canPublish;
    }

    [[nodiscard]] bool isLeading() const noexcept
    {
        return m_leading;
    }

    [[nodiscard]] bool isCaughtUp() const noexcept
    {
        return m_caughtUp;
    }

    [[nodiscard]] std::int64_t lastClusterSeqNo() const noexcept
    {
        return m_lastClusterSeqNo;
    }

    [[nodiscard]] Failure failure() const noexcept
    {
        return m_failure;
    }

    [[nodiscard]] bool fenced() const noexcept
    {
        return m_fenced;
    }

    /** Commands the stub declined for want of room in H-5's window. */
    [[nodiscard]] std::int64_t windowDeclines() const noexcept
    {
        return m_windowDeclines;
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
        if (m_config.sourceId == -1 || payloadId == 0 || payloadId == 1 || payloadLength > protocol::MAX_PAYLOAD_LENGTH)
        {
            return protocol::Publish::Refused;
        }
        if (!m_attached || !m_canPublish)
        {
            return protocol::Publish::Declined;
        }
        const std::int64_t length = static_cast<std::int64_t>(detail::HEADER + wire::Publish::sbeBlockLength() +
                                                              sizeof(std::uint32_t) + payloadLength);
        if (m_offeredBytes + length - retired() > m_window)
        {
            m_windowDeclines++;
            return protocol::Publish::Declined;
        }
        wire::Publish publish;
        publish.wrapAndApplyHeader(out(), 0, m_out.size())
            .epoch(m_epoch)
            .connectionId(0)
            .groupMask(0)
            .payloadId(payloadId)
            .putBody(reinterpret_cast<const char*>(payload), payloadLength);
        if (m_commands->offer(buffer(), 0,
                              static_cast<aeron::util::index_t>(detail::HEADER + publish.encodedLength())) < 0)
        {
            return protocol::Publish::Declined;
        }
        m_offeredBytes += length;
        return protocol::Publish::Published;
    }

    /** Writes the client heartbeat, as a direct Application's keepAlive() keeps its session: call it while spinning. */
    void keepAlive()
    {
        beat();
    }

    /** An orderly shutdown (H-9): the host closes the façade and releases the cluster session. */
    void close()
    {
        if (!m_attached)
        {
            return;
        }
        wire::Detach detach;
        detach.wrapAndApplyHeader(out(), 0, m_out.size()).clientId(m_clientId);
        m_requests->offer(buffer(), 0, static_cast<aeron::util::index_t>(detail::HEADER + detach.encodedLength()));
        m_attached = false;
    }

  private:
    std::shared_ptr<aeron::Counter> counter(const std::int32_t typeId, const std::string& label)
    {
        const std::int64_t id = m_aeron->addCounter(typeId, nullptr, 0, label);
        std::shared_ptr<aeron::Counter> found;
        while (!(found = m_aeron->findCounter(id)))
        {
            std::this_thread::yield();
        }
        return found;
    }

    void beat()
    {
        m_heartbeat->setOrdered(detail::nowNs() / 1'000'000);
    }

    char* out()
    {
        return reinterpret_cast<char*>(m_out.data());
    }

    aeron::concurrent::AtomicBuffer buffer()
    {
        return aeron::concurrent::AtomicBuffer(m_out.data(), m_out.size());
    }

    std::int64_t retired() const
    {
        return m_aeron->countersReader().getCounterValue(m_retiredCounterId);
    }

    // H-8: resend the request until the host answers it; a reply for another client or request is ignored (H-15).
    int attach()
    {
        const int work = m_replies->poll(m_onReply, 16);
        const std::int64_t now = detail::nowNs();
        if (m_accepted)
        {
            const std::int64_t eventsId = m_aeron->addSubscription(
                detail::EVENTS_CHANNEL + std::to_string(m_eventSessionId), detail::EVENTS_STREAM);
            while (!(m_events = m_aeron->findSubscription(eventsId)))
            {
                std::this_thread::yield();
            }
            m_replies.reset();
            m_attached = true;
            return work + 1;
        }
        if (m_failure != Failure::None)
        {
            return work;
        }
        if (now > m_attachDeadlineNs)
        {
            m_failure = Failure::AttachTimedOut;
        }
        else if (now >= m_nextAttachNs)
        {
            sendAttach();
            m_nextAttachNs = now + detail::ATTACH_RETRY_NS;
        }
        return work;
    }

    void sendAttach()
    {
        const std::uint32_t format = m_config.snapshots != nullptr ? m_config.snapshots->formatVersion() : 0;
        wire::Attach attach;
        attach.wrapAndApplyHeader(out(), 0, m_out.size())
            .clientId(m_clientId)
            .requestId(m_requestId)
            .kind(wire::Kind::APPLICATION)
            .protocolVersion(detail::PROTOCOL_VERSION)
            .commandSessionId(m_commands->sessionId())
            .heartbeatCounterId(m_heartbeat->id())
            .heartbeatRegistrationId(m_heartbeat->registrationId())
            .appliedCounterId(m_applied->id())
            .appliedRegistrationId(m_applied->registrationId())
            .formatVersion(static_cast<std::int32_t>(format))
            .supportedFormatsCount(1)
            .next()
            .formatVersion(static_cast<std::int32_t>(format));
        attach.putHostName(std::string{ detail::HOST_NAME }).putAeronVersion(std::string{ aeron_version_text() });
        m_requests->offer(buffer(), 0, static_cast<aeron::util::index_t>(detail::HEADER + attach.encodedLength()));
    }

    void onReply(const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
                 const aeron::util::index_t length)
    {
        char* base = reinterpret_cast<char*>(buf.buffer()) + offset;
        wire::MessageHeader header(base, static_cast<std::uint64_t>(length));
        if (header.schemaId() != detail::SCHEMA_ID || header.templateId() != wire::Attached::sbeTemplateId())
        {
            return;
        }
        wire::Attached attached(base, detail::HEADER, length, header.blockLength(), header.version());
        if (attached.clientId() != m_clientId || attached.requestId() != m_requestId)
        {
            return;
        }
        const wire::Refusal::Value refusal = attached.refusal();
        if (refusal == wire::Refusal::BUSY)
        {
            return;
        }
        if (refusal != wire::Refusal::NONE)
        {
            m_failure = Failure::AttachRefused;
            return;
        }
        if (attached.sourceId() != m_config.sourceId)
        {
            m_failure = Failure::AttachRefused;
            return;
        }
        m_retiredCounterId = attached.retiredCounterId();
        m_window = attached.commandWindow();
        m_hostHeartbeatId = attached.heartbeatCounterId();
        m_hostHeartbeatRegistrationId = attached.heartbeatRegistrationId();
        m_hostHeartbeatValue = m_aeron->countersReader().getCounterValue(m_hostHeartbeatId);
        m_hostChangeNs = detail::nowNs();
        m_eventSessionId = attached.eventSessionId();
        m_accepted = true;
    }

    // §17.2.1: stale when the value has not changed for the timeout on this clock, or it is no longer the counter
    // given.
    bool hostLost()
    {
        const auto& reader = m_aeron->countersReader();
        if (reader.getCounterState(m_hostHeartbeatId) != aeron::concurrent::CountersReader::RECORD_ALLOCATED ||
            reader.getCounterRegistrationId(m_hostHeartbeatId) != m_hostHeartbeatRegistrationId)
        {
            return true;
        }
        const std::int64_t now = detail::nowNs();
        const std::int64_t value = reader.getCounterValue(m_hostHeartbeatId);
        if (value != m_hostHeartbeatValue)
        {
            m_hostHeartbeatValue = value;
            m_hostChangeNs = now;
        }
        return now - m_hostChangeNs > m_config.hostTimeoutMs * 1'000'000LL;
    }

    void onEvent(const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
                 const aeron::util::index_t length)
    {
        char* base = reinterpret_cast<char*>(buf.buffer()) + offset;
        wire::MessageHeader header(base, static_cast<std::uint64_t>(length));
        const std::uint16_t blockLength = header.blockLength();
        const std::uint16_t version = header.version();
        switch (header.templateId())
        {
            case wire::Payload::sbeTemplateId():
                onPayload(wire::Payload(base, detail::HEADER, length, blockLength, version));
                break;
            case wire::Gate::sbeTemplateId():
                onGate(wire::Gate(base, detail::HEADER, length, blockLength, version));
                break;
            case wire::CaughtUp::sbeTemplateId(): {
                wire::CaughtUp caughtUp(base, detail::HEADER, length, blockLength, version);
                m_caughtUp = true;
                if constexpr (requires { m_listener.onCaughtUp(std::int64_t{}); })
                {
                    m_listener.onCaughtUp(caughtUp.clusterSeqNo());
                }
                break;
            }
            case wire::RestoreRecord::sbeTemplateId():
                onRestore(wire::RestoreRecord(base, detail::HEADER, length, blockLength, version));
                break;
            case wire::SnapshotCut::sbeTemplateId():
                onCut(wire::SnapshotCut(base, detail::HEADER, length, blockLength, version).roundId());
                break;
            case wire::Fenced::sbeTemplateId(): {
                wire::Fenced fenced(base, detail::HEADER, length, blockLength, version);
                m_fenced = true;
                m_listener.onFenced(static_cast<app::ClusterError>(fenced.errorCode()), fenced.getDetailAsString());
                break;
            }
            default:
                break;
        }
    }

    void onPayload(wire::Payload payload)
    {
        protocol::SequencedEvent event{};
        event.clusterSeqNo = payload.clusterSeqNo();
        event.groupSeqNo = payload.groupSeqNo();
        event.sourceId = payload.sourceId();
        event.connectionId = static_cast<std::int32_t>(payload.connectionId());
        event.groupMask = static_cast<std::uint16_t>(payload.groupMask());
        event.sourceSessionId = payload.sourceSessionId();
        event.clusterTimestampNs = payload.clusterTimestampNs();
        event.receiveTimeNs = payload.receiveTimeNs();
        event.system = false;
        event.payloadId = payload.payloadId();
        event.position = payload.position();
        const std::string_view body = payload.getBodyAsStringView();
        // The body carries its own messageHeader: blockLength, templateId, schemaId, version.
        if (body.size() >= detail::HEADER)
        {
            std::memcpy(&event.blockLength, body.data(), sizeof event.blockLength);
            std::memcpy(&event.templateId, body.data() + 2, sizeof event.templateId);
            std::memcpy(&event.version, body.data() + 6, sizeof event.version);
        }
        event.payload = body.data();
        event.payloadLength = body.size();
        m_lastClusterSeqNo = event.clusterSeqNo;
        m_listener.onSequenced(app::detail::makePayload(event));
        m_applied->setOrdered(event.position);
    }

    // H-3: the stub stamps commands with the epoch of the last Gate; H-16: the queries mirror it.
    void onGate(const wire::Gate& gate)
    {
        m_epoch = gate.epoch();
        const bool leading = gate.leading() != 0;
        m_canPublish = gate.canPublish() != 0;
        if (gate.caughtUp() != 0)
        {
            m_caughtUp = true;
        }
        if (leading != m_leading)
        {
            m_leading = leading;
            m_listener.onLeadershipChanged(leading);
        }
    }

    void onRestore(wire::RestoreRecord record)
    {
        if (m_config.snapshots == nullptr)
        {
            return;
        }
        const std::string_view data = record.getDataAsStringView();
        m_config.snapshots->onRestore(
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(data.data()), data.size()),
            record.recordIndex(), static_cast<std::uint32_t>(record.formatVersion()));
    }

    // H-6: the records for this cut, then the end; none of it can be declined (H-5), so each offer is retried.
    void onCut(const std::int64_t roundId)
    {
        std::int32_t count = 0;
        if (m_config.snapshots != nullptr)
        {
            std::array<std::uint8_t, detail::MAX_RECORD> record;
            for (std::int32_t length; (length = m_config.snapshots->onSnapshot(record, count)) > 0; count++)
            {
                wire::SnapshotRecord message;
                message.wrapAndApplyHeader(out(), 0, m_out.size())
                    .roundId(roundId)
                    .recordIndex(count)
                    .putData(reinterpret_cast<const char*>(record.data()), static_cast<std::uint32_t>(length));
                sendCommand(detail::HEADER + message.encodedLength());
            }
        }
        wire::SnapshotEnd end;
        end.wrapAndApplyHeader(out(), 0, m_out.size())
            .roundId(roundId)
            .recordCount(m_config.snapshots != nullptr ? count : -1);
        sendCommand(detail::HEADER + end.encodedLength());
    }

    // Retried until it lands, writing the heartbeat on every spin (H-5); the host's liveness bounds it.
    void sendCommand(const std::size_t length)
    {
        const std::int64_t deadline = detail::nowNs() + detail::SEND_BOUND_NS;
        while (m_commands->offer(buffer(), 0, static_cast<aeron::util::index_t>(length)) < 0)
        {
            beat();
            if (detail::nowNs() > deadline || hostLost())
            {
                m_failure = Failure::HostLost;
                return;
            }
        }
    }

    Config m_config;
    Listener& m_listener;
    std::shared_ptr<aeron::Aeron> m_aeron;
    std::shared_ptr<aeron::Counter> m_heartbeat;
    std::shared_ptr<aeron::Counter> m_applied;
    std::shared_ptr<aeron::Subscription> m_replies;
    std::shared_ptr<aeron::Subscription> m_events;
    std::shared_ptr<aeron::Publication> m_requests;
    std::shared_ptr<aeron::ExclusivePublication> m_commands;
    std::vector<std::uint8_t> m_out;
    const aeron::fragment_handler_t m_onReply =
        [this](const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
               const aeron::util::index_t length, const aeron::Header&) { onReply(buf, offset, length); };
    aeron::FragmentAssembler m_assembler{ [this](const aeron::concurrent::AtomicBuffer& buf,
                                                 const aeron::util::index_t offset, const aeron::util::index_t length,
                                                 const aeron::Header&) { onEvent(buf, offset, length); } };
    std::int64_t m_clientId = 0;
    std::int64_t m_requestId = 1;
    std::int64_t m_attachDeadlineNs = 0;
    std::int64_t m_nextAttachNs = 0;
    std::int32_t m_eventSessionId = -1;
    std::int32_t m_retiredCounterId = 0;
    std::int32_t m_hostHeartbeatId = 0;
    std::int64_t m_hostHeartbeatRegistrationId = 0;
    std::int64_t m_hostHeartbeatValue = 0;
    std::int64_t m_hostChangeNs = 0;
    std::int64_t m_window = 0;
    std::int64_t m_offeredBytes = 0;
    std::int64_t m_windowDeclines = 0;
    std::int64_t m_epoch = 0;
    std::int64_t m_lastClusterSeqNo = 0;
    Failure m_failure = Failure::None;
    bool m_accepted = false;
    bool m_attached = false;
    bool m_leading = false;
    bool m_canPublish = false;
    bool m_caughtUp = false;
    bool m_fenced = false;
};

} // namespace org::limitless::seqeron::hosted
