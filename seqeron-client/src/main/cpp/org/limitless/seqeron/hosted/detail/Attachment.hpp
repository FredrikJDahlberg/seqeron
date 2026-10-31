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

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/app/detail/GroupResolution.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

#include "org_limitless_seqeron_sbe_hosted/Attach.h"
#include "org_limitless_seqeron_sbe_hosted/Attached.h"
#include "org_limitless_seqeron_sbe_hosted/CaughtUp.h"
#include "org_limitless_seqeron_sbe_hosted/Detach.h"
#include "org_limitless_seqeron_sbe_hosted/Fenced.h"
#include "org_limitless_seqeron_sbe_hosted/Gate.h"
#include "org_limitless_seqeron_sbe_hosted/GroupsResolved.h"
#include "org_limitless_seqeron_sbe_hosted/MessageHeader.h"
#include "org_limitless_seqeron_sbe_hosted/Payload.h"
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

/**
 * What a hosted Application and a hosted Gateway share (doc/hosted-facade-draft.md §17): the attach, the counters, the
 * command window, the host's events and the snapshot cut. The derived class supplies the hooks
 *
 *   onGate(bool leading), onOther(templateId, base, length, blockLength, version)
 *
 * and the surface of its own façade. Single-threaded, as they are.
 */
template<typename Derived, typename Listener>
class Attachment
{
  public:
    Attachment(const Attachment&) = delete;
    Attachment& operator=(const Attachment&) = delete;

    /**
     * Allocates the counters and the command publication, and starts sending Attach (H-8). The gate stays shut until
     * the host's first Gate.
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        m_clientId = m_aeron->nextCorrelationId();
        m_heartbeat = counter(CLIENT_HEARTBEAT_TYPE_ID, "seqeron.client.heartbeat hosted");
        m_applied = counter(CLIENT_APPLIED_TYPE_ID, "seqeron.client.applied hosted");
        const std::int64_t commandsId = m_aeron->addExclusivePublication(COMMANDS_CHANNEL, COMMANDS_STREAM);
        const std::int64_t requestsId = m_aeron->addPublication(ATTACH_REQUEST_CHANNEL, ATTACH_REQUEST_STREAM);
        const std::int64_t repliesId = m_aeron->addSubscription(ATTACH_REPLY_CHANNEL, ATTACH_REPLY_STREAM);
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
        m_attachDeadlineNs = nowNs() + m_attachTimeoutMs * 1'000'000LL;
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

    /** Writes the client heartbeat, as a direct façade's keepAlive() keeps its session: call it while spinning. */
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
        m_requests->offer(buffer(), 0, static_cast<aeron::util::index_t>(HEADER + detach.encodedLength()));
        m_attached = false;
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

  protected:
    Attachment(Listener& listener, const wire::Kind::Value kind, const std::int32_t sourceId,
               app::SnapshotListener* snapshots, const std::int64_t hostTimeoutMs, const std::int64_t attachTimeoutMs) :
      m_listener{ listener },
      m_snapshots{ snapshots },
      m_kind{ kind },
      m_sourceId{ sourceId },
      m_hostTimeoutMs{ hostTimeoutMs },
      m_attachTimeoutMs{ attachTimeoutMs }
    {
        m_out.resize(128 * 1024);
    }

    char* out()
    {
        return reinterpret_cast<char*>(m_out.data());
    }

    aeron::concurrent::AtomicBuffer buffer()
    {
        return aeron::concurrent::AtomicBuffer(m_out.data(), m_out.size());
    }

    void beat()
    {
        m_heartbeat->setOrdered(nowNs() / 1'000'000);
    }

    // The local checks every payload offer shares, then the group mask, then the window and the offer.
    template<typename Command, typename Fill>
    [[nodiscard]] protocol::Publish offerPayload(const std::uint16_t groupMask, const std::uint16_t payloadId,
                                                 const std::uint16_t payloadLength, Fill&& fill)
    {
        if (payloadId == 0 || payloadId == 1 || payloadLength > protocol::MAX_PAYLOAD_LENGTH)
        {
            return protocol::Publish::Refused;
        }
        if (!m_attached || !m_canPublish || m_groups.directory() == nullptr)
        {
            return protocol::Publish::Declined;
        }
        if (m_groups.frameMask(groupMask) == app::detail::GroupResolution::NOT_ADDRESSABLE)
        {
            return protocol::Publish::Refused;
        }
        return offerWindowed<Command>(payloadLength, std::forward<Fill>(fill)) ? protocol::Publish::Published
                                                                               : protocol::Publish::Declined;
    }

    // The window (H-5), then the offer on stream 304; false when either declines. bodyLength is the varData's.
    template<typename Command, typename Fill>
    [[nodiscard]] bool offerWindowed(const std::size_t bodyLength, Fill&& fill)
    {
        const std::int64_t length =
            static_cast<std::int64_t>(HEADER + Command::sbeBlockLength() + sizeof(std::uint32_t) + bodyLength);
        if (m_offeredBytes + length - retired() > m_window)
        {
            m_windowDeclines++;
            return false;
        }
        Command command;
        command.wrapAndApplyHeader(out(), 0, m_out.size()).epoch(m_epoch);
        fill(command);
        if (m_commands->offer(buffer(), 0, static_cast<aeron::util::index_t>(HEADER + command.encodedLength())) < 0)
        {
            return false;
        }
        m_offeredBytes += length;
        return true;
    }

    // What cannot be declined is outside the window (H-5): retried until it lands, the heartbeat written on every
    // spin; the host's liveness bounds it.
    template<typename Command, typename Fill>
    void sendUndeclinable(Fill&& fill)
    {
        Command command;
        command.wrapAndApplyHeader(out(), 0, m_out.size());
        fill(command);
        sendCommand(HEADER + command.encodedLength());
    }

    void sendCommand(const std::size_t length)
    {
        const std::int64_t deadline = nowNs() + SEND_BOUND_NS;
        while (m_commands->offer(buffer(), 0, static_cast<aeron::util::index_t>(length)) < 0)
        {
            beat();
            if (nowNs() > deadline || hostLost())
            {
                m_failure = Failure::HostLost;
                return;
            }
        }
    }

    Listener& m_listener;
    app::detail::GroupResolution m_groups{ {} };
    std::int64_t m_epoch = 0;
    bool m_attached = false;
    bool m_canPublish = false;

  private:
    Derived& self()
    {
        return static_cast<Derived&>(*this);
    }

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

    std::int64_t retired() const
    {
        return m_aeron->countersReader().getCounterValue(m_retiredCounterId);
    }

    // H-8: resend the request until the host answers it; a reply for another client or request is ignored (H-15).
    int attach()
    {
        const int work = m_replies->poll(m_onReply, 16);
        const std::int64_t now = nowNs();
        if (m_accepted)
        {
            const std::int64_t eventsId =
                m_aeron->addSubscription(EVENTS_CHANNEL + std::to_string(m_eventSessionId), EVENTS_STREAM);
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
            m_nextAttachNs = now + ATTACH_RETRY_NS;
        }
        return work;
    }

    void sendAttach()
    {
        const std::uint32_t format = m_snapshots != nullptr ? m_snapshots->formatVersion() : 0;
        wire::Attach attach;
        attach.wrapAndApplyHeader(out(), 0, m_out.size())
            .clientId(m_clientId)
            .requestId(m_requestId)
            .kind(m_kind)
            .protocolVersion(PROTOCOL_VERSION)
            .commandSessionId(m_commands->sessionId())
            .heartbeatCounterId(m_heartbeat->id())
            .heartbeatRegistrationId(m_heartbeat->registrationId())
            .appliedCounterId(m_applied->id())
            .appliedRegistrationId(m_applied->registrationId())
            .formatVersion(static_cast<std::int32_t>(format))
            .supportedFormatsCount(1)
            .next()
            .formatVersion(static_cast<std::int32_t>(format));
        attach.putHostName(std::string{ HOST_NAME }).putAeronVersion(std::string{ aeron_version_text() });
        m_requests->offer(buffer(), 0, static_cast<aeron::util::index_t>(HEADER + attach.encodedLength()));
    }

    void onAttached(const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
                    const aeron::util::index_t length)
    {
        char* base = reinterpret_cast<char*>(buf.buffer()) + offset;
        wire::MessageHeader header(base, static_cast<std::uint64_t>(length));
        if (header.schemaId() != SCHEMA_ID || header.templateId() != wire::Attached::sbeTemplateId())
        {
            return;
        }
        wire::Attached attached(base, HEADER, length, header.blockLength(), header.version());
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
        if (attached.sourceId() != m_sourceId)
        {
            m_failure = Failure::AttachRefused;
            return;
        }
        m_retiredCounterId = attached.retiredCounterId();
        m_window = attached.commandWindow();
        m_hostHeartbeatId = attached.heartbeatCounterId();
        m_hostHeartbeatRegistrationId = attached.heartbeatRegistrationId();
        m_hostHeartbeatValue = m_aeron->countersReader().getCounterValue(m_hostHeartbeatId);
        m_hostChangeNs = nowNs();
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
        const std::int64_t now = nowNs();
        const std::int64_t value = reader.getCounterValue(m_hostHeartbeatId);
        if (value != m_hostHeartbeatValue)
        {
            m_hostHeartbeatValue = value;
            m_hostChangeNs = now;
        }
        return now - m_hostChangeNs > m_hostTimeoutMs * 1'000'000LL;
    }

    void onGroupsResolved(wire::GroupsResolved message)
    {
        std::vector<protocol::SnapshotGroup> table;
        auto& groups = message.groups();
        while (groups.hasNext())
        {
            groups.next();
            table.push_back(protocol::SnapshotGroup{ groups.groupId(), groups.getNameAsString() });
        }
        if (table.empty())
        {
            return;
        }
        // The own group id only sets the bit a direct frame adds to its mask; the stub reads the directory alone.
        m_groups.onSnapshotHeader(table);
        m_groups.onCaughtUp(table.front().groupId);
        if constexpr (requires { m_listener.onGroupsResolved(*m_groups.directory()); })
        {
            m_listener.onGroupsResolved(*m_groups.directory());
        }
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
            case wire::GroupsResolved::sbeTemplateId():
                onGroupsResolved(wire::GroupsResolved(base, HEADER, length, blockLength, version));
                break;
            case wire::Payload::sbeTemplateId():
                onPayload(wire::Payload(base, HEADER, length, blockLength, version));
                break;
            case wire::Gate::sbeTemplateId():
                onGateEvent(wire::Gate(base, HEADER, length, blockLength, version));
                break;
            case wire::CaughtUp::sbeTemplateId(): {
                wire::CaughtUp caughtUp(base, HEADER, length, blockLength, version);
                m_caughtUp = true;
                if constexpr (requires { m_listener.onCaughtUp(std::int64_t{}); })
                {
                    m_listener.onCaughtUp(caughtUp.clusterSeqNo());
                }
                break;
            }
            case wire::RestoreRecord::sbeTemplateId():
                onRestore(wire::RestoreRecord(base, HEADER, length, blockLength, version));
                break;
            case wire::SnapshotCut::sbeTemplateId():
                onCut(wire::SnapshotCut(base, HEADER, length, blockLength, version).roundId());
                break;
            case wire::Fenced::sbeTemplateId(): {
                wire::Fenced fenced(base, HEADER, length, blockLength, version);
                m_fenced = true;
                m_listener.onFenced(static_cast<app::ClusterError>(fenced.errorCode()), fenced.getDetailAsString());
                break;
            }
            default:
                self().onOther(header.templateId(), base, length, blockLength, version);
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
        if (body.size() >= HEADER)
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
    void onGateEvent(const wire::Gate& gate)
    {
        m_epoch = gate.epoch();
        m_canPublish = gate.canPublish() != 0;
        if (gate.caughtUp() != 0)
        {
            m_caughtUp = true;
        }
        self().onGate(gate.leading() != 0);
    }

    void onRestore(wire::RestoreRecord record)
    {
        if (m_snapshots == nullptr)
        {
            return;
        }
        const std::string_view data = record.getDataAsStringView();
        m_snapshots->onRestore(
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(data.data()), data.size()),
            record.recordIndex(), static_cast<std::uint32_t>(record.formatVersion()));
    }

    // H-6: the records for this cut, then the end; none of it can be declined (H-5), so each offer is retried.
    void onCut(const std::int64_t roundId)
    {
        std::int32_t count = 0;
        if (m_snapshots != nullptr)
        {
            std::array<std::uint8_t, MAX_RECORD> record;
            for (std::int32_t length; (length = m_snapshots->onSnapshot(record, count)) > 0; count++)
            {
                wire::SnapshotRecord message;
                message.wrapAndApplyHeader(out(), 0, m_out.size())
                    .roundId(roundId)
                    .recordIndex(count)
                    .putData(reinterpret_cast<const char*>(record.data()), static_cast<std::uint32_t>(length));
                sendCommand(HEADER + message.encodedLength());
            }
        }
        wire::SnapshotEnd end;
        end.wrapAndApplyHeader(out(), 0, m_out.size())
            .roundId(roundId)
            .recordCount(m_snapshots != nullptr ? count : -1);
        sendCommand(HEADER + end.encodedLength());
    }

    app::SnapshotListener* m_snapshots;
    wire::Kind::Value m_kind;
    std::int32_t m_sourceId;
    std::int64_t m_hostTimeoutMs;
    std::int64_t m_attachTimeoutMs;
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
               const aeron::util::index_t length, const aeron::Header&) { onAttached(buf, offset, length); };
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
    std::int64_t m_lastClusterSeqNo = 0;
    Failure m_failure = Failure::None;
    bool m_accepted = false;
    bool m_caughtUp = false;
    bool m_fenced = false;
};

} // namespace detail

} // namespace org::limitless::seqeron::hosted
