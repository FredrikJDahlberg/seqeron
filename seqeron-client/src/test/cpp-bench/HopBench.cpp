// The hosted-façade hop from a C++ client (doc/hosted-facade-draft.md §17.10), bench only and not wired into any
// suite: the same open-loop publish of an 18-byte ProbeMarker, timed from its intended send until this process sees
// its own frame sequenced, through either
//
//   direct: app::Application against the member's driver, as a C++ application runs today; or
//   hosted: the Java proxy (ClientProxy) in schema 218 over ProxyProtocol's four aeron:ipc
//           streams and its counters, as a stub would. ProxyProtocol.java is the transport; this is its only other reader.
//
//   hop_bench <direct|hosted> --member M --rate R/s --count N [--client-id C] [--egress-port P] [--aeron-dir D]
//
// The first 20% of the markers are warm-up and are not reported. Transport constants below are ProxyProtocol.java's.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "Aeron.h"

#include "org_limitless_seqeron_sbe_hosted/Attach.h"
#include "org_limitless_seqeron_sbe_hosted/Attached.h"
#include "org_limitless_seqeron_sbe_hosted/Detach.h"
#include "org_limitless_seqeron_sbe_hosted/Fenced.h"
#include "org_limitless_seqeron_sbe_hosted/Gate.h"
#include "org_limitless_seqeron_sbe_hosted/MessageHeader.h"
#include "org_limitless_seqeron_sbe_hosted/Payload.h"
#include "org_limitless_seqeron_sbe_hosted/Publish.h"

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace protocol = org::limitless::seqeron::protocol;
namespace util = org::limitless::seqeron::util;
namespace wire = org::limitless::seqeron::sbe::hosted;

constexpr std::int32_t SOURCE_ID = 16;
constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;
constexpr std::uint16_t PROBE_SCHEMA_ID = 214;
constexpr std::uint16_t MARKER_BLOCK_LENGTH = 8;
constexpr std::size_t MESSAGE_HEADER_LENGTH = 8;
constexpr std::uint16_t MARKER_LENGTH = MESSAGE_HEADER_LENGTH + MARKER_BLOCK_LENGTH + sizeof(std::uint16_t);
// Set in the seqNo of every marker, as HostedTestApplicationClient does, to tell them from other sources'. Bits 24-39
// salt it per run, since a replica replays the queue's history, earlier runs' markers included; the index is below.
constexpr std::int64_t OWN_TAG = 1LL << 40;
constexpr int SALT_SHIFT = 24;
constexpr std::int64_t INDEX_MASK = (1LL << SALT_SHIFT) - 1;
constexpr std::int64_t TIMEOUT_NS = 90'000'000'000LL;
constexpr std::int64_t START_DELAY_NS = 200'000'000LL;
constexpr std::int64_t ATTACH_RETRY_NS = 500'000'000LL;

// ProxyProtocol.java.
constexpr const char* HOST_NAME = "hosted";
constexpr std::uint16_t PROTOCOL_VERSION = 1;
constexpr std::uint16_t HOSTED_SCHEMA_ID = 218;
constexpr const char* ATTACH_REQUEST_CHANNEL = "aeron:ipc?term-length=64k";
constexpr const char* ATTACH_REPLY_CHANNEL = "aeron:ipc?tether=false";
constexpr const char* EVENTS_CHANNEL = "aeron:ipc?session-id=";
constexpr const char* COMMANDS_CHANNEL = "aeron:ipc?term-length=1m";
constexpr std::int32_t ATTACH_REQUEST_STREAM = 301;
constexpr std::int32_t ATTACH_REPLY_STREAM = 302;
constexpr std::int32_t EVENTS_STREAM = 303;
constexpr std::int32_t COMMANDS_STREAM = 304;
constexpr std::int32_t CLIENT_HEARTBEAT_TYPE_ID = 5300;
constexpr std::int32_t CLIENT_APPLIED_TYPE_ID = 5301;
constexpr std::int64_t HOST_TIMEOUT_NS = 3'000'000'000LL;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class Options
{
  public:
    Options(const int argc, char** argv)
    {
        for (int i = 0; i + 1 < argc; i += 2)
        {
            std::string name = argv[i];
            name.erase(0, name.find_first_not_of('-'));
            m_values[name] = argv[i + 1];
        }
    }

    [[nodiscard]] std::int64_t integer(const std::string& name, const std::int64_t fallback) const
    {
        const auto it = m_values.find(name);
        return it == m_values.end() ? fallback : std::stoll(it->second);
    }

    [[nodiscard]] std::string string(const std::string& name, const std::string& fallback) const
    {
        const auto it = m_values.find(name);
        return it == m_values.end() ? fallback : it->second;
    }

  private:
    std::map<std::string, std::string> m_values;
};

// One run's open-loop schedule and the latencies it produced; marker i is sent at t0 + i * interval.
struct Run
{
    Run(const std::int64_t rate, const std::int64_t count) :
      count{ count },
      interval{ 1'000'000'000LL / rate },
      salt{ ((nowNs() / 1'000'000'000LL) & 0xFFFF) << SALT_SHIFT },
      latency(static_cast<std::size_t>(count), -1)
    {}

    [[nodiscard]] std::int64_t due(const std::int64_t index) const
    {
        return t0 + index * interval;
    }

    void received(const std::int64_t seqNo, const std::int64_t now)
    {
        const std::int64_t index = (seqNo & INDEX_MASK) - 1;
        if ((seqNo & (OWN_TAG - 1) & ~INDEX_MASK) == salt && index >= 0 && index < sent &&
            latency[static_cast<std::size_t>(index)] < 0)
        {
            latency[static_cast<std::size_t>(index)] = now - due(index);
            seen++;
        }
    }

    [[nodiscard]] bool done(const std::int64_t now) const
    {
        return seen >= count || (t0 != 0 && now > t0 + TIMEOUT_NS + count * interval);
    }

    std::int64_t count;
    std::int64_t interval;
    std::int64_t salt;
    std::int64_t t0 = 0;
    std::int64_t sent = 0;
    std::int64_t seen = 0;
    std::int64_t declines = 0;
    std::vector<std::int64_t> latency;
};

class Marker
{
  public:
    Marker()
    {
        const std::array<std::uint16_t, 4> header{ MARKER_BLOCK_LENGTH, PROBE_MARKER_TEMPLATE_ID, PROBE_SCHEMA_ID, 0 };
        std::memcpy(m_bytes.data(), header.data(), sizeof header);
    }

    // The payload for marker number index (from 1) of a run, its own MessageHeader first.
    const std::uint8_t* encode(const Run& run, const std::int64_t index)
    {
        const std::int64_t seqNo = OWN_TAG | run.salt | index;
        std::memcpy(m_bytes.data() + MESSAGE_HEADER_LENGTH, &seqNo, sizeof seqNo);
        return m_bytes.data();
    }

  private:
    std::array<std::uint8_t, MARKER_LENGTH> m_bytes{};
};

void report(const char* arm, const Run& run, const std::int64_t rate)
{
    const auto warm = static_cast<std::size_t>(std::max<std::int64_t>(2000, run.count / 5));
    std::vector<std::int64_t> samples;
    for (std::size_t i = warm; i < run.latency.size(); i++)
    {
        if (run.latency[i] >= 0)
        {
            samples.push_back(run.latency[i]);
        }
    }
    if (samples.empty())
    {
        std::printf("arm=%s rate=%lld: no samples (seen %lld of %lld)\n", arm, static_cast<long long>(rate),
                    static_cast<long long>(run.seen), static_cast<long long>(run.count));
        return;
    }
    std::sort(samples.begin(), samples.end());
    const auto at = [&](const double q) {
        return static_cast<double>(samples[static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1))]) /
               1000.0;
    };
    std::printf("arm=%s rate=%lld n=%zu seen=%lld declines=%lld p50=%.1f p99=%.1f p99.9=%.1f max=%.1f (us)\n", arm,
                static_cast<long long>(rate), samples.size(), static_cast<long long>(run.seen),
                static_cast<long long>(run.declines), at(0.5), at(0.99), at(0.999), at(1.0));
}

std::shared_ptr<aeron::Aeron> connect(const Options& options, const std::int32_t member)
{
    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", util::resolveAeronDir("SEQERON_AERON_DIR", member)));
    return aeron::Aeron::connect(context);
}

// ── direct ───────────────────────────────────────────────────────────────────────────────────────────────────

struct DirectListener
{
    void onLeadershipChanged(const bool) const
    {}

    void onSequenced(const app::Payload& payload) const
    {
        if (payload.sourceId() != SOURCE_ID || payload.payloadId() != PROBE_PAYLOAD_ID ||
            payload.templateId() != PROBE_MARKER_TEMPLATE_ID)
        {
            return;
        }
        std::int64_t seqNo = 0;
        std::memcpy(&seqNo, payload.body(), sizeof seqNo);
        if ((seqNo & OWN_TAG) != 0)
        {
            run->received(seqNo, nowNs());
        }
    }

    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        fault = std::string(app::clusterErrorName(fence)) + ": " + detail;
    }

    Run* run = nullptr;
    std::string fault;
};

int direct(const Options& options, const std::int64_t rate, const std::int64_t count)
{
    const auto member = static_cast<std::int32_t>(options.integer("member", 0));
    const std::shared_ptr<aeron::Aeron> aeron = connect(options, member);
    Run run{ rate, count };
    DirectListener listener;
    listener.run = &run;
    app::Application<DirectListener> application{
        { .sourceId = SOURCE_ID,
          .clientId = static_cast<std::int32_t>(options.integer("client-id", 11)),
          .memberId = member,
          .egressChannel = "aeron:udp?endpoint=localhost:" + std::to_string(options.integer("egress-port", 9230)) },
        listener
    };
    application.start(aeron);
    Marker marker;
    auto idle = util::resolveIdleStrategy();
    while (!run.done(nowNs()) && listener.fault.empty())
    {
        int work = application.doWork();
        const std::int64_t now = nowNs();
        if (run.t0 == 0)
        {
            if (application.canPublish())
            {
                run.t0 = now + START_DELAY_NS;
            }
        }
        else if (run.sent < count && now >= run.due(run.sent) && application.canPublish())
        {
            const protocol::Publish outcome =
                application.publish(PROBE_PAYLOAD_ID, marker.encode(run, run.sent + 1), MARKER_LENGTH);
            if (outcome == protocol::Publish::Published)
            {
                run.sent++;
                work++;
            }
            else if (outcome == protocol::Publish::Refused)
            {
                throw std::logic_error("a ProbeMarker the sequencer would reject");
            }
            else
            {
                run.declines++;
            }
        }
        idle.idle(work);
    }
    if (!listener.fault.empty())
    {
        std::printf("FENCED %s\n", listener.fault.c_str());
        return 1;
    }
    report("direct", run, rate);
    return 0;
}

// ── hosted ───────────────────────────────────────────────────────────────────────────────────────────────────

class HostedClient
{
  public:
    HostedClient(const std::shared_ptr<aeron::Aeron>& aeron, Run& run) :
      m_aeron{ aeron },
      m_run{ run },
      m_clientId{ aeron->nextCorrelationId() }
    {
        m_heartbeat = counter(CLIENT_HEARTBEAT_TYPE_ID, "seqeron.client.heartbeat hop_bench");
        m_applied = counter(CLIENT_APPLIED_TYPE_ID, "seqeron.client.applied hop_bench");
        const std::int64_t commandsId = aeron->addExclusivePublication(COMMANDS_CHANNEL, COMMANDS_STREAM);
        const std::int64_t requestsId = aeron->addPublication(ATTACH_REQUEST_CHANNEL, ATTACH_REQUEST_STREAM);
        const std::int64_t repliesId = aeron->addSubscription(ATTACH_REPLY_CHANNEL, ATTACH_REPLY_STREAM);
        while (!(m_commands = aeron->findExclusivePublication(commandsId)))
        {
            std::this_thread::yield();
        }
        while (!(m_requests = aeron->findPublication(requestsId)))
        {
            std::this_thread::yield();
        }
        while (!(m_replies = aeron->findSubscription(repliesId)))
        {
            std::this_thread::yield();
        }
    }

    // H-8: resend the request until the host answers it; a reply for another client or request is ignored (H-15).
    bool attach()
    {
        const std::int64_t deadline = nowNs() + TIMEOUT_NS;
        std::int64_t nextNs = 0;
        while (m_eventSessionId < 0 && !m_refused && nowNs() < deadline)
        {
            beat();
            m_replies->poll(m_onReply, 16);
            const std::int64_t now = nowNs();
            if (now >= nextNs)
            {
                sendAttach();
                nextNs = now + ATTACH_RETRY_NS;
            }
        }
        if (m_eventSessionId < 0)
        {
            return false;
        }
        const std::int64_t eventsId =
            m_aeron->addSubscription(EVENTS_CHANNEL + std::to_string(m_eventSessionId), EVENTS_STREAM);
        while (!(m_events = m_aeron->findSubscription(eventsId)))
        {
            std::this_thread::yield();
        }
        m_replies.reset();
        return true;
    }

    int work()
    {
        beat();
        return m_events->poll(m_onEvent, 16);
    }

    [[nodiscard]] bool canPublish() const
    {
        return m_canPublish;
    }

    // False while the window is full or the stream is back-pressured; the caller retries. The windowed length is the
    // encoded message, header included, which is what the host retires.
    bool publish(const std::uint8_t* payload)
    {
        constexpr std::int64_t length =
            MESSAGE_HEADER_LENGTH + wire::Publish::sbeBlockLength() + sizeof(std::uint32_t) + MARKER_LENGTH;
        if (m_window > 0 &&
            m_offeredBytes + length - m_aeron->countersReader().getCounterValue(m_retiredCounterId) > m_window)
        {
            m_windowDeclines++;
            return false;
        }
        wire::Publish publish;
        publish.wrapAndApplyHeader(out(), 0, m_out.size())
            .epoch(m_epoch)
            .connectionId(0)
            .groupMask(0)
            .payloadId(PROBE_PAYLOAD_ID)
            .putBody(reinterpret_cast<const char*>(payload), MARKER_LENGTH);
        if (m_commands->offer(buffer(), 0,
                              static_cast<aeron::util::index_t>(MESSAGE_HEADER_LENGTH + publish.encodedLength())) < 0)
        {
            return false;
        }
        m_offeredBytes += length;
        return true;
    }

    void detach()
    {
        wire::Detach detach;
        detach.wrapAndApplyHeader(out(), 0, m_out.size()).clientId(m_clientId);
        m_requests->offer(buffer(), 0,
                          static_cast<aeron::util::index_t>(MESSAGE_HEADER_LENGTH + detach.encodedLength()));
    }

    [[nodiscard]] bool fenced() const
    {
        return m_fenced;
    }

    [[nodiscard]] bool hostLost()
    {
        const std::int64_t now = nowNs();
        const auto& reader = m_aeron->countersReader();
        if (reader.getCounterState(m_hostHeartbeatId) != aeron::concurrent::CountersReader::RECORD_ALLOCATED ||
            reader.getCounterRegistrationId(m_hostHeartbeatId) != m_hostHeartbeatRegistrationId)
        {
            return true;
        }
        const std::int64_t value = reader.getCounterValue(m_hostHeartbeatId);
        if (value != m_hostHeartbeatValue)
        {
            m_hostHeartbeatValue = value;
            m_hostChangeNs = now;
        }
        return now - m_hostChangeNs > HOST_TIMEOUT_NS;
    }

    [[nodiscard]] bool refused() const
    {
        return m_refused;
    }

    [[nodiscard]] std::int64_t windowDeclines() const
    {
        return m_windowDeclines;
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
        m_heartbeat->setOrdered(nowNs() / 1'000'000);
    }

    char* out()
    {
        return reinterpret_cast<char*>(m_out.data());
    }

    aeron::concurrent::AtomicBuffer buffer()
    {
        return aeron::concurrent::AtomicBuffer(m_out.data(), m_out.size());
    }

    void sendAttach()
    {
        wire::Attach attach;
        attach.wrapAndApplyHeader(out(), 0, m_out.size())
            .clientId(m_clientId)
            .requestId(m_requestId)
            .kind(wire::Kind::APPLICATION)
            .protocolVersion(PROTOCOL_VERSION)
            .commandSessionId(m_commands->sessionId())
            .heartbeatCounterId(m_heartbeat->id())
            .heartbeatRegistrationId(m_heartbeat->registrationId())
            .appliedCounterId(m_applied->id())
            .appliedRegistrationId(m_applied->registrationId())
            .formatVersion(1)
            .supportedFormatsCount(1)
            .next()
            .formatVersion(1);
        attach.putHostName(std::string{ HOST_NAME }).putAeronVersion(std::string{ aeron_version_text() });
        m_requests->offer(buffer(), 0,
                          static_cast<aeron::util::index_t>(MESSAGE_HEADER_LENGTH + attach.encodedLength()));
    }

    void onReply(const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
                 const aeron::util::index_t length)
    {
        char* base = reinterpret_cast<char*>(buf.buffer()) + offset;
        wire::MessageHeader header(base, static_cast<std::uint64_t>(length));
        if (header.schemaId() != HOSTED_SCHEMA_ID || header.templateId() != wire::Attached::sbeTemplateId())
        {
            return;
        }
        wire::Attached attached(base, MESSAGE_HEADER_LENGTH, length, header.blockLength(), header.version());
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
            m_refused = true;
            return;
        }
        m_retiredCounterId = attached.retiredCounterId();
        m_window = attached.commandWindow();
        m_hostHeartbeatId = attached.heartbeatCounterId();
        m_hostHeartbeatRegistrationId = attached.heartbeatRegistrationId();
        m_hostHeartbeatValue = m_aeron->countersReader().getCounterValue(m_hostHeartbeatId);
        m_hostChangeNs = nowNs();
        m_eventSessionId = attached.eventSessionId();
    }

    void onEvent(const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
                 const aeron::util::index_t length)
    {
        char* base = reinterpret_cast<char*>(buf.buffer()) + offset;
        wire::MessageHeader header(base, static_cast<std::uint64_t>(length));
        switch (header.templateId())
        {
            case wire::Payload::sbeTemplateId(): {
                wire::Payload payload(base, MESSAGE_HEADER_LENGTH, length, header.blockLength(), header.version());
                m_applied->setOrdered(payload.position());
                if (payload.sourceId() == SOURCE_ID && payload.payloadId() == PROBE_PAYLOAD_ID)
                {
                    // The body carries its own MessageHeader: templateId at 2, then the marker's seqNo.
                    const std::string_view body = payload.getBodyAsStringView();
                    if (body.size() >= MARKER_LENGTH)
                    {
                        std::uint16_t templateId;
                        std::int64_t seqNo;
                        std::memcpy(&templateId, body.data() + 2, sizeof templateId);
                        std::memcpy(&seqNo, body.data() + MESSAGE_HEADER_LENGTH, sizeof seqNo);
                        if (templateId == PROBE_MARKER_TEMPLATE_ID && (seqNo & OWN_TAG) != 0)
                        {
                            m_run.received(seqNo, nowNs());
                        }
                    }
                }
                break;
            }
            case wire::Gate::sbeTemplateId(): {
                wire::Gate gate(base, MESSAGE_HEADER_LENGTH, length, header.blockLength(), header.version());
                m_epoch = gate.epoch();
                m_canPublish = gate.canPublish() != 0;
                break;
            }
            case wire::Fenced::sbeTemplateId():
                m_fenced = true;
                break;
            default:
                break;
        }
    }

    std::shared_ptr<aeron::Aeron> m_aeron;
    Run& m_run;
    std::int64_t m_clientId;
    std::int64_t m_requestId = 1;
    std::shared_ptr<aeron::Counter> m_heartbeat;
    std::shared_ptr<aeron::Counter> m_applied;
    std::shared_ptr<aeron::Subscription> m_replies;
    std::shared_ptr<aeron::Subscription> m_events;
    std::shared_ptr<aeron::Publication> m_requests;
    std::shared_ptr<aeron::ExclusivePublication> m_commands;
    alignas(16) std::array<std::uint8_t, 256> m_out{};
    const aeron::fragment_handler_t m_onReply =
        [this](const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
               const aeron::util::index_t length, const aeron::Header&) { onReply(buf, offset, length); };
    const aeron::fragment_handler_t m_onEvent =
        [this](const aeron::concurrent::AtomicBuffer& buf, const aeron::util::index_t offset,
               const aeron::util::index_t length, const aeron::Header&) { onEvent(buf, offset, length); };
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
    bool m_canPublish = false;
    bool m_fenced = false;
    bool m_refused = false;
};

int hosted(const Options& options, const std::int64_t rate, const std::int64_t count)
{
    const auto member = static_cast<std::int32_t>(options.integer("member", 0));
    const std::shared_ptr<aeron::Aeron> aeron = connect(options, member);
    Run run{ rate, count };
    HostedClient client{ aeron, run };
    if (!client.attach())
    {
        std::printf(client.refused() ? "attach refused\n" : "no ATTACHED within %lld s\n",
                    static_cast<long long>(TIMEOUT_NS / 1'000'000'000LL));
        return 3;
    }
    Marker marker;
    auto idle = util::resolveIdleStrategy();
    while (!run.done(nowNs()) && !client.fenced() && !client.hostLost())
    {
        int work = client.work();
        const std::int64_t now = nowNs();
        if (run.t0 == 0)
        {
            if (client.canPublish())
            {
                run.t0 = now + START_DELAY_NS;
            }
        }
        else if (run.sent < count && now >= run.due(run.sent) && client.canPublish())
        {
            if (client.publish(marker.encode(run, run.sent + 1)))
            {
                run.sent++;
                work++;
            }
        }
        idle.idle(work);
    }
    client.detach();
    if (client.fenced() || client.hostLost())
    {
        std::printf(client.fenced() ? "FENCED\n" : "HOST_LOST\n");
        return 1;
    }
    report("hosted", run, rate);
    std::printf("window declines=%lld\n", static_cast<long long>(client.windowDeclines()));
    return 0;
}

} // namespace

int main(const int argc, char** argv)
{
    if (argc < 2)
    {
        std::fprintf(stderr, "usage: hop_bench <direct|hosted> --member M --rate R --count N [--client-id C]\n");
        return 2;
    }
    const std::string mode = argv[1];
    const Options options(argc - 2, argv + 2);
    try
    {
        const std::int64_t rate = options.integer("rate", 5000);
        const std::int64_t count = options.integer("count", 30000);
        if (mode == "direct")
        {
            return direct(options, rate, count);
        }
        if (mode == "hosted")
        {
            return hosted(options, rate, count);
        }
        std::fprintf(stderr, "unknown mode %s\n", mode.c_str());
        return 2;
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "%s failed: %s\n", mode.c_str(), ex.what());
        return 1;
    }
}
