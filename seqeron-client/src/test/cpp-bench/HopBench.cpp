// The hosted-façade hop from a C++ client (doc/hosted-facade-draft.md §17.10), bench only and not wired into any
// suite: the same open-loop publish of an 18-byte ProbeMarker, timed from its intended send until this process sees
// its own frame sequenced, through either
//
//   direct: app::Application against the member's driver, as a C++ application runs today; or
//   hosted: the prototype Java host (HostedTestApplicationHost) over HostedWire's two aeron:ipc streams, byte for
//           byte, as a stub would. HostedWire.java is the layout; this is its only other reader.
//
//   hop_bench <direct|hosted> --member M --rate R/s --count N [--client-id C] [--egress-port P] [--aeron-dir D]
//
// The first 20% of the markers are warm-up and are not reported. Wire constants below are copied from HostedWire.java.

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

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace protocol = org::limitless::seqeron::protocol;
namespace util = org::limitless::seqeron::util;

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
constexpr std::int64_t HEARTBEAT_NS = 100'000'000LL;
constexpr std::int64_t ATTACH_RETRY_NS = 500'000'000LL;

// HostedWire.java.
constexpr std::int32_t EVENTS_STREAM = 301;
constexpr std::int32_t COMMANDS_STREAM = 302;
constexpr std::uint8_t ATTACHED = 1;
constexpr std::uint8_t PAYLOAD = 2;
constexpr std::uint8_t GATE = 3;
constexpr std::uint8_t FENCED = 8;
constexpr std::uint8_t VERDICT = 9;
constexpr std::uint8_t ATTACH = 21;
constexpr std::uint8_t PUBLISH = 22;
constexpr std::uint8_t CLIENT_HEARTBEAT = 25;
constexpr std::uint8_t DETACH = 26;

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
    HostedClient(const std::shared_ptr<aeron::Aeron>& aeron, const std::int32_t clientId, Run& run) :
      m_run{ run },
      m_clientId{ clientId }
    {
        const std::int64_t subId = aeron->addSubscription("aeron:ipc", EVENTS_STREAM);
        const std::int64_t pubId = aeron->addExclusivePublication("aeron:ipc", COMMANDS_STREAM);
        while (!(m_events = aeron->findSubscription(subId)))
        {
            std::this_thread::yield();
        }
        while (!(m_commands = aeron->findExclusivePublication(pubId)))
        {
            std::this_thread::yield();
        }
        std::random_device device;
        m_nonce = (static_cast<std::int64_t>(device()) << 32) | device();
    }

    bool attach()
    {
        const std::int64_t deadline = nowNs() + TIMEOUT_NS;
        std::int64_t nextNs = 0;
        while (!m_attached && nowNs() < deadline)
        {
            m_events->poll(m_onEvent, 16);
            const std::int64_t now = nowNs();
            if (now >= nextNs)
            {
                header(ATTACH, 0);
                put<std::int64_t>(16, m_nonce);
                put<std::int32_t>(24, m_clientId);
                put<std::int32_t>(28, 1);
                send(32);
                nextNs = now + ATTACH_RETRY_NS;
            }
        }
        return m_attached;
    }

    int work()
    {
        const int polled = m_events->poll(m_onEvent, 16);
        const std::int64_t now = nowNs();
        if (now >= m_nextHeartbeatNs)
        {
            header(CLIENT_HEARTBEAT, m_attachId);
            put<std::int64_t>(16, now);
            put<std::int64_t>(24, m_lastApplied);
            send(32);
            m_nextHeartbeatNs = now + HEARTBEAT_NS;
        }
        return polled;
    }

    [[nodiscard]] bool canPublish() const
    {
        return m_canPublish;
    }

    // False while the stream is back-pressured; the caller retries.
    bool publish(const std::uint8_t* payload)
    {
        header(PUBLISH, m_attachId);
        put<std::int64_t>(16, ++m_correlationId);
        put<std::int64_t>(24, m_epoch);
        put<std::int32_t>(32, PROBE_PAYLOAD_ID);
        put<std::int32_t>(36, MARKER_LENGTH);
        std::memcpy(m_out.data() + 40, payload, MARKER_LENGTH);
        return send(40 + MARKER_LENGTH);
    }

    void detach()
    {
        header(DETACH, m_attachId);
        send(16);
    }

    [[nodiscard]] bool fenced() const
    {
        return m_fenced;
    }

  private:
    template<typename T>
    void put(const std::size_t offset, const T value)
    {
        std::memcpy(m_out.data() + offset, &value, sizeof value);
    }

    template<typename T>
    static T get(const std::uint8_t* base, const std::size_t offset)
    {
        T value;
        std::memcpy(&value, base + offset, sizeof value);
        return value;
    }

    void header(const std::uint8_t type, const std::int64_t attachId)
    {
        m_out[0] = type;
        put<std::int64_t>(8, attachId);
    }

    bool send(const std::size_t length)
    {
        aeron::concurrent::AtomicBuffer buffer(m_out.data(), m_out.size());
        return m_commands->offer(buffer, 0, static_cast<aeron::util::index_t>(length)) >= 0;
    }

    void onEvent(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset)
    {
        const std::uint8_t* base = buffer.buffer() + offset;
        const std::uint8_t type = base[0];
        if (type == ATTACHED)
        {
            if (!m_attached && get<std::int64_t>(base, 16) == m_nonce)
            {
                m_attachId = get<std::int64_t>(base, 8);
                m_attached = true;
            }
            return;
        }
        if (!m_attached || get<std::int64_t>(base, 8) != m_attachId)
        {
            return;
        }
        switch (type)
        {
            case PAYLOAD:
                m_lastApplied = get<std::int64_t>(base, 16);
                if (get<std::int32_t>(base, 24) == SOURCE_ID && get<std::int32_t>(base, 28) == PROBE_PAYLOAD_ID &&
                    get<std::int32_t>(base, 32) == PROBE_MARKER_TEMPLATE_ID)
                {
                    const auto seqNo = get<std::int64_t>(base, 40);
                    if ((seqNo & OWN_TAG) != 0)
                    {
                        m_run.received(seqNo, nowNs());
                    }
                }
                break;
            case GATE:
                m_epoch = get<std::int64_t>(base, 16);
                m_canPublish = base[25] != 0;
                break;
            case VERDICT:
                m_run.declines++;
                break;
            case FENCED:
                m_fenced = true;
                break;
            default:
                break;
        }
    }

    Run& m_run;
    std::int32_t m_clientId;
    std::shared_ptr<aeron::Subscription> m_events;
    std::shared_ptr<aeron::ExclusivePublication> m_commands;
    alignas(16) std::array<std::uint8_t, 256> m_out{};
    const aeron::fragment_handler_t m_onEvent = [this](const aeron::concurrent::AtomicBuffer& buffer,
                                                       const aeron::util::index_t offset, const aeron::util::index_t,
                                                       const aeron::Header&) { onEvent(buffer, offset); };
    std::int64_t m_nonce = 0;
    std::int64_t m_attachId = 0;
    std::int64_t m_epoch = 0;
    std::int64_t m_correlationId = 0;
    std::int64_t m_lastApplied = 0;
    std::int64_t m_nextHeartbeatNs = 0;
    bool m_attached = false;
    bool m_canPublish = false;
    bool m_fenced = false;
};

int hosted(const Options& options, const std::int64_t rate, const std::int64_t count)
{
    const auto member = static_cast<std::int32_t>(options.integer("member", 0));
    const std::shared_ptr<aeron::Aeron> aeron = connect(options, member);
    Run run{ rate, count };
    HostedClient client{ aeron, static_cast<std::int32_t>(options.integer("client-id", 11)), run };
    if (!client.attach())
    {
        std::printf("no ATTACHED within %lld s\n", static_cast<long long>(TIMEOUT_NS / 1'000'000'000LL));
        return 3;
    }
    Marker marker;
    auto idle = util::resolveIdleStrategy();
    while (!run.done(nowNs()) && !client.fenced())
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
    if (client.fenced())
    {
        std::printf("FENCED\n");
        return 1;
    }
    report("hosted", run, rate);
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
