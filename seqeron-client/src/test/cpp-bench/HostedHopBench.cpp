// The hosted-façade hop through the real stub (doc/ec2-tests.md, Test 1): an open-loop publish of a ProbeMarker of
// --payload bytes through hosted::Application, timed from its intended send until this process sees its own frame
// sequenced. The direct arm is hop_bench from the tag cpp-direct-client-final; the two print the same line.
//
//   hosted_hop_bench --rate R/s --count N [--member M] [--aeron-dir D] [--payload B] [--warmup-ms W]
//                    [--source-id S] [--host-name H]
//
// The first --warmup-ms of the run (default 20% of it) is discarded. Allocations are counted from the first
// published marker to the last; only operator new is counted, so the Aeron client's own mallocs are not.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "Aeron.h"

#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/hosted/Application.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

std::atomic<std::int64_t> g_allocations{ 0 };

} // namespace

void* operator new(const std::size_t size)
{
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    if (void* pointer = std::malloc(size))
    {
        return pointer;
    }
    throw std::bad_alloc{};
}

void operator delete(void* pointer) noexcept
{
    std::free(pointer);
}

void operator delete(void* pointer, std::size_t) noexcept
{
    std::free(pointer);
}

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace protocol = org::limitless::seqeron::protocol;
namespace util = org::limitless::seqeron::util;

constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;
constexpr std::uint16_t PROBE_SCHEMA_ID = 214;
constexpr std::uint16_t MARKER_BLOCK_LENGTH = 8;
constexpr std::size_t HEADER = 8;
constexpr std::size_t MIN_PAYLOAD = HEADER + MARKER_BLOCK_LENGTH + sizeof(std::uint16_t);
constexpr std::size_t MAX_PAYLOAD = 8875;
// Set in the seqNo of every marker to tell them from other sources'. Bits 24-39 salt it per run, since the host
// replays the queue's history; the index is below.
constexpr std::int64_t OWN_TAG = 1LL << 40;
constexpr int SALT_SHIFT = 24;
constexpr std::int64_t INDEX_MASK = (1LL << SALT_SHIFT) - 1;
constexpr std::int64_t TIMEOUT_NS = 90'000'000'000LL;
constexpr std::int64_t START_DELAY_NS = 200'000'000LL;

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
    explicit Marker(const std::size_t length) : m_bytes(length, 0)
    {
        const std::uint16_t header[4]{ MARKER_BLOCK_LENGTH, PROBE_MARKER_TEMPLATE_ID, PROBE_SCHEMA_ID, 0 };
        std::memcpy(m_bytes.data(), header, sizeof header);
    }

    [[nodiscard]] std::uint16_t length() const
    {
        return static_cast<std::uint16_t>(m_bytes.size());
    }

    // The payload for marker number index (from 1) of a run, its own MessageHeader first.
    const std::uint8_t* encode(const Run& run, const std::int64_t index)
    {
        const std::int64_t seqNo = OWN_TAG | run.salt | index;
        std::memcpy(m_bytes.data() + HEADER, &seqNo, sizeof seqNo);
        return m_bytes.data();
    }

  private:
    std::vector<std::uint8_t> m_bytes;
};

struct Listener
{
    void onLeadershipChanged(const bool) const
    {}

    void onSequenced(const app::Payload& payload) const
    {
        if (payload.sourceId() != sourceId || payload.payloadId() != PROBE_PAYLOAD_ID ||
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
    std::int32_t sourceId = 0;
    std::string fault;
};

void report(const Run& run, const std::int64_t rate, const std::size_t payload, const std::int64_t warmupMs,
            const std::int64_t windowDeclines, const std::int64_t allocations)
{
    const auto warm = std::min<std::size_t>(
        run.latency.size(), warmupMs >= 0 ? static_cast<std::size_t>(warmupMs * 1'000'000LL / run.interval)
                                          : static_cast<std::size_t>(std::max<std::int64_t>(2000, run.count / 5)));
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
        std::printf("arm=hosted rate=%lld: no samples (seen %lld of %lld)\n", static_cast<long long>(rate),
                    static_cast<long long>(run.seen), static_cast<long long>(run.count));
        return;
    }
    std::sort(samples.begin(), samples.end());
    const auto at = [&](const double q) {
        return static_cast<double>(samples[static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1))]) /
               1000.0;
    };
    std::printf("arm=hosted rate=%lld payload=%zu n=%zu seen=%lld declines=%lld window_declines=%lld allocs=%lld "
                "p50=%.1f p99=%.1f p99.9=%.1f max=%.1f (us)\n",
                static_cast<long long>(rate), payload, samples.size(), static_cast<long long>(run.seen),
                static_cast<long long>(run.declines), static_cast<long long>(windowDeclines),
                static_cast<long long>(allocations), at(0.5), at(0.99), at(0.999), at(1.0));
}

int hostedRun(const Options& options)
{
    const std::int64_t rate = options.integer("rate", 5000);
    const std::int64_t count = options.integer("count", 30000);
    const auto member = static_cast<std::int32_t>(options.integer("member", 0));
    const auto payload = static_cast<std::size_t>(options.integer("payload", static_cast<std::int64_t>(MIN_PAYLOAD)));
    if (rate <= 0 || count <= 0 || payload < MIN_PAYLOAD || payload > MAX_PAYLOAD)
    {
        throw std::invalid_argument("rate and count must be positive and payload within 18..8875");
    }

    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", util::resolveAeronDir("SEQERON_AERON_DIR", member)));
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    Run run{ rate, count };
    Listener listener;
    listener.run = &run;
    listener.sourceId = static_cast<std::int32_t>(options.integer("source-id", 16));
    hosted::Application<Listener> application{
        { .sourceId = listener.sourceId, .hostName = options.string("host-name", protocol::PROXY_DEFAULT_HOST_NAME) },
        listener
    };
    application.start(aeron);

    Marker marker{ payload };
    auto idle = util::resolveIdleStrategy();
    std::int64_t allocationsBefore = 0;
    std::int64_t allocations = 0;
    while (!run.done(nowNs()) && listener.fault.empty() && application.failure() == hosted::Failure::None)
    {
        int work = application.doWork();
        const std::int64_t now = nowNs();
        if (run.t0 == 0)
        {
            if (application.canPublish())
            {
                run.t0 = now + START_DELAY_NS;
                allocationsBefore = g_allocations.load(std::memory_order_relaxed);
            }
        }
        else if (run.sent < count && now >= run.due(run.sent) && application.canPublish())
        {
            const protocol::Publish outcome =
                application.publish(PROBE_PAYLOAD_ID, marker.encode(run, run.sent + 1), marker.length());
            if (outcome == protocol::Publish::Published)
            {
                run.sent++;
                work++;
                if (run.sent == count)
                {
                    allocations = g_allocations.load(std::memory_order_relaxed) - allocationsBefore;
                }
            }
            else if (outcome == protocol::Publish::Refused)
            {
                throw std::logic_error("a ProbeMarker the host would refuse");
            }
            else
            {
                run.declines++;
            }
        }
        idle.idle(work);
    }
    const std::int64_t windowDeclines = application.windowDeclines();
    const std::size_t retained = application.retainedCount();
    application.close();
    if (!listener.fault.empty())
    {
        std::printf("FENCED %s\n", listener.fault.c_str());
        return 1;
    }
    switch (application.failure())
    {
        case hosted::Failure::AttachTimedOut:
            std::printf("no ATTACHED within the attach timeout\n");
            return 3;
        case hosted::Failure::AttachRefused:
            std::printf("attach refused\n");
            return 3;
        case hosted::Failure::HostLost:
            std::printf("HOST_LOST\n");
            return 1;
        case hosted::Failure::None:
            break;
    }
    report(run, rate, payload, options.integer("warmup-ms", -1), windowDeclines, allocations);
    std::printf("retained=%zu\n", retained);
    return 0;
}

} // namespace

int main(const int argc, char** argv)
{
    const Options options(argc - 1, argv + 1);
    try
    {
        return hostedRun(options);
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "hosted_hop_bench failed: %s\n", ex.what());
        return 1;
    }
}
