// The hosted-façade hop through the real stub (doc/ec2-tests.md, Test 1): an open-loop publish of a ProbeMarker of
// --payload bytes through hosted::Application, timed from its intended send until this process sees its own frame
// sequenced. The direct arm is hop_bench from the tag cpp-direct-client-final; the two print the same line.
//
//   hosted_hop_bench --rate R/s --count N [--member M] [--aeron-dir D] [--payload B] [--warmup-ms W]
//                    [--source-id S] [--host-name H] [--flood-ms F] [--settle-ms T]
//
// The first --warmup-ms of the run (default 20% of it) is discarded, and so are the declines before it: declines= and
// window_declines= count the rest (window_declines= being those for want of room in H-5's window, declines= all of
// them), warmup_declines= the declines before. hist= is the reported latencies in nanoseconds as bucket:count pairs,
// 32 buckets to each power of two (hop-analyze.py merges them across runs). Allocations are counted from the first
// published marker to the last; only operator new is counted, so the Aeron client's own mallocs are not.
//
// With --flood-ms the schedule is dropped: markers are offered as fast as the stub takes them for F ms, or until N,
// and the run ends once every one offered is sequenced, or T ms (default 20000) after the last sequenced. It prints
// FLOODING when it starts, then a line of the offered, sequenced and lost markers, any duplicated or out of order, and
// the gate's last edge: how long the gate was shut, and how long after it reopened the last marker offered before it
// shut was sequenced (doc/ec2-tests.md, the gate-edge set).

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
// Set in the seqNo of every marker to tell them from other sources'. Bits 28-39 salt it per run, since the host
// replays the queue's history; the index is below.
constexpr std::int64_t OWN_TAG = 1LL << 40;
constexpr int SALT_SHIFT = 28;
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
      salt{ ((nowNs() / 1'000'000'000LL) & 0xFFF) << SALT_SHIFT },
      latency(static_cast<std::size_t>(count), -1)
    {}

    [[nodiscard]] std::int64_t due(const std::int64_t index) const
    {
        return t0 + index * interval;
    }

    void received(const std::int64_t seqNo, const std::int64_t now)
    {
        const std::int64_t index = (seqNo & INDEX_MASK) - 1;
        if ((seqNo & (OWN_TAG - 1) & ~INDEX_MASK) != salt || index < 0 || index >= sent)
        {
            return;
        }
        if (latency[static_cast<std::size_t>(index)] >= 0)
        {
            duplicates++;
        }
        else
        {
            latency[static_cast<std::size_t>(index)] = now - due(index);
            seen++;
            lastNs = now;
            outOfOrder += index < highest ? 1 : 0;
            highest = std::max(highest, index);
            if (recoveredNs == 0 && reopenedNs > closedNs && index + 1 >= sentAtClose)
            {
                recoveredNs = now;
            }
        }
    }

    [[nodiscard]] bool done(const std::int64_t now) const
    {
        if (floodNs > 0)
        {
            return t0 != 0 && (sent >= count || now > t0 + floodNs) && (seen >= sent || now > lastNs + settleNs);
        }
        return seen >= count || (t0 != 0 && now > t0 + TIMEOUT_NS + count * interval);
    }

    // A gate edge: the gate shut, or reopened after one.
    void gate(const bool leading, const std::int64_t now)
    {
        if (!leading)
        {
            edges++;
            closedNs = now;
            sentAtClose = sent;
            recoveredNs = 0;
        }
        else if (edges > 0)
        {
            reopenedNs = now;
        }
    }

    std::int64_t count;
    std::int64_t interval;
    std::int64_t salt;
    std::int64_t t0 = 0;
    std::int64_t sent = 0;
    std::int64_t seen = 0;
    std::int64_t declines = 0;
    std::int64_t floodNs = 0;
    std::int64_t settleNs = 0;
    std::int64_t lastNs = 0;
    std::int64_t highest = -1;
    std::int64_t duplicates = 0;
    std::int64_t outOfOrder = 0;
    std::int64_t edges = 0;
    std::int64_t closedNs = 0;
    std::int64_t reopenedNs = 0;
    std::int64_t sentAtClose = 0;
    std::int64_t recoveredNs = 0;
    std::int64_t declinesAtWarm = 0;
    std::int64_t windowDeclinesAtWarm = 0;
    std::vector<std::int64_t> latency;
};

// The markers before the first one reported.
std::size_t warmCount(const Run& run, const std::int64_t warmupMs)
{
    return std::min<std::size_t>(run.latency.size(),
                                 warmupMs >= 0 ? static_cast<std::size_t>(warmupMs * 1'000'000LL / run.interval)
                                               : static_cast<std::size_t>(std::max<std::int64_t>(2000, run.count / 5)));
}

// The samples as "bucket:count" pairs, so that runs merge: 32 buckets to each power of two of the nanoseconds (3%
// wide), a bucket's index being (log2 - 4) * 32 + the five bits under the leading one, and below 32 ns the value.
std::string histogram(const std::vector<std::int64_t>& samples)
{
    std::map<int, std::int64_t> buckets;
    for (const std::int64_t ns : samples)
    {
        const auto value = static_cast<std::uint64_t>(ns);
        const int log = value < 32 ? 0 : 63 - __builtin_clzll(value);
        buckets[value < 32 ? static_cast<int>(value) : (log - 4) * 32 + static_cast<int>((value >> (log - 5)) & 31)]++;
    }
    std::string text;
    for (const auto& [bucket, count] : buckets)
    {
        text += (text.empty() ? "" : ",") + std::to_string(bucket) + ":" + std::to_string(count);
    }
    return text;
}

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
    void onLeadershipChanged(const bool leading) const
    {
        if (run != nullptr)
        {
            run->gate(leading, nowNs());
        }
    }

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

void report(const Run& run, const std::int64_t rate, const std::size_t payload, const std::size_t warm,
            const std::int64_t windowDeclines, const std::int64_t allocations)
{
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
    std::printf("arm=hosted rate=%lld payload=%zu n=%zu seen=%lld declines=%lld window_declines=%lld "
                "warmup_declines=%lld allocs=%lld p50=%.1f p99=%.1f p99.9=%.1f max=%.1f (us) hist=%s\n",
                static_cast<long long>(rate), payload, samples.size(), static_cast<long long>(run.seen),
                static_cast<long long>(run.declines - run.declinesAtWarm),
                static_cast<long long>(windowDeclines - run.windowDeclinesAtWarm),
                static_cast<long long>(run.declinesAtWarm), static_cast<long long>(allocations), at(0.5), at(0.99),
                at(0.999), at(1.0), histogram(samples).c_str());
}

// The flood's line: what was offered against what was sequenced, and the gate's edges (times in ms, -1 when none).
void reportFlood(const Run& run, const std::size_t payload, const std::int64_t windowDeclines)
{
    const bool edged = run.edges > 0 && run.reopenedNs > run.closedNs;
    const auto ms = [](const std::int64_t ns) { return static_cast<double>(ns) / 1'000'000.0; };
    std::printf("arm=hosted-edge payload=%zu offered=%lld sequenced=%lld lost=%lld duplicates=%lld out_of_order=%lld "
                "window_declines=%lld edges=%lld gate_shut_ms=%.1f recover_ms=%.1f\n",
                payload, static_cast<long long>(run.sent), static_cast<long long>(run.seen),
                static_cast<long long>(run.sent - run.seen), static_cast<long long>(run.duplicates),
                static_cast<long long>(run.outOfOrder), static_cast<long long>(windowDeclines),
                static_cast<long long>(run.edges), edged ? ms(run.reopenedNs - run.closedNs) : -1.0,
                edged && run.recoveredNs > 0 ? ms(run.recoveredNs - run.reopenedNs) : -1.0);
    std::fflush(stdout);
}

int hostedRun(const Options& options)
{
    const std::int64_t rate = options.integer("rate", 5000);
    const std::int64_t count = options.integer("count", 30000);
    if (count > INDEX_MASK)
    {
        throw std::invalid_argument("count exceeds the marker index");
    }
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
    run.floodNs = options.integer("flood-ms", 0) * 1'000'000LL;
    run.settleNs = options.integer("settle-ms", 20'000) * 1'000'000LL;
    Listener listener;
    listener.run = &run;
    listener.sourceId = static_cast<std::int32_t>(options.integer("source-id", 16));
    hosted::Application<Listener> application{
        { .sourceId = listener.sourceId, .hostName = options.string("host-name", protocol::PROXY_DEFAULT_HOST_NAME) },
        listener
    };
    application.start(aeron);

    Marker marker{ payload };
    const std::size_t warm = warmCount(run, options.integer("warmup-ms", -1));
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
                run.lastNs = run.t0;
                if (run.floodNs > 0)
                {
                    std::printf("FLOODING\n");
                    std::fflush(stdout);
                }
                allocationsBefore = g_allocations.load(std::memory_order_relaxed);
            }
        }
        else if (run.sent < count && now >= run.t0 &&
                 (run.floodNs > 0 ? now < run.t0 + run.floodNs : now >= run.due(run.sent)) && application.canPublish())
        {
            const protocol::Publish outcome =
                application.publish(PROBE_PAYLOAD_ID, marker.encode(run, run.sent + 1), marker.length());
            if (outcome == protocol::Publish::Published)
            {
                run.sent++;
                work++;
                if (run.sent == static_cast<std::int64_t>(warm))
                {
                    run.declinesAtWarm = run.declines;
                    run.windowDeclinesAtWarm = application.windowDeclines();
                }
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
    if (run.floodNs > 0)
    {
        reportFlood(run, payload, windowDeclines);
    }
    else
    {
        report(run, rate, payload, warm, windowDeclines, allocations);
    }
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
