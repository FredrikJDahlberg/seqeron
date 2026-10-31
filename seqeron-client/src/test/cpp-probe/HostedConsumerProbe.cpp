// A hosted::Consumer stub for HostedConsumerStubTest (doc/hosted-facade-draft.md §17.1): it follows a queue through
// its host and counts the probe markers of one source, checking the order the queue delivers them in.
//
//   hosted_consumer_probe --aeron-dir D [--source-id 16] [--host-timeout-ms 3000]
//
// Exits 70 when fenced or the host is lost, 78 when the attach is refused and 3 when it times out.

#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/hosted/Consumer.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace util = org::limitless::seqeron::util;

constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

__attribute__((format(printf, 1, 2))) void log(const char* format, ...)
{
    std::printf("[cppconsumer] ");
    va_list args;
    va_start(args, format);
    std::vprintf(format, args);
    va_end(args);
    std::printf("\n");
    std::fflush(stdout);
}

class Probe
{
  public:
    explicit Probe(const std::int32_t sourceId) : m_sourceId{ sourceId }
    {}

    void onSequenced(const app::Payload& payload)
    {
        if (payload.clusterSeqNo() <= m_lastClusterSeqNo)
        {
            log("OUT OF ORDER: %lld after %lld", static_cast<long long>(payload.clusterSeqNo()),
                static_cast<long long>(m_lastClusterSeqNo));
        }
        m_lastClusterSeqNo = payload.clusterSeqNo();
        m_payloads++;
        if (payload.sourceId() == m_sourceId && payload.payloadId() == PROBE_PAYLOAD_ID &&
            payload.templateId() == PROBE_MARKER_TEMPLATE_ID)
        {
            m_markers++;
        }
    }

    void onCaughtUp(const std::int64_t clusterSeqNo)
    {
        log("Caught up - following live at clusterSeqNo %lld", static_cast<long long>(clusterSeqNo));
    }

    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        log("FENCED: %s - %s", app::clusterErrorName(fence), detail.c_str());
    }

    void state() const
    {
        log("state: payloads %lld, markers %lld, last %lld", static_cast<long long>(m_payloads),
            static_cast<long long>(m_markers), static_cast<long long>(m_lastClusterSeqNo));
    }

  private:
    std::int32_t m_sourceId;
    std::int64_t m_lastClusterSeqNo = 0;
    std::int64_t m_payloads = 0;
    std::int64_t m_markers = 0;
};

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

} // namespace

int main(const int argc, char** argv)
{
    const Options options(argc - 1, argv + 1);
    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", ""));
    const auto aeron = aeron::Aeron::connect(context);

    Probe probe{ static_cast<std::int32_t>(options.integer("source-id", 16)) };
    hosted::Consumer<Probe> consumer{ { .hostTimeoutMs = options.integer("host-timeout-ms", 3000) }, probe };
    consumer.start(aeron);

    auto idle = util::resolveIdleStrategy();
    std::int64_t nextStateNs = 0;
    while (consumer.failure() == hosted::Failure::None && !consumer.fenced())
    {
        const int work = consumer.doWork();
        if (nowNs() >= nextStateNs)
        {
            probe.state();
            nextStateNs = nowNs() + 1'000'000'000LL;
        }
        idle.idle(work);
    }
    consumer.close();
    switch (consumer.failure())
    {
        case hosted::Failure::AttachTimedOut:
            log("no ATTACHED within the attach timeout");
            return 3;
        case hosted::Failure::AttachRefused:
            log("attach refused");
            return 78;
        case hosted::Failure::HostLost:
            log("HOST_LOST");
            return 70;
        case hosted::Failure::None:
            break;
    }
    return 70;
}
