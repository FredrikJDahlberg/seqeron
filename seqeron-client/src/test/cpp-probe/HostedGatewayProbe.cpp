// A hosted::Gateway stub for HostedGatewayStubTest (doc/hosted-facade-draft.md §17.5): it opens connections under the
// ids the stub allocates (H-4), publishes markers on them, checks each comes back under its connection, closes them,
// and with --rounds 2 closes the edge itself and does it again once the host reactivates it.
//
//   hosted_gateway_probe --aeron-dir D [--source-id 9] [--connections 3] [--per-connection 20] [--rounds 1]
//                        [--host-timeout-ms 3000]
//
// Exits 70 when fenced or the host is lost, 78 when the attach is refused and 3 when it times out.

#include <array>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "Aeron.h"

#include "org/limitless/seqeron/hosted/Gateway.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace protocol = org::limitless::seqeron::protocol;
namespace util = org::limitless::seqeron::util;

constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;
constexpr std::uint16_t PROBE_SCHEMA_ID = 214;
constexpr std::size_t HEADER = 8;
constexpr std::size_t MARKER_LENGTH = HEADER + 8 + sizeof(std::uint16_t);
// Set in the seqNo of this client's markers; bits 24-39 salt it per run, since the host replays the queue's history.
constexpr std::int64_t OWN_TAG = 1LL << 40;
constexpr int SALT_SHIFT = 24;
constexpr std::int64_t INDEX_MASK = (1LL << SALT_SHIFT) - 1;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

__attribute__((format(printf, 1, 2))) void log(const char* format, ...)
{
    std::printf("[cppgateway] ");
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
    {
        m_salt = ((nowNs() / 1'000'000'000LL) & 0xFFFF) << SALT_SHIFT;
        const std::array<std::uint16_t, 4> header{ 8, PROBE_MARKER_TEMPLATE_ID, PROBE_SCHEMA_ID, 0 };
        std::memcpy(m_marker.data(), header.data(), sizeof header);
    }

    bool onActivated(const std::int32_t firstConnectionId)
    {
        m_activations++;
        m_firstConnectionId = firstConnectionId;
        log("activated %d first %d", m_activations, firstConnectionId);
        return true;
    }

    void onStandby()
    {
        log("standby");
    }

    void onConnectionOpened(const std::int32_t connectionId, const char* data, const std::size_t length)
    {
        m_openedSeen++;
        log("connection opened %d data %.*s", connectionId, static_cast<int>(length), data);
    }

    void onConnectionClosed(const std::int32_t connectionId)
    {
        m_closedSeen++;
        log("connection closed %d", connectionId);
    }

    void onSequenced(const app::Payload& payload)
    {
        if (payload.sourceId() != m_sourceId || payload.payloadId() != PROBE_PAYLOAD_ID ||
            payload.templateId() != PROBE_MARKER_TEMPLATE_ID)
        {
            return;
        }
        std::int64_t seqNo;
        std::memcpy(&seqNo, payload.body(), sizeof seqNo);
        if ((seqNo & OWN_TAG) == 0 || (seqNo & ~(OWN_TAG | INDEX_MASK)) != m_salt)
        {
            return;
        }
        const auto it = m_expected.find(seqNo & INDEX_MASK);
        if (it == m_expected.end() || it->second != payload.connectionId())
        {
            m_bad++;
            log("BAD CONNECTION for marker %lld: %d", static_cast<long long>(seqNo & INDEX_MASK),
                payload.connectionId());
            return;
        }
        m_expected.erase(it);
        m_sequenced++;
    }

    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        log("FENCED: %s - %s", app::clusterErrorName(fence), detail.c_str());
    }

    std::int32_t activations() const
    {
        return m_activations;
    }

    std::int32_t firstConnectionId() const
    {
        return m_firstConnectionId;
    }

    std::int64_t openedSeen() const
    {
        return m_openedSeen;
    }

    std::int64_t closedSeen() const
    {
        return m_closedSeen;
    }

    std::int64_t sequenced() const
    {
        return m_sequenced;
    }

    bool drained() const
    {
        return m_expected.empty();
    }

    // The next marker, its own MessageHeader first, recorded as belonging to connectionId.
    const std::uint8_t* nextMarker(const std::int32_t connectionId)
    {
        const std::int64_t index = m_published + 1;
        const std::int64_t seqNo = OWN_TAG | m_salt | index;
        std::memcpy(m_marker.data() + HEADER, &seqNo, sizeof seqNo);
        m_pending = { index, connectionId };
        return m_marker.data();
    }

    void published()
    {
        m_published++;
        m_expected[m_pending.first] = m_pending.second;
    }

    void state() const
    {
        log("state: published %lld, sequenced %lld, bad %lld, opened %lld, closed %lld",
            static_cast<long long>(m_published), static_cast<long long>(m_sequenced), static_cast<long long>(m_bad),
            static_cast<long long>(m_openedSeen), static_cast<long long>(m_closedSeen));
    }

  private:
    std::int32_t m_sourceId;
    std::int32_t m_activations = 0;
    std::int32_t m_firstConnectionId = -1;
    std::int64_t m_salt = 0;
    std::int64_t m_published = 0;
    std::int64_t m_sequenced = 0;
    std::int64_t m_bad = 0;
    std::int64_t m_openedSeen = 0;
    std::int64_t m_closedSeen = 0;
    std::map<std::int64_t, std::int32_t> m_expected;
    std::pair<std::int64_t, std::int32_t> m_pending{};
    std::array<std::uint8_t, MARKER_LENGTH> m_marker{};
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

enum class Phase : std::uint8_t
{
    Open,
    Publish,
    Drain,
    Close,
    Closed,
    Reactivate,
    Done,
};

} // namespace

int main(const int argc, char** argv)
{
    const Options options(argc - 1, argv + 1);
    const auto sourceId = static_cast<std::int32_t>(options.integer("source-id", 9));
    const auto connections = static_cast<std::int32_t>(options.integer("connections", 3));
    const auto perConnection = static_cast<std::int32_t>(options.integer("per-connection", 20));
    const auto rounds = static_cast<std::int32_t>(options.integer("rounds", 1));

    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", ""));
    const auto aeron = aeron::Aeron::connect(context);

    Probe probe{ sourceId };
    hosted::Gateway<Probe> gateway{ { .sourceId = sourceId, .hostTimeoutMs = options.integer("host-timeout-ms", 3000) },
                                    probe };
    gateway.start(aeron);

    auto idle = util::resolveIdleStrategy();
    std::vector<std::int32_t> ids;
    std::int32_t round = 1;
    std::int64_t toPublish = 0;
    std::int64_t previousHighest = -1;
    std::int64_t nextStateNs = 0;
    Phase phase = Phase::Open;
    while (gateway.failure() == hosted::Failure::None && !gateway.fenced())
    {
        int work = gateway.doWork();
        switch (phase)
        {
            case Phase::Open:
                if (gateway.canAccept() && probe.activations() == round)
                {
                    const std::string data = "conn-" + std::to_string(ids.size());
                    const std::int32_t id = gateway.openConnection(data.data(), data.size());
                    if (id != hosted::Gateway<Probe>::NO_CONNECTION)
                    {
                        ids.push_back(id);
                        work++;
                    }
                    if (static_cast<std::int32_t>(ids.size()) == connections)
                    {
                        bool consecutive = ids.front() == probe.firstConnectionId() && ids.front() > previousHighest;
                        for (std::size_t i = 1; i < ids.size(); ++i)
                        {
                            consecutive = consecutive && ids[i] == ids[i - 1] + 1;
                        }
                        log(consecutive ? "round %d ids ok from %d" : "round %d BAD IDS from %d", round, ids.front());
                        toPublish = static_cast<std::int64_t>(connections) * perConnection;
                        phase = Phase::Publish;
                    }
                }
                break;
            case Phase::Publish:
                if (toPublish > 0)
                {
                    const std::int32_t id = ids[static_cast<std::size_t>(toPublish) % ids.size()];
                    if (gateway.publish(id, PROBE_PAYLOAD_ID, probe.nextMarker(id), MARKER_LENGTH) ==
                        protocol::Publish::Published)
                    {
                        probe.published();
                        toPublish--;
                        work++;
                    }
                }
                else
                {
                    phase = Phase::Drain;
                }
                break;
            case Phase::Drain:
                if (probe.drained())
                {
                    log("round %d sequenced %lld under their connections", round,
                        static_cast<long long>(probe.sequenced()));
                    phase = Phase::Close;
                }
                break;
            case Phase::Close:
                for (const std::int32_t id : ids)
                {
                    gateway.closeConnection(id);
                }
                phase = Phase::Closed;
                break;
            case Phase::Closed:
                if (probe.closedSeen() == static_cast<std::int64_t>(round) * connections)
                {
                    log("round %d closed", round);
                    if (round == rounds)
                    {
                        log("DONE");
                        phase = Phase::Done;
                    }
                    else
                    {
                        previousHighest = ids.back();
                        ids.clear();
                        round++;
                        gateway.gateClosed();
                        phase = Phase::Reactivate;
                    }
                }
                break;
            case Phase::Reactivate:
                if (probe.activations() == round)
                {
                    phase = Phase::Open;
                }
                break;
            case Phase::Done:
                break;
        }
        if (nowNs() >= nextStateNs)
        {
            probe.state();
            nextStateNs = nowNs() + 2'000'000'000LL;
        }
        idle.idle(work);
    }
    gateway.close();
    switch (gateway.failure())
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
