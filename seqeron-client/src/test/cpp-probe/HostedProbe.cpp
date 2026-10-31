// The C++ twin of HostedTestApplicationClient (doc/hosted-facade-draft.md §17): a hosted::Application stub holding
// the probe's state, a count of the markers sequenced under its source and a digest of their clusterSeqNos, for the
// prototype host to snapshot. HostedCppStubTest drives it against a cluster and the host; nothing else runs it.
//
//   hosted_probe --aeron-dir D [--source-id 16] [--interval-ms 100] [--flood 1] [--max-markers N]
//                [--host-timeout-ms 3000] [--host-name hosted] [--check-refusals 1] [--stall-cut-no N] [--slow-cut-ms
//                N]
//                [--corrupt-record 1] [--check-groups 1] [--reply-every N]
//
// Exits 70 when fenced or the host is lost, 78 when the attach is refused and 3 when it times out, as the Java client.

#include <array>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"

#include "org/limitless/seqeron/hosted/Application.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace protocol = org::limitless::seqeron::protocol;
namespace util = org::limitless::seqeron::util;

constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;
constexpr std::uint16_t PROBE_REPLY_TEMPLATE_ID = 2;
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
    std::printf("[cppclient] ");
    va_list args;
    va_start(args, format);
    std::vprintf(format, args);
    va_end(args);
    std::printf("\n");
    std::fflush(stdout);
}

class Probe final : public app::SnapshotListener
{
  public:
    Probe(const std::int32_t sourceId, const std::int64_t stallCutNo, const std::int64_t slowCutMs,
          const bool corruptRecord, const std::int64_t replyEvery) :
      m_sourceId{ sourceId },
      m_replyEvery{ replyEvery },
      m_stallCutNo{ stallCutNo },
      m_slowCutMs{ slowCutMs },
      m_corruptRecord{ corruptRecord }
    {
        m_salt = ((nowNs() / 1'000'000'000LL) & 0xFFFF) << SALT_SHIFT;
        const std::array<std::uint16_t, 4> header{ 8, PROBE_MARKER_TEMPLATE_ID, PROBE_SCHEMA_ID, 0 };
        std::memcpy(m_marker.data(), header.data(), sizeof header);
    }

    void onLeadershipChanged(const bool leading)
    {
        log(leading ? "now leading - submitting markers" : "not leading - silent");
    }

    void onGroupsResolved(const app::GroupDirectory& directory)
    {
        for (std::int32_t groupId = 1; groupId <= protocol::MAX_GROUPS; ++groupId)
        {
            const std::string_view name = directory.name(groupId);
            if (!name.empty())
            {
                log("group %d %.*s mask %u", groupId, static_cast<int>(name.size()), name.data(), directory.mask(name));
            }
        }
    }

    void onSequenced(const app::Payload& payload)
    {
        if (payload.sourceId() != m_sourceId || payload.payloadId() != PROBE_PAYLOAD_ID)
        {
            return;
        }
        std::int64_t seqNo;
        std::memcpy(&seqNo, payload.body(), sizeof seqNo);
        if (payload.templateId() == PROBE_REPLY_TEMPLATE_ID)
        {
            onReply(payload, seqNo);
            return;
        }
        if (payload.templateId() != PROBE_MARKER_TEMPLATE_ID)
        {
            return;
        }
        if ((seqNo & OWN_TAG) != 0 && (seqNo & ~(OWN_TAG | INDEX_MASK)) == m_salt)
        {
            const std::int64_t own = seqNo & INDEX_MASK;
            if (own <= m_lastOwn)
            {
                log("OWN MARKER OUT OF ORDER: %lld after %lld", static_cast<long long>(own),
                    static_cast<long long>(m_lastOwn));
            }
            m_lastOwn = own;
            m_ownSequenced++;
            if (payload.groupMask() != 1)
            {
                log("UNEXPECTED groupMask %u", payload.groupMask());
            }
            if (m_replyEvery > 0 && own % m_replyEvery == 0)
            {
                m_toReply.push_back({ seqNo, payload.requestRef() });
            }
        }
        m_markers++;
        m_digest = m_digest * 31 + static_cast<std::uint64_t>(payload.clusterSeqNo());
    }

    void onCaughtUp(const std::int64_t clusterSeqNo)
    {
        log("Caught up - following live at clusterSeqNo %lld, %llu marker(s) sequenced, digest %016llx",
            static_cast<long long>(clusterSeqNo), static_cast<unsigned long long>(m_markers),
            static_cast<unsigned long long>(m_digest));
    }

    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        log("FENCED: %s - %s", app::clusterErrorName(fence), detail.c_str());
    }

    // The next marker's payload, its own MessageHeader first.
    const std::uint8_t* nextMarker()
    {
        const std::int64_t seqNo = OWN_TAG | m_salt | (m_published + 1);
        std::memcpy(m_marker.data() + HEADER, &seqNo, sizeof seqNo);
        return m_marker.data();
    }

    void published()
    {
        m_published++;
    }

    void state(const std::int64_t declined) const
    {
        log("state: %llu marker(s), digest %016llx, published %lld, declined %lld, own %lld, replied %lld, "
            "replies %lld, bad %lld",
            static_cast<unsigned long long>(m_markers), static_cast<unsigned long long>(m_digest),
            static_cast<long long>(m_published), static_cast<long long>(declined),
            static_cast<long long>(m_ownSequenced), static_cast<long long>(m_replied),
            static_cast<long long>(m_repliesSequenced), static_cast<long long>(m_badReplies));
    }

    // Answers the sequenced markers waiting for a reply, each with the RequestRef it was sequenced under.
    template<typename Application>
    int sendReplies(Application& application)
    {
        int sent = 0;
        while (!m_toReply.empty())
        {
            const auto& [seqNo, ref] = m_toReply.front();
            std::array<std::uint8_t, MARKER_LENGTH> reply = m_marker;
            const std::uint16_t templateId = PROBE_REPLY_TEMPLATE_ID;
            std::memcpy(reply.data() + 2, &templateId, sizeof templateId);
            std::memcpy(reply.data() + HEADER, &seqNo, sizeof seqNo);
            if (application.reply(ref, PROBE_PAYLOAD_ID, reply.data(), MARKER_LENGTH) != protocol::Publish::Published)
            {
                break;
            }
            m_expected[seqNo] = ref;
            m_replied++;
            m_toReply.pop_front();
            sent++;
        }
        return sent;
    }

    [[nodiscard]] std::int64_t publishedCount() const
    {
        return m_published;
    }

    // Called while a cut is slow, as a direct Application's keepAlive() is by a callback that spins.
    void keepAliveWith(std::function<void()> keepAlive)
    {
        m_keepAlive = std::move(keepAlive);
    }

    // One record: the marker count, then the digest (TestApplication's, byte for byte).
    [[nodiscard]] std::uint32_t formatVersion() const override
    {
        return 1;
    }

    std::int32_t onSnapshot(const std::span<std::uint8_t> buffer, const std::int32_t recordIndex) override
    {
        if (recordIndex != 0)
        {
            return 0;
        }
        m_cuts++;
        if (m_cuts == m_stallCutNo)
        {
            log("IN CUT round %lld", static_cast<long long>(m_cuts));
            std::this_thread::sleep_for(std::chrono::hours(24));
        }
        const std::int64_t slowEndNs = nowNs() + m_slowCutMs * 1'000'000LL;
        while (nowNs() < slowEndNs)
        {
            m_keepAlive();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        const std::uint64_t digest = m_corruptRecord ? m_digest + 1 : m_digest;
        std::memcpy(buffer.data(), &m_markers, sizeof m_markers);
        std::memcpy(buffer.data() + sizeof m_markers, &digest, sizeof digest);
        return 2 * sizeof(std::uint64_t);
    }

    void onRestore(const std::span<const std::uint8_t> record, const std::int32_t recordIndex,
                   const std::uint32_t formatVersion) override
    {
        if (recordIndex == 0 && record.size() >= 2 * sizeof(std::uint64_t))
        {
            std::memcpy(&m_markers, record.data(), sizeof m_markers);
            std::memcpy(&m_digest, record.data() + sizeof m_markers, sizeof m_digest);
        }
        log("restored record %d (format %u): %llu marker(s), digest %016llx", recordIndex, formatVersion,
            static_cast<unsigned long long>(m_markers), static_cast<unsigned long long>(m_digest));
    }

  private:
    // A reply must come back under the request's RequestRef (H-11), exactly once.
    void onReply(const app::Payload& payload, const std::int64_t seqNo)
    {
        const auto it = m_expected.find(seqNo);
        if (it == m_expected.end() || it->second.sourceId != payload.sourceId() ||
            it->second.connectionId != payload.connectionId() || it->second.groupMask != payload.groupMask())
        {
            m_badReplies++;
            log("BAD REPLY for %lld", static_cast<long long>(seqNo & INDEX_MASK));
            return;
        }
        m_expected.erase(it);
        m_repliesSequenced++;
    }

    std::int32_t m_sourceId;
    std::int64_t m_replyEvery;
    std::deque<std::pair<std::int64_t, app::RequestRef>> m_toReply;
    std::map<std::int64_t, app::RequestRef> m_expected;
    std::int64_t m_replied = 0;
    std::int64_t m_repliesSequenced = 0;
    std::int64_t m_badReplies = 0;
    std::int64_t m_stallCutNo;
    std::int64_t m_slowCutMs;
    bool m_corruptRecord;
    std::function<void()> m_keepAlive = [] {};
    std::int64_t m_cuts = 0;
    std::int64_t m_salt = 0;
    std::int64_t m_lastOwn = 0;
    std::int64_t m_ownSequenced = 0;
    std::int64_t m_published = 0;
    std::uint64_t m_markers = 0;
    std::uint64_t m_digest = 0;
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

} // namespace

int main(const int argc, char** argv)
{
    const Options options(argc - 1, argv + 1);
    const auto sourceId = static_cast<std::int32_t>(options.integer("source-id", 16));
    const std::int64_t intervalNs = options.integer("interval-ms", 100) * 1'000'000LL;
    const bool flood = options.integer("flood", 0) != 0;
    const std::int64_t maxMarkers = options.integer("max-markers", 0);
    bool checkRefusals = options.integer("check-refusals", 0) != 0;
    bool checkGroups = options.integer("check-groups", 0) != 0;
    const std::vector<std::uint8_t> oversized(9000);

    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", ""));
    const auto aeron = aeron::Aeron::connect(context);

    Probe probe{ sourceId, options.integer("stall-cut-no", 0), options.integer("slow-cut-ms", 0),
                 options.integer("corrupt-record", 0) != 0, options.integer("reply-every", 0) };
    hosted::Application<Probe> application{
        { .sourceId = sourceId,
          .snapshots = &probe,
          .hostTimeoutMs = options.integer("host-timeout-ms", 3000),
          .hostName = options.string("host-name", org::limitless::seqeron::protocol::PROXY_DEFAULT_HOST_NAME) },
        probe
    };
    application.start(aeron);
    probe.keepAliveWith([&application] { application.keepAlive(); });

    auto idle = util::resolveIdleStrategy();
    std::int64_t nextMarkerNs = 0;
    std::int64_t nextStateNs = 0;
    while (application.failure() == hosted::Failure::None && !application.fenced())
    {
        int work = application.doWork();
        const std::int64_t now = nowNs();
        if (checkRefusals && application.canPublish())
        {
            checkRefusals = false;
            if (application.publish(1, probe.nextMarker(), MARKER_LENGTH) == protocol::Publish::Refused)
            {
                log("REFUSED retired payloadId");
            }
            if (application.publish(PROBE_PAYLOAD_ID, oversized.data(), 8875) == protocol::Publish::Refused)
            {
                log("REFUSED oversized payload");
            }
        }
        if (checkGroups && application.canPublish())
        {
            checkGroups = false;
            if (application.publish(0x2, PROBE_PAYLOAD_ID, probe.nextMarker(), MARKER_LENGTH) ==
                protocol::Publish::Refused)
            {
                log("REFUSED unknown group");
            }
            if (application.publish(0x1, PROBE_PAYLOAD_ID, probe.nextMarker(), MARKER_LENGTH) ==
                protocol::Publish::Published)
            {
                probe.published();
                log("published to the own group by mask");
            }
        }
        work += probe.sendReplies(application);
        if (application.canPublish() && (maxMarkers == 0 || probe.publishedCount() < maxMarkers) &&
            (flood || now >= nextMarkerNs) &&
            application.publish(PROBE_PAYLOAD_ID, probe.nextMarker(), MARKER_LENGTH) == protocol::Publish::Published)
        {
            probe.published();
            nextMarkerNs = now + intervalNs;
            work++;
        }
        if (now >= nextStateNs)
        {
            probe.state(application.windowDeclines());
            nextStateNs = now + 2'000'000'000LL;
        }
        idle.idle(work);
    }
    application.close();
    switch (application.failure())
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
