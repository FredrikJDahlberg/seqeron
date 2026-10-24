// The C++ client tier against a live cluster: the twin of tools/ClusterProbe's confirm and follow modes, speaking the
// same ProbeMarker (payloadId 5, sourceId 8) and printing the same verdict lines, so cpp-client-test.sh judges it as
// failover-test.sh judges the Java one.
//
//   confirm: catches up on the co-located queue, then streams --count markers through publishPayload — tracked by
//            PendingSends unless --untracked — and exits 0 iff its own queue showed 1..count exactly once, in order.
//            --colocated connects through the member's IPC ingress first, falling back to UDP.
//   follow:  cold-starts a ReplayerStreamReceiver, follows the queue live for --live-seconds after catching up, and
//            exits 0 iff it dispatched every frame from groupSeqNo 1 with no gap.
//   scan:    replays the co-located archive's recording of the queue with ClusterStreamClient, from the recording's
//            start, and exits 0 iff it reaches the recording's position at connect; it reports the first frame.
//
// Both read the queue of --group, or else the default queue (spec §3.1), and address their frames to it.
//
// ProbeMarker is encoded by hand: sbe-probe.xml (schema 214) generates no C++ codecs. The egress port is explicit,
// since ClusterStreamSender sends its egress channel to the cluster as given rather than resolving an ephemeral one.

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "Aeron.h"
#include "org_limitless_seqeron_sbe_replay/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/QueueRequest.h"
#include "org_limitless_seqeron_sbe_replay/QueueResolved.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"

#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/protocol/ReplayProtocol.hpp"
#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/sequencer/client/ClusterStreamClient.hpp"
#include "org/limitless/seqeron/sequencer/client/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressPublisher.hpp"
#include "org/limitless/seqeron/sequencer/client/PendingSends.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"

namespace {

namespace client = org::limitless::seqeron::sequencer::client;
namespace protocol = org::limitless::seqeron::protocol;
namespace replayer = org::limitless::seqeron::replayer::client;
namespace replay = org::limitless::seqeron::sbe::replay;
namespace util = org::limitless::seqeron::util;

constexpr util::Component CLUSTER_PROBE{ "ClusterProbe" };

constexpr std::uint16_t PROBE_PAYLOAD_ID = 5;
constexpr std::int32_t PROBE_SOURCE_ID = 8;
constexpr std::int32_t NO_CONNECTION = -1;
constexpr std::int64_t QUEUE_REQUEST_RESEND_MS = 500;

// ProbeMarker's MessageHeader, then seqNo uint64, then filler as uint16-length var data.
constexpr std::uint16_t PROBE_MARKER_TEMPLATE_ID = 1;
constexpr std::uint16_t PROBE_SCHEMA_ID = 214;
constexpr std::uint16_t PROBE_MARKER_BLOCK_LENGTH = sizeof(std::int64_t);
constexpr std::size_t MESSAGE_HEADER_LENGTH = 8;
constexpr std::uint16_t PROBE_MARKER_LENGTH = MESSAGE_HEADER_LENGTH + PROBE_MARKER_BLOCK_LENGTH + sizeof(std::uint16_t);

constexpr std::size_t PENDING_CAPACITY = 4096;
constexpr std::int64_t IPC_CONNECT_TIMEOUT_MS = 2'000;
constexpr std::int64_t DRAIN_TIMEOUT_MS = 15'000;
constexpr std::int64_t SCAN_TIMEOUT_MS = 15'000;

std::int64_t monotonicMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::int64_t monotonicUs()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class Options
{
  public:
    Options(const int argc, char** argv)
    {
        for (int i = 0; i < argc; i++)
        {
            std::string name = argv[i];
            name.erase(0, name.find_first_not_of('-'));
            if (i + 1 < argc && std::strncmp(argv[i + 1], "--", 2) != 0)
            {
                m_values[name] = argv[++i];
            }
            else
            {
                m_flags.insert(name);
            }
        }
    }

    [[nodiscard]] bool flag(const std::string& name) const
    {
        return m_flags.contains(name);
    }

    [[nodiscard]] std::int32_t integer(const std::string& name, const std::int32_t fallback) const
    {
        const auto it = m_values.find(name);
        return it == m_values.end() ? fallback : std::stoi(it->second);
    }

    [[nodiscard]] std::string string(const std::string& name, const std::string& fallback) const
    {
        const auto it = m_values.find(name);
        return it == m_values.end() ? fallback : it->second;
    }

    [[nodiscard]] std::int32_t memberId() const
    {
        return integer("member", 0);
    }

    [[nodiscard]] std::int32_t clientId() const
    {
        return integer("client-id", 9);
    }

  private:
    std::map<std::string, std::string> m_values;
    std::set<std::string> m_flags;
};

// One ProbeMarker with no filler, written in place, in host order: little-endian on every supported target.
class ProbeMarker
{
  public:
    ProbeMarker()
    {
        const std::array<std::uint16_t, 4> header{ PROBE_MARKER_BLOCK_LENGTH, PROBE_MARKER_TEMPLATE_ID, PROBE_SCHEMA_ID,
                                                   0 };
        std::memcpy(m_payload.data(), header.data(), sizeof header);
    }

    const std::uint8_t* encode(const std::int64_t seqNo)
    {
        std::memcpy(m_payload.data() + MESSAGE_HEADER_LENGTH, &seqNo, sizeof seqNo);
        return m_payload.data();
    }

  private:
    alignas(8) std::array<std::uint8_t, PROBE_MARKER_LENGTH> m_payload{};
};

// The seqNos this process's own sessions put on the queue, in queue order.
class OwnFrames
{
  public:
    void onSequenced(const protocol::SequencedEvent& event)
    {
        if (event.system || event.payloadId != PROBE_PAYLOAD_ID || event.templateId != PROBE_MARKER_TEMPLATE_ID ||
            !sessions.contains(event.sourceSessionId))
        {
            return;
        }
        std::int64_t seqNo = 0;
        std::memcpy(&seqNo, event.payload + MESSAGE_HEADER_LENGTH, sizeof seqNo);
        seqNos.push_back(seqNo);
    }

    [[nodiscard]] std::int64_t last() const
    {
        return seqNos.empty() ? 0 : seqNos.back();
    }

    // 0 if the queue showed 1..count exactly once, in order; logs the tally either way.
    [[nodiscard]] int judge(const std::int32_t count, const std::size_t resent) const
    {
        const std::unordered_set<std::int64_t> distinct(seqNos.begin(), seqNos.end());
        std::size_t outOfOrder = 0;
        for (std::size_t i = 1; i < seqNos.size(); i++)
        {
            if (seqNos[i] <= seqNos[i - 1])
            {
                outOfOrder++;
            }
        }
        const auto missing = static_cast<long long>(count) - static_cast<long long>(distinct.size());
        const std::size_t duplicated = seqNos.size() - distinct.size();
        const bool exact = missing == 0 && duplicated == 0 && outOfOrder == 0;
        util::Logger::info(CLUSTER_PROBE,
                           "confirm: %s — queue showed %zu of %d: missing %lld, duplicated %zu, out of order %zu; "
                           "resent %zu",
                           exact ? "EXACT" : "NOT EXACT", seqNos.size(), count, missing, duplicated, outOfOrder,
                           resent);
        return exact ? 0 : 1;
    }

    std::unordered_set<std::int64_t> sessions;
    std::vector<std::int64_t> seqNos;
};

std::shared_ptr<aeron::Aeron> connect(const Options& options)
{
    aeron::Context context;
    context.aeronDir(options.string("aeron-dir", util::resolveAeronDir("SEQERON_AERON_DIR", options.memberId())));
    return aeron::Aeron::connect(context);
}

// This client's own egress endpoint on its member's host.
// --group, or else the default queue's group, which the node's Replayer names in answer to a QueueRequest naming no
// source (R-6); waits until it does.
std::int32_t groupId(const std::shared_ptr<aeron::Aeron>& aeron, const Options& options)
{
    const std::int32_t configured = options.integer("group", 0);
    if (configured != 0)
    {
        return configured;
    }
    const std::int64_t clientId = aeron->nextCorrelationId();
    std::int64_t requestId = 0;
    std::int32_t resolved = 0;
    const std::int64_t controlId =
        aeron->addSubscription(replayer::REPLAYER_CONTROL_CHANNEL, protocol::REPLAYER_CONTROL_STREAM_ID);
    const std::int64_t requestsId =
        aeron->addPublication(protocol::REPLAYER_IPC_CHANNEL, protocol::REPLAYER_REQUEST_STREAM_ID);
    std::shared_ptr<aeron::Subscription> control;
    std::shared_ptr<aeron::Publication> requests;
    const aeron::fragment_handler_t onControl = [&](const aeron::concurrent::AtomicBuffer& buffer,
                                                    const aeron::util::index_t offset,
                                                    const aeron::util::index_t length, const aeron::Header&) {
        char* const message = reinterpret_cast<char*>(buffer.buffer()) + offset;
        const auto bufferLength = static_cast<std::uint64_t>(length);
        replay::MessageHeader header;
        header.wrap(message, 0, 0, bufferLength);
        if (header.schemaId() != replay::MessageHeader::sbeSchemaId() ||
            header.templateId() != replay::QueueResolved::sbeTemplateId())
        {
            return;
        }
        replay::QueueResolved answer;
        answer.wrapForDecode(message, replay::MessageHeader::encodedLength(), header.blockLength(), header.version(),
                             bufferLength);
        if (answer.clientId() == clientId && answer.requestId() == requestId)
        {
            resolved = answer.queueId();
        }
    };
    auto idle = util::resolveIdleStrategy();
    std::int64_t lastRequestMs = monotonicMs() - QUEUE_REQUEST_RESEND_MS;
    while (resolved == 0)
    {
        control = control ? control : aeron->findSubscription(controlId);
        requests = requests ? requests : aeron->findPublication(requestsId);
        if (requests && requests->isConnected() && monotonicMs() - lastRequestMs >= QUEUE_REQUEST_RESEND_MS)
        {
            alignas(16) std::array<std::uint8_t, 64> buffer{};
            replay::QueueRequest request;
            request.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
            request.clientId(clientId).requestId(++requestId).sourceId(protocol::REPLAYER_NO_SOURCE_ID);
            aeron::concurrent::AtomicBuffer atomic(buffer.data(), buffer.size());
            requests->offer(
                atomic, 0,
                static_cast<aeron::util::index_t>(replay::MessageHeader::encodedLength() + request.encodedLength()));
            lastRequestMs = monotonicMs();
        }
        idle.idle(control ? control->poll(onControl, 10) : 0);
    }
    util::Logger::info(CLUSTER_PROBE, "reads group %d's queue, the default", resolved);
    return resolved;
}

// The probe is in no group: its frames address the queue it reads.
std::uint16_t groupMask(const std::int32_t groupId)
{
    return static_cast<std::uint16_t>(1U << (groupId - 1));
}

std::string egressChannel(const Options& options)
{
    const auto& hosts = protocol::clusterHosts();
    const auto member = static_cast<std::size_t>(options.memberId());
    const std::string host = member < hosts.size() ? hosts[member] : "localhost";
    const std::int32_t port = options.integer("egress-port", 0);
    if (port == 0)
    {
        throw std::invalid_argument("--egress-port is required: the cluster answers on the port given");
    }
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(port);
}

int confirm(const Options& options)
{
    const bool tracked = !options.flag("untracked");
    const std::int32_t count = options.integer("count", 1000);
    const std::int64_t pacingMicros = options.integer("pacing-micros", 0);
    client::PendingSends pending(PENDING_CAPACITY);
    OwnFrames own;
    bool caughtUp = false;
    auto idle = util::resolveIdleStrategy();

    const std::shared_ptr<aeron::Aeron> aeron = connect(options);
    const std::int32_t group = groupId(aeron, options);
    client::ClusterStreamSender sender;
    replayer::ReplayerStreamReceiver receiver(
        options.clientId(),
        [&](const protocol::SequencedEvent& event) {
            if (tracked)
            {
                pending.onSequenced(event);
            }
            own.onSequenced(event);
        },
        {}, {}, [&](std::int32_t, const std::int64_t term, std::int64_t) { pending.onLeadershipChanged(term); },
        [&] { caughtUp = true; });
    receiver.start(aeron, options.memberId(), group);
    while (!caughtUp)
    {
        idle.idle(receiver.poll());
    }
    if (tracked)
    {
        sender.setIngressHold(&pending);
    }
    if (options.flag("colocated"))
    {
        sender.connectColocated(aeron, options.memberId(), IPC_CONNECT_TIMEOUT_MS, egressChannel(options));
    }
    else
    {
        sender.connect(aeron, egressChannel(options));
    }
    ProbeMarker marker;
    util::Logger::info(CLUSTER_PROBE, "confirm: sending %d frame(s)%s", count,
                       tracked ? " through PendingSends" : " untracked (control)");

    std::int64_t next = 1;
    std::int64_t nextSendUs = 0;
    std::int64_t lastProgressMs = monotonicMs();
    std::size_t resent = 0;
    while (true)
    {
        const std::int64_t nowMs = monotonicMs();
        const std::size_t seen = own.seqNos.size();
        const int work = receiver.poll();
        sender.pollEgress([](const std::uint8_t*, std::int32_t) {});
        sender.keepAlive();
        if (sender.isSessionLost() || pending.isFaulted())
        {
            util::Logger::error(CLUSTER_PROBE, util::eventCode::ClusterSessionError, "confirm: %s",
                                pending.isFaulted() ? "PendingSends faulted" : "cluster session lost");
            return 1;
        }
        const std::size_t resentNow = tracked ? pending.resendMissing(sender) : 0;
        if (resentNow > 0)
        {
            resent += resentNow;
            own.sessions.insert(sender.clusterSessionId());
        }
        const std::int64_t nowUs = monotonicUs();
        if (next <= count && nowUs >= nextSendUs)
        {
            const protocol::Publish outcome =
                client::publishPayload(sender, tracked ? &pending : nullptr, PROBE_SOURCE_ID, NO_CONNECTION,
                                       PROBE_PAYLOAD_ID, groupMask(group), marker.encode(next), PROBE_MARKER_LENGTH);
            if (outcome == protocol::Publish::Refused)
            {
                throw std::logic_error("a ProbeMarker the sequencer would reject");
            }
            if (outcome == protocol::Publish::Published)
            {
                own.sessions.insert(sender.clusterSessionId());
                next++;
                nextSendUs = nowUs + pacingMicros;
                lastProgressMs = nowMs;
            }
        }
        if (resentNow > 0 || own.seqNos.size() > seen)
        {
            lastProgressMs = nowMs;
        }
        const bool drained = next > count && (tracked ? pending.size() == 0 : own.last() == count);
        if (drained || nowMs - lastProgressMs > DRAIN_TIMEOUT_MS)
        {
            break;
        }
        idle.idle(work + static_cast<int>(resentNow));
    }
    sender.close();
    receiver.close();
    return own.judge(count, resent);
}

int follow(const Options& options)
{
    const std::int64_t liveMs = options.integer("live-seconds", 5) * 1000LL;
    std::int64_t first = 0;
    std::int64_t last = 0;
    std::int64_t gaps = 0;
    bool caughtUp = false;
    auto idle = util::resolveIdleStrategy();

    const std::shared_ptr<aeron::Aeron> aeron = connect(options);
    // Every frame: with their own callbacks empty, the connection events and LeadershipChanged arrive here too.
    replayer::ReplayerStreamReceiver receiver(
        options.clientId(),
        [&](const protocol::SequencedEvent& event) {
            if (first == 0)
            {
                first = event.groupSeqNo;
            }
            else if (event.groupSeqNo != last + 1)
            {
                gaps++;
            }
            last = event.groupSeqNo;
        },
        {}, {}, {},
        [&] {
            if (!caughtUp)
            {
                util::Logger::info(CLUSTER_PROBE, "Caught up at groupSeqNo %lld — following live",
                                   static_cast<long long>(last));
            }
            caughtUp = true;
        });
    receiver.start(aeron, options.memberId(), groupId(aeron, options));
    util::Logger::info(CLUSTER_PROBE, "Following the node queue — replaying history via the Replayer, then live");
    std::int64_t deadlineMs = INT64_MAX;
    while (monotonicMs() < deadlineMs)
    {
        if (caughtUp && deadlineMs == INT64_MAX)
        {
            deadlineMs = monotonicMs() + liveMs;
        }
        idle.idle(receiver.poll());
    }
    receiver.close();
    const bool exact = first == 1 && gaps == 0 && caughtUp;
    util::Logger::info(CLUSTER_PROBE, "follow: %s — dispatched groupSeqNo %lld to %lld, gaps %lld",
                       exact ? "CONTIGUOUS" : "NOT CONTIGUOUS", static_cast<long long>(first),
                       static_cast<long long>(last), static_cast<long long>(gaps));
    return exact ? 0 : 1;
}

int scan(const Options& options)
{
    std::int64_t firstGroupSeqNo = 0;
    std::int64_t firstPosition = -1;
    bool firstIsCut = false;
    bool ended = false;
    auto idle = util::resolveIdleStrategy();

    const std::shared_ptr<aeron::Aeron> aeron = connect(options);
    std::int64_t recordingId = -1;
    std::int64_t catchUpPosition = 0;
    const auto archive =
        client::connectLocalArchive(aeron, protocol::ARCHIVE_CONTROL_STREAM_ID, groupId(aeron, options),
                                    "[ClusterProbe]", recordingId, catchUpPosition);
    client::ClusterStreamClient stream(
        [&](const protocol::SequencedEvent& event) {
            if (firstPosition < 0)
            {
                firstGroupSeqNo = event.groupSeqNo;
                firstPosition = event.position;
                firstIsCut = event.system && event.systemEventType == protocol::SNAPSHOT_STARTED;
            }
        },
        {}, {}, {}, [&] { ended = true; });
    stream.start(aeron, archive, recordingId, catchUpPosition);
    const std::int64_t deadlineMs = monotonicMs() + SCAN_TIMEOUT_MS;
    while (!stream.isCaughtUp() && !ended && monotonicMs() < deadlineMs)
    {
        idle.idle(stream.poll());
    }
    const bool caughtUp = stream.isCaughtUp();
    util::Logger::info(CLUSTER_PROBE,
                       "scan: %s — recording %lld to position %lld, first frame groupSeqNo %lld at position %lld%s",
                       caughtUp ? "CAUGHT UP" : "NOT CAUGHT UP", static_cast<long long>(recordingId),
                       static_cast<long long>(catchUpPosition), static_cast<long long>(firstGroupSeqNo),
                       static_cast<long long>(firstPosition), firstIsCut ? ", a cut" : "");
    return caughtUp ? 0 : 1;
}

int usage()
{
    std::fputs("usage: cluster_probe confirm|follow|scan --member <id> --client-id <id> [--aeron-dir <dir>] "
               "[--group <id>]\n"
               "         confirm: --egress-port <port> [--count <n>] [--pacing-micros <us>] [--untracked] "
               "[--colocated]\n"
               "         follow:  [--live-seconds <s>]\n",
               stderr);
    return 1;
}

} // namespace

int main(const int argc, char** argv)
{
    if (argc < 2)
    {
        return usage();
    }
    const std::string mode = argv[1];
    const Options options(argc - 2, argv + 2);
    try
    {
        if (mode == "confirm")
        {
            return confirm(options);
        }
        if (mode == "follow")
        {
            return follow(options);
        }
        if (mode == "scan")
        {
            return scan(options);
        }
        return usage();
    }
    catch (const std::exception& ex)
    {
        util::Logger::error(CLUSTER_PROBE, util::eventCode::ClusterSessionError, "%s failed: %s", mode.c_str(),
                            ex.what());
        return 1;
    }
}
