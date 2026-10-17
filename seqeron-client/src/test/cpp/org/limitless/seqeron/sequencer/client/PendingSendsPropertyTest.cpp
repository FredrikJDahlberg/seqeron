// PendingSends driving a producer against a model cluster: a leader that appends ingress stamped with its own
// term and drops the rest, commits a prefix, and loses everything uncommitted at an election. After one, the
// old leader either still takes ingress and drops it, or is gone and a send spins until egress brings the
// NewLeader — giving the send up if the hold is on, as ClusterStreamSender does. The sender may reconnect on a
// new session then. The cluster also closes the producer's session at random, and drops what it sends on it
// until the producer replaces it; a close an election loses, the next leader makes again. The queue lags, a
// sibling shares the producer's sourceId, and term ids skip, as after a failed ballot.
//
// Checked once the faults stop and everything has drained: the log holds every frame the producer sent
// exactly once, in send order, and nothing faulted. Seeds are fixed; the failing one is in the test name. The
// twin of PendingSendsPropertyTest.java — keep the two in step.

#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/helpers/SessionClosedEvent.hpp"
#include "org/limitless/seqeron/helpers/SplitMix64.hpp"
#include "org/limitless/seqeron/sequencer/client/PendingSends.hpp"

#include "org_limitless_seqeron_sbe_frame/Unsequenced.h"

namespace org::limitless::seqeron::sequencer::client {
namespace {

namespace frm = sbe::frame;

constexpr std::size_t CAPACITY = 64;
constexpr int STEPS = 5'000;
constexpr int QUIESCE_ROUNDS = 100;
constexpr std::int64_t SIBLING = 1'000'000;
constexpr std::int64_t LEADERSHIP_CHANGED = -1;
constexpr std::int64_t SESSION_CLOSED = -2;
constexpr std::uint16_t PAYLOAD_ID = 6;
constexpr int ALL = std::numeric_limits<int>::max();

// One log entry: frame value from session, a term's LeadershipChanged, or a session's close.
struct Entry
{
    std::int64_t session;
    std::int64_t value;
};

struct Model
{
    // The cluster as a sender sees it.
    struct Sender
    {
        bool send(const std::uint8_t* bytes, std::uint16_t)
        {
            if (model.senderTerm != model.clusterTerm && !model.oldLeaderTakesIngress)
            {
                model.newLeader(); // from inside the spin, as pollEgress would
                if (model.pending.isHolding())
                {
                    return false;
                }
            }
            if (model.senderTerm == model.clusterTerm && !model.closedAtCluster)
            {
                std::int64_t value = 0;
                std::memcpy(&value, bytes + protocol::MIN_INGRESS_LENGTH, sizeof(value));
                model.uncommitted.push_back({ model.session, value });
            }
            return true;
        }

        [[nodiscard]] std::int64_t clusterSessionId() const
        {
            return model.session;
        }

        [[nodiscard]] std::int64_t leadershipTermId() const
        {
            return model.senderTerm;
        }

        Model& model;
    };

    explicit Model(const std::uint64_t seed) : rng(seed)
    {}

    int roll(const int bound)
    {
        return rng.roll(bound);
    }

    void send()
    {
        if (pending.isHolding() || pending.isFull())
        {
            return;
        }
        frm::Unsequenced encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(frame.data()), 0, frame.size());
        encoder.header().sourceId(1).connectionId(2).sessionId(session).payloadId(PAYLOAD_ID);
        encoder.putPayload(reinterpret_cast<const char*>(&next), sizeof(next));
        const auto length = static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + encoder.encodedLength());
        if (sender.send(frame.data(), length))
        {
            pending.track(frame.data(), length, sender.clusterSessionId(), sender.leadershipTermId());
            ++next;
        }
    }

    void newLeader()
    {
        senderTerm = clusterTerm;
        if (roll(2) == 0)
        {
            ++session;
            closedAtCluster = false;
        }
        pending.onNewLeader(senderTerm);
    }

    // Everything uncommitted is lost but the closes, which the new leader makes again.
    void elect()
    {
        std::vector<Entry> closes;
        for (const Entry& entry : uncommitted)
        {
            if (entry.session == SESSION_CLOSED)
            {
                closes.push_back(entry);
            }
        }
        uncommitted.clear();
        clusterTerm += 1 + roll(2);
        queue.push_back({ LEADERSHIP_CHANGED, clusterTerm });
        uncommitted.insert(uncommitted.end(), closes.begin(), closes.end());
        oldLeaderTakesIngress = roll(2) == 0;
    }

    // The producer opens a new session in the current term, and resends in the same duty cycle, as Session does.
    void replaceSession()
    {
        ++session;
        senderTerm = clusterTerm;
        closedAtCluster = false;
        resent += pending.resendMissing(sender);
    }

    void commit(const int count)
    {
        for (int k = count; k > 0 && !uncommitted.empty(); --k)
        {
            if (uncommitted.front().session != SESSION_CLOSED)
            {
                log.push_back(uncommitted.front().value);
            }
            queue.push_back(uncommitted.front());
            uncommitted.pop_front();
        }
    }

    void deliver(const int count)
    {
        for (int k = count; k > 0 && !queue.empty(); --k)
        {
            const Entry entry = queue.front();
            queue.pop_front();
            if (entry.session == LEADERSHIP_CHANGED)
            {
                pending.onLeadershipChanged(entry.value);
                continue;
            }
            if (entry.session == SESSION_CLOSED)
            {
                pending.onSequenced(helpers::SessionClosedEvent{ entry.value }.event);
                continue;
            }
            protocol::SequencedEvent event{};
            event.sourceSessionId = entry.session;
            event.payloadId = PAYLOAD_ID;
            event.payload = reinterpret_cast<const char*>(&entry.value);
            event.payloadLength = sizeof(entry.value);
            pending.onSequenced(event);
        }
    }

    helpers::SplitMix64 rng;
    PendingSends pending{ CAPACITY };
    Sender sender{ *this };
    std::array<std::uint8_t, protocol::MAX_INGRESS_LENGTH> frame{};
    std::deque<Entry> uncommitted; // accepted by the current leader, not yet committed
    std::deque<Entry> queue;       // committed, not yet delivered to the producer's queue
    std::vector<std::int64_t> log; // the producer's frames in log order
    std::int64_t clusterTerm = 0;
    std::int64_t senderTerm = 0;
    std::int64_t session = 1;
    bool oldLeaderTakesIngress = false;
    bool closedAtCluster = false; // the cluster has closed session, and the producer has not replaced it yet
    std::int64_t next = 0;
    std::size_t resent = 0;
};

class PendingSendsProperty : public testing::TestWithParam<std::uint64_t>
{};

TEST_P(PendingSendsProperty, LogsEveryFrameOnceInOrderAcrossFailovers)
{
    Model model(GetParam());
    for (int step = 0; step < STEPS; ++step)
    {
        const int action = model.roll(100);
        if (action < 30)
        {
            model.send();
        }
        else if (action < 50)
        {
            model.commit(model.roll(4));
        }
        else if (action < 55)
        {
            model.queue.push_back({ SIBLING, model.roll(8) });
        }
        else if (action < 80)
        {
            model.deliver(model.roll(4));
        }
        else if (action < 85)
        {
            model.resent += model.pending.resendMissing(model.sender);
        }
        else if (action < 87)
        {
            model.elect();
        }
        else if (action < 88)
        {
            if (!model.closedAtCluster)
            {
                model.closedAtCluster = true;
                model.uncommitted.push_back({ SESSION_CLOSED, model.session });
            }
        }
        else if (action < 91)
        {
            if (model.closedAtCluster)
            {
                model.replaceSession();
            }
        }
        else if (model.senderTerm != model.clusterTerm)
        {
            model.newLeader();
        }
    }
    for (int round = 0; round < QUIESCE_ROUNDS && model.pending.size() > 0; ++round)
    {
        if (model.closedAtCluster)
        {
            model.replaceSession();
        }
        if (model.senderTerm != model.clusterTerm)
        {
            model.newLeader();
        }
        model.resent += model.pending.resendMissing(model.sender);
        model.commit(ALL);
        model.deliver(ALL);
    }

    EXPECT_FALSE(model.pending.isFaulted());
    EXPECT_EQ(0u, model.pending.size()) << "everything sent came back";
    EXPECT_GT(model.resent, 0u) << "the run lost and resent something";
    std::vector<std::int64_t> expected(static_cast<std::size_t>(model.next));
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(expected, model.log);
}

INSTANTIATE_TEST_SUITE_P(Seeds, PendingSendsProperty,
                         testing::Values(1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597));

} // namespace
} // namespace org::limitless::seqeron::sequencer::client
