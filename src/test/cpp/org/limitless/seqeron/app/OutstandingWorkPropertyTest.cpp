// LeaderGate and OutstandingWork on three simulated replicas sharing one log, with a seeded generator
// interleaving requests, replica lag and batched polls, recovery, elections, failed reply offers and replies
// lost in an election. The twin of OutstandingWorkPropertyTest.java — keep the two in step.
//
// Safety, checked on every step: a replica's outstanding set matches the log prefix it has applied; only a
// caught-up replica that sees itself as leader dispatches; within one gate opening a request is dispatched
// at most once unless its offer failed; a sweep dispatches in globalSeqNo order; and a request this opening
// dispatched is never silently lost — its reply is pending, in flight or in the log, or a leadership change
// this replica has yet to act on will close the gate.
//
// Liveness: once the faults stop, every request has a reply in the log and every replica's outstanding set
// is empty. Replies may appear more than once; that is the at-least-once contract.
//
// Seeds are fixed; the failing one is in the test name.

#include <algorithm>
#include <cstdint>
#include <deque>
#include <random>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/LeaderGate.hpp"
#include "org/limitless/seqeron/app/OutstandingWork.hpp"

namespace org::limitless::seqeron::app {
namespace {

constexpr int REPLICAS = 3;
// Long enough that a gate which ignores a flip away and back fails several of the seeds below.
constexpr int CHAOS_STEPS = 10'000;
constexpr int QUIESCE_ROUNDS = 100;

enum class Kind : std::uint8_t
{
    Request,
    Reply,
    Leader,
};

struct Entry
{
    Kind kind;
    std::int64_t value;
};

class Simulation;

struct Replica
{
    Replica(Simulation& simulation, std::int32_t memberId) :
      simulation{ simulation },
      memberId{ memberId },
      gate{ memberId }
    {}

    void applyNext();
    void applyAll();
    void dutyCycle();
    void offer();
    void checkNoLostDispatch() const;
    bool isQuiet() const;

    Simulation& simulation;
    std::int32_t memberId;
    LeaderGate gate;
    OutstandingWork<std::int64_t> work;
    std::deque<std::int64_t> pendingReplies; // dispatched, reply not yet offered
    std::unordered_set<std::int64_t> tenure; // the model of what this gate opening has dispatched
    std::size_t applied = 0;
    std::int32_t viewLeader = -1;
    bool recovering = false;
    bool leadershipApplied = false; // a LeadershipChanged applied since the last duty cycle
};

class Simulation
{
  public:
    explicit Simulation(const std::uint64_t seed) : rng(seed)
    {
        for (std::int32_t member = 0; member < REPLICAS; ++member)
        {
            replicas.emplace_back(*this, member);
        }
        append({ Kind::Leader, leader });
    }

    int roll(const int bound)
    {
        return static_cast<int>(rng() % static_cast<std::uint64_t>(bound));
    }

    void append(const Entry entry)
    {
        log.push_back(entry);
        if (entry.kind == Kind::Request)
        {
            open.insert(entry.value);
        }
        else if (entry.kind == Kind::Reply)
        {
            open.erase(entry.value);
            replied.insert(entry.value);
        }
        outstandingAfter.push_back(open.size());
    }

    std::size_t outstandingAt(const std::size_t length) const
    {
        return length == 0 ? 0 : outstandingAfter[length - 1];
    }

    void deliver()
    {
        const std::int64_t key = inFlight.front();
        inFlight.erase(inFlight.begin());
        append({ Kind::Reply, key });
    }

    void elect()
    {
        leader = (leader + 1 + roll(REPLICAS - 1)) % REPLICAS;
        lastLeaderIndex = log.size();
        append({ Kind::Leader, leader });
        if (faults)
        {
            std::erase_if(inFlight, [&](std::int64_t) { return roll(2) == 0; }); // uncommitted on the old leader
        }
    }

    void chaosStep()
    {
        Replica& replica = replicas[static_cast<std::size_t>(roll(REPLICAS))];
        const int r = roll(100);
        if (r < 20)
        {
            append({ Kind::Request, static_cast<std::int64_t>(log.size()) + 1 });
        }
        else if (r < 45)
        {
            replica.applyNext();
        }
        else if (r < 50)
        {
            replica.applyAll();
        }
        else if (r < 70)
        {
            replica.dutyCycle();
        }
        else if (r < 80)
        {
            replica.offer();
        }
        else if (r < 90)
        {
            if (!inFlight.empty())
            {
                deliver();
            }
        }
        else if (r < 93)
        {
            elect();
        }
        else
        {
            replica.recovering = !replica.recovering;
        }
    }

    bool isQuiet() const
    {
        if (!inFlight.empty())
        {
            return false;
        }
        for (const Replica& replica : replicas)
        {
            if (!replica.isQuiet())
            {
                return false;
            }
        }
        return true;
    }

    std::mt19937_64 rng;
    bool faults = true;
    std::int32_t leader = 0;
    std::size_t lastLeaderIndex = 0;
    std::vector<Entry> log;
    std::unordered_set<std::int64_t> open;     // the outstanding set after the whole log
    std::vector<std::size_t> outstandingAfter; // its size after each entry
    std::unordered_set<std::int64_t> replied;  // every key with a reply in the log, applied or not
    std::vector<std::int64_t> inFlight;        // replies offered to the cluster and not yet in the log
    std::deque<Replica> replicas;
};

void Replica::applyNext()
{
    if (applied == simulation.log.size())
    {
        return;
    }
    const Entry entry = simulation.log[applied++];
    switch (entry.kind)
    {
        case Kind::Request:
            work.onRequest(entry.value, entry.value);
            break;
        case Kind::Reply:
            work.onReply(entry.value);
            tenure.erase(entry.value);
            break;
        case Kind::Leader:
            viewLeader = static_cast<std::int32_t>(entry.value);
            gate.onLeadershipChanged();
            leadershipApplied = true;
            break;
    }
    EXPECT_EQ(simulation.outstandingAt(applied), work.size()) << "member " << memberId << " at log index " << applied;
}

// One poll that delivers everything behind this replica.
void Replica::applyAll()
{
    while (applied < simulation.log.size())
    {
        applyNext();
    }
}

void Replica::dutyCycle()
{
    if (gate.update(!recovering, viewLeader) == LeaderGate::Transition::Closed)
    {
        work.onNotLeader();
        tenure.clear();
    }
    leadershipApplied = false;
    if (!gate.isOpen())
    {
        return;
    }
    int slots = simulation.faults ? simulation.roll(3) : 1'000'000'000;
    std::int64_t lastInSweep = 0;
    work.dispatchUndispatched([&](const std::int64_t key, const std::int64_t request) {
        if (slots-- <= 0)
        {
            return false;
        }
        EXPECT_TRUE(!recovering && viewLeader == memberId) << "member " << memberId << " dispatched while not leader";
        EXPECT_GT(key, lastInSweep) << "member " << memberId << " dispatched out of order";
        EXPECT_TRUE(tenure.insert(key).second) << "member " << memberId << " dispatched " << key << " twice";
        EXPECT_EQ(key, request);
        lastInSweep = key;
        pendingReplies.push_back(key);
        return true;
    });
}

void Replica::offer()
{
    if (pendingReplies.empty())
    {
        return;
    }
    const std::int64_t key = pendingReplies.front();
    pendingReplies.pop_front();
    if (!gate.isOpen())
    {
        return; // a reply produced after the gate closed is dropped; the next leader re-dispatches
    }
    if (simulation.faults && simulation.roll(4) == 0)
    {
        work.onReplyNotEmitted(key);
        tenure.erase(key);
    }
    else
    {
        simulation.inFlight.push_back(key);
    }
}

// A request held as dispatched whose reply is gone is never dispatched again by this opening.
void Replica::checkNoLostDispatch() const
{
    if (!gate.isOpen() || leadershipApplied || simulation.lastLeaderIndex >= applied)
    {
        return; // a close is already due
    }
    for (const std::int64_t key : tenure)
    {
        EXPECT_TRUE(std::ranges::find(pendingReplies, key) != pendingReplies.end() ||
                    std::ranges::find(simulation.inFlight, key) != simulation.inFlight.end() ||
                    simulation.replied.contains(key))
            << "member " << memberId << " holds " << key << " as dispatched, but its reply is gone";
    }
}

bool Replica::isQuiet() const
{
    return applied == simulation.log.size() && pendingReplies.empty() && work.size() == 0;
}

class OutstandingWorkProperty : public testing::TestWithParam<std::uint64_t>
{};

TEST_P(OutstandingWorkProperty, AnswersEveryRequestUnderRandomFailovers)
{
    Simulation simulation(GetParam());

    for (int step = 0; step < CHAOS_STEPS && !HasFailure(); ++step)
    {
        simulation.chaosStep();
        for (const Replica& replica : simulation.replicas)
        {
            replica.checkNoLostDispatch();
        }
    }
    ASSERT_FALSE(HasFailure());

    simulation.faults = false;
    for (Replica& replica : simulation.replicas)
    {
        replica.recovering = false;
    }
    for (int round = 0; round < QUIESCE_ROUNDS && !simulation.isQuiet() && !HasFailure(); ++round)
    {
        while (!simulation.inFlight.empty())
        {
            simulation.deliver();
        }
        for (Replica& replica : simulation.replicas)
        {
            replica.applyAll();
        }
        for (Replica& replica : simulation.replicas)
        {
            replica.dutyCycle();
        }
        for (Replica& replica : simulation.replicas)
        {
            while (!replica.pendingReplies.empty())
            {
                replica.offer();
            }
        }
    }

    EXPECT_TRUE(simulation.isQuiet()) << "did not converge once the faults stopped";
    EXPECT_EQ(0u, simulation.outstandingAt(simulation.log.size())) << "every request has a reply in the log";
}

INSTANTIATE_TEST_SUITE_P(Seeds, OutstandingWorkProperty,
                         testing::Values(1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597));

} // namespace
} // namespace org::limitless::seqeron::app
