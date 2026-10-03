// A co-located application whose state survives its restart through a snapshot, and the C++ twin of src/java's
// SnapshotApp. The state is a ledger: eight accounts that open with 1000 each, and the transfers between them.
// The leading replica submits a random transfer once a second; every replica applies each one in log order, and
// one that would overdraw its source account is rejected — so what a transfer does depends on every transfer
// before it, and a restart must restore the balances exactly. A restart restores the newest round the log
// confirms and applies only the transfers after its cut.
//
// Rounds need this application's snapshot="true" row and a <snapshots> policy, both in
// seqeron-examples/topology.xml. Environment: SEQERON_NODE_MEMBER_ID (default 0), SEQERON_REPLAYER_CLIENT_ID
// (default 18), SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY, SEQERON_EXAMPLE_EGRESS_PORT (default 9207 + member),
// SEQERON_EXAMPLE_SNAPSHOT_DIR.

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/app/SnapshotListener.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;

// A transfer: from int32, to int32, amount int64, in host order (little-endian on every supported target), no
// schema.
constexpr std::uint16_t TRANSFER_PAYLOAD_ID = 7;
struct Transfer
{
    std::int32_t from;
    std::int32_t to;
    std::int64_t amount;
};
static_assert(sizeof(Transfer) == 16);

constexpr std::int32_t SOURCE_ID = 15;

constexpr std::int32_t ACCOUNTS = 8;
constexpr std::int64_t OPENING_BALANCE = 1000;
constexpr std::int64_t MAX_AMOUNT = 400;

// Raised when a record's layout changes; a restore stops on a file of another one.
constexpr std::uint32_t FORMAT_VERSION = 1;

constexpr std::int64_t TRANSFER_INTERVAL_NS = 1'000'000'000;

std::atomic_bool running{ true };
std::string fault;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void onSignal(int)
{
    running.store(false, std::memory_order_relaxed);
}

struct Ledger final : app::SnapshotListener
{
    // The replicated state: derived from the log alone, so identical on every replica.
    std::array<std::int64_t, ACCOUNTS> balances{};
    std::int64_t applied = 0;
    std::int64_t rejected = 0;

    Ledger()
    {
        balances.fill(OPENING_BALANCE);
    }

    void printBalances() const
    {
        for (const std::int64_t balance : balances)
        {
            std::printf(" %lld", static_cast<long long>(balance));
        }
        std::putchar('\n');
    }

    // The state transition: every replica applies the same transfers in the same order.
    void onSequenced(const app::Payload& payload)
    {
        if (payload.sourceId() != SOURCE_ID || payload.payloadId() != TRANSFER_PAYLOAD_ID ||
            payload.payloadLength() != sizeof(Transfer))
        {
            return;
        }
        Transfer transfer{};
        std::memcpy(&transfer, payload.payload(), sizeof transfer);
        const bool ok = balances[transfer.from] >= transfer.amount;
        if (ok)
        {
            balances[transfer.from] -= transfer.amount;
            balances[transfer.to] += transfer.amount;
            ++applied;
        }
        else
        {
            ++rejected;
        }
        std::printf("%lld transfer %lld from %d to %d %s", static_cast<long long>(payload.globalSeqNo()),
                    static_cast<long long>(transfer.amount), transfer.from, transfer.to, ok ? "applied" : "rejected");
        printBalances();
    }

    std::uint32_t formatVersion() const override
    {
        return FORMAT_VERSION;
    }

    // Record 0 holds the counters, applied and rejected int64; records 1 to 8 one balance each, int64 in account
    // order. A ledger of many accounts is why a snapshot is a sequence of records rather than one: each record is
    // at most 65535 bytes, and the number of them is unbounded.
    std::int32_t onSnapshot(const std::span<std::uint8_t> buffer, const std::int32_t recordIndex) override
    {
        if (recordIndex == 0)
        {
            std::memcpy(buffer.data(), &applied, sizeof applied);
            std::memcpy(buffer.data() + sizeof applied, &rejected, sizeof rejected);
            std::printf("# snapshot after %lld transfers, %lld rejected\n", static_cast<long long>(applied),
                        static_cast<long long>(rejected));
            return sizeof applied + sizeof rejected;
        }
        if (recordIndex > ACCOUNTS)
        {
            return 0;
        }
        std::memcpy(buffer.data(), &balances[recordIndex - 1], sizeof(std::int64_t));
        return sizeof(std::int64_t);
    }

    void onRestore(const std::span<const std::uint8_t> record, const std::int32_t recordIndex) override
    {
        if (recordIndex == 0)
        {
            std::memcpy(&applied, record.data(), sizeof applied);
            std::memcpy(&rejected, record.data() + sizeof applied, sizeof rejected);
            return;
        }
        std::memcpy(&balances[recordIndex - 1], record.data(), sizeof(std::int64_t));
        if (recordIndex == ACCOUNTS)
        {
            std::printf("# restored after %lld transfers, %lld rejected", static_cast<long long>(applied),
                        static_cast<long long>(rejected));
            printBalances();
        }
    }

    void onLeadershipChanged(const bool leading) const
    {
        std::puts(leading ? "# leading — submitting transfers" : "# not leading — silent");
    }

    // Transfers move money and never make it, so the total is the opening one whatever was restored.
    void onCaughtUp(const std::int64_t globalSeqNo) const
    {
        std::printf("# caught up at %lld — %lld transfers, %lld rejected, total %lld\n",
                    static_cast<long long>(globalSeqNo), static_cast<long long>(applied),
                    static_cast<long long>(rejected),
                    static_cast<long long>(std::accumulate(balances.begin(), balances.end(), std::int64_t{ 0 })));
    }

    void onClusterHeartbeat(std::int64_t, std::int64_t) const
    {}

    // SNAPSHOT_DIVERGED and SNAPSHOT_UNRESTORABLE arrive here like every other fence.
    void onFenced(const app::ClusterError fence, const std::string& detail) const
    {
        fault = std::string(app::clusterErrorName(fence)) + ": " + detail;
    }
};

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;

    const std::int32_t memberId = util::envInt("SEQERON_NODE_MEMBER_ID", 0);
    const std::int32_t clientId = util::envInt("SEQERON_REPLAYER_CLIENT_ID", 18);
    const std::string aeronDir = util::resolveAeronDir("SEQERON_AERON_DIR", memberId);
    // The replica's own, and it must outlive the process: a restart restores from it.
    const char* snapshotDirEnv = std::getenv("SEQERON_EXAMPLE_SNAPSHOT_DIR");
    const std::filesystem::path snapshotDir =
        snapshotDirEnv != nullptr
            ? std::filesystem::path(snapshotDirEnv)
            : std::filesystem::temp_directory_path() /
                  ("seqeron-example-snapshots-" + std::to_string(SOURCE_ID) + "-" + std::to_string(memberId));

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    aeron::Context context;
    context.aeronDir(aeronDir);
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    Ledger ledger;
    app::Application<Ledger> application{
        { .sourceId = SOURCE_ID,
          .clientId = clientId,
          .memberId = memberId,
          .egressChannel = "aeron:udp?endpoint=localhost:" +
                           std::to_string(util::envInt("SEQERON_EXAMPLE_EGRESS_PORT", 9207 + memberId)),
          .snapshotListener = &ledger,
          .snapshotDirectory = snapshotDir },
        ledger
    };
    application.start(aeron);
    std::printf("# member %d via %s — snapshots in %s\n", memberId, aeronDir.c_str(), snapshotDir.c_str());

    // A command, not a state change: the randomness is the leader's alone, and the replicas see only the
    // payload. Whether it overdraws is decided when it is applied, not here. A declined one is not retried.
    std::mt19937 random{ std::random_device{}() };
    std::uniform_int_distribution<std::int32_t> account{ 0, ACCOUNTS - 1 };
    std::uniform_int_distribution<std::int32_t> offset{ 1, ACCOUNTS - 1 };
    std::uniform_int_distribution<std::int64_t> amount{ 1, MAX_AMOUNT };

    auto idle = util::resolveIdleStrategy();
    std::int64_t nextTransferNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty())
    {
        const int work = application.doWork();
        const std::int64_t now = nowNs();
        if (application.canPublish() && now >= nextTransferNs)
        {
            Transfer transfer{ .from = account(random), .to = 0, .amount = amount(random) };
            transfer.to = (transfer.from + offset(random)) % ACCOUNTS;
            static_cast<void>(application.publish(TRANSFER_PAYLOAD_ID, reinterpret_cast<const std::uint8_t*>(&transfer),
                                                  sizeof transfer));
            nextTransferNs = now + TRANSFER_INTERVAL_NS;
        }
        idle.idle(work);
    }
    application.close();

    std::fflush(stdout);
    if (!fault.empty())
    {
        std::fprintf(stderr, "# %s\n", fault.c_str());
        return 1;
    }
    return 0;
}
