// A ReplayScan for ReplayScanCppTest: it reads a member's default queue three ways and reports each.
//
//   replay_scan_probe --aeron-dir D [--timeout-ms 20000]
//
// "full" reads the queue from its start. "resumed" resumes from the position of the frame halfway through what
// "full" read, and must deliver the rest. "bogus" resumes from a position one past a frame's start, which is no
// frame boundary and which the Replayer refuses (R-2). Exits 3 when a scan does not finish in the timeout.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"

#include "org/limitless/seqeron/replayer/client/ReplayScan.hpp"

namespace {

namespace client = org::limitless::seqeron::replayer::client;
namespace protocol = org::limitless::seqeron::protocol;

struct Frame
{
    std::int64_t clusterSeqNo;
    std::int64_t position;
};

struct Result
{
    client::ReplayScan::State state;
    std::vector<Frame> frames;
};

const char* name(const client::ReplayScan::State state)
{
    return state == client::ReplayScan::State::Done ? "Done" : "Failed";
}

bool scan(const std::shared_ptr<aeron::Aeron>& aeron, const std::int64_t fromPosition,
          const std::chrono::milliseconds timeout, Result& result)
{
    result.frames.clear();
    client::ReplayScan replay(aeron, "", fromPosition, [&](const protocol::FrameView& frame, const std::int64_t position) {
        result.frames.push_back({ frame.clusterSeqNo, position });
    });
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!replay.finished())
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            return false;
        }
        if (replay.poll() == 0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    result.state = replay.state();
    return true;
}

void report(const char* label, const Result& result)
{
    std::printf("[replayscan] %s state=%s frames=%zu first=%lld last=%lld\n", label, name(result.state),
                result.frames.size(), static_cast<long long>(result.frames.empty() ? -1 : result.frames.front().clusterSeqNo),
                static_cast<long long>(result.frames.empty() ? -1 : result.frames.back().clusterSeqNo));
    std::fflush(stdout);
}

} // namespace

int main(int argc, char** argv)
{
    std::string aeronDir;
    std::chrono::milliseconds timeout{ 20'000 };
    for (int i = 1; i + 1 < argc; i += 2)
    {
        if (std::strcmp(argv[i], "--aeron-dir") == 0)
        {
            aeronDir = argv[i + 1];
        }
        else if (std::strcmp(argv[i], "--timeout-ms") == 0)
        {
            timeout = std::chrono::milliseconds{ std::atoll(argv[i + 1]) };
        }
    }

    aeron::Context context;
    context.aeronDir(aeronDir);
    const auto aeron = aeron::Aeron::connect(context);

    Result full{};
    if (!scan(aeron, client::ReplayScan::FROM_START, timeout, full))
    {
        std::printf("[replayscan] full timed out\n");
        return 3;
    }
    report("full", full);

    Result resumed{};
    const std::size_t half = full.frames.size() / 2;
    std::printf("[replayscan] resume index=%zu position=%lld clusterSeqNo=%lld\n", half,
                static_cast<long long>(full.frames.empty() ? -1 : full.frames[half].position),
                static_cast<long long>(full.frames.empty() ? -1 : full.frames[half].clusterSeqNo));
    if (!full.frames.empty() && !scan(aeron, full.frames[half].position, timeout, resumed))
    {
        std::printf("[replayscan] resumed timed out\n");
        return 3;
    }
    report("resumed", resumed);

    Result bogus{};
    if (!full.frames.empty() && !scan(aeron, full.frames.front().position + 1, timeout, bogus))
    {
        std::printf("[replayscan] bogus timed out\n");
        return 3;
    }
    report("bogus", bogus);
    return 0;
}
