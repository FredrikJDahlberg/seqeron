#pragma once

// Process-startup configuration read from the environment — the handful of lookups every phixeron
// binary's main() does before it can connect to anything. Each of these was written out once per
// binary; the aeron-directory one had additionally diverged (see resolveAeronDir).

#include <cstdint>
#include <cstdlib>
#include <string>

namespace org::limitless::phixeron::util {

// Reads a base-10 int from the environment, falling back when unset/empty.
inline std::int32_t envInt(const char* name, const std::int32_t fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? static_cast<std::int32_t>(std::strtol(value, nullptr, 10)) : fallback;
}

inline std::string envString(const char* name, const std::string& fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? std::string{ value } : fallback;
}

// Set and non-empty — the shape every PHIXERON_* opt-in switch uses.
inline bool envFlag(const char* name)
{
    const char* value = std::getenv(name);
    return value != nullptr && *value != '\0';
}

// Joins a directory and a name with exactly one separator. macOS's $TMPDIR ends in a slash and Linux's
// /tmp does not, so concatenating a name onto either gets one of the two platforms wrong — see
// doc/portability-linux.md §2d.
inline std::string joinPath(const std::string& dir, const std::string& name)
{
    if (dir.empty())
    {
        return name;
    }
    return dir.back() == '/' ? dir + name : dir + '/' + name;
}

// The co-located SequencerServer member's Aeron directory, which an app shares so it can reach that
// node's tap, its Replayer and (while that member leads) cluster ingress over aeron:ipc. `envName` is
// the per-binary override (PHIXERON_ORDER_EXEC_AERON_DIR, PHIXERON_BASICDATA_AERON_DIR, …).
//
// The default is the directory of the member `memberId` names, matching SequencerServer.java's own
// default. It was previously spelled out once per binary and two of the three hardcoded member 0, so an
// unset override on member 1 or 2 attached the app to a different node's media driver than
// PHIXERON_NODE_MEMBER_ID named. Every launcher under src/*/scripts sets the override explicitly, so
// that only ever mattered for a hand-started process — but it is a trap, and unifying removes it.
inline std::string resolveAeronDir(const char* envName, const std::int32_t memberId)
{
    const char* value = std::getenv(envName);
    if (value != nullptr && *value != '\0')
    {
        return value;
    }
    const char* tmpDir = std::getenv("TMPDIR");
    return joinPath(tmpDir != nullptr && *tmpDir != '\0' ? tmpDir : "/tmp",
                    "phixeron-seq-aeron-" + std::to_string(memberId));
}

// A "host:port" cluster-egress endpoint: the per-binary override, else localhost on the port this app
// owns. Every client co-located on one node shares that node's media driver, so each needs a distinct
// port — the defaults come from PortLayout's per-role bases.
inline std::string resolveEgressEndpoint(const char* envName, const std::uint16_t defaultPort)
{
    return envString(envName, "localhost:" + std::to_string(defaultPort));
}

// Per-message logging on the poll thread (doc/audit.md C1) blocks on stdout under load, so it is opt-in
// via PHIXERON_VERBOSE_LOG rather than unconditional: a normal run's poll thread never stalls on a
// write() nobody is watching. Cached — this is checked once per message.
inline bool verboseLoggingEnabled()
{
    static const bool enabled = envFlag("PHIXERON_VERBOSE_LOG");
    return enabled;
}

} // namespace org::limitless::phixeron::util
