#pragma once

// Process-startup configuration read from the environment: the lookups every seqeron binary's main()
// does before it can connect to anything.

#include <cstdint>
#include <cstdlib>
#include <string>

namespace org::limitless::seqeron::util {

/**
 * Reads a base-10 int from the environment.
 *
 * @param name     the variable to read
 * @param fallback what an unset or empty variable reads as
 */
inline std::int32_t envInt(const char* name, const std::int32_t fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? static_cast<std::int32_t>(std::strtol(value, nullptr, 10)) : fallback;
}

/**
 * Reads a string from the environment.
 *
 * @param name     the variable to read
 * @param fallback what an unset or empty variable reads as
 */
inline std::string envString(const char* name, const std::string& fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && *value != '\0') ? std::string{ value } : fallback;
}

/**
 * Whether a variable is set and non-empty — the shape every SEQERON_* opt-in switch uses.
 *
 * @param name the variable to read
 */
inline bool envFlag(const char* name)
{
    const char* value = std::getenv(name);
    return value != nullptr && *value != '\0';
}

/**
 * Joins a directory and a name with exactly one separator. macOS's $TMPDIR ends in a slash and Linux's
 * /tmp does not, so concatenating a name onto either gets one of the two platforms wrong.
 *
 * @param dir  the directory, with or without a trailing slash; empty yields name alone
 * @param name the name to join onto it
 */
inline std::string joinPath(const std::string& dir, const std::string& name)
{
    if (dir.empty())
    {
        return name;
    }
    return dir.back() == '/' ? dir + name : dir + '/' + name;
}

/**
 * Resolves the co-located cluster member's Aeron directory, which an app shares to reach that node's tap,
 * Replayer and (while it leads) IPC ingress.
 *
 * @param envName  the per-binary override variable
 * @param memberId the member whose directory is the default, matching SequencerServer.java's
 */
inline std::string resolveAeronDir(const char* envName, const std::int32_t memberId)
{
    const char* value = std::getenv(envName);
    if (value != nullptr && *value != '\0')
    {
        return value;
    }
    const char* tmpDir = std::getenv("TMPDIR");
    return joinPath(tmpDir != nullptr && *tmpDir != '\0' ? tmpDir : "/tmp",
                    "seqeron-seq-aeron-" + std::to_string(memberId));
}

/**
 * Resolves a "host:port" cluster-egress endpoint. Every client co-located on one node shares that node's
 * media driver, so each needs a distinct port.
 *
 * @param envName     the per-binary override variable
 * @param defaultPort the port this app owns on localhost, from its own application port table
 */
inline std::string resolveEgressEndpoint(const char* envName, const std::uint16_t defaultPort)
{
    return envString(envName, "localhost:" + std::to_string(defaultPort));
}

// Per-message logging blocks the poll thread on stdout under load, so it is opt-in via SEQERON_VERBOSE_LOG.
// Cached: checked once per message.
inline bool verboseLoggingEnabled()
{
    static const bool enabled = envFlag("SEQERON_VERBOSE_LOG");
    return enabled;
}

} // namespace org::limitless::seqeron::util
