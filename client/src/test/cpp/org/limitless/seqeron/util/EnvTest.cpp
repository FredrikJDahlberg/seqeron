// Env.hpp's lookups: what every seqeron binary's main() resolves before it can connect to anything.
// Pure but process-global, so each case sets the variables it reads and an EnvVar guard puts the
// process back the way it found it.

#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "org/limitless/seqeron/util/Env.hpp"

namespace {

using namespace org::limitless::seqeron::util;

// Sets one variable for the life of the guard and restores whatever was there — unset included.
class EnvVar
{
  public:
    EnvVar(const char* name, const char* value) : m_name(name)
    {
        const char* previous = std::getenv(name);
        if (previous != nullptr)
        {
            m_previous = previous;
        }
        set(value);
    }

    ~EnvVar()
    {
        set(m_previous.has_value() ? m_previous->c_str() : nullptr);
    }

    EnvVar(const EnvVar&) = delete;
    EnvVar& operator=(const EnvVar&) = delete;

  private:
    void set(const char* value) const
    {
        if (value != nullptr)
        {
            ::setenv(m_name, value, 1);
        }
        else
        {
            ::unsetenv(m_name);
        }
    }

    const char* m_name;
    std::optional<std::string> m_previous;
};

constexpr const char* VAR = "SEQERON_ENV_TEST_VALUE";

TEST(Env, IntFallsBackWhenUnsetOrEmpty)
{
    ::unsetenv(VAR);
    EXPECT_EQ(7, envInt(VAR, 7));

    const EnvVar empty{ VAR, "" };
    EXPECT_EQ(7, envInt(VAR, 7)) << "empty is the shape an unset variable takes in a shell script";
}

TEST(Env, IntReadsBaseTen)
{
    const EnvVar var{ VAR, "9300" };
    EXPECT_EQ(9300, envInt(VAR, 0));
}

TEST(Env, IntTakesANegativeValue)
{
    const EnvVar var{ VAR, "-1" };
    EXPECT_EQ(-1, envInt(VAR, 0));
}

// strtol's answer for text that is not a number, not the fallback: a typo'd value reads as 0 rather
// than as unset. PortLayout::parseClusterPortBase is the validating seam for the one knob that cares.
TEST(Env, IntReadsGarbageAsZeroRatherThanFallingBack)
{
    const EnvVar var{ VAR, "not-a-number" };
    EXPECT_EQ(0, envInt(VAR, 7));
}

TEST(Env, StringFallsBackWhenUnsetOrEmpty)
{
    ::unsetenv(VAR);
    EXPECT_EQ("fallback", envString(VAR, "fallback"));

    const EnvVar empty{ VAR, "" };
    EXPECT_EQ("fallback", envString(VAR, "fallback"));
}

TEST(Env, StringTakesTheValueAsGiven)
{
    const EnvVar var{ VAR, "localhost:9301,localhost:9311" };
    EXPECT_EQ("localhost:9301,localhost:9311", envString(VAR, ""));
}

// Set and non-empty, so "0" and "false" are both on — every SEQERON_* switch is an opt-in, never a
// value to parse.
TEST(Env, FlagIsSetAndNonEmpty)
{
    ::unsetenv(VAR);
    EXPECT_FALSE(envFlag(VAR));

    const EnvVar empty{ VAR, "" };
    EXPECT_FALSE(envFlag(VAR));
}

TEST(Env, FlagIsOnForAnyNonEmptyValue)
{
    {
        const EnvVar var{ VAR, "1" };
        EXPECT_TRUE(envFlag(VAR));
    }
    const EnvVar var{ VAR, "0" };
    EXPECT_TRUE(envFlag(VAR)) << "an opt-in switch reads its presence, not its text";
}

// The case the comment in Env.hpp names: macOS's $TMPDIR ends in a slash and Linux's /tmp does not,
// so a concatenation gets exactly one of the two platforms wrong.
TEST(Env, JoinPathAddsExactlyOneSeparator)
{
    EXPECT_EQ("/tmp/seqeron", joinPath("/tmp", "seqeron"));
    EXPECT_EQ("/var/folders/t/", joinPath("/var/folders/t/", ""));
    EXPECT_EQ("/var/folders/t/seqeron", joinPath("/var/folders/t/", "seqeron"));
}

TEST(Env, JoinPathOnAnEmptyDirectoryIsTheNameAlone)
{
    EXPECT_EQ("seqeron", joinPath("", "seqeron"));
}

TEST(Env, AeronDirTakesTheOverride)
{
    const EnvVar var{ VAR, "/dev/shm/aeron-app" };
    EXPECT_EQ("/dev/shm/aeron-app", resolveAeronDir(VAR, 0));
}

// The default is member `memberId`'s own directory, which is what SequencerServer.java names.
TEST(Env, AeronDirDefaultsToTheMembersDirectoryUnderTmpdir)
{
    const EnvVar tmp{ "TMPDIR", "/var/folders/t/" };

    ::unsetenv(VAR);
    EXPECT_EQ("/var/folders/t/seqeron-seq-aeron-2", resolveAeronDir(VAR, 2));

    const EnvVar empty{ VAR, "" };
    EXPECT_EQ("/var/folders/t/seqeron-seq-aeron-2", resolveAeronDir(VAR, 2)) << "an empty override is no override";
}

TEST(Env, AeronDirFallsBackToSlashTmpWhenTmpdirIsUnsetOrEmpty)
{
    ::unsetenv(VAR);
    {
        const EnvVar tmp{ "TMPDIR", "" };
        EXPECT_EQ("/tmp/seqeron-seq-aeron-0", resolveAeronDir(VAR, 0));
    }
    const char* previous = std::getenv("TMPDIR");
    const std::string restore = previous != nullptr ? previous : "";
    ::unsetenv("TMPDIR");
    const std::string resolved = resolveAeronDir(VAR, 1);
    if (!restore.empty())
    {
        ::setenv("TMPDIR", restore.c_str(), 1);
    }
    EXPECT_EQ("/tmp/seqeron-seq-aeron-1", resolved);
}

TEST(Env, EgressEndpointTakesTheOverride)
{
    const EnvVar var{ VAR, "10.0.0.4:9500" };
    EXPECT_EQ("10.0.0.4:9500", resolveEgressEndpoint(VAR, 9400));
}

TEST(Env, EgressEndpointDefaultsToLocalhostOnTheCallersPort)
{
    ::unsetenv(VAR);
    EXPECT_EQ("localhost:9400", resolveEgressEndpoint(VAR, 9400));
}

// verboseLoggingEnabled caches its answer in a function-local static, so the first call in the process
// decides it. This is the suite's only caller, which is what lets the case set the variable first.
TEST(Env, VerboseLoggingIsReadOnceAndCached)
{
    const EnvVar on{ "SEQERON_VERBOSE_LOG", "1" };
    ASSERT_TRUE(verboseLoggingEnabled());

    const EnvVar off{ "SEQERON_VERBOSE_LOG", "" };
    EXPECT_TRUE(verboseLoggingEnabled()) << "cached: a later change does not reach the poll thread";
}

} // namespace
