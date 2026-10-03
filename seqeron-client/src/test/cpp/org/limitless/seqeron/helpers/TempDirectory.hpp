#pragma once

// A directory of the test's own, removed with everything in it when the test ends: what JUnit's @TempDir gives
// the Java suites.

#include <atomic>
#include <filesystem>
#include <string>
#include <system_error>

#include <unistd.h>

namespace org::limitless::seqeron::helpers {

class TempDirectory
{
  public:
    TempDirectory() :
      m_path(std::filesystem::temp_directory_path() /
             ("seqeron-test-" + std::to_string(::getpid()) + "-" + std::to_string(next()++)))
    {
        std::filesystem::remove_all(m_path);
        std::filesystem::create_directories(m_path);
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    ~TempDirectory()
    {
        std::error_code ignored;
        std::filesystem::remove_all(m_path, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const
    {
        return m_path;
    }

  private:
    static std::atomic<int>& next()
    {
        static std::atomic<int> counter{ 0 };
        return counter;
    }

    std::filesystem::path m_path;
};

} // namespace org::limitless::seqeron::helpers
