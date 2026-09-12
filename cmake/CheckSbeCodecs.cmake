# Fails if the committed SBE C++ codecs differ from what the schemas generate now.
#
# The codecs are checked in so a C++ consumer needs no JDK and no SBE tool (doc/publishing.md), which
# makes them a second copy of the schemas — and a second copy drifts. SBE's C++ output is
# deterministic, so "generate again and compare" is exact rather than approximate.
#
# Invoked as: cmake -DCOMMITTED=<dir> -DFRESH=<dir> -P CheckSbeCodecs.cmake
#
# Required, and not boilerplate: `cmake -P` sets no policy version, so under CMake 3.x every policy is
# unset and `if(... IN_LIST ...)` (CMP0057) is not recognised — the script dies with "Unknown arguments
# specified" rather than running. CMake 4.x defaults them NEW in script mode, which is why this passed
# locally and failed in CI. Stating the floor sets CMP0057 NEW on every version.
cmake_minimum_required(VERSION 3.28)

file(GLOB_RECURSE committed_files RELATIVE "${COMMITTED}" "${COMMITTED}/*.h")
file(GLOB_RECURSE fresh_files     RELATIVE "${FRESH}"     "${FRESH}/*.h")
list(SORT committed_files)
list(SORT fresh_files)

set(drift "")

foreach(f IN LISTS fresh_files)
    if(NOT f IN_LIST committed_files)
        list(APPEND drift "missing: ${f}")
    else()
        file(SHA256 "${COMMITTED}/${f}" a)
        file(SHA256 "${FRESH}/${f}" b)
        if(NOT a STREQUAL b)
            list(APPEND drift "differs: ${f}")
        endif()
    endif()
endforeach()

foreach(f IN LISTS committed_files)
    if(NOT f IN_LIST fresh_files)
        list(APPEND drift "stale (schema no longer generates it): ${f}")
    endif()
endforeach()

if(drift)
    list(JOIN drift "\n  " report)
    message(FATAL_ERROR
        "Committed SBE C++ codecs are out of date with src/main/sbe:\n  ${report}\n\n"
        "Regenerate and commit them:\n"
        "  cmake --build <build dir> --target RegenerateSbeCodecs")
endif()

list(LENGTH committed_files n)
message(STATUS "SBE C++ codecs are current (${n} headers)")
