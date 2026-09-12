# seqeron's C++ half — the cluster tier's client side: the cluster stream sender/client, the replayer
# client, the pure policy classes, and the codecs for the three schemas the cluster tier speaks. It
# declares no simdfix and generates no application codecs, and there is no C++ replay server: the
# server side of the replay protocol is Java only.
cmake_minimum_required(VERSION 3.28)
project(seqeron_core VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
include(CheckCXXCompilerFlag)

check_cxx_compiler_flag("-msse4.1" SEQERON_HAS_MSSE41)

add_library(seqeron_flags INTERFACE)
target_compile_options(seqeron_flags INTERFACE
    -Wall -Wextra
    $<$<CONFIG:Debug>:-g -O0 -fsanitize=address -fno-omit-frame-pointer>
    $<$<CONFIG:Release>:-O3>
    $<$<BOOL:${SEQERON_HAS_MSSE41}>:-msse4.1>
)
target_link_options(seqeron_flags INTERFACE
    $<$<CONFIG:Debug>:-fsanitize=address>
)

option(SEQERON_COVERAGE "Instrument for coverage" OFF)
if(SEQERON_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang") # matches both Clang and AppleClang
        target_compile_options(seqeron_flags INTERFACE -fprofile-instr-generate -fcoverage-mapping)
        target_link_options(seqeron_flags INTERFACE -fprofile-instr-generate)
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(seqeron_flags INTERFACE --coverage)
        target_link_options(seqeron_flags INTERFACE --coverage)
    else()
        message(WARNING "SEQERON_COVERAGE=ON but ${CMAKE_CXX_COMPILER_ID} has no known coverage flags")
    endif()
endif()

# Aeron — fetched and built from source.
set(AERON_TESTS            OFF CACHE BOOL "" FORCE)
set(AERON_SAMPLES          OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_SAMPLES    OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_AERON_ARCHIVE_API ON  CACHE BOOL "" FORCE)

# PATCH_COMMAND corrects Aeron's own declared OUTPUTs for the C archive codecs, which name a
# directory generateCCodecs never writes.
FetchContent_Declare(
    aeron
    GIT_REPOSITORY https://github.com/real-logic/aeron.git
    GIT_TAG        1.51.0
    GIT_SHALLOW    TRUE
    SYSTEM
    UPDATE_COMMAND ""
    PATCH_COMMAND  ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FixAeronCodecOutputs.cmake
)
FetchContent_MakeAvailable(aeron)

# core_tests and everything it needs belong to whoever builds this repo, not to a consumer that adds
# it: without this, `cmake --build .` in a consumer's tree compiles all nine test files and GoogleTest.
# Off by default when added as a subdirectory — a parent that vendors this build AND wants GoogleTest
# registered by it (rather than fetching its own) turns it back on.
option(SEQERON_BUILD_TESTS "Build seqeron's own test suite" ${PROJECT_IS_TOP_LEVEL})

# Google Test — FetchContent rather than find_package, so a consumer that vendors this build and looks
# GTest up with find_package resolves the targets FetchContent already registered.
if(SEQERON_BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.16.0
        GIT_SHALLOW    TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

# ── SBE C++ codecs ─────────────────────────────────────────────────────────────
# The codecs for the three schemas this repo owns are GENERATED AND COMMITTED, under
# src/main/generated/sbe/core. The git tag is the C++ artifact (doc/future-arch.md §6), so shipping
# them with it is what lets a consumer build against seqeron with no JDK and no SBE tool — a C++23
# compiler is the whole prerequisite list. The schema is core's; there is nothing for a consumer to
# regenerate.
#
# The cost is a second copy of the schemas, and a second copy drifts. SBE's C++ output is
# deterministic, so CheckSbeCodecsCurrent regenerates into the build tree and compares — exactly,
# not approximately — and this repo's own run_tests depends on it.
#
# generated/sbe/core, not generated/sbe: the root is named for what it holds — the schemas the cluster
# tier owns (frame, replay, cluster) and nothing else. A deployment that builds an application's codecs
# alongside these gives them their own root, so a cluster-tier file that reached for an application
# codec fails to compile rather than merely being asked not to.
set(SBE_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/main/generated/sbe/core")

set(SBE_VERSION "1.38.1")
set(SBE_SCHEMAS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/sbe/sbe-cluster.xml"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/sbe/sbe-replay.xml"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/sbe/sbe-frame.xml"
)

# QUIET, not REQUIRED: a consumer compiles the committed codecs and never runs the tool. Java is
# needed only to regenerate them, to check them, or by a parent build generating its own schemas.
find_package(Java QUIET COMPONENTS Runtime)

if(Java_JAVA_EXECUTABLE)
    set(SBE_JAR "${CMAKE_BINARY_DIR}/sbe-all-${SBE_VERSION}.jar")

    if(NOT EXISTS "${SBE_JAR}")
        message(STATUS "Downloading SBE tool ${SBE_VERSION}…")
        file(DOWNLOAD
            "https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/${SBE_VERSION}/sbe-all-${SBE_VERSION}.jar"
            "${SBE_JAR}"
            TLS_VERIFY ON
            SHOW_PROGRESS
        )
    endif()

    # A parent build that adds this one generates its own schemas with the same downloaded tool.
    if(NOT PROJECT_IS_TOP_LEVEL)
        set(SBE_JAR "${SBE_JAR}" PARENT_SCOPE)
    endif()

    # Wipes `dir` first: SBE only ever writes, so without that a message deleted from a schema leaves
    # its header behind and stays compilable.
    function(sbe_generate_target name dir)
        add_custom_target(${name}
            COMMAND ${CMAKE_COMMAND} -E rm -rf "${dir}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${dir}"
            COMMENT "Generating SBE C++ codecs into ${dir}"
            VERBATIM
        )
        foreach(schema IN LISTS SBE_SCHEMAS)
            add_custom_command(TARGET ${name} POST_BUILD
                COMMAND ${Java_JAVA_EXECUTABLE}
                            -Dsbe.output.dir=${dir}
                            -Dsbe.target.language=Cpp
                            -Dsbe.cpp.namespaces.collapse=false
                            -jar ${SBE_JAR}
                            ${schema}
                VERBATIM
            )
        endforeach()
    endfunction()

    # Explicit, and the only thing that writes into the source tree: run it when a schema changes,
    # then commit what it produced.
    sbe_generate_target(RegenerateSbeCodecs "${SBE_CORE_DIR}")

    set(SBE_CHECK_DIR "${CMAKE_BINARY_DIR}/sbe-codec-check")
    sbe_generate_target(SbeCodecsForCheck "${SBE_CHECK_DIR}")

    add_custom_target(CheckSbeCodecsCurrent
        COMMAND ${CMAKE_COMMAND}
                    -DCOMMITTED=${SBE_CORE_DIR}
                    -DFRESH=${SBE_CHECK_DIR}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckSbeCodecs.cmake
        COMMENT "Checking the committed SBE C++ codecs against src/main/sbe"
        VERBATIM
    )
    add_dependencies(CheckSbeCodecsCurrent SbeCodecsForCheck)
endif()

# seqeron_core — header-only interface library for the cluster tier. Its include root is
# src/main/cpp, so nothing linking only this can reach a product header,
# and it carries no simdfix and no generated FIX codecs: its SBE root is SBE_CORE_DIR, which holds
# only frame/replay/cluster, so the cluster tier decodes no FIX by construction rather than by habit.
#
# BUILD_INTERFACE only, and no install()/export(): seqeron_core interface-links aeron_client_wrapper,
# whose own install is gated on Aeron's AERON_INSTALL_TARGETS (default ${STANDALONE_BUILD}, off under
# FetchContent), so install(EXPORT) would fail at generate time on a target in no export set. A
# consumer adds this build — see examples/cpp — rather than find_package()ing it.
add_library(seqeron_core INTERFACE)
target_include_directories(seqeron_core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp>
)
# SYSTEM: SBE-tool output. Regenerating from the schema is the only way to change these, so their
# diagnostics are noise — -Wtype-limits fires on every get*AsJsonEscapedString wherever char is
# unsigned (aarch64). Keeps -Wall -Wextra live on the hand-written sources.
target_include_directories(seqeron_core SYSTEM INTERFACE
    $<BUILD_INTERFACE:${SBE_CORE_DIR}>
)
# Not seqeron_flags: those are THIS build's policy, not part of the library's interface, and in Debug
# they carry -fsanitize=address — a consumer linking seqeron_core would silently get AddressSanitizer
# in its own code. Targets in this repo add seqeron_flags themselves.
target_link_libraries(seqeron_core INTERFACE
    aeron_client_wrapper
)

if(SEQERON_BUILD_TESTS)
    # core_tests — the cluster tier's own suite. Links seqeron_core only, so a core file that reached for
    # a product header fails to build.
    enable_testing()

    add_executable(core_tests
        src/test/cpp/org/limitless/seqeron/sequencer/ClusterCodecTest.cpp
        src/test/cpp/org/limitless/seqeron/sequencer/ConformanceTest.cpp
        src/test/cpp/org/limitless/seqeron/sequencer/ClusterStreamSenderTest.cpp
        src/test/cpp/org/limitless/seqeron/sequencer/FrameStartPositionTest.cpp
        src/test/cpp/org/limitless/seqeron/sequencer/PortLayoutTest.cpp
        src/test/cpp/org/limitless/seqeron/replayer/client/ReplayerRecoveryTest.cpp
        src/test/cpp/org/limitless/seqeron/replayer/client/ReplayerRecoveryPropertyTest.cpp
        src/test/cpp/org/limitless/seqeron/replayer/client/RecoveryProgressPolicyTest.cpp
        src/main/cpp/org/limitless/seqeron/sequencer/ClusterStreamSender.hpp
        src/main/cpp/org/limitless/seqeron/util/Logger.hpp
    )
    target_link_libraries(core_tests PRIVATE
        seqeron_core
        seqeron_flags
        aeron_archive_wrapper
        aeron_archive_c_client
        aeron
        GTest::gtest_main
    )

    check_cxx_compiler_flag("-Wcharacter-conversion" SEQERON_HAS_WCHARACTER_CONVERSION)
    if(SEQERON_HAS_WCHARACTER_CONVERSION)
        target_compile_options(core_tests PRIVATE -Wno-character-conversion)
    endif()

    include(GoogleTest)
    gtest_discover_tests(core_tests)

    # run_tests — build the suite and run it. One binary now that the product half is its own repo, so
    # this is `ctest` with the output turned on; the target keeps its name because the CI workflow, the
    # docs and habit all name it. Plain ctest is clean here too: simdfix's own suite was what used to be
    # registered alongside and report spurious ..._NOT_BUILT failures, and this build declares no simdfix.
    add_custom_target(run_tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS core_tests
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running core_tests"
        USES_TERMINAL
    )

    # The committed codecs are a second copy of the schemas; this is what keeps them honest, and it
    # runs in the build that owns them rather than in a consumer's.
    if(TARGET CheckSbeCodecsCurrent)
        add_dependencies(run_tests CheckSbeCodecsCurrent)
    endif()
endif()
