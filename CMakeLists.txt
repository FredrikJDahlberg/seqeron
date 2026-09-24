# seqeron's C++ half — the cluster tier's client side: the cluster stream sender/client, the replayer
# client, the pure policy classes, and the codecs for the three schemas the cluster tier speaks. It
# generates no application codecs, and there is no C++ replay server: the
# server side of the replay protocol is Java only.
# 3.30: Aeron 1.53.2's own CMakeLists requires it, so an older CMake should stop here, not inside Aeron.
cmake_minimum_required(VERSION 3.30)
# VERSION is the one place the number is written; build.gradle reads the same file.
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" SEQERON_VERSION LIMIT_COUNT 1)
# Aeron and SBE versions come from versions.properties, which build.gradle reads too.
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/versions.properties" SEQERON_AERON_VERSION REGEX "^aeron=")
string(REPLACE "aeron=" "" SEQERON_AERON_VERSION "${SEQERON_AERON_VERSION}")
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/versions.properties" SBE_VERSION REGEX "^sbe=")
string(REPLACE "sbe=" "" SBE_VERSION "${SBE_VERSION}")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" "${CMAKE_CURRENT_SOURCE_DIR}/versions.properties")
project(seqeron_core VERSION ${SEQERON_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
include(CheckCXXCompilerFlag)
include(GNUInstallDirs)

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

# Aeron — an installed one (built with -DAERON_INSTALL_TARGETS=ON, on CMAKE_PREFIX_PATH) is used when
# find_package finds one at or above SEQERON_AERON_VERSION, the same floor cmake/seqeronConfig.cmake.in
# states; otherwise it is fetched and built from source. GLOBAL, so the imported targets reach a parent
# that adds this build. Everything below links the aeron:: names, which Aeron defines as aliases when
# fetched and exports when installed.

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
    GIT_TAG        ${SEQERON_AERON_VERSION}
    GIT_SHALLOW    TRUE
    SYSTEM
    UPDATE_COMMAND ""
    PATCH_COMMAND  ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FixAeronCodecOutputs.cmake
    FIND_PACKAGE_ARGS ${SEQERON_AERON_VERSION} GLOBAL
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
# seqeron-client/src/main/generated/sbe/core. The git tag is the C++ artifact, so shipping
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
set(SBE_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/generated/sbe/core")

set(SBE_SCHEMAS
    "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/sbe/sbe-cluster.xml"
    "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/sbe/sbe-replay.xml"
    "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/sbe/sbe-frame.xml"
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
        COMMENT "Checking the committed SBE C++ codecs against seqeron-client/src/main/sbe"
        VERBATIM
    )
    add_dependencies(CheckSbeCodecsCurrent SbeCodecsForCheck)
endif()

# seqeron_core — header-only interface library for the cluster tier. Its include root is
# seqeron-client/src/main/cpp, so nothing linking only this can reach a product header,
# and it carries no application codecs: its SBE root is SBE_CORE_DIR, which holds only
# frame/replay/cluster, so the cluster tier decodes no application payload by construction.
#
# Consumable two ways and the target name is the same in both: add_subdirectory/FetchContent over the
# checkout (seqeron-examples), or find_package(seqeron) against an installed tree (the SEQERON_INSTALL
# block below).
add_library(seqeron_core INTERFACE)
add_library(seqeron::seqeron_core ALIAS seqeron_core)
# The standard is part of the interface: CMAKE_CXX_STANDARD above reaches this directory only.
target_compile_features(seqeron_core INTERFACE cxx_std_23)
target_include_directories(seqeron_core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/cpp>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# SYSTEM: SBE-tool output. Regenerating from the schema is the only way to change these, so their
# diagnostics are noise — -Wtype-limits fires on every get*AsJsonEscapedString wherever char is
# unsigned (aarch64). Keeps -Wall -Wextra live on the hand-written sources. It installs to a directory
# of its own rather than merging into the include root, because SYSTEM is a property of the root.
target_include_directories(seqeron_core SYSTEM INTERFACE
    $<BUILD_INTERFACE:${SBE_CORE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/seqeron-sbe>
)
# The whole Aeron surface these headers need, not just the C++ client wrapper: Aeron.h reaches for
# aeronc.h in the C client, and ClusterStreamClient.hpp for client/archive/AeronArchive.h and the C
# archive client behind that. Declaring the wrapper alone left every consumer to work the rest out.
#
# $<BUILD_INTERFACE:> on all four: Aeron arrives by FetchContent, where AERON_INSTALL_TARGETS defaults
# off and none of these is in an export set, so an install(EXPORT) that named them would fail at
# generate time. The exported target carries no Aeron at all and cmake/seqeronConfig.cmake.in
# re-attaches the same four under aeron::, which is what lets find_package(seqeron) work.
#
# Not seqeron_flags: those are THIS build's policy, not part of the library's interface, and in Debug
# they carry -fsanitize=address — a consumer linking seqeron_core would silently get AddressSanitizer
# in its own code. Targets in this repo add seqeron_flags themselves.
target_link_libraries(seqeron_core INTERFACE
    $<BUILD_INTERFACE:aeron::aeron_client_wrapper>
    $<BUILD_INTERFACE:aeron::aeron>
    $<BUILD_INTERFACE:aeron::aeron_archive_wrapper>
    $<BUILD_INTERFACE:aeron::aeron_archive_c_client>
)

# Installing is the top-level build's business: a parent that vendors this one should not find
# seqeron's headers landing in its own `install` target.
option(SEQERON_INSTALL "Generate seqeron's install and export rules" ${PROJECT_IS_TOP_LEVEL})

if(SEQERON_INSTALL)
    include(CMakePackageConfigHelpers)

    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/cpp/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        FILES_MATCHING PATTERN "*.hpp")
    install(DIRECTORY "${SBE_CORE_DIR}/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/seqeron-sbe"
        FILES_MATCHING PATTERN "*.h")

    install(TARGETS seqeron_core EXPORT seqeronTargets)
    install(EXPORT seqeronTargets
        FILE seqeronTargets.cmake
        NAMESPACE seqeron::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/seqeron")

    configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/seqeronConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/seqeronConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/seqeron"
        PATH_VARS CMAKE_INSTALL_INCLUDEDIR)

    # SameMinorVersion, not SameMajorVersion: at 0.x the minor is the breaking number.
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/seqeronConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMinorVersion)

    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/seqeronConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/seqeronConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/seqeron")
endif()

# docs — the client tier's API reference, the C++ counterpart of `./gradlew :seqeron-client:javadoc`. The
# same surface: detail/ is not API and the generated codecs are documented by their schema. Optional, since
# nothing else needs Doxygen.
if(PROJECT_IS_TOP_LEVEL)
    find_package(Doxygen QUIET)
    if(DOXYGEN_FOUND)
        set(SEQERON_DOCS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/cpp")
        set(DOXYGEN_PROJECT_NAME seqeron)
        set(DOXYGEN_PROJECT_NUMBER ${PROJECT_VERSION})
        set(DOXYGEN_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/docs")
        set(DOXYGEN_FILE_PATTERNS *.hpp)
        set(DOXYGEN_EXCLUDE_PATTERNS */detail/*)
        set(DOXYGEN_EXCLUDE_SYMBOLS *::detail *::detail::*)
        set(DOXYGEN_STRIP_FROM_PATH "${SEQERON_DOCS_ROOT}")
        set(DOXYGEN_STRIP_FROM_INC_PATH "${SEQERON_DOCS_ROOT}")
        set(DOXYGEN_EXTRACT_ALL YES)
        set(DOXYGEN_JAVADOC_AUTOBRIEF YES)
        set(DOXYGEN_WARN_IF_UNDOCUMENTED NO)
        set(DOXYGEN_GENERATE_TREEVIEW YES)
        set(SEQERON_DOCS_MAINPAGE "${CMAKE_CURRENT_SOURCE_DIR}/doc/site/cpp-mainpage.md")
        set(DOXYGEN_USE_MDFILE_AS_MAINPAGE "${SEQERON_DOCS_MAINPAGE}")
        doxygen_add_docs(docs "${SEQERON_DOCS_ROOT}" "${SEQERON_DOCS_MAINPAGE}"
            COMMENT "Rendering the C++ API reference into docs/html")
    endif()
endif()

if(SEQERON_BUILD_TESTS)
    # core_tests — the cluster tier's own suite. Links seqeron_core only, so a core file that reached for
    # a product header fails to build.
    enable_testing()

    add_executable(core_tests
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/AppFacadeTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/GatewayLifecycleTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/LeaderGateTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/OutstandingWorkTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/OutstandingWorkPropertyTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/RecoveryStallFenceTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/app/TapStallFenceTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/sequencer/client/ClusterCodecTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/sequencer/client/PendingSendsTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/sequencer/client/PendingSendsPropertyTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/sequencer/ConformanceTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/sequencer/client/ClusterStreamSenderTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/protocol/FrameStartPositionTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/protocol/PortLayoutTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/protocol/SequencedEventTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/protocol/SequencedFrameTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/replayer/client/ReplayerRecoveryTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/replayer/client/ReplayerRecoveryPropertyTest.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/helpers/SplitMix64Test.cpp
        seqeron-client/src/test/cpp/org/limitless/seqeron/util/EnvTest.cpp
        seqeron-client/src/main/cpp/org/limitless/seqeron/sequencer/client/ClusterStreamSender.hpp
        seqeron-client/src/main/cpp/org/limitless/seqeron/util/Logger.hpp
    )
    target_link_libraries(core_tests PRIVATE
        seqeron_core
        seqeron_flags
        aeron::aeron_archive_wrapper
        aeron::aeron_archive_c_client
        aeron::aeron
        GTest::gtest_main
    )

    # helpers/ — what the suites share, such as the generator both languages' property tests draw from.
    target_include_directories(core_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/test/cpp)

    check_cxx_compiler_flag("-Wcharacter-conversion" SEQERON_HAS_WCHARACTER_CONVERSION)
    if(SEQERON_HAS_WCHARACTER_CONVERSION)
        target_compile_options(core_tests PRIVATE -Wno-character-conversion)
    endif()

    include(GoogleTest)
    gtest_discover_tests(core_tests)

    # core_headers — every public header compiled on its own under -Werror, as a consumer's build would.
    # A header no test includes is otherwise never compiled here at all.
    set(SEQERON_HEADER_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/seqeron-client/src/main/cpp")
    file(GLOB_RECURSE SEQERON_HEADERS CONFIGURE_DEPENDS RELATIVE "${SEQERON_HEADER_ROOT}" "${SEQERON_HEADER_ROOT}/*.hpp")
    set(SEQERON_HEADER_TUS "")
    foreach(header IN LISTS SEQERON_HEADERS)
        string(MAKE_C_IDENTIFIER "${header}" tu)
        set(tu "${CMAKE_CURRENT_BINARY_DIR}/core_headers/${tu}.cpp")
        file(CONFIGURE OUTPUT "${tu}" CONTENT "#include \"${header}\"\n")
        list(APPEND SEQERON_HEADER_TUS "${tu}")
    endforeach()
    add_library(core_headers OBJECT ${SEQERON_HEADER_TUS})
    target_link_libraries(core_headers PRIVATE seqeron_core seqeron_flags)
    target_compile_options(core_headers PRIVATE -Werror)

    # run_tests — build the suite and run it: `ctest` with the output turned on. The target keeps its
    # name because the CI workflow and the docs name it.
    add_custom_target(run_tests
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS core_tests core_headers
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
