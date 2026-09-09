# A consumer of seqeron, built outside its build: the header-only seqeron_core target, pulled in the one
# way it is consumable today. There is no install()/export() for it, so find_package(seqeron) cannot
# work — FetchContent over the repository is the supported path, and it is how seqeron itself consumes
# Aeron and GoogleTest.
cmake_minimum_required(VERSION 3.28)
project(seqeron_follow_example VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)

# Defaults to the checkout this example ships in. An outside consumer swaps SOURCE_DIR for
# GIT_REPOSITORY/GIT_TAG; nothing else here changes.
set(SEQERON_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "seqeron checkout to build against")
FetchContent_Declare(seqeron SOURCE_DIR "${SEQERON_SOURCE_DIR}")
FetchContent_MakeAvailable(seqeron)

add_executable(follow_stream FollowStream.cpp)

# seqeron_core is header-only and carries its own include roots and aeron_client_wrapper; the C client
# is what the wrapper links against.
target_link_libraries(follow_stream PRIVATE seqeron_core aeron)

# seqeron_core names the generated SBE include dir but depends on nothing that fills it — only
# core_tests does, in the parent build. A consumer therefore has to ask for the codecs itself, or the
# first seqeron header it includes fails on a missing org_limitless_seqeron_sbe_frame/*.h.
add_dependencies(follow_stream GenerateFrameSbeCodecs GenerateReplaySbeCodecs GenerateClusterSbeCodecs)
