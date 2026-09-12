# A consumer of seqeron, built outside its build: the header-only seqeron_core target, pulled in over
# the repository with FetchContent — the same way seqeron itself consumes Aeron and GoogleTest. The
# other supported path is find_package(seqeron) against a `cmake --install`ed tree (doc/publishing.md
# §7), which wants an installed Aeron beside it; this example builds Aeron from source anyway, which is
# exactly the case where FetchContent costs a consumer nothing.
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

# seqeron_core is header-only and carries its own include roots and every Aeron target its headers
# reach for — the C++ client wrapper, the C client behind it, and the archive pair — so this is the
# whole link line.
target_link_libraries(follow_stream PRIVATE seqeron_core)
