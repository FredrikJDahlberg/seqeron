# A consumer of seqeron, built outside its build: the header-only seqeron_core target, pulled in over
# the repository with FetchContent — the same way seqeron itself consumes Aeron and GoogleTest — or,
# with SEQERON_FIND_PACKAGE=ON, found in a `cmake --install`ed tree with an installed Aeron beside it
# on CMAKE_PREFIX_PATH. CI builds it both ways.
cmake_minimum_required(VERSION 3.30)
project(seqeron_examples VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SEQERON_FIND_PACKAGE "Use an installed seqeron instead of the checkout" OFF)

if(SEQERON_FIND_PACKAGE)
    find_package(seqeron REQUIRED)
else()
    include(FetchContent)

    # Defaults to the checkout this example ships in. An outside consumer swaps SOURCE_DIR for
    # GIT_REPOSITORY/GIT_TAG; nothing else here changes.
    set(SEQERON_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.." CACHE PATH "seqeron checkout to build against")
    FetchContent_Declare(seqeron SOURCE_DIR "${SEQERON_SOURCE_DIR}")
    FetchContent_MakeAvailable(seqeron)
endif()

add_executable(follow_stream src/cpp/FollowStream.cpp)

# seqeron_core is header-only and carries its own include roots and every Aeron target its headers
# reach for — the C++ client wrapper, the C client behind it, and the archive pair — so this is the
# whole link line. seqeron::seqeron_core is the name both paths define.
target_link_libraries(follow_stream PRIVATE seqeron::seqeron_core)
