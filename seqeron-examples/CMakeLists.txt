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

add_executable(colocated_app src/cpp/ColocatedApp.cpp)
add_executable(gateway_app src/cpp/GatewayApp.cpp)

# seqeron_core is header-only and carries its own include roots and the Aeron targets its headers reach for,
# so this is the whole link line. seqeron::seqeron_core is the name both paths define.
target_link_libraries(colocated_app PRIVATE seqeron::seqeron_core)
target_link_libraries(gateway_app PRIVATE seqeron::seqeron_core)

# What these examples are for: the hosted stub and the types it shares are sufficient on their own. Compiling them
# proves nothing about what they include, so the includes are the assertion, checked here the way the Java half's
# checkFacadeOnly checks its imports. util is on the list because it is support code beside the stub rather than a
# tier under it (doc/client-api.md, "Support code").
foreach(_example IN ITEMS ColocatedApp GatewayApp)
    file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/src/cpp/${_example}.cpp" _example_includes
         REGEX "^#include \"org/limitless/seqeron/")
    foreach(_include IN LISTS _example_includes)
        if(NOT _include MATCHES "org/limitless/seqeron/(app|hosted|util)/|org/limitless/seqeron/protocol/Publish\\.hpp"
           OR _include MATCHES "/detail/")
            message(FATAL_ERROR
                    "${_example}.cpp must include only app, hosted, util and protocol/Publish.hpp: ${_include}")
        endif()
    endforeach()
endforeach()
