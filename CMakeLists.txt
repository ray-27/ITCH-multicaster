cmake_minimum_required(VERSION 3.20)
project(itch_multicast CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # lets clangd resolve headers in your IDE

# ── Homebrew paths (Apple Silicon) ───────────────────────────────────────────
# CMake's find_package doesn't search /opt/homebrew by default on macOS.
list(APPEND CMAKE_PREFIX_PATH
    /opt/homebrew/opt/boost
    /opt/homebrew/opt/openssl@3
)

# ── Dependencies ──────────────────────────────────────────────────────────────
find_package(Boost REQUIRED)          # header-only: Beast + Asio
find_package(OpenSSL REQUIRED)        # TLS for WSS

include(FetchContent)
FetchContent_Declare(simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG        v3.12.3)
FetchContent_MakeAvailable(simdjson)

# ── Shared interface target ───────────────────────────────────────────────────
# Every binary in this repo links this to get include paths + compile flags.
add_library(itch_common INTERFACE)
target_include_directories(itch_common INTERFACE
    ${CMAKE_SOURCE_DIR}/include       # sprc/sprc.hpp, itch/, common/
    ${CMAKE_SOURCE_DIR}/src           # so #include "ingestor/ws_ingestor.hpp" works
    ${Boost_INCLUDE_DIRS}
)
target_link_libraries(itch_common INTERFACE
    OpenSSL::SSL
    OpenSSL::Crypto
)
target_compile_options(itch_common INTERFACE
    -Wall -Wextra
    -O3 -march=native
    # Boost.Asio requires one of these to use the standalone or Boost executor
    -DBOOST_ASIO_NO_DEPRECATED
)

# ── Publisher binary ──────────────────────────────────────────────────────────
add_executable(publisher
    src/main.cpp
    src/ingestor/ws_ingestor.cpp
    src/encoder/itch_encoder.cpp
    src/multicast/udp_multicaster.cpp
)
target_link_libraries(publisher PRIVATE itch_common simdjson)

# ── SPSC ring unit test ───────────────────────────────────────────────────────
add_executable(test_sprc
    include/sprc/test_sprc.cpp
)
target_link_libraries(test_sprc PRIVATE itch_common)
