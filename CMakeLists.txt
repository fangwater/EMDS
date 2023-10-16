cmake_minimum_required(VERSION 3.22)
set(CMAKE_C_COMPILER "/usr/local/bin/gcc")
set(CMAKE_CXX_COMPILER "/usr/local/bin/g++")
SET(CMAKE_TOOLCHAIN_FILE "/home/fanghaizhou/lib/vcpkg/scripts/buildsystems/vcpkg.cmake")
project(EMDS)
set(CMAKE_CXX_STANDARD 20)
# SET(CMAKE_BUILD_TYPE "Debug")
SET(CMAKE_BUILD_TYPE "Release")
include_directories(SYSTEM "/home/fanghaizhou/lib/vcpkg/installed/x64-linux/include")
find_package(Threads REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(cppzmq CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(xsimd CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)

set(absl_LIBRARIES
absl::algorithm
absl::base
absl::debugging
absl::flat_hash_map
absl::memory
absl::meta
absl::numeric
absl::str_format
absl::strings
absl::synchronization
absl::time
absl::flags
absl::strings
absl::log
absl::flags_internal
absl::log_initialize
absl::config
absl::base
absl::flags_parse
absl::utility)

include_directories(${CMAKE_SOURCE_DIR}/src/cache_layer)
include_directories(${CMAKE_SOURCE_DIR}/src/collect_layer)
include_directories(${CMAKE_SOURCE_DIR}/src/common)
add_executable(emds src/server.cc)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    target_compile_options(emds PRIVATE -O3 -march=native)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(emds PRIVATE -O3 -march=native)
endif ()
target_link_libraries(emds ${absl_LIBRARIES})
target_link_libraries(emds fmt::fmt)
target_link_libraries(emds "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
target_link_libraries(emds "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")
target_link_libraries(emds Folly::folly Folly::folly_deps Folly::follybenchmark Folly::folly_test_util)
target_link_libraries(emds xsimd)
target_link_libraries(emds cppzmq cppzmq-static)
target_link_libraries(emds nlohmann_json::nlohmann_json)
target_link_libraries(emds spdlog::spdlog)
target_link_libraries(emds TBB::tbb TBB::tbbmalloc)

install(TARGETS emds DESTINATION .)
install(DIRECTORY ${CMAKE_SOURCE_DIR}/config/ DESTINATION config)
install(FILES ${CMAKE_SOURCE_DIR}/script.py DESTINATION .
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)




