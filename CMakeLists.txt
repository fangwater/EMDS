cmake_minimum_required(VERSION 3.22)
set(CMAKE_C_COMPILER "/usr/local/bin/gcc")
set(CMAKE_CXX_COMPILER "/usr/local/bin/g++")
SET(CMAKE_TOOLCHAIN_FILE "/home/fanghaizhou/lib/vcpkg/scripts/buildsystems/vcpkg.cmake")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
project(EMDS)
set(CMAKE_CXX_STANDARD 20)
SET(CMAKE_BUILD_TYPE "Debug")
# SET(CMAKE_BUILD_TYPE "Release")
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

find_package(OpenMP)
if(OPENMP_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

include_directories(${CMAKE_SOURCE_DIR}/src/cache_layer)
include_directories(${CMAKE_SOURCE_DIR}/src/collect_layer)
include_directories(${CMAKE_SOURCE_DIR}/src/common)
add_executable(emds src/test_collector.cc)
# if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
#     target_compile_options(example PRIVATE -O3 -march=native -fopt-info-vec-optimized)
# elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
#     target_compile_options(example PRIVATE -O3 -march=native -Rpass=loop-vectorize -Rpass-analysis=loop-vectorize -Rpass-missed=loop-vectorize)
# endif ()

# if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
#     target_compile_options(filter_sz PRIVATE -O3 -march=native -fopt-info-vec-optimized)
# elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
#     target_compile_options(filter_sz PRIVATE -O3 -march=native)
# endif ()

target_link_libraries(emds ${absl_LIBRARIES})
target_link_libraries(emds fmt::fmt)
target_link_libraries(emds "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
target_link_libraries(emds "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")
target_link_libraries(emds Folly::folly Folly::folly_deps Folly::follybenchmark Folly::folly_test_util)
target_link_libraries(emds xsimd)
target_link_libraries(emds cppzmq cppzmq-static)
target_link_libraries(emds nlohmann_json::nlohmann_json)
target_link_libraries(emds spdlog::spdlog)


