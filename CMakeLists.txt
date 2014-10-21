cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project("aeron")

enable_testing()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/cppbuild/binaries")

# TODO: add MSVC flags
set(CMAKE_CXX_FLAGS "-Wall -std=c++11 -fexceptions -pthread -g -m64")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_RELEASE "-Os")

set(AERON_COMMON_PATH "${CMAKE_SOURCE_DIR}/aeron-common/src/main/cpp")
set(AERON_3RDPARTY_PATH "${AERON_COMMON_PATH}/3rdparty")
set(AERON_EXAMPLES_PATH "${CMAKE_SOURCE_DIR}/aeron-examples/src/main/cpp")

##########################################################
# 3rd party libs

# gmock - includes gtest
include_directories(${AERON_3RDPARTY_PATH}/gmock-1.7.0/include)
include_directories(${AERON_3RDPARTY_PATH}/gmock-1.7.0/gtest/include)
add_subdirectory(${AERON_3RDPARTY_PATH}/gmock-1.7.0)

# mintomic - header only
include_directories(${AERON_3RDPARTY_PATH}/mintomic/include)

##########################################################

include_directories(${AERON_COMMON_PATH})

add_subdirectory(${AERON_COMMON_PATH}/concurrent)
add_subdirectory(${AERON_COMMON_PATH}/util)
add_subdirectory(${AERON_EXAMPLES_PATH})



