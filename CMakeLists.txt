#
# Copyright 2014 Real Logic Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project("aeron")

enable_testing()

##########################################################
# Platform flags, etc.

# all UNIX-based platform compiler flags
if(UNIX)
    set(CMAKE_CXX_FLAGS "-Wall -std=c++11 -fexceptions -pthread -g -m64")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0")
    set(CMAKE_CXX_FLAGS_RELEASE "-Os")
endif()

# platform specific flags
if(APPLE)
    add_definitions(-DDarwin)
elseif(WIN32)
    add_definitions(-DWIN32)

    set(CMAKE_CXX_FLAGS_DEBUG "/MTd /Od /ZI")
    set(CMAKE_CXX_FLAGS_RELEASE "/MT")

    # TODO: add MSVC flags
endif()

##########################################################
# Project variables, etc.

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/cppbuild/binaries")

set(AERON_COMMON_PATH "${CMAKE_SOURCE_DIR}/aeron-common/src/main/cpp")
set(AERON_3RDPARTY_PATH "${AERON_COMMON_PATH}/3rdparty")
set(AERON_SAMPLES_PATH "${CMAKE_SOURCE_DIR}/aeron-samples/src/main/cpp")

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
add_subdirectory(${AERON_SAMPLES_PATH})



