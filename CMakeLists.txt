#
# Copyright 2014 - 2015 Real Logic Ltd.
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

include(ExternalProject)

project("aeron")

enable_testing()

# default built type is Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif(NOT CMAKE_BUILD_TYPE)

##########################################################
# gmock usage

ExternalProject_Add(
    gmock
    URL ${CMAKE_CURRENT_SOURCE_DIR}/aeron-common/src/main/cpp/3rdparty/gmock-1.7.0.zip
    URL_MD5 073b984d8798ea1594f5e44d85b20d66
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(gmock source_dir)
set(GMOCK_SOURCE_DIR ${source_dir})
ExternalProject_Get_Property(gmock binary_dir)
set(GMOCK_BINARY_DIR ${binary_dir})

set(GMOCK_LIBS
    ${GMOCK_BINARY_DIR}/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${GMOCK_BINARY_DIR}/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}
)

##########################################################
# Platform flags, etc.

find_package(Threads)

# all UNIX-based platform compiler flags
if(UNIX)
    set(CMAKE_CXX_FLAGS "-Wall -std=c++11 -fexceptions -g -m64")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0")
    set(CMAKE_CXX_FLAGS_RELEASE "-Ofast")
endif()

# platform specific flags
if(APPLE)
    add_definitions(-DDarwin)
    add_definitions(-Wno-deprecated-register)
elseif(WIN32)
    add_definitions(-DWIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-DNOMINMAX)

    set(CMAKE_CXX_FLAGS_DEBUG "/MTd /Od /Zi")
    set(CMAKE_CXX_FLAGS_RELEASE "/MT")

    # TODO: add MSVC flags
endif()

##########################################################
# Project variables, etc.

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/cppbuild/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/cppbuild/lib")

set(AERON_COMMON_PATH "${CMAKE_SOURCE_DIR}/aeron-common/src/main/cpp")
set(AERON_3RDPARTY_PATH "${AERON_COMMON_PATH}/3rdparty")
set(AERON_SAMPLES_PATH "${CMAKE_SOURCE_DIR}/aeron-samples/src/main/cpp")
set(AERON_CLIENT_SOURCE_PATH "${CMAKE_SOURCE_DIR}/aeron-client/src/main/cpp")
set(AERON_CLIENT_TEST_PATH "${CMAKE_SOURCE_DIR}/aeron-client/src/test/cpp")

##########################################################
# 3rd party libs

# gmock - includes gtest
include_directories(${GMOCK_SOURCE_DIR}/include)
include_directories(${GMOCK_SOURCE_DIR}/gtest/include)

##########################################################

include_directories(${AERON_COMMON_PATH} ${AERON_CLIENT_SOURCE_PATH})

add_subdirectory(${CMAKE_SOURCE_DIR}/aeron-common/src/main/cpp)
add_subdirectory(${CMAKE_SOURCE_DIR}/aeron-common/src/test/cpp)
add_subdirectory(${AERON_CLIENT_SOURCE_PATH})
add_subdirectory(${AERON_CLIENT_TEST_PATH})
add_subdirectory(${AERON_SAMPLES_PATH})


