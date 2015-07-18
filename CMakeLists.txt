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
cmake_minimum_required(VERSION 2.8.4 FATAL_ERROR)

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif()

include(ExternalProject)

project("aeron")

# version info
set(aeron_VERSION_MAJOR 0)
set(aeron_VERSION_MINOR 1)
set(aeron_VERSION_PATCH 0)

enable_testing()

# default built type is Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif(NOT CMAKE_BUILD_TYPE)

##########################################################
# gmock usage

ExternalProject_Add(
    gmock
    URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/gmock-1.7.0.zip
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
# HdrHistogram usage - use MD5 as means to identify snapshot

ExternalProject_Add(
    hdr_histogram
    URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/HdrHistogram_c-master.zip
    URL_MD5 0c89f11747433c1b00a364aca45f8fe5
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(hdr_histogram source_dir)
set(HDRHISTOGRAM_SOURCE_DIR ${source_dir})
ExternalProject_Get_Property(hdr_histogram binary_dir)
set(HDRHISTOGRAM_BINARY_DIR ${binary_dir})

set(HDRHISTOGRAM_LIBS
    ${HDRHISTOGRAM_BINARY_DIR}/src/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}
)

##########################################################
# Platform flags, etc.

find_package(Threads)

##########################################################
# Doxygen for generating doc

find_package(Doxygen)

# all UNIX-based platform compiler flags
if(UNIX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fexceptions -g -m64")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Ofast -DDISABLE_BOUNDS_CHECKS")
endif()

# platform specific flags
if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare")
    add_definitions(-DDarwin)
    add_definitions(-Wno-deprecated-register)
elseif(CYGWIN)
    add_definitions(-DWIN32)
    add_definitions(-DGTEST_HAS_PTHREAD)
    string(REPLACE "-std=c++11" "-std=gnu++11" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
elseif(MSVC)
    add_definitions(-DWIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-DNOMINMAX)

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd /Od /Zi")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT -DDISABLE_BOUNDS_CHECKS")
endif()

##########################################################
# Project variables, etc.

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")

set(AERON_SAMPLES_PATH "${CMAKE_SOURCE_DIR}/aeron-samples/src/main/cpp")
set(AERON_CLIENT_SOURCE_PATH "${CMAKE_SOURCE_DIR}/aeron-client/src/main/cpp")
set(AERON_CLIENT_TEST_PATH "${CMAKE_SOURCE_DIR}/aeron-client/src/test/cpp")

# gmock - includes gtest
include_directories(${GMOCK_SOURCE_DIR}/include)
include_directories(${GMOCK_SOURCE_DIR}/gtest/include)

# hdr_histogram
include_directories(${HDRHISTOGRAM_SOURCE_DIR}/src)

##########################################################

include_directories(${AERON_CLIENT_SOURCE_PATH})

add_subdirectory(${AERON_CLIENT_SOURCE_PATH})
add_subdirectory(${AERON_CLIENT_TEST_PATH})
add_subdirectory(${AERON_SAMPLES_PATH})

##########################################################
# doc target

if(DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)

    add_custom_target(
        doc
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )

    # install the doc if it has been built
    install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doc DESTINATION share OPTIONAL)
endif(DOXYGEN_FOUND)

##########################################################
# package target

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VERSION_MAJOR "${aeron_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${aeron_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${aeron_VERSION_PATCH}")

set(CPACK_GENERATOR "TGZ;STGZ")
include(CPack)
