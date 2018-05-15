#
# Copyright 2014-2018 Real Logic Ltd.
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
cmake_minimum_required(VERSION 3.0.2 FATAL_ERROR)
cmake_policy(VERSION 3.0.2)

file(STRINGS version.txt VERSION_TXT LIMIT_COUNT 1 REGEX "^[0-9]+(\\.[0-9]+)+")
string(REGEX REPLACE "^([0-9]+(\\.[0-9]+)+).*$" "\\1" VERSION_FROM_FILE "${VERSION_TXT}")

if (${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif()

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
   set(STANDALONE_BUILD TRUE)
endif()

option(BUILD_AERON_DRIVER "Build Aeron driver" OFF)

option(C_WARNINGS_AS_ERRORS "Enable warnings as errors for C" OFF)
option(CXX_WARNINGS_AS_ERRORS "Enable warnings as errors for C++" OFF)
option(SANITISE_BUILD "Enable sanitise options" OFF)
option(COVERAGE_BUILD "Enable code coverage" OFF)
option(AERON_TESTS "Enable tests" ${STANDALONE_BUILD})
option(AERON_BUILD_SAMPLES "Enable building the sample projects" ${STANDALONE_BUILD})

unset(STANDALONE_BUILD)

include(ExternalProject)

project("aeron" VERSION "${VERSION_FROM_FILE}")

if(AERON_TESTS)
    enable_testing()
    include(CTest)
endif()

# default built type is Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif(NOT CMAKE_BUILD_TYPE)

set(AERON_THIRDPARTY_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty")

##########################################################
# gmock usage

if(AERON_TESTS)
   ExternalProject_Add(
       gmock
       URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/googletest-release-1.8.0.zip
       URL_MD5 adfafc8512ab65fd3cf7955ef0100ff5
       PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/gmock"
       BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/gmock/src/gmock-build/googlemock/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX};${AERON_THIRDPARTY_BINARY_DIR}/gmock/src/gmock-build/googlemock/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
       INSTALL_COMMAND ""
   )

    ExternalProject_Get_Property(gmock source_dir)
    set(GMOCK_SOURCE_DIR ${source_dir})
    ExternalProject_Get_Property(gmock binary_dir)
    set(GMOCK_BINARY_DIR ${binary_dir})

    set(GMOCK_LIBS
        ${GMOCK_BINARY_DIR}/googlemock/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${GMOCK_BINARY_DIR}/googlemock/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endif()

##########################################################
# HdrHistogram usage - use MD5 as means to identify snapshot

if(AERON_BUILD_SAMPLES)
    ExternalProject_Add(
        hdr_histogram
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/HdrHistogram_c.zip
        URL_MD5 8d1d6c6df1ee8342c180529a89568081
        PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram"
        BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram/src/hdr_histogram-build/src/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}"
        INSTALL_COMMAND ""
    )

    ExternalProject_Get_Property(hdr_histogram source_dir)
    set(HDRHISTOGRAM_SOURCE_DIR ${source_dir})
    ExternalProject_Get_Property(hdr_histogram binary_dir)
    set(HDRHISTOGRAM_BINARY_DIR ${binary_dir})

    set(HDRHISTOGRAM_LIBS
        ${HDRHISTOGRAM_BINARY_DIR}/src/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endif()

##########################################################
# Google benchmark

if(AERON_TESTS)
    ExternalProject_Add(
        google_benchmark
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/benchmark-1.4.0.tar.gz
        URL_MD5 ccfaf2cd93ae20191b94f730b945423e
        CMAKE_ARGS -DBENCHMARK_ENABLE_GTEST_TESTS=OFF;-DBENCHMARK_ENABLE_ASSEMBLY_TESTS=OFF
        PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/google_benchmark"
        BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/google_benchmark/src/google_benchmark-build/src/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
        INSTALL_COMMAND ""
    )

    ExternalProject_Get_Property(google_benchmark source_dir)
    set(GOOGLE_BENCHMARK_SOURCE_DIR ${source_dir})
    ExternalProject_Get_Property(google_benchmark binary_dir)
    set(GOOGLE_BENCHMARK_BINARY_DIR ${binary_dir})

    set(
        GOOGLE_BENCHMARK_LIBS
        ${GOOGLE_BENCHMARK_BINARY_DIR}/src/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endif()

unset(AERON_THIRDPARTY_BINARY_DIR)

##########################################################
# Platform flags, etc.

find_package(Threads)

##########################################################
# Doxygen for generating doc

find_package(Doxygen)

set(CMAKE_CXX_STANDARD 11)

# all UNIX-based platform compiler flags
if(UNIX)
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wpedantic -Wextra -Wno-unused-parameter -std=c++11 -fexceptions -g -m64")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wpedantic -Wextra -Wno-unused-parameter -std=c11 -g -m64")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wpedantic -Wextra -Wno-unused-parameter -std=c++11 -fexceptions -g")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wpedantic -Wextra -Wno-unused-parameter -std=c11 -g")
    endif()

    if(C_WARNINGS_AS_ERRORS)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
    endif(C_WARNINGS_AS_ERRORS)

    if(CXX_WARNINGS_AS_ERRORS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    endif(CXX_WARNINGS_AS_ERRORS)

    if(SANITISE_BUILD)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined")
    endif(SANITISE_BUILD)

    if (COVERAGE_BUILD)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fno-inline --coverage -g")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fno-inline --coverage -g")
        SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    endif(COVERAGE_BUILD)

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Ofast")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Ofast")
endif()

# platform specific flags
if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wsign-compare")
    add_definitions(-DDarwin)
    add_definitions(-Wno-deprecated-register)
elseif(CYGWIN)
    add_definitions(-DWIN32)
    if(AERON_TESTS)
        add_definitions(-DGTEST_HAS_PTHREAD)
    endif()
    string(REPLACE "-std=c++11" "-std=gnu++11" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
elseif(MSVC)
    add_definitions(-DWIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-DNOMINMAX)

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd /Od /Zi /MP")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT /MP")

    if(C_WARNINGS_AS_ERRORS)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /WX")
    endif(C_WARNINGS_AS_ERRORS)

    if(CXX_WARNINGS_AS_ERRORS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
    endif(CXX_WARNINGS_AS_ERRORS)
endif()


##########################################################
# Project variables, etc.

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")

if(AERON_BUILD_SAMPLES)
    set(AERON_SAMPLES_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-samples/src/main/cpp")
endif()

set(AERON_CLIENT_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/cpp")

set(AERON_DRIVER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/main/c")

if(AERON_TESTS)
    set(AERON_CLIENT_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/cpp")
    set(AERON_DRIVER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/test/c")

    # gmock - includes gtest
    include_directories(${GMOCK_SOURCE_DIR}/googletest/include)
    include_directories(${GMOCK_SOURCE_DIR}/googlemock/include)
endif()

if(AERON_BUILD_SAMPLES)
   # hdr_histogram
   include_directories(${HDRHISTOGRAM_SOURCE_DIR}/src)
endif()

if(AERON_TESTS)
    # google benchmark
    include_directories(${GOOGLE_BENCHMARK_SOURCE_DIR}/include)
endif()

##########################################################

add_definitions(-D_FILE_OFFSET_BITS=64)

add_subdirectory(${AERON_CLIENT_SOURCE_PATH})
if(AERON_TESTS)
   add_subdirectory(${AERON_CLIENT_TEST_PATH})
endif()
if(AERON_BUILD_SAMPLES)
   add_subdirectory(${AERON_SAMPLES_PATH})
endif()

if(BUILD_AERON_DRIVER)
   add_subdirectory(${AERON_DRIVER_SOURCE_PATH})
   if(AERON_TESTS)
      add_subdirectory(${AERON_DRIVER_TEST_PATH})
   endif()
endif(BUILD_AERON_DRIVER)

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
