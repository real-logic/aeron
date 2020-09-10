#
# Copyright 2014-2020 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 3.1.3 FATAL_ERROR)
cmake_policy(VERSION 3.1.3)

file(STRINGS version.txt AERON_VERSION_TXT LIMIT_COUNT 1 REGEX "^[0-9]+(\\.[0-9]+)+")
string(REGEX REPLACE "^([0-9]+(\\.[0-9]+)+).*$" "\\1" AERON_VERSION_FROM_FILE "${AERON_VERSION_TXT}")

if (${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif ()

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
    set(STANDALONE_BUILD TRUE)
endif ()

option(BUILD_AERON_DRIVER "Build Aeron driver" ON)
option(BUILD_AERON_ARCHIVE_API "Build Aeron Archive API" OFF)

option(C_WARNINGS_AS_ERRORS "Enable warnings as errors for C" OFF)
option(CXX_WARNINGS_AS_ERRORS "Enable warnings as errors for C++" OFF)
option(SANITISE_BUILD "Enable sanitise options" OFF)
option(COVERAGE_BUILD "Enable code coverage" OFF)
option(AERON_TESTS "Enable tests" ${STANDALONE_BUILD})
option(AERON_SYSTEM_TESTS "Enable system tests" ${STANDALONE_BUILD})
option(AERON_SLOW_SYSTEM_TESTS "Enable slow system tests" OFF)
option(AERON_BUILD_SAMPLES "Enable building the sample projects" ${STANDALONE_BUILD})
option(LINK_SAMPLES_CLIENT_SHARED "Enable shared linking for sample projects" OFF)
option(AERON_BUILD_DOCUMENTATION "Build Aeron documentation" ${STANDALONE_BUILD})
option(AERON_INSTALL_TARGETS "Enable installation step" ${STANDALONE_BUILD})
if (UNIX)
    option(AERON_ENABLE_NONSTANDARD_OPTIMIZATIONS "Enable Ofast for release builds" ${STANDALONE_BUILD})
endif ()

unset(STANDALONE_BUILD)

include(ExternalProject)

project("aeron" VERSION "${AERON_VERSION_FROM_FILE}")

if (AERON_TESTS)
    enable_testing()
    include(CTest)
endif ()

# default built type is Release
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif (NOT CMAKE_BUILD_TYPE)

set(AERON_THIRDPARTY_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty")

##########################################################
# gmock usage

if (AERON_TESTS)
    ExternalProject_Add(
        gmock
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/googletest-release-1.10.0.zip
        URL_MD5 82358affdd7ab94854c8ee73a180fc53
        PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/gmock"
        CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER};-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER};-DCMAKE_CXX_FLAGS=-D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING
        BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/gmock/src/gmock-build/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX};${AERON_THIRDPARTY_BINARY_DIR}/gmock/src/gmock-build/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
        INSTALL_COMMAND ""
    )

    ExternalProject_Get_Property(gmock source_dir)
    set(GMOCK_SOURCE_DIR ${source_dir})
    ExternalProject_Get_Property(gmock binary_dir)
    set(GMOCK_BINARY_DIR ${binary_dir})

    if (MSVC AND CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(GMOCK_POSTFIX "d")
    else ()
        set(GMOCK_POSTFIX "")
    endif ()

    set(GMOCK_LIBS
        ${GMOCK_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${GMOCK_POSTFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${GMOCK_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${GMOCK_POSTFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${GMOCK_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${GMOCK_POSTFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endif ()

##########################################################
# HdrHistogram usage - use MD5 as means to identify snapshot

if (AERON_BUILD_SAMPLES)
    ExternalProject_Add(
        hdr_histogram
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/HdrHistogram_c-0.11.0.zip
        URL_MD5 ed24e82b10c91fe32efefe0ac71b4763
        CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER};-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER};-DCMAKE_C_STANDARD=99;-DCMAKE_PREFIX_PATH=${CMAKE_CURRENT_BINARY_DIR}/zlib64;-DCMAKE_IGNORE_PATH="optimized;C:/Program Files/PostgreSQL/12/lib"
        PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram"
        BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram/src/hdr_histogram-build/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}"
        INSTALL_COMMAND ""
    )

    ExternalProject_Get_Property(hdr_histogram source_dir)
    set(HDRHISTOGRAM_SOURCE_DIR ${source_dir})
    ExternalProject_Get_Property(hdr_histogram binary_dir)
    set(HDRHISTOGRAM_BINARY_DIR ${binary_dir})

    set(HDRHISTOGRAM_LIBS
        ${HDRHISTOGRAM_BINARY_DIR}/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endif ()

unset(AERON_THIRDPARTY_BINARY_DIR)

##########################################################
# Platform flags, etc.

find_package(Threads)

##########################################################
# Doxygen for generating doc

if (AERON_BUILD_DOCUMENTATION)
    find_package(Doxygen)
endif ()

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_CXX_EXTENSIONS)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

if (NOT DEFINED CMAKE_C_STANDARD)
    set(CMAKE_C_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_C_EXTENSIONS)
    set(CMAKE_C_EXTENSIONS OFF)
endif ()

add_definitions(-DAERON_VERSION_TXT="${AERON_VERSION_TXT}")
add_definitions(-DAERON_VERSION_MAJOR=${aeron_VERSION_MAJOR})
add_definitions(-DAERON_VERSION_MINOR=${aeron_VERSION_MINOR})
add_definitions(-DAERON_VERSION_PATCH=${aeron_VERSION_PATCH})

# all UNIX-based platform compiler flags
if (UNIX)
    add_compile_options(-Wall -Wpedantic -Wextra -Wno-unused-parameter)

    if (C_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:C>:-Werror>)
    endif (C_WARNINGS_AS_ERRORS)

    if (CXX_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Werror>)
    endif (CXX_WARNINGS_AS_ERRORS)

    if (SANITISE_BUILD)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
    endif (SANITISE_BUILD)

    if (COVERAGE_BUILD)
        add_compile_options(-O0 -fno-inline --coverage -g)
        SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_fEXE_LINKER_FLAGS} --coverage")
    endif (COVERAGE_BUILD)

    if (AERON_ENABLE_NONSTANDARD_OPTIMIZATIONS)
        add_compile_options($<$<CONFIG:Release>:-Ofast>)
    endif ()
endif ()

# platform specific flags
if (APPLE)
    add_compile_options(-Wsign-compare)
    add_definitions(-DDarwin)
    add_compile_options(-Wno-deprecated-register)
elseif (CYGWIN)
    add_definitions(-DWIN32)
    if (AERON_TESTS)
        add_definitions(-DGTEST_HAS_PTHREAD)
    endif ()
    set(CMAKE_CXX_EXTENSIONS ON)
elseif (MSVC)
    add_definitions(-DWIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-D_CRT_NONSTDC_NO_WARNINGS)
    add_definitions(-DNOMINMAX)

    if (${MSVC_VERSION} GREATER_EQUAL 1915)
        # Acknowledge that we understand MSVC resolved a byte alignment issue in this compiler
        add_definitions(-D_ENABLE_EXTENDED_ALIGNED_STORAGE)
    endif ()

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd /Od /Zi /MP")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT /MP")

    if (C_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:C>:/WX>)
    endif (C_WARNINGS_AS_ERRORS)

    if (CXX_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/WX>)
    endif (CXX_WARNINGS_AS_ERRORS)
endif ()


##########################################################
# Project variables, etc.

if (MSVC)
    set(GRADLE_WRAPPER "gradlew.bat" CACHE INTERNAL "Location of the Gradle wrapper script")
else ()
    set(GRADLE_WRAPPER "./gradlew" CACHE INTERNAL "Location of the Gradle wrapper script")
endif ()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")

if (AERON_BUILD_SAMPLES)
    set(AERON_SAMPLES_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-samples/src/main/cpp")
    set(AERON_C_SAMPLES_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-samples/src/main/c")
endif ()

set(AERON_CLIENT_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/cpp")

set(AERON_CLIENT_WRAPPER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/cpp_wrapper")

set(AERON_C_CLIENT_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/c")

set(AERON_DRIVER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/main/c")

set(AERON_ARCHIVE_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/cpp")

if (AERON_TESTS)
    set(AERON_CLIENT_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/cpp")
    set(AERON_DRIVER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/test/c")
    set(AERON_C_CLIENT_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/c")
    set(AERON_CLIENT_WRAPPER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/cpp_wrapper")
    set(AERON_ARCHIVE_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/test/cpp")
    set(AERON_SYSTEM_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-system-tests")

    # gmock - includes gtest
    include_directories(${GMOCK_SOURCE_DIR}/googletest/include)
    include_directories(${GMOCK_SOURCE_DIR}/googlemock/include)
endif ()

if (AERON_BUILD_SAMPLES)
    # hdr_histogram
    include_directories(${HDRHISTOGRAM_SOURCE_DIR}/src)
endif ()

##########################################################

add_definitions(-D_FILE_OFFSET_BITS=64)

add_subdirectory(${AERON_C_CLIENT_SOURCE_PATH})

add_subdirectory(${AERON_CLIENT_SOURCE_PATH})
add_subdirectory(${AERON_CLIENT_WRAPPER_SOURCE_PATH})

if (AERON_TESTS)
    add_subdirectory(${AERON_CLIENT_TEST_PATH})
    add_subdirectory(${AERON_C_CLIENT_TEST_PATH})
    add_subdirectory(${AERON_CLIENT_WRAPPER_TEST_PATH})
endif ()
if (AERON_BUILD_SAMPLES)
    add_subdirectory(${AERON_SAMPLES_PATH})
    add_subdirectory(${AERON_C_SAMPLES_PATH})
endif ()

if (BUILD_AERON_DRIVER)
    add_subdirectory(${AERON_DRIVER_SOURCE_PATH})
    if (AERON_TESTS)
        add_subdirectory(${AERON_DRIVER_TEST_PATH})
        add_subdirectory(${AERON_SYSTEM_TEST_PATH})
    endif ()
endif (BUILD_AERON_DRIVER)

if (BUILD_AERON_ARCHIVE_API)
    set(ARCHIVE_CODEC_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
    set(ARCHIVE_CODEC_SCHEMA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/resources/archive")
    set(ARCHIVE_CODEC_WORKING_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

    add_subdirectory(${AERON_ARCHIVE_SOURCE_PATH})
    set(AERON_ALL_JAR "${CMAKE_CURRENT_SOURCE_DIR}/aeron-all/build/libs/aeron-all-${AERON_VERSION_TXT}.jar")

    file(GLOB_RECURSE AERON_ALL_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}}/aeron-archive/src/main/java/*.java")

    add_custom_command(OUTPUT ${AERON_ALL_JAR}
        COMMAND ${GRADLE_WRAPPER} :aeron-all:clean :aeron-all:assemble --no-daemon -x javadoc --console=plain -q
        DEPENDS ${AERON_ALL_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Generating aeron-all jar")

    add_custom_target(aeron-all-jar
        DEPENDS ${AERON_ALL_JAR})

    if (AERON_TESTS)
        add_subdirectory(${AERON_ARCHIVE_TEST_PATH})
    endif ()
endif (BUILD_AERON_ARCHIVE_API)
##########################################################
# doc target

if (AERON_BUILD_DOCUMENTATION AND DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)

    add_custom_target(
        doc
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )

    if (AERON_INSTALL_TARGETS)
        # install the doc if it has been built
        install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doc DESTINATION share OPTIONAL)
    endif ()
endif ()

##########################################################
# package target

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VERSION_MAJOR "${aeron_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${aeron_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${aeron_VERSION_PATCH}")

set(CPACK_GENERATOR "TGZ;STGZ")
include(CPack)
