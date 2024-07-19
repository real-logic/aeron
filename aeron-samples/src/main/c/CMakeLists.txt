#
# Copyright 2014-2024 Real Logic Limited.
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

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif ()

if (LINK_SAMPLES_CLIENT_SHARED OR (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows"))
    set(CLIENT_LINK_LIB "aeron")
    add_definitions(-DCLIENT_SHARED)
else()
    set(CLIENT_LINK_LIB "aeron_static")
endif ()

include_directories(${AERON_CLIENT_SOURCE_PATH})

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

set(SOURCES
    sample_util.c)

set(HEADERS
    samples_configuration.h
    sample_util.h)

add_executable(archive_test archive_test.c ${HEADERS} ${SOURCES})
add_executable(basic_publisher basic_publisher.c ${HEADERS} ${SOURCES})
add_executable(basic_subscriber basic_subscriber.c ${HEADERS} ${SOURCES})
add_executable(basic_mds_subscriber basic_mds_subscriber.c ${HEADERS} ${SOURCES})
add_executable(rate_subscriber rate_subscriber.c ${HEADERS} ${SOURCES})
add_executable(streaming_publisher streaming_publisher.c ${HEADERS} ${SOURCES})
add_executable(streaming_exclusive_publisher streaming_exclusive_publisher.c ${HEADERS} ${SOURCES})
add_executable(cpong cpong.c ${HEADERS} ${SOURCES})
add_executable(cping cping.c ${HEADERS} ${SOURCES})
add_executable(response_client response/response_client.c ${HEADERS} ${SOURCES})
add_executable(response_server response/response_server.c ${HEADERS} ${SOURCES})

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    add_executable(ping_pong_raw raw/ping_pong_raw.c)
    add_dependencies(ping_pong_raw hdr_histogram)
    target_link_libraries(ping_pong_raw m hdr_histogram_static)
endif ()

target_include_directories(archive_test
    PUBLIC ${AERON_C_ARCHIVE_SOURCE_PATH})
target_link_libraries(archive_test
    ${CLIENT_LINK_LIB}
    "aeron_archive_c_client")

target_link_libraries(basic_publisher
    ${CLIENT_LINK_LIB})

target_link_libraries(basic_subscriber
    ${CLIENT_LINK_LIB})

target_link_libraries(basic_mds_subscriber
    ${CLIENT_LINK_LIB})

target_link_libraries(rate_subscriber
    ${CLIENT_LINK_LIB})

target_link_libraries(streaming_publisher
    ${CLIENT_LINK_LIB})

target_link_libraries(streaming_exclusive_publisher
    ${CLIENT_LINK_LIB})

target_link_libraries(cpong
    ${CLIENT_LINK_LIB})

target_link_libraries(cping
    ${CLIENT_LINK_LIB}
    hdr_histogram_static)

target_link_libraries(response_client
    ${CLIENT_LINK_LIB})

target_link_libraries(response_server
    ${CLIENT_LINK_LIB})

add_dependencies(cping hdr_histogram)

add_executable(AeronStat aeron_stat.c ${HEADERS})
target_link_libraries(AeronStat ${CLIENT_LINK_LIB})

add_executable(DriverTool driver_tool.c ${HEADERS})
target_link_libraries(DriverTool ${CLIENT_LINK_LIB})

add_executable(ErrorStat error_stat.c ${HEADERS})
target_link_libraries(ErrorStat ${CLIENT_LINK_LIB})

add_executable(LossStat loss_stat.c ${HEADERS})
target_link_libraries(LossStat ${CLIENT_LINK_LIB})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS
        archive_test
        basic_publisher
        basic_subscriber
        rate_subscriber
        streaming_publisher
        streaming_exclusive_publisher
        cpong
        cping
        response_client
        response_server
        AeronStat
        DriverTool
        ErrorStat
        LossStat
        DESTINATION bin)
endif ()
