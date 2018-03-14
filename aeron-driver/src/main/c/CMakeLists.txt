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

include(CheckSymbolExists)
include(CheckIncludeFile)
include(CheckTypeSize)

if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif()

check_include_file("bsd/stdlib.h" BSDSTDLIB_H_EXISTS)
check_include_file("uuid/uuid.h" UUID_H_EXISTS)
find_library(LIBBSD_EXISTS NAMES bsd libbsd)
find_library(LIBUUID_EXISTS NAMES uuid libuuid libuuid.dll)

if(LIBBSD_EXISTS)
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} -lbsd")
endif()

if(LIBUUID_EXISTS)
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} -luuid")
endif()

if(NOT BSDSTDLIB_H_EXISTS)
    check_symbol_exists(arc4random "stdlib.h" ARC4RANDOM_PROTOTYPE_EXISTS)
else()
    add_definitions(-DHAVE_BSDSTDLIB_H)
    check_symbol_exists(arc4random "bsd/stdlib.h" ARC4RANDOM_PROTOTYPE_EXISTS)
endif()

if(UUID_H_EXISTS)
    add_definitions(-DHAVE_UUID_H)
endif()

check_symbol_exists(uuid_generate "uuid/uuid.h" UUID_GENERATE_PROTOTYPE_EXISTS)

check_symbol_exists(poll "poll.h" POLL_PROTOTYPE_EXISTS)
check_symbol_exists(epoll_create "sys/epoll.h" EPOLL_PROTOTYPE_EXISTS)

set(CMAKE_EXTRA_INCLUDE_FILES sys/socket.h)
check_type_size("struct mmsghdr" STRUCT_MMSGHDR_TYPE_EXISTS)
set(CMAKE_EXTRA_INCLUDE_FILES)

check_symbol_exists(recvmmsg "sys/socket.h" RECVMMSG_PROTOTYPE_EXISTS)
check_symbol_exists(sendmmsg "sys/socket.h" SENDMMSG_PROTOTYPE_EXISTS)
check_symbol_exists(fallocate "fcntl.h" FALLOCATE_PROTOTYPE_EXISTS)

if(ARC4RANDOM_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_ARC4RANDOM)
else()
    message(WARNING "Could not find arc4random. If on Linux, is libbsd installed?")
endif()

if(UUID_GENERATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_UUID_GENERATE)
else()
    message(WARNING "Could not find uuid_generate. If on Linux, is libuuid installed?")
endif()

if(POLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_POLL)
endif()

if(EPOLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_EPOLL)
endif()

if(STRUCT_MMSGHDR_TYPE_EXISTS)
    add_definitions(-DHAVE_STRUCT_MMSGHDR)
endif()

if(RECVMMSG_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_RECVMMSG)
endif()

if(SENDMMSG_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_SENDMMSG)
endif()

if(FALLOCATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_FALLOCATE)
endif()

SET(SOURCE
    concurrent/aeron_spsc_rb.c
    concurrent/aeron_mpsc_rb.c
    concurrent/aeron_atomic.c
    concurrent/aeron_distinct_error_log.c
    concurrent/aeron_broadcast_transmitter.c
    concurrent/aeron_counters_manager.c
    concurrent/aeron_logbuffer_descriptor.c
    concurrent/aeron_spsc_concurrent_array_queue.c
    concurrent/aeron_mpsc_concurrent_array_queue.c
    concurrent/aeron_term_scanner.c
    concurrent/aeron_term_rebuilder.c
    concurrent/aeron_term_gap_scanner.c
    concurrent/aeron_term_unblocker.c
    concurrent/aeron_logbuffer_unblocker.c
    concurrent/aeron_term_gap_filler.c
    util/aeron_strutil.c
    util/aeron_fileutil.c
    util/aeron_arrayutil.c
    util/aeron_error.c
    util/aeron_netutil.c
    aeron_driver_context.c
    aeron_alloc.c
    aeron_driver.c
    aeron_agent.c
    aeron_system_counters.c
    aeron_driver_conductor.c
    aeron_driver_sender.c
    aeron_driver_receiver.c
    aeron_ipc_publication.c
    aeron_network_publication.c
    aeron_position.c
    aeron_driver_sender_proxy.c
    aeron_driver_conductor_proxy.c
    aeron_driver_receiver_proxy.c
    aeron_flow_control.c
    aeron_data_packet_dispatcher.c
    aeron_publication_image.c
    aeron_congestion_control.c
    aeron_loss_detector.c
    aeron_retransmit_handler.c
    media/aeron_udp_channel_transport.c
    media/aeron_udp_channel.c
    media/aeron_send_channel_endpoint.c
    media/aeron_udp_transport_poller.c
    media/aeron_receive_channel_endpoint.c
    media/aeron_udp_destination_tracker.c
    uri/aeron_uri.c
    collections/aeron_int64_to_ptr_hash_map.c
    collections/aeron_str_to_ptr_hash_map.c
    reports/aeron_loss_reporter.c)

SET(HEADERS
    util/aeron_platform.h
    util/aeron_bitutil.h
    util/aeron_strutil.h
    util/aeron_fileutil.h
    util/aeron_arrayutil.h
    util/aeron_error.h
    util/aeron_netutil.h
    concurrent/aeron_atomic.h
    concurrent/aeron_atomic64_gcc_x86_64.h
    concurrent/aeron_spsc_rb.h
    concurrent/aeron_rb.h
    concurrent/aeron_mpsc_rb.h
    concurrent/aeron_distinct_error_log.h
    concurrent/aeron_broadcast_transmitter.h
    concurrent/aeron_counters_manager.h
    concurrent/aeron_logbuffer_descriptor.h
    concurrent/aeron_concurrent_array_queue.h
    concurrent/aeron_spsc_concurrent_array_queue.h
    concurrent/aeron_mpsc_concurrent_array_queue.h
    concurrent/aeron_term_scanner.h
    concurrent/aeron_term_rebuilder.h
    concurrent/aeron_term_gap_scanner.h
    concurrent/aeron_term_unblocker.h
    concurrent/aeron_logbuffer_unblocker.h
    concurrent/aeron_term_gap_filler.h
    command/aeron_control_protocol.h
    protocol/aeron_udp_protocol.h
    aeronmd.h
    aeron_driver.h
    aeron_driver_context.h
    aeron_alloc.h
    aeron_agent.h
    aeron_system_counters.h
    aeron_driver_conductor.h
    aeron_driver_sender.h
    aeron_driver_receiver.h
    aeron_driver_common.h
    aeron_ipc_publication.h
    aeron_network_publication.h
    aeron_position.h
    aeron_driver_sender_proxy.h
    aeron_driver_conductor_proxy.h
    aeron_driver_receiver_proxy.h
    aeron_flow_control.h
    aeron_data_packet_dispatcher.h
    aeron_publication_image.h
    aeron_congestion_control.h
    aeron_loss_detector.h
    aeron_retransmit_handler.h
    media/aeron_udp_channel_transport.h
    media/aeron_udp_channel.h
    media/aeron_send_channel_endpoint.h
    media/aeron_udp_transport_poller.h
    media/aeron_receive_channel_endpoint.h
    media/aeron_udp_destination_tracker.h
    uri/aeron_uri.h
    collections/aeron_int64_to_ptr_hash_map.h
    collections/aeron_str_to_ptr_hash_map.h
    reports/aeron_loss_reporter.h)

set(AGENT_SOURCE
    agent/aeron_driver_agent.c
    concurrent/aeron_mpsc_rb.c
    concurrent/aeron_atomic.c
    util/aeron_fileutil.c
    util/aeron_error.c
    aeron_alloc.c)

set(AGENT_HEADERS
    agent/aeron_driver_agent.h
    concurrent/aeron_mpsc_rb.h
    util/aeron_fileutil.h
    util/aeron_error.h
    aeron_alloc.h)

add_library(aeron_driver_agent SHARED ${AGENT_SOURCE} ${AGENT_HEADERS})
target_include_directories(aeron_driver_agent
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(aeron_driver SHARED ${SOURCE} ${HEADERS})
target_include_directories(aeron_driver
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
add_executable(aeronmd aeronmd.c)
target_include_directories(aeronmd
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    if(LIBBSD_EXISTS)
        set(AERON_LIB_BSD_LIBS bsd)
    endif()

    if(LIBUUID_EXISTS)
        set(AERON_LIB_UUID_LIBS uuid)
    endif()

    set(AERON_LIB_M_LIBS m)
endif()

if(CYGWIN)
    if(LIBUUID_EXISTS)
        set(AERON_LIB_UUID_LIBS uuid)
    endif()
endif()

target_link_libraries(
    aeron_driver
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_UUID_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(
    aeronmd
    aeron_driver
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_UUID_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(
    aeron_driver_agent
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_UUID_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT})

install(
    TARGETS aeron_driver aeron_driver_agent
    RUNTIME DESTINATION lib
    LIBRARY DESTINATION lib)
install(TARGETS aeronmd DESTINATION bin)
install(DIRECTORY . DESTINATION  include/aeronmd FILES_MATCHING PATTERN "*.h")
