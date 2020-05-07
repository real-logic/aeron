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

include(CheckSymbolExists)
include(CheckIncludeFile)
include(CheckTypeSize)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif ()

find_library(LIBBSD_EXISTS NAMES bsd libbsd)

if (LIBBSD_EXISTS)
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} -lbsd")
endif ()

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
    set(WSAPOLL_PROTOTYPE_EXISTS True)
endif ()

check_symbol_exists(poll "poll.h" POLL_PROTOTYPE_EXISTS)
check_symbol_exists(epoll_create "sys/epoll.h" EPOLL_PROTOTYPE_EXISTS)

set(CMAKE_EXTRA_INCLUDE_FILES sys/socket.h)
check_type_size("struct mmsghdr" STRUCT_MMSGHDR_TYPE_EXISTS)
set(CMAKE_EXTRA_INCLUDE_FILES)

check_symbol_exists(recvmmsg "sys/socket.h" RECVMMSG_PROTOTYPE_EXISTS)
check_symbol_exists(sendmmsg "sys/socket.h" SENDMMSG_PROTOTYPE_EXISTS)
check_symbol_exists(fallocate "fcntl.h" FALLOCATE_PROTOTYPE_EXISTS)

if (POLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_POLL)
endif ()

if (EPOLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_EPOLL)
endif ()

if (WSAPOLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_WSAPOLL)
endif ()

if (NOT POLL_PROTOTYPE_EXISTS AND NOT EPOLL_PROTOTYPE_EXISTS AND NOT WSAPOLL_PROTOTYPE_EXISTS)
    message(FATAL_ERROR "Unsupported configuration: neither POLL nor EPOLL nor WSAPoll found")
endif ()

if (STRUCT_MMSGHDR_TYPE_EXISTS)
    add_definitions(-DHAVE_STRUCT_MMSGHDR)
endif ()

if (RECVMMSG_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_RECVMMSG)
endif ()

if (SENDMMSG_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_SENDMMSG)
endif ()

if (FALLOCATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_FALLOCATE)
endif ()

SET(SOURCE
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_spsc_rb.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_mpsc_rb.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_atomic.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_distinct_error_log.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_broadcast_transmitter.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_counters_manager.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_logbuffer_descriptor.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_spsc_concurrent_array_queue.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_mpsc_concurrent_array_queue.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_scanner.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_rebuilder.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_gap_scanner.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_unblocker.c
    concurrent/aeron_logbuffer_unblocker.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_gap_filler.c
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_thread.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_strutil.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_fileutil.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_arrayutil.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_error.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_math.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_netutil.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_dlopen.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_parse_util.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_properties_util.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_http_util.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_clock.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_env.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_uuid.c
    ${AERON_C_CLIENT_SOURCE_PATH}/util/randombytes.c
    aeron_driver_context.c
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_alloc.c
    aeron_driver.c
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_agent.c
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_socket.c
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
    aeron_min_flow_control.c
    aeron_data_packet_dispatcher.c
    aeron_publication_image.c
    aeron_congestion_control.c
    aeron_loss_detector.c
    aeron_retransmit_handler.c
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_windows.c
    aeron_termination_validator.c
    aeron_name_resolver.c
    aeron_csv_table_name_resolver.c
    aeron_driver_name_resolver.c
    aeron_name_resolver_cache.c
    media/aeron_udp_channel_transport.c
    media/aeron_udp_channel.c
    media/aeron_send_channel_endpoint.c
    media/aeron_udp_transport_poller.c
    media/aeron_receive_channel_endpoint.c
    media/aeron_udp_destination_tracker.c
    media/aeron_udp_channel_transport_bindings.c
    media/aeron_udp_channel_transport_loss.c
    media/aeron_receive_destination.c
    ${AERON_C_CLIENT_SOURCE_PATH}/protocol/aeron_udp_protocol.c
    uri/aeron_uri.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_map.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_int64_to_ptr_hash_map.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_int64_to_tagged_ptr_hash_map.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_str_to_ptr_hash_map.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_bit_set.c
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_int64_counter_map.c
    reports/aeron_loss_reporter.c
    agent/aeron_driver_agent.c)

SET(HEADERS
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_socket.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_dlopen.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_platform.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_bitutil.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_math.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_strutil.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_fileutil.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_arrayutil.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_error.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_netutil.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_parse_util.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_properties_util.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_http_util.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_clock.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_env.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/aeron_uuid.h
    ${AERON_C_CLIENT_SOURCE_PATH}/util/randombytes.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_thread.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_atomic.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_atomic64_gcc_x86_64.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_atomic64_msvc.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_spsc_rb.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_rb.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_mpsc_rb.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_distinct_error_log.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_broadcast_transmitter.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_counters_manager.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_logbuffer_descriptor.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_concurrent_array_queue.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_spsc_concurrent_array_queue.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_mpsc_concurrent_array_queue.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_scanner.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_rebuilder.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_gap_scanner.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_unblocker.h
    concurrent/aeron_logbuffer_unblocker.h
    ${AERON_C_CLIENT_SOURCE_PATH}/concurrent/aeron_term_gap_filler.h
    ${AERON_C_CLIENT_SOURCE_PATH}/command/aeron_control_protocol.h
    ${AERON_C_CLIENT_SOURCE_PATH}/protocol/aeron_udp_protocol.h
    aeronmd.h
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_windows.h
    aeron_driver.h
    aeron_driver_context.h
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_alloc.h
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_agent.h
    ${AERON_C_CLIENT_SOURCE_PATH}/aeron_cnc_file_descriptor.h
    aeron_system_counters.h
    aeron_driver_conductor.h
    aeron_driver_sender.h
    aeron_driver_receiver.h
    aeron_driver_common.h
    aeron_ipc_publication.h
    aeron_name_resolver.h
    aeron_driver_name_resolver.h
    aeron_name_resolver_cache.h
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
    aeron_termination_validator.h
    media/aeron_udp_channel_transport.h
    media/aeron_udp_channel.h
    media/aeron_send_channel_endpoint.h
    media/aeron_udp_transport_poller.h
    media/aeron_receive_channel_endpoint.h
    media/aeron_udp_destination_tracker.h
    media/aeron_udp_channel_transport_bindings.h
    media/aeron_udp_channel_transport_loss.h
    media/aeron_receive_destination.h
    uri/aeron_uri.h
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_map.h
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_int64_to_ptr_hash_map.h
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_str_to_ptr_hash_map.h
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_bit_set.h
    ${AERON_C_CLIENT_SOURCE_PATH}/collections/aeron_int64_counter_map.h
    reports/aeron_loss_reporter.h
    agent/aeron_driver_agent.h)

add_library(aeron_driver SHARED ${SOURCE} ${HEADERS})
target_include_directories(aeron_driver
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${AERON_C_CLIENT_SOURCE_PATH}
    )
set_target_properties(aeron_driver
    PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)

add_library(aeron_driver_static STATIC ${SOURCE} ${HEADERS})
target_include_directories(aeron_driver_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${AERON_C_CLIENT_SOURCE_PATH}
    )

add_executable(aeronmd aeronmd.c)
target_include_directories(aeronmd
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${AERON_C_CLIENT_SOURCE_PATH}
    )

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    if (LIBBSD_EXISTS)
        set(AERON_LIB_BSD_LIBS bsd)
    endif ()

    set(AERON_LIB_M_LIBS m)
endif ()

target_link_libraries(
    aeron_driver
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${AERON_LIB_WINSOCK_LIBS})

target_link_libraries(
    aeronmd
    aeron_driver
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${AERON_LIB_WINSOCK_LIBS})

target_compile_definitions(aeron_driver PRIVATE -DAERON_DRIVER)
target_compile_definitions(aeron_driver_static PRIVATE -DAERON_DRIVER)

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_driver aeron_driver_static
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(TARGETS aeronmd DESTINATION bin)
    install(DIRECTORY . DESTINATION include/aeronmd FILES_MATCHING PATTERN "*.h")
endif ()
