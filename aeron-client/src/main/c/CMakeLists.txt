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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
    set(WSAPOLL_PROTOTYPE_EXISTS True)
endif ()

if (WSAPOLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_WSAPOLL)
endif ()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif ()

set(SOURCE
    collections/aeron_map.c
    collections/aeron_bit_set.c
    collections/aeron_int64_counter_map.c
    collections/aeron_int64_to_ptr_hash_map.c
    collections/aeron_int64_to_tagged_ptr_hash_map.c
    collections/aeron_str_to_ptr_hash_map.c
    concurrent/aeron_atomic.c
    concurrent/aeron_broadcast_receiver.c
    concurrent/aeron_broadcast_transmitter.c
    concurrent/aeron_counters_manager.c
    concurrent/aeron_distinct_error_log.c
    concurrent/aeron_exclusive_term_appender.c
    concurrent/aeron_logbuffer_descriptor.c
    concurrent/aeron_mpsc_concurrent_array_queue.c
    concurrent/aeron_mpsc_rb.c
    concurrent/aeron_spsc_concurrent_array_queue.c
    concurrent/aeron_spsc_rb.c
    concurrent/aeron_term_appender.c
    concurrent/aeron_term_gap_filler.c
    concurrent/aeron_term_gap_scanner.c
    concurrent/aeron_term_rebuilder.c
    concurrent/aeron_term_scanner.c
    concurrent/aeron_term_unblocker.c
    concurrent/aeron_thread.c
    protocol/aeron_udp_protocol.c
    util/aeron_arrayutil.c
    util/aeron_clock.c
    util/aeron_dlopen.c
    util/aeron_env.c
    util/aeron_error.c
    util/aeron_fileutil.c
    util/aeron_http_util.c
    util/aeron_math.c
    util/aeron_netutil.c
    util/aeron_parse_util.c
    util/aeron_properties_util.c
    util/aeron_strutil.c
    aeronc.c
    aeron_agent.c
    aeron_alloc.c
    aeron_client.c
    aeron_client_conductor.c
    aeron_context.c
    aeron_counter.c
    aeron_exclusive_publication.c
    aeron_image.c
    aeron_log_buffer.c
    aeron_publication.c
    aeron_socket.c
    aeron_subscription.c
    aeron_windows.c
    )

set(HEADERS
    collections/aeron_map.h
    collections/aeron_bit_set.h
    collections/aeron_int64_counter_map.h
    collections/aeron_int64_to_ptr_hash_map.h
    collections/aeron_int64_to_tagged_ptr_hash_map.h
    collections/aeron_str_to_ptr_hash_map.h
    command/aeron_control_protocol.h
    concurrent/aeron_atomic.h
    concurrent/aeron_atomic64_gcc_x86_64.h
    concurrent/aeron_atomic64_msvc.h
    concurrent/aeron_broadcast_descriptor.h
    concurrent/aeron_broadcast_receiver.h
    concurrent/aeron_broadcast_transmitter.h
    concurrent/aeron_concurrent_array_queue.h
    concurrent/aeron_counters_manager.h
    concurrent/aeron_distinct_error_log.h
    concurrent/aeron_exclusive_term_appender.h
    concurrent/aeron_logbuffer_descriptor.h
    concurrent/aeron_mpsc_concurrent_array_queue.h
    concurrent/aeron_mpsc_rb.h
    concurrent/aeron_rb.h
    concurrent/aeron_spsc_concurrent_array_queue.h
    concurrent/aeron_spsc_rb.h
    concurrent/aeron_term_appender.h
    concurrent/aeron_term_gap_filler.h
    concurrent/aeron_term_gap_scanner.h
    concurrent/aeron_term_rebuilder.h
    concurrent/aeron_term_scanner.h
    concurrent/aeron_term_unblocker.h
    concurrent/aeron_thread.h
    protocol/aeron_udp_protocol.h
    util/aeron_arrayutil.h
    util/aeron_bitutil.h
    util/aeron_clock.h
    util/aeron_dlopen.h
    util/aeron_env.h
    util/aeron_error.h
    util/aeron_fileutil.h
    util/aeron_http_util.h
    util/aeron_math.h
    util/aeron_netutil.h
    util/aeron_parse_util.h
    util/aeron_platform.h
    util/aeron_properties_util.h
    util/aeron_strutil.h
    aeronc.h
    aeron_agent.h
    aeron_alloc.h
    aeron_client.h
    aeron_client_conductor.h
    aeron_cnc_file_descriptor.h
    aeron_common.h
    aeron_context.h
    aeron_counter.h
    aeron_exclusive_publication.h
    aeron_image.h
    aeron_log_buffer.h
    aeron_publication.h
    aeron_socket.h
    aeron_subscription.h
    aeron_windows.h
    )

add_library(aeron SHARED ${SOURCE} ${HEADERS})
target_include_directories(aeron
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(aeron
    PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)

add_library(aeron_static STATIC ${SOURCE} ${HEADERS})
target_include_directories(aeron_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(AERON_LIB_M_LIBS m)
endif ()

target_link_libraries(
    aeron
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_M_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${AERON_LIB_WINSOCK_LIBS})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron aeron_static
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY . DESTINATION include/aeron FILES_MATCHING PATTERN "*.h")
endif ()
