#
# Copyright 2014-2017 Real Logic Ltd.
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

if(BUILD_TESTING)
    include_directories(${AERON_DRIVER_SOURCE_PATH})
    include_directories(${AERON_CLIENT_SOURCE_PATH})

    function(aeron_driver_test name file)
        add_executable(${name} ${file})
        target_link_libraries(${name} aeron_driver ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
        add_dependencies(${name} gmock)
        add_test(NAME ${name} COMMAND ${name})
    endfunction()

    aeron_driver_test(spsc_rb_test aeron_spsc_rb_test.cpp)
    aeron_driver_test(mpsc_rb_test aeron_mpsc_rb_test.cpp)
    aeron_driver_test(broadcast_transmitter_test aeron_broadcast_transmitter_test.cpp)
    aeron_driver_test(counters_manager_test aeron_counters_manager_test.cpp)
    aeron_driver_test(distinct_error_log_test aeron_distinct_error_log_test.cpp)
    aeron_driver_test(driver_conductor_test aeron_driver_conductor_test.cpp)
    aeron_driver_test(spsc_queue_test aeron_spsc_concurrent_array_queue_test.cpp)
    aeron_driver_test(mpsc_queue_test aeron_mpsc_concurrent_array_queue_test.cpp)
    aeron_driver_test(uri_test aeron_uri_test.cpp)
    aeron_driver_test(udp_channel_test aeron_udp_channel_test.cpp)
    aeron_driver_test(int64_to_ptr_hash_map_test collections/aeron_int64_to_ptr_hash_masp_test.cpp)
    aeron_driver_test(str_to_ptr_hash_map_test collections/aeron_str_to_ptr_hash_map_test.cpp)
    aeron_driver_test(term_scanner_test aeron_term_scanner_test.cpp)
    aeron_driver_test(loss_detector_test aeron_loss_detector_test.cpp)

    function(aeron_driver_benchmark name file)
        add_executable(${name} ${file})
        target_link_libraries(${name} aeron_driver ${GOOGLE_BENCHMARK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
        add_dependencies(${name} google_benchmark)
    endfunction()
endif(BUILD_TESTING)
