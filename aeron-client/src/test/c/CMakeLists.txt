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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
endif ()

include_directories(${AERON_C_CLIENT_SOURCE_PATH})

set(TEST_HEADERS
    aeron_client_test_utils.h)

function(aeron_c_client_test name file)
    add_executable(${name} ${file} ${TEST_HEADERS})
    target_link_libraries(${name} aeron ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT} ${AERON_LIB_WINSOCK_LIBS})
    target_compile_definitions(${name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_c_client_test(broadcast_receiver_test concurrent/aeron_broadcast_receiver_test.cpp)
aeron_c_client_test(broadcast_transmitter_test concurrent/aeron_broadcast_transmitter_test.cpp)
aeron_c_client_test(term_appender_test concurrent/aeron_term_appender_test.cpp)
aeron_c_client_test(counters_reader_test concurrent/aeron_counters_test.cpp)
aeron_c_client_test(exclusive_term_appender_test concurrent/aeron_exclusive_term_appender_test.cpp)
aeron_c_client_test(client_conductor_test aeron_client_conductor_test.cpp)
aeron_c_client_test(subscription_test aeron_subscription_test.cpp)
aeron_c_client_test(image_test aeron_image_test.cpp)
aeron_c_client_test(fragment_assembler_test aeron_fragment_assembler_test.cpp)
aeron_c_client_test(array_to_ptr_hash_map_test collections/aeron_array_to_ptr_hash_map_test.cpp)
aeron_c_client_test(aeron_fileutil_test util/aeron_fileutil_test.cpp)
