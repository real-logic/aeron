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

set(TEST_HEADERS
    EmbeddedMediaDriver.h)
#
#set(TEST_SOURCE)

#add_library(aeron_client_test STATIC ${TEST_SOURCE} ${TEST_HEADERS})
#target_include_directories(aeron_client_test
#    PUBLIC ${AERON_CLIENT_TEST_PATH})
#target_link_libraries(aeron_client_test aeron_client)
#target_compile_definitions(aeron_client_test PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
#add_dependencies(aeron_client_test gmock)

if (MSVC AND ${CMAKE_VERSION} VERSION_LESS "3.13.0")
    target_compile_definitions(aeron_client_test PUBLIC "MSVC_FILE_IS_LOWER_CASE")
endif ()

function(aeron_client_wrapper_test name file)
    set(wrapper_name "${name}W")
    add_executable(${wrapper_name} ${file} ${TEST_HEADERS})
    target_link_libraries(${wrapper_name} aeron_client_wrapper aeron_driver ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${wrapper_name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_dependencies(${wrapper_name} gmock)
    add_test(NAME "\"C Wrapper: ${name}\"" COMMAND ${wrapper_name})

    add_executable(${name} ${file} ${TEST_HEADERS})
    target_link_libraries(${name} aeron_client aeron_driver ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_dependencies(${name} gmock)
    add_test(NAME "\"C++: ${name}\"" COMMAND ${name})
endfunction()

aeron_client_wrapper_test(systemTest SystemTest.cpp)
aeron_client_wrapper_test(pubSubTest PubSubTest.cpp)
