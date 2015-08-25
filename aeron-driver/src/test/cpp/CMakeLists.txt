#
# Copyright 2016 Real Logic Ltd.
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

set(TEST_HEADERS)

set(TEST_SOURCE
    concurrent/OneToOneConcurrentArrayQueueTest.cpp)

add_library(aeron_driver_test STATIC ${TEST_SOURCE} ${TEST_HEADERS})
add_dependencies(aeron_driver_test gmock)

function(aeron_driver_test name file)
    add_executable(${name} ${file})
    target_link_libraries(${name} aeron_client aeron_driver aeron_driver_test ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_driver_test(mediaDriverTest MediaDriverTest.cpp)
aeron_driver_test(oneToOneConcurrentArrayQueueTest concurrent/OneToOneConcurrentArrayQueueTest.cpp)
