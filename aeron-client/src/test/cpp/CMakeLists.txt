#
# Copyright 2014 - 2015 Real Logic Ltd.
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

function(aeron_client_test name file)
    add_executable(${name} ${file} ClientConductorFixture.h)
    target_link_libraries(${name} aeron_client ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

function(aeron_common_test name file)
    add_executable(${name} ${file} concurrent/MockAtomicBuffer.h)
    target_link_libraries(${name} aeron_client ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_client_test(clientConductorTest ClientConductorTest.cpp)
aeron_client_test(publicationTest PublicationTest.cpp)
aeron_common_test(commandTest command/CommandTest.cpp)
aeron_common_test(utilTest util/UtilTest.cpp)
aeron_common_test(memoryMappedFileTest util/MemoryMappedFileTest.cpp)
aeron_common_test(broadcastReceiverTest concurrent/BroadcastReceiverTest.cpp)
aeron_common_test(broadcastTransmitterTest concurrent/BroadcastTransmitterTest.cpp)
aeron_common_test(concurrentTest concurrent/ConcurrentTest.cpp)
aeron_common_test(countersManagerTest concurrent/CountersManagerTest.cpp)
aeron_common_test(termAppenderTest concurrent/TermAppenderTest.cpp)
aeron_common_test(termReaderTest concurrent/TermReaderTest.cpp)
aeron_common_test(manyToOneRingBufferTest concurrent/ManyToOneRingBufferTest.cpp)
