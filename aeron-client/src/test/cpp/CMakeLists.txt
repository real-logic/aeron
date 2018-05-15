#
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
#

set(TEST_HEADERS
    ClientConductorFixture.h
    util/TestUtils.h
    concurrent/MockAtomicBuffer.h)

set(TEST_SOURCE
    ClientConductorFixture.cpp
    concurrent/MockAtomicBuffer.cpp)

add_library(aeron_client_test STATIC ${TEST_SOURCE} ${TEST_HEADERS})
target_include_directories(aeron_client_test
    PUBLIC ${AERON_CLIENT_TEST_PATH}
)
target_link_libraries(aeron_client_test aeron_client)
add_dependencies(aeron_client_test gmock)

function(aeron_client_test name file)
    add_executable(${name} ${file})
    target_link_libraries(${name} aeron_client aeron_client_test ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_client_test(clientConductorTest ClientConductorTest.cpp)
aeron_client_test(publicationTest PublicationTest.cpp)
aeron_client_test(exclusivePublicationTest ExclusivePublicationTest.cpp)
aeron_client_test(imageTest ImageTest.cpp)
aeron_client_test(fragmentAssemblyTest FragmentAssemblerTest.cpp)
aeron_client_test(commandTest command/CommandTest.cpp)
aeron_client_test(utilTest util/UtilTest.cpp)
aeron_client_test(memoryMappedFileTest util/MemoryMappedFileTest.cpp)
aeron_client_test(broadcastReceiverTest concurrent/BroadcastReceiverTest.cpp)
aeron_client_test(broadcastTransmitterTest concurrent/BroadcastTransmitterTest.cpp)
aeron_client_test(concurrentTest concurrent/ConcurrentTest.cpp)
aeron_client_test(countersManagerTest concurrent/CountersManagerTest.cpp)
aeron_client_test(termAppenderTest concurrent/TermAppenderTest.cpp)
aeron_client_test(termReaderTest concurrent/TermReaderTest.cpp)
aeron_client_test(termBlockScannerTest concurrent/TermBlockScannerTest.cpp)
aeron_client_test(termRebuilderTest concurrent/TermRebuilderTest.cpp)
aeron_client_test(termGapScannerTest concurrent/TermGapScannerTest.cpp)
aeron_client_test(termScannerTest concurrent/TermScannerTest.cpp)
aeron_client_test(manyToOneRingBufferTest concurrent/ManyToOneRingBufferTest.cpp)
aeron_client_test(distinctErrorLogTest concurrent/DistinctErrorLogTest.cpp)
aeron_client_test(errorLogReaderTest concurrent/ErrorLogReaderTest.cpp)
aeron_client_test(oneToOneRingBuffertest concurrent/OneToOneRingBufferTest.cpp)
