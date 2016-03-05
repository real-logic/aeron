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

function(aeron_driver_test name file)
    add_executable(${name} ${file})
    target_link_libraries(${name} aeron_client aeron_driver aeron_client_test ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_driver_test(oneToOneConcurrentArrayQueueTest concurrent/OneToOneConcurrentArrayQueueTest.cpp)
aeron_driver_test(aeronUriTest uri/AeronUriTest.cpp)
aeron_driver_test(netUtilTest uri/NetUtilTest.cpp)
aeron_driver_test(inetAddressTest media/InetAddressTest.cpp)
aeron_driver_test(udpChannelTest media/UdpChannelTest.cpp)
aeron_driver_test(interfaceSearchAddressTest media/InterfaceSearchAddressTest.cpp)
aeron_driver_test(udpChannelTransportTest media/UdpChannelTransportTest.cpp)
aeron_driver_test(channelEndpointTest media/ChannelEndpointTest.cpp)
aeron_driver_test(dataPacketDispatcherTest DataPacketDispatcherTest.cpp)

function(aeron_driver_benchmark name file)
    add_executable(${name} ${file})
    target_link_libraries(${name} aeron_client aeron_driver ${GOOGLE_BENCHMARK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} google_benchmark)
endfunction()

aeron_driver_benchmark(oneToOneConcurrentArrayQueueBenchmark concurrent/OneToOneConcurrentArrayQueueBenchmark.cpp)