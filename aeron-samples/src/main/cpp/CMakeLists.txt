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

include_directories(${AERON_CLIENT_SOURCE_PATH})

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

set(HEADERS
    Configuration.h
    RateReporter.h)

add_executable(AeronStat AeronStat.cpp ${HEADERS})
add_executable(BasicPublisher BasicPublisher.cpp ${HEADERS})
add_executable(TimeTests raw/TimeTests.cpp ${HEADERS})
add_executable(BasicSubscriber BasicSubscriber.cpp ${HEADERS})
add_executable(StreamingPublisher StreamingPublisher.cpp ${HEADERS})
add_executable(RateSubscriber RateSubscriber.cpp ${HEADERS})
add_executable(Pong Pong.cpp ${HEADERS})
add_executable(Ping Ping.cpp ${HEADERS})
add_executable(Throughput Throughput.cpp ${HEADERS})
add_executable(ErrorStat ErrorStat.cpp ${HEADERS})
add_executable(ExclusiveThroughput ExclusiveThroughput.cpp ${HEADERS})
add_executable(PingPong PingPong.cpp ${HEADERS})

target_link_libraries(AeronStat
    aeron_client)

target_link_libraries(BasicPublisher
    aeron_client)

target_link_libraries(TimeTests
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(BasicSubscriber
    aeron_client)

target_link_libraries(StreamingPublisher
    aeron_client)

target_link_libraries(RateSubscriber
    aeron_client)

target_link_libraries(Pong
    aeron_client)

target_link_libraries(Ping
    aeron_client
    ${HDRHISTOGRAM_LIBS})

add_dependencies(Ping hdr_histogram)

target_link_libraries(Throughput
    aeron_client)

target_link_libraries(ErrorStat
    aeron_client)

target_link_libraries(ExclusiveThroughput
    aeron_client)

target_link_libraries(PingPong
    aeron_client
    ${HDRHISTOGRAM_LIBS})

add_dependencies(PingPong hdr_histogram)

install(
    TARGETS AeronStat BasicPublisher TimeTests BasicSubscriber StreamingPublisher RateSubscriber Ping Pong Throughput ErrorStat ExclusiveThroughput PingPong
    DESTINATION bin)