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

target_link_libraries(AeronStat
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(BasicPublisher
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(TimeTests
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(BasicSubscriber
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(StreamingPublisher
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(RateSubscriber
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(Pong
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(Ping
    aeron_client
    ${HDRHISTOGRAM_LIBS}
    ${CMAKE_THREAD_LIBS_INIT})

add_dependencies(Ping hdr_histogram)

install(
    TARGETS AeronStat BasicPublisher TimeTests BasicSubscriber StreamingPublisher RateSubscriber Ping Pong
    DESTINATION bin)