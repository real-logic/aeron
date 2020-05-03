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

set(CLIENT_LINK_LIB "aeron_client")
if (LINK_SAMPLES_CLIENT_SHARED)
    set(CLIENT_LINK_LIB "aeron_client_shared")
    add_definitions(-DCLIENT_SHARED)
endif ()

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
add_executable(LossStat LossStat.cpp ${HEADERS})
add_executable(DriverTool DriverTool.cpp ${HEADERS})
add_executable(ExclusiveThroughput ExclusiveThroughput.cpp ${HEADERS})
add_executable(PingPong PingPong.cpp ${HEADERS})

target_link_libraries(AeronStat
    ${CLIENT_LINK_LIB})

target_link_libraries(BasicPublisher
    ${CLIENT_LINK_LIB})

target_link_libraries(TimeTests
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(BasicSubscriber
    ${CLIENT_LINK_LIB})

target_link_libraries(StreamingPublisher
    ${CLIENT_LINK_LIB})

target_link_libraries(RateSubscriber
    ${CLIENT_LINK_LIB})

target_link_libraries(Pong
    ${CLIENT_LINK_LIB})

target_link_libraries(Ping
    ${CLIENT_LINK_LIB}
    ${HDRHISTOGRAM_LIBS})

add_dependencies(Ping hdr_histogram_static)

target_link_libraries(Throughput
    ${CLIENT_LINK_LIB})

target_link_libraries(ErrorStat
    ${CLIENT_LINK_LIB})

target_link_libraries(LossStat
    ${CLIENT_LINK_LIB})

target_link_libraries(DriverTool
    ${CLIENT_LINK_LIB})

target_link_libraries(ExclusiveThroughput
    ${CLIENT_LINK_LIB})

target_link_libraries(PingPong
    ${CLIENT_LINK_LIB}
    ${HDRHISTOGRAM_LIBS})

add_dependencies(PingPong hdr_histogram_static)

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS AeronStat BasicPublisher TimeTests BasicSubscriber StreamingPublisher RateSubscriber Ping Pong Throughput ErrorStat LossStat DriverTool ExclusiveThroughput PingPong
        DESTINATION bin)
endif ()
