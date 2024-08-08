#
# Copyright 2014-2024 Real Logic Limited.
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
set(CLIENT_WRAPPER_LINK_LIB "aeron_client_wrapper")
set(C_CLIENT_LINK_LIB "aeron_static")
if (LINK_SAMPLES_CLIENT_SHARED OR MSVC)
    set(C_CLIENT_LINK_LIB "aeron")
    set(CLIENT_LINK_LIB "aeron_client_shared")
    add_definitions(-DCLIENT_SHARED)
endif ()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

set(HEADERS
    Configuration.h
    RateReporter.h)

add_executable(BasicPublisher BasicPublisher.cpp ${HEADERS})
add_executable(aeron::BasicPublisher ALIAS BasicPublisher)
add_executable(BasicSubscriber BasicSubscriber.cpp ${HEADERS})
add_executable(aeron::BasicSubscriber ALIAS BasicSubscriber)
add_executable(StreamingPublisher StreamingPublisher.cpp ${HEADERS})
add_executable(aeron::StreamingPublisher ALIAS StreamingPublisher)
add_executable(RateSubscriber RateSubscriber.cpp ${HEADERS})
add_executable(aeron::RateSubscriber ALIAS RateSubscriber)
add_executable(Pong Pong.cpp ${HEADERS})
add_executable(aeron::Pong ALIAS Pong)
add_executable(Ping Ping.cpp ${HEADERS})
add_executable(aeron::Ping ALIAS Ping)
add_executable(BasicPublisherW BasicPublisher.cpp ${HEADERS})
add_executable(aeron::BasicPublisherW ALIAS BasicPublisherW)
add_executable(BasicSubscriberW BasicSubscriber.cpp ${HEADERS})
add_executable(aeron::BasicSubscriberW ALIAS BasicSubscriberW)
add_executable(StreamingPublisherW StreamingPublisher.cpp ${HEADERS})
add_executable(aeron::StreamingPublisherW ALIAS StreamingPublisherW)
add_executable(RateSubscriberW RateSubscriber.cpp ${HEADERS})
add_executable(aeron::RateSubscriberW ALIAS RateSubscriberW)
add_executable(PongW Pong.cpp ${HEADERS})
add_executable(aeron::PongW ALIAS PongW)
add_executable(PingW Ping.cpp ${HEADERS})
add_executable(aeron::PingW ALIAS PingW)
add_executable(TimeTests raw/TimeTests.cpp ${HEADERS})
add_executable(aeron::TimeTests ALIAS TimeTests)
add_executable(Throughput Throughput.cpp ${HEADERS})
add_executable(aeron::Throughput ALIAS Throughput)
add_executable(ExclusiveThroughput ExclusiveThroughput.cpp ${HEADERS})
add_executable(aeron::ExclusiveThroughput ALIAS ExclusiveThroughput)
add_executable(PingPong PingPong.cpp ${HEADERS})
add_executable(aeron::PingPong ALIAS PingPong)
add_executable(PingPongW PingPong.cpp ${HEADERS})
add_executable(aeron::PingPongW ALIAS PingPongW)
add_executable(ExclusivePingPong ExclusivePingPong.cpp ${HEADERS})
add_executable(aeron::ExclusivePingPong ALIAS ExclusivePingPong)
add_executable(ExclusivePingPongW ExclusivePingPong.cpp ${HEADERS})
add_executable(aeron::ExclusivePingPongW ALIAS ExclusivePingPongW)

target_link_libraries(BasicPublisher
    ${CLIENT_LINK_LIB})

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
    hdr_histogram_static)
add_dependencies(Ping hdr_histogram)

target_link_libraries(BasicPublisherW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB})

target_link_libraries(BasicSubscriberW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB})

target_link_libraries(StreamingPublisherW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB})

target_link_libraries(RateSubscriberW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB})

target_link_libraries(PongW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB})

target_link_libraries(PingW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB}
    hdr_histogram_static)
add_dependencies(PingW hdr_histogram)

target_link_libraries(Throughput
    ${CLIENT_LINK_LIB})

target_link_libraries(ExclusiveThroughput
    ${CLIENT_LINK_LIB})

target_link_libraries(PingPong
    ${CLIENT_LINK_LIB}
    hdr_histogram_static)
add_dependencies(PingPong hdr_histogram)

target_link_libraries(PingPongW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB}
    hdr_histogram_static)
add_dependencies(PingPongW hdr_histogram)

target_link_libraries(ExclusivePingPong
    ${CLIENT_LINK_LIB}
    hdr_histogram_static)
add_dependencies(ExclusivePingPong hdr_histogram)

target_link_libraries(ExclusivePingPongW
    ${CLIENT_WRAPPER_LINK_LIB} ${C_CLIENT_LINK_LIB}
    hdr_histogram_static)
add_dependencies(ExclusivePingPongW hdr_histogram)

target_link_libraries(TimeTests
    ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS
        BasicPublisher
        TimeTests
        BasicSubscriber
        StreamingPublisher
        RateSubscriber
        Ping
        Pong
        Throughput
        ExclusiveThroughput
        PingPong
        ExclusivePingPong
        BasicPublisherW
        BasicSubscriberW
        RateSubscriberW
        PingW
        PongW
        PingPongW
        ExclusivePingPongW
        EXPORT aeron-targets
        DESTINATION bin)
endif ()
