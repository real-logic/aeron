#
# Copyright 2014 Real Logic Ltd.
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

SET(SOURCE
    AtomicCounter.cpp
    CountersManager.cpp)

SET(HEADERS
    AtomicBuffer.h
    AtomicCounter.h
    CountersManager.h
    ringbuffer/RecordDescriptor.h
    ringbuffer/ManyToOneRingBuffer.h
    ringbuffer/RingBufferDescriptor.h
    logbuffer/LogBuffer.h
    logbuffer/LogBufferDescriptor.h
    logbuffer/FrameDescriptor.h
    logbuffer/LogAppender.h
    logbuffer/BufferClaim.h
    logbuffer/LogReader.h
    logbuffer/Header.h
    broadcast/BroadcastBufferDescriptor.h
    broadcast/RecordDescriptor.h
    broadcast/BroadcastTransmitter.h
    broadcast/BroadcastReceiver.h
    broadcast/CopyBroadcastReceiver.h
    tests/MockAtomicBuffer.h)

# static library
add_library(aeron_concurrent STATIC ${SOURCE} ${HEADERS})

# executable for this modules tests
add_executable(concurrentTests
                tests/testConcurrent.cpp
                tests/testCountersManager.cpp
                tests/testManyToOneRingBuffer.cpp
                tests/testLogAppender.cpp
                tests/testLogReader.cpp
                tests/testBroadcastTransmitter.cpp)

target_link_libraries(concurrentTests
                        aeron_concurrent
                        aeron_util
                        gmock_main)

# make test
add_test(NAME testConcurrent COMMAND concurrentTests)

