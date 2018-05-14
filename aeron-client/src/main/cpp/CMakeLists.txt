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

SET(SOURCE
    Publication.cpp
    ExclusivePublication.cpp
    Subscription.cpp
    ClientConductor.cpp
    Aeron.cpp
    LogBuffers.cpp
    Counter.cpp
    util/MemoryMappedFile.cpp
    util/CommandOption.cpp
    util/CommandOptionParser.cpp)

SET(HEADERS
    ClientConductor.h
    CncFileDescriptor.h
    Image.h
    Context.h
    Aeron.h
    Publication.h
    Subscription.h
    DriverProxy.h
    DriverListenerAdapter.h
    LogBuffers.h
    BufferBuilder.h
    FragmentAssembler.h
    ControlledFragmentAssembler.h
    ExclusivePublication.h
    Counter.h
    command/ImageMessageFlyweight.h
    command/ImageBuffersReadyFlyweight.h
    command/ControlProtocolEvents.h
    command/CorrelatedMessageFlyweight.h
    command/ErrorResponseFlyweight.h
    command/Flyweight.h
    command/PublicationMessageFlyweight.h
    command/PublicationBuffersReadyFlyweight.h
    command/RemoveMessageFlyweight.h
    command/SubscriptionMessageFlyweight.h
    command/DestinationMessageFlyweight.h
    command/SubscriptionReadyFlyweight.h
    command/OperationSucceededFlyweight.h
    command/CounterMessageFlyweight.h
    command/CounterUpdateFlyweight.h
    concurrent/AgentRunner.h
    concurrent/AgentInvoker.h
    concurrent/Atomic64.h
    concurrent/AtomicBuffer.h
    concurrent/AtomicCounter.h
    concurrent/BusySpinIdleStrategy.h
    concurrent/CountersManager.h
    concurrent/CountersReader.h
    concurrent/SleepingIdleStrategy.h
    concurrent/YieldingIdleStrategy.h
    concurrent/atomic/Atomic64_gcc_cpp11.h
    concurrent/atomic/Atomic64_gcc_x86_64.h
    concurrent/atomic/Atomic64_msvc.h
    concurrent/broadcast/BroadcastBufferDescriptor.h
    concurrent/broadcast/BroadcastReceiver.h
    concurrent/broadcast/BroadcastTransmitter.h
    concurrent/broadcast/CopyBroadcastReceiver.h
    concurrent/broadcast/RecordDescriptor.h
    concurrent/errors/ErrorLogDescriptor.h
    concurrent/errors/ErrorLogReader.h
    concurrent/errors/DistinctErrorLog.h
    concurrent/logbuffer/BufferClaim.h
    concurrent/logbuffer/DataFrameHeader.h
    concurrent/logbuffer/FrameDescriptor.h
    concurrent/logbuffer/Header.h
    concurrent/logbuffer/HeaderWriter.h
    concurrent/logbuffer/TermAppender.h
    concurrent/logbuffer/LogBufferDescriptor.h
    concurrent/logbuffer/TermRebuilder.h
    concurrent/logbuffer/TermGapScanner.h
    concurrent/logbuffer/TermBlockScanner.h
    concurrent/logbuffer/TermReader.h
    concurrent/logbuffer/TermScanner.h
    concurrent/logbuffer/ExclusiveTermAppender.h
    concurrent/logbuffer/ExclusiveBufferClaim.h
    concurrent/ringbuffer/ManyToOneRingBuffer.h
    concurrent/ringbuffer/RecordDescriptor.h
    concurrent/ringbuffer/RingBufferDescriptor.h
    concurrent/ringbuffer/OneToOneRingBuffer.h
    concurrent/status/ReadablePosition.h
    concurrent/status/Position.h
    concurrent/status/UnsafeBufferPosition.h
    concurrent/status/StatusIndicatorReader.h
    protocol/HeaderFlyweight.h
    protocol/NakFlyweight.h
    protocol/StatusMessageFlyweight.h
    util/MemoryMappedFile.h
    util/CommandOption.h
    util/CommandOptionParser.h
    util/StringUtil.h
    util/Exceptions.h
    util/LangUtil.h
    util/MacroUtil.h
    util/ScopeUtils.h
    util/BitUtil.h
    util/Index.h
    util/Platform.h
    protocol/SetupFlyweight.h
    protocol/DataHeaderFlyweight.h)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

# static library
add_library(aeron_client STATIC ${SOURCE} ${HEADERS})

target_include_directories(aeron_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

if (NOT WIN32)
	set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
	set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif()

target_link_libraries(aeron_client
    INTERFACE ${CMAKE_THREAD_LIBS_INIT}
)

install(TARGETS aeron_client ARCHIVE DESTINATION lib)
install(DIRECTORY . DESTINATION  include FILES_MATCHING PATTERN "*.h")
