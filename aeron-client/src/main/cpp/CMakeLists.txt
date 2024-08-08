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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(BUILD_SHARED_LIBS ON)
endif ()

SET(SOURCE
    ${CMAKE_CURRENT_SOURCE_DIR}/AeronVersion.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Publication.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ExclusivePublication.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Subscription.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ClientConductor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Aeron.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/LogBuffers.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Counter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Context.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/MemoryMappedFile.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOption.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOptionParser.cpp)

SET(HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/Aeron.h
    ${CMAKE_CURRENT_SOURCE_DIR}/AeronCounters.h
    ${CMAKE_CURRENT_SOURCE_DIR}/AeronVersion.h
    ${CMAKE_CURRENT_SOURCE_DIR}/BufferBuilder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ChannelUri.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ChannelUriStringBuilder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ClientConductor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/CncFileDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/CncFileReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Context.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ControlledFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Counter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/DriverListenerAdapter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/DriverProxy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ExclusivePublication.h
    ${CMAKE_CURRENT_SOURCE_DIR}/FragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Image.h
    ${CMAKE_CURRENT_SOURCE_DIR}/HeartbeatTimestamp.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ImageControlledFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ImageFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/LogBuffers.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Publication.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Subscription.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/ImageMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/ImageBuffersReadyFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/ControlProtocolEvents.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/CorrelatedMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/ErrorResponseFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/Flyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/PublicationMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/PublicationBuffersReadyFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/RemoveMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/SubscriptionMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/DestinationMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/SubscriptionReadyFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/OperationSucceededFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/CounterMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/CounterUpdateFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/ClientTimeoutFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/command/TerminateDriverFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AgentRunner.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AgentInvoker.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/Atomic64.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AtomicBuffer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AtomicCounter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/BusySpinIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/CountersManager.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/CountersReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/NoOpIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/SleepingIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/YieldingIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/BackOffIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_gcc_cpp11.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_gcc_x86_64.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_msvc.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/broadcast/BroadcastBufferDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/broadcast/BroadcastReceiver.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/broadcast/BroadcastTransmitter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/broadcast/CopyBroadcastReceiver.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/broadcast/RecordDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/ErrorLogDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/ErrorLogReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/DistinctErrorLog.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/BufferClaim.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/DataFrameHeader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/FrameDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/Header.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/HeaderWriter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/LogBufferDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermRebuilder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermGapScanner.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermBlockScanner.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermScanner.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/ringbuffer/ManyToOneRingBuffer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/ringbuffer/RecordDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/ringbuffer/RingBufferDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/ringbuffer/OneToOneRingBuffer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/LocalSocketAddressStatus.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/ReadablePosition.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/Position.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/UnsafeBufferPosition.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/StatusIndicatorReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/reports/LossReportDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/reports/LossReportReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AtomicArrayUpdater.h
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol/HeaderFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol/NakFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol/StatusMessageFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol/SetupFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol/DataHeaderFlyweight.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/MemoryMappedFile.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOption.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOptionParser.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/StringUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Exceptions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/LangUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/MacroUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/ScopeUtils.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/BitUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Index.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Platform.h)

# static library
add_library(aeron_client STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_client ALIAS aeron_client)
add_library(aeron_client_shared SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_client_shared ALIAS aeron_client_shared)

target_include_directories(aeron_client
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include>")

target_include_directories(aeron_client_shared
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include>")

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

target_compile_definitions(aeron_client
    PUBLIC $<$<NOT:$<CONFIG:Debug>>:DISABLE_BOUNDS_CHECKS>
    # relative file paths for use in exceptions
    PUBLIC "$<BUILD_INTERFACE:__PROJECT_SOURCE_DIR__=\"${NATIVE_PROJECT_SOURCE_DIR}\">")

target_compile_definitions(aeron_client_shared
    PUBLIC $<$<NOT:$<CONFIG:Debug>>:DISABLE_BOUNDS_CHECKS>
    # relative file paths for use in exceptions
    PUBLIC "$<BUILD_INTERFACE:__PROJECT_SOURCE_DIR__=\"${NATIVE_PROJECT_SOURCE_DIR}\">"
    PUBLIC CLIENT_SHARED DLL_EXPORT)

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_client
    INTERFACE ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(aeron_client_shared
    INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(TARGETS aeron_client_shared aeron_client
        EXPORT aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY ./ DESTINATION include FILES_MATCHING PATTERN "*.h")
endif ()
