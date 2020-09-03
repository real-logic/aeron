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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(BUILD_SHARED_LIBS ON)
endif ()

SET(HEADERS
    CncFileDescriptor.h
    Image.h
    Context.h
    Aeron.h
    Publication.h
    Subscription.h
    BufferBuilder.h
    FragmentAssembler.h
    ControlledFragmentAssembler.h
    ImageFragmentAssembler.h
    ImageControlledFragmentAssembler.h
    ExclusivePublication.h
    Counter.h
    ChannelUri.h
    ChannelUriStringBuilder.h
    concurrent/AgentRunner.h
    concurrent/AgentInvoker.h
    concurrent/Atomic64.h
    concurrent/AtomicBuffer.h
    concurrent/AtomicCounter.h
    concurrent/BusySpinIdleStrategy.h
    concurrent/CountersReader.h
    concurrent/NoOpIdleStrategy.h
    concurrent/SleepingIdleStrategy.h
    concurrent/YieldingIdleStrategy.h
    concurrent/BackOffIdleStrategy.h
    concurrent/atomic/Atomic64_gcc_cpp11.h
    concurrent/atomic/Atomic64_gcc_x86_64.h
    concurrent/atomic/Atomic64_msvc.h
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
    concurrent/logbuffer/TermReader.h
    concurrent/ringbuffer/ManyToOneRingBuffer.h
    concurrent/ringbuffer/RecordDescriptor.h
    concurrent/ringbuffer/RingBufferDescriptor.h
    concurrent/ringbuffer/OneToOneRingBuffer.h
    concurrent/status/LocalSocketAddressStatus.h
    concurrent/status/ReadablePosition.h
    concurrent/status/Position.h
    concurrent/status/UnsafeBufferPosition.h
    concurrent/status/StatusIndicatorReader.h
    concurrent/reports/LossReportDescriptor.h
    concurrent/reports/LossReportReader.h
    concurrent/AtomicArrayUpdater.h
    util/CommandOption.h
    util/CommandOptionParser.h
    util/StringUtil.h
    util/Exceptions.h
    util/LangUtil.h
    util/MacroUtil.h
    util/ScopeUtils.h
    util/BitUtil.h
    util/Index.h
    util/Platform.h)

# header only library
add_library(aeron_client_wrapper INTERFACE)
target_include_directories(aeron_client_wrapper INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

list(TRANSFORM HEADERS PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
target_sources(aeron_client_wrapper INTERFACE ${HEADERS})

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
    if (${CMAKE_VERSION} VERSION_LESS "3.13.0")
        string(TOLOWER "${NATIVE_PROJECT_SOURCE_DIR}" NATIVE_PROJECT_SOURCE_DIR)
    endif ()
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_client_wrapper INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
   install(DIRECTORY . DESTINATION include FILES_MATCHING PATTERN "*.h")
endif ()
