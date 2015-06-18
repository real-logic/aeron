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

SET(SOURCE
    Publication.cpp
    Subscription.cpp
    ClientConductor.cpp
    Aeron.cpp
    LogBuffers.cpp
    util/MemoryMappedFile.cpp
    util/CommandOption.cpp
    util/CommandOptionParser.cpp
    util/StringUtil.cpp)

SET(HEADERS
    ClientConductor.h
    CncFileDescriptor.h
    Connection.h
    Context.h
    Aeron.h
    Publication.h
    Subscription.h
    DriverProxy.h
    DriverListenerAdapter.h
    LogBuffers.h
    command/ConnectionMessageFlyweight.h
    command/ConnectionBuffersReadyFlyweight.h
    command/ControlProtocolEvents.h
    command/CorrelatedMessageFlyweight.h
    command/Flyweight.h
    command/PublicationMessageFlyweight.h
    command/PublicationBuffersReadyFlyweight.h
    command/RemoveMessageFlyweight.h
    command/SubscriptionMessageFlyweight.h
    concurrent/AgentRunner.h
    concurrent/Atomic64.h
    concurrent/AtomicBuffer.h
    concurrent/AtomicCounter.h
    concurrent/BusySpinIdleStrategy.h
    concurrent/CountersManager.h
    concurrent/SleepingIdleStrategy.h
    concurrent/atomic/Atomic64_gcc_x86_64.h
    concurrent/atomic/Atomic64_msvc.h
    concurrent/broadcast/BroadcastBufferDescriptor.h
    concurrent/broadcast/BroadcastReceiver.h
    concurrent/broadcast/BroadcastTransmitter.h
    concurrent/broadcast/CopyBroadcastReceiver.h
    concurrent/broadcast/RecordDescriptor.h
    concurrent/logbuffer/BufferClaim.h
    concurrent/logbuffer/DataFrameHeader.h
    concurrent/logbuffer/FrameDescriptor.h
    concurrent/logbuffer/Header.h
    concurrent/logbuffer/TermAppender.h
    concurrent/logbuffer/LogBufferPartition.h
    concurrent/logbuffer/LogBufferDescriptor.h
    concurrent/logbuffer/TermReader.h
    concurrent/ringbuffer/ManyToOneRingBuffer.h
    concurrent/ringbuffer/RecordDescriptor.h
    concurrent/ringbuffer/RingBufferDescriptor.h
    concurrent/status/ReadablePosition.h
    concurrent/status/Position.h
    concurrent/status/UnsafeBufferPosition.h
    util/MemoryMappedFile.h
    util/CommandOption.h
    util/CommandOptionParser.h
    util/StringUtil.h
    util/Exceptions.h
    util/MacroUtil.h
    util/ScopeUtils.h
    util/BitUtil.h
    util/Index.h
    util/Platform.h)

# static library
add_library(aeron_client STATIC ${SOURCE} ${HEADERS})
