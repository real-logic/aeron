/*
 * Copyright 2014 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INCLUDED_AERON_CONCURRENT_BROADCAST_RECEIVER__
#define INCLUDED_AERON_CONCURRENT_BROADCAST_RECEIVER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "BroadcastBufferDescriptor.h"
#include "RecordDescriptor.h"

namespace aeron { namespace common { namespace concurrent { namespace broadcast {

class BroadcastReceiver
{
public:
    BroadcastReceiver(AtomicBuffer& buffer) :
        m_buffer(buffer),
        m_capacity(buffer.getCapacity() - BroadcastBufferDescriptor::TRAILER_LENGTH),
        m_mask(m_capacity - 1),
        m_tailCounterIndex(m_capacity + BroadcastBufferDescriptor::TAIL_COUNTER_OFFSET),
        m_latestCounterIndex(m_capacity + BroadcastBufferDescriptor::LATEST_COUNTER_OFFSET),
        m_recordOffset(0),
        m_cursor(0),
        m_nextRecord(0),
        m_lappedCount(0)
    {
        BroadcastBufferDescriptor::checkCapacity(m_capacity);
    }

    inline util::index_t capacity()
    {
        return m_capacity;
    }

    inline long lappedCount()
    {
        return m_lappedCount;
    }

    inline std::int32_t typeId()
    {
        return m_buffer.getInt32(RecordDescriptor::msgTypeOffset(m_recordOffset));
    }

    inline util::index_t offset()
    {
        return RecordDescriptor::msgOffset(m_recordOffset);
    }

    inline std::int32_t length()
    {
        return m_buffer.getInt32(RecordDescriptor::msgLengthOffset(m_recordOffset));
    }

    inline AtomicBuffer& buffer()
    {
        return m_buffer;
    }

    bool receiveNext()
    {
        
    }

private:
    AtomicBuffer& m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_tailCounterIndex;
    util::index_t m_latestCounterIndex;

    util::index_t m_recordOffset;
    util::index_t m_cursor;
    util::index_t m_nextRecord;
    std::atomic_long m_lappedCount;

};

}}}}

#endif
