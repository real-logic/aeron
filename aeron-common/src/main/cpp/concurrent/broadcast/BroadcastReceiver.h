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

#include <atomic>
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
        const std::int64_t tail = m_buffer.getInt64Ordered(m_tailCounterIndex);
        std::int64_t cursor = m_nextRecord;

        if (tail > cursor)
        {
            m_recordOffset = (std::int32_t)cursor & m_mask;

            if (!validate(m_buffer, cursor))
            {
                m_lappedCount += 1;
                cursor = m_buffer.getInt64Ordered(m_latestCounterIndex);
                m_recordOffset = (std::int32_t)cursor & m_mask;
            }

            m_cursor = cursor;
            m_nextRecord = cursor + m_buffer.getInt32(RecordDescriptor::recLengthOffset(m_recordOffset));

            if (RecordDescriptor::PADDING_MSG_TYPE_ID == m_buffer.getInt32(RecordDescriptor::msgTypeOffset(m_recordOffset)))
            {
                m_recordOffset = 0;
                m_cursor = m_nextRecord;
                m_nextRecord += m_buffer.getInt32(RecordDescriptor::recLengthOffset(m_recordOffset));
            }

            return true;
        }

        return false;
    }

    inline bool validate()
    {
        // TODO: full fence
#if !WIN32
        __asm__ volatile ("lock; addl $0,0(%%rsp)" : : : "cc", "memory");  // TODO: temp!!
#endif

        return validate(m_buffer, m_cursor);
    }

private:
    AtomicBuffer& m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_tailCounterIndex;
    util::index_t m_latestCounterIndex;

    util::index_t m_recordOffset;
    std::int64_t m_cursor;
    std::int64_t m_nextRecord;
    std::atomic<long> m_lappedCount;

    inline bool validate(AtomicBuffer& buffer, std::int64_t cursor)
    {
        return cursor == buffer.getInt64Ordered(RecordDescriptor::tailSequenceOffset(m_recordOffset));
    }
};

}}}}

#endif
