/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

namespace aeron { namespace concurrent { namespace broadcast {

class BroadcastReceiver
{
public:
    BroadcastReceiver(AtomicBuffer& buffer) :
        m_buffer(buffer),
        m_capacity(buffer.capacity() - BroadcastBufferDescriptor::TRAILER_LENGTH),
        m_mask(m_capacity - 1),
        m_tailIntentCounterIndex(m_capacity + BroadcastBufferDescriptor::TAIL_INTENT_COUNTER_OFFSET),
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
        return m_buffer.getInt32(RecordDescriptor::typeOffset(m_recordOffset));
    }

    inline util::index_t offset()
    {
        return RecordDescriptor::msgOffset(m_recordOffset);
    }

    inline std::int32_t length()
    {
        return m_buffer.getInt32(RecordDescriptor::lengthOffset(m_recordOffset)) - RecordDescriptor::HEADER_LENGTH;
    }

    inline AtomicBuffer& buffer()
    {
        return m_buffer;
    }

    bool receiveNext()
    {
        bool isAvailable = false;
        const std::int64_t tail = m_buffer.getInt64Volatile(m_tailCounterIndex);
        std::int64_t cursor = m_nextRecord;

        if (tail > cursor)
        {
            util::index_t recordOffset = (std::int32_t)cursor & m_mask;

            if (!validate(cursor))
            {
                m_lappedCount += 1;
                cursor = m_buffer.getInt64(m_latestCounterIndex);
                recordOffset = (std::int32_t)cursor & m_mask;
            }

            m_cursor = cursor;
            m_nextRecord = cursor +
                util::BitUtil::align(
                    m_buffer.getInt32(RecordDescriptor::lengthOffset(recordOffset)),
                    RecordDescriptor::RECORD_ALIGNMENT);

            if (RecordDescriptor::PADDING_MSG_TYPE_ID == m_buffer.getInt32(RecordDescriptor::typeOffset(recordOffset)))
            {
                recordOffset = 0;
                m_cursor = m_nextRecord;
                m_nextRecord +=
                    util::BitUtil::align(
                        m_buffer.getInt32(RecordDescriptor::lengthOffset(recordOffset)),
                        RecordDescriptor::RECORD_ALIGNMENT);
            }

            m_recordOffset = recordOffset;
            isAvailable = true;
        }

        return isAvailable;
    }

    inline bool validate()
    {
        // load fence = acquire()
        atomic::acquire();

        return validate(m_cursor);
    }

private:
    AtomicBuffer& m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_tailIntentCounterIndex;
    util::index_t m_tailCounterIndex;
    util::index_t m_latestCounterIndex;

    util::index_t m_recordOffset;
    std::int64_t m_cursor;
    std::int64_t m_nextRecord;
    std::atomic<long> m_lappedCount;

    inline bool validate(std::int64_t cursor)
    {
        return (cursor + m_capacity) > m_buffer.getInt64Volatile(m_tailIntentCounterIndex);
    }
};

}}}

#endif
