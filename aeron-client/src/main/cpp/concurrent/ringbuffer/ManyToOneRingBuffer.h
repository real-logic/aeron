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

#ifndef INCLUDED_AERON_CONCURRENT_RINGBUFFER_MANY_TO_ONE_RING_BUFFER__
#define INCLUDED_AERON_CONCURRENT_RINGBUFFER_MANY_TO_ONE_RING_BUFFER__

#include <limits.h>
#include <functional>
#include <algorithm>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/Atomic64.h>
#include "RingBufferDescriptor.h"
#include "RecordDescriptor.h"

namespace aeron { namespace concurrent { namespace ringbuffer {

/** The read handler function signature */
typedef std::function<void(std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)> handler_t;

class ManyToOneRingBuffer
{
public:
    ManyToOneRingBuffer(concurrent::AtomicBuffer& buffer)
        : m_buffer(buffer)
    {
        m_capacity = buffer.capacity() - RingBufferDescriptor::TRAILER_LENGTH;

        RingBufferDescriptor::checkCapacity(m_capacity);

        m_mask = m_capacity - 1;
        m_maxMsgLength = m_capacity / 8;

        m_tailCounterIndex = m_capacity + RingBufferDescriptor::TAIL_COUNTER_OFFSET;
        m_headCounterIndex = m_capacity + RingBufferDescriptor::HEAD_COUNTER_OFFSET;
        m_correlationIdCounterIndex = m_capacity + RingBufferDescriptor::CORRELATION_COUNTER_OFFSET;
        m_consumerHeartbeatIndex = m_capacity + RingBufferDescriptor::CONSUMER_HEARTBEAT_OFFSET;
    }

    ManyToOneRingBuffer(const ManyToOneRingBuffer&) = delete;
    ManyToOneRingBuffer& operator=(const ManyToOneRingBuffer&) = delete;

    inline util::index_t capacity() const
    {
        return m_capacity;
    }

    bool write(std::int32_t msgTypeId, concurrent::AtomicBuffer& srcBuffer, util::index_t srcIndex, util::index_t length)
    {
        bool isSuccessful = false;

        RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMsgLength(length);

        const util::index_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
        const util::index_t requiredCapacity = util::BitUtil::align(recordLength, RecordDescriptor::ALIGNMENT);
        const util::index_t recordIndex = claimCapacity(requiredCapacity);

        if (INSUFFICIENT_CAPACITY != recordIndex)
        {
            m_buffer.putInt32(RecordDescriptor::lengthOffset(recordIndex), -recordLength);
            atomic::thread_fence();

            m_buffer.putBytes(RecordDescriptor::encodedMsgOffset(recordIndex), srcBuffer, srcIndex, length);

            m_buffer.putInt32(RecordDescriptor::typeOffset(recordIndex), msgTypeId);
            m_buffer.putInt32Ordered(RecordDescriptor::lengthOffset(recordIndex), recordLength);

            isSuccessful = true;
        }

        return isSuccessful;
    }

    int read(const handler_t& handler, int messageCountLimit)
    {
        const std::int64_t tail = m_buffer.getInt64Volatile(m_tailCounterIndex);
        const std::int64_t head = m_buffer.getInt64Volatile(m_headCounterIndex);
        const std::int32_t available = (std::int32_t)(tail - head);
        int messagesRead = 0;

        if (available > 0)
        {
            const std::int32_t headIndex = (std::int32_t)head & m_mask;
            const std::int32_t contiguousBlockSize = std::min(available, m_capacity - headIndex);
            int bytesRead = 0;

            while ((bytesRead < contiguousBlockSize) && (messagesRead < messageCountLimit))
            {
                const std::int32_t recordIndex = headIndex + bytesRead;
                const std::int32_t recordLength = m_buffer.getInt32Volatile(
                    RecordDescriptor::lengthOffset(recordIndex));
                if (recordLength <= 0)
                {
                    break;
                }

                bytesRead += util::BitUtil::align(recordLength, RecordDescriptor::ALIGNMENT);

                const std::int32_t msgTypeId = m_buffer.getInt32(RecordDescriptor::typeOffset(recordIndex));
                if (RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId)
                {
                    continue;
                }

                ++messagesRead;
                handler(
                    msgTypeId, m_buffer, RecordDescriptor::encodedMsgOffset(recordIndex),
                    recordLength - RecordDescriptor::HEADER_LENGTH);
            }

            // TODO: RAII for catching exceptions from handler call
            m_buffer.setMemory(headIndex, bytesRead, 0);
            m_buffer.putInt64Ordered(m_headCounterIndex, head + bytesRead);
        }

        return messagesRead;
    }

    inline int read(const handler_t& handler)
    {
        return read(handler, INT_MAX);
    }

    inline util::index_t maxMsgLength()
    {
        return m_maxMsgLength;
    }

    inline std::int64_t nextCorrelationId()
    {
        return m_buffer.getAndAddInt64(m_correlationIdCounterIndex, 1);
    }

    inline void consumerHeartbeatTime(std::int64_t time)
    {
        m_buffer.putInt64Ordered(m_consumerHeartbeatIndex, time);
    }

    inline std::int64_t consumerHeartbeatTime() const
    {
        return m_buffer.getInt64Volatile(m_consumerHeartbeatIndex);
    }

private:

    static const util::index_t INSUFFICIENT_CAPACITY = -2;

    concurrent::AtomicBuffer &m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_maxMsgLength;
    util::index_t m_headCounterIndex;
    util::index_t m_tailCounterIndex;
    util::index_t m_correlationIdCounterIndex;
    util::index_t m_consumerHeartbeatIndex;

    util::index_t claimCapacity(util::index_t requiredCapacity)
    {
        const std::int64_t head = m_buffer.getInt64Volatile(m_headCounterIndex);
        const util::index_t headIndex = (util::index_t)head & m_mask;

        std::int64_t tail;
        util::index_t tailIndex;
        util::index_t padding;
        do
        {
            tail = m_buffer.getInt64Volatile(m_tailCounterIndex);
            const util::index_t availableCapacity = m_capacity - (util::index_t)(tail - head);

            if (requiredCapacity > availableCapacity)
            {
                return INSUFFICIENT_CAPACITY;
            }

            padding = 0;
            tailIndex = (util::index_t)tail & m_mask;
            const util::index_t bufferEndLength = m_capacity - tailIndex;

            if (requiredCapacity > bufferEndLength)
            {
                if (requiredCapacity > headIndex)
                {
                    return INSUFFICIENT_CAPACITY;
                }

                padding = bufferEndLength;
            }
        }
        while (!m_buffer.compareAndSetInt64(m_tailCounterIndex, tail, tail + requiredCapacity + padding));

        if (0 != padding)
        {
            m_buffer.putInt32(RecordDescriptor::lengthOffset(tailIndex), -padding);
            atomic::thread_fence();

            m_buffer.putInt32(RecordDescriptor::typeOffset(tailIndex), RecordDescriptor::PADDING_MSG_TYPE_ID);
            m_buffer.putInt32Ordered(RecordDescriptor::lengthOffset(tailIndex), padding);

            tailIndex = 0;
        }

        return tailIndex;
    }

    inline void checkMsgLength(util::index_t length) const
    {
        if (length > m_maxMsgLength)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("encoded message exceeds maxMsgLength of %d, length=%d", m_maxMsgLength, length), SOURCEINFO);
        }
    }
};

}}}

#endif
