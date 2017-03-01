/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef AERON_CONCURRENT_RINGBUFFER_MANY_TO_ONE_RING_BUFFER__
#define AERON_CONCURRENT_RINGBUFFER_MANY_TO_ONE_RING_BUFFER__

#include <limits.h>
#include <functional>
#include <algorithm>
#include <util/Index.h>
#include <util/LangUtil.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/Atomic64.h>
#include "RingBufferDescriptor.h"
#include "RecordDescriptor.h"

namespace aeron { namespace concurrent { namespace ringbuffer {

class ManyToOneRingBuffer
{
public:
    ManyToOneRingBuffer(concurrent::AtomicBuffer& buffer)
        : m_buffer(buffer)
    {
        m_capacity = buffer.capacity() - RingBufferDescriptor::TRAILER_LENGTH;

        RingBufferDescriptor::checkCapacity(m_capacity);

        m_maxMsgLength = m_capacity / 8;

        m_tailPositionIndex = m_capacity + RingBufferDescriptor::TAIL_POSITION_OFFSET;
        m_headCachePositionIndex = m_capacity + RingBufferDescriptor::HEAD_CACHE_POSITION_OFFSET;
        m_headPositionIndex = m_capacity + RingBufferDescriptor::HEAD_POSITION_OFFSET;
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
            m_buffer.putInt64Ordered(recordIndex, RecordDescriptor::makeHeader(-recordLength, msgTypeId));
            m_buffer.putBytes(RecordDescriptor::encodedMsgOffset(recordIndex), srcBuffer, srcIndex, length);
            m_buffer.putInt32Ordered(RecordDescriptor::lengthOffset(recordIndex), recordLength);

            isSuccessful = true;
        }

        return isSuccessful;
    }

    int read(const handler_t& handler, int messageCountLimit)
    {
        const std::int64_t head = m_buffer.getInt64(m_headPositionIndex);
        const std::int32_t headIndex = (std::int32_t)head & (m_capacity - 1);
        const std::int32_t contiguousBlockLength = m_capacity - headIndex;
        int messagesRead = 0;
        int bytesRead = 0;

        auto cleanup = util::InvokeOnScopeExit {
            [&]()
            {
                if (bytesRead != 0)
                {
                    m_buffer.setMemory(headIndex, bytesRead, 0);
                    m_buffer.putInt64Ordered(m_headPositionIndex, head + bytesRead);
                }
            }};

        while ((bytesRead < contiguousBlockLength) && (messagesRead < messageCountLimit))
        {
            const std::int32_t recordIndex = headIndex + bytesRead;
            const std::int64_t header = m_buffer.getInt64Volatile(recordIndex);
            const std::int32_t recordLength = RecordDescriptor::recordLength(header);

            if (recordLength <= 0)
            {
                break;
            }

            bytesRead += util::BitUtil::align(recordLength, RecordDescriptor::ALIGNMENT);

            const std::int32_t msgTypeId = RecordDescriptor::messageTypeId(header);
            if (RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId)
            {
                continue;
            }

            ++messagesRead;
            handler(
                msgTypeId, m_buffer, RecordDescriptor::encodedMsgOffset(recordIndex),
                recordLength - RecordDescriptor::HEADER_LENGTH);
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

    inline std::int64_t producerPosition() const
    {
        return m_buffer.getInt64Volatile(m_tailPositionIndex);
    }

    inline std::int64_t consumerPosition() const
    {
        return m_buffer.getInt64Volatile(m_headPositionIndex);
    }

    inline std::int32_t size() const
    {
        std::int64_t headBefore;
        std::int64_t tail;
        std::int64_t headAfter = m_buffer.getInt64Volatile(m_headPositionIndex);

        do
        {
            headBefore = headAfter;
            tail = m_buffer.getInt64Volatile(m_tailPositionIndex);
            headAfter = m_buffer.getInt64Volatile(m_headPositionIndex);
        }
        while (headAfter != headBefore);

        return (std::int32_t)(tail - headAfter);
    }

    bool unblock()
    {
        const util::index_t mask = m_capacity - 1;
        const std::int32_t consumerIndex = (std::int32_t)(m_buffer.getInt64Volatile(m_headPositionIndex) & mask);
        const std::int32_t producerIndex = (std::int32_t)(m_buffer.getInt64Volatile(m_tailPositionIndex) & mask);

        if (producerIndex == consumerIndex)
        {
            return false;
        }

        bool unblocked = false;
        std::int32_t length = m_buffer.getInt32Volatile(consumerIndex);
        if (length < 0)
        {
            m_buffer.putInt64Ordered(consumerIndex, RecordDescriptor::makeHeader(-length, RecordDescriptor::PADDING_MSG_TYPE_ID));
            unblocked = true;
        }
        else if (0 == length)
        {
            const std::int32_t limit = producerIndex > consumerIndex ? producerIndex : m_buffer.capacity();
            std::int32_t i = consumerIndex + RecordDescriptor::ALIGNMENT;

            do
            {
                length = m_buffer.getInt32Volatile(i);
                if (0 != length)
                {
                    if (scanBackToConfirmStillZeroed(m_buffer, i, consumerIndex))
                    {
                        m_buffer.putInt64Ordered(consumerIndex, RecordDescriptor::makeHeader(i - consumerIndex, RecordDescriptor::PADDING_MSG_TYPE_ID));
                        unblocked = true;
                    }

                    break;
                }

                i += RecordDescriptor::ALIGNMENT;
            }
            while (i < limit);
        }

        return unblocked;
    }

private:

    static const util::index_t INSUFFICIENT_CAPACITY = -2;

    concurrent::AtomicBuffer &m_buffer;
    util::index_t m_capacity;
    util::index_t m_maxMsgLength;
    util::index_t m_headPositionIndex;
    util::index_t m_headCachePositionIndex;
    util::index_t m_tailPositionIndex;
    util::index_t m_correlationIdCounterIndex;
    util::index_t m_consumerHeartbeatIndex;

    util::index_t claimCapacity(util::index_t requiredCapacity)
    {
        const util::index_t mask = m_capacity - 1;
        std::int64_t head = m_buffer.getInt64Volatile(m_headCachePositionIndex);

        std::int64_t tail;
        util::index_t tailIndex;
        util::index_t padding;
        do
        {
            tail = m_buffer.getInt64Volatile(m_tailPositionIndex);
            const util::index_t availableCapacity = m_capacity - (util::index_t)(tail - head);

            if (requiredCapacity > availableCapacity)
            {
                head = m_buffer.getInt64Volatile(m_headPositionIndex);

                if (requiredCapacity > (m_capacity - (util::index_t)(tail - head)))
                {
                    return INSUFFICIENT_CAPACITY;
                }

                m_buffer.putInt64Ordered(m_headCachePositionIndex, head);
            }

            padding = 0;
            tailIndex = (util::index_t)tail & mask;
            const util::index_t toBufferEndLength = m_capacity - tailIndex;

            if (requiredCapacity > toBufferEndLength)
            {
                std::int32_t headIndex = (std::int32_t)head & mask;

                if (requiredCapacity > headIndex)
                {
                    head = m_buffer.getInt64Volatile(m_headPositionIndex);
                    headIndex = (std::int32_t)head & mask;

                    if (requiredCapacity > headIndex)
                    {
                        return INSUFFICIENT_CAPACITY;
                    }

                    m_buffer.putInt64Ordered(m_headCachePositionIndex, head);
                }

                padding = toBufferEndLength;
            }
        }
        while (!m_buffer.compareAndSetInt64(m_tailPositionIndex, tail, tail + requiredCapacity + padding));

        if (0 != padding)
        {
            m_buffer.putInt64Ordered(tailIndex, RecordDescriptor::makeHeader(padding, RecordDescriptor::PADDING_MSG_TYPE_ID));
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

    inline static bool scanBackToConfirmStillZeroed(AtomicBuffer& buffer, std::int32_t from, std::int32_t limit)
    {
        std::int32_t i = from - RecordDescriptor::ALIGNMENT;
        bool allZeroes = true;

        while (i >= limit)
        {
            if (0 != buffer.getInt32Volatile(i))
            {
                allZeroes = false;
                break;
            }

            i -= RecordDescriptor::ALIGNMENT;
        }

        return allZeroes;
    }
};

}}}

#endif
