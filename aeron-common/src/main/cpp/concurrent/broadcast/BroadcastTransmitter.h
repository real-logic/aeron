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

#ifndef INCLUDED_AERON_CONCURRENT_BROADCAST_TRANSMITTER__
#define INCLUDED_AERON_CONCURRENT_BROADCAST_TRANSMITTER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "BroadcastBufferDescriptor.h"
#include "RecordDescriptor.h"

namespace aeron { namespace common { namespace concurrent { namespace broadcast {

class BroadcastTransmitter
{
public:
    BroadcastTransmitter(AtomicBuffer& buffer) :
        m_buffer(buffer),
        m_capacity(buffer.getCapacity() - BroadcastBufferDescriptor::TRAILER_LENGTH),
        m_mask(m_capacity - 1),
        m_maxMsgLength(m_capacity),
        m_tailCounterIndex(m_capacity + BroadcastBufferDescriptor::TAIL_COUNTER_OFFSET),
        m_latestCounterIndex(m_capacity + BroadcastBufferDescriptor::LATEST_COUNTER_OFFSET)
    {
        BroadcastBufferDescriptor::checkCapacity(m_capacity);
    }

    inline util::index_t capacity()
    {
        return m_capacity;
    }

    inline util::index_t maxMsgLength()
    {
        return m_maxMsgLength;
    }

    void transmit(std::int32_t msgTypeId, concurrent::AtomicBuffer& srcBuffer, util::index_t srcIndex, util::index_t length)
    {
        RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMessageLength(length);

        std::int64_t tail = m_buffer.getInt64(m_tailCounterIndex);
        std::int32_t recordOffset = (std::int32_t)tail & m_mask;
        const std::int32_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::RECORD_ALIGNMENT);

        const std::int32_t remainingBuffer = m_capacity - recordOffset;
        if (remainingBuffer < recordLength)
        {
            insertPaddingRecord(m_buffer, tail, recordOffset, remainingBuffer);
            tail += remainingBuffer;
            recordOffset = 0;
        }

        m_buffer.putInt64Ordered(RecordDescriptor::tailSequenceOffset(recordOffset), tail);
        m_buffer.putInt32(RecordDescriptor::recLengthOffset(recordOffset), recordLength);
        m_buffer.putInt32(RecordDescriptor::msgLengthOffset(recordOffset), length);
        m_buffer.putInt32(RecordDescriptor::msgTypeOffset(recordOffset), msgTypeId);

        m_buffer.putBytes(RecordDescriptor::msgOffset(recordOffset), srcBuffer, srcIndex, length);

        putLatestCounter(m_buffer, tail);
        incrementTailOrdered(m_buffer, tail, recordLength);
    }

private:
    AtomicBuffer& m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_maxMsgLength;
    util::index_t m_tailCounterIndex;
    util::index_t m_latestCounterIndex;

    inline void checkMessageLength(util::index_t length)
    {
        if (length > m_maxMsgLength)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("encoded message exceeds maxMsgLength of %d, length=%d", m_maxMsgLength, length), SOURCEINFO);
        }
    }

    inline void putLatestCounter(AtomicBuffer& buffer, std::int64_t tail)
    {
        buffer.putInt64(m_latestCounterIndex, tail);
    }

    inline void incrementTailOrdered(AtomicBuffer& buffer, std::int64_t tail, util::index_t recordLength)
    {
        buffer.putInt64Ordered(m_tailCounterIndex, tail + recordLength);
    }

    inline static void insertPaddingRecord(AtomicBuffer& buffer, std::int64_t tail, std::int32_t recordOffset, std::int32_t remainingBuffer)
    {
        buffer.putInt64Ordered(RecordDescriptor::tailSequenceOffset(recordOffset), tail);
        buffer.putInt32(RecordDescriptor::recLengthOffset(recordOffset), remainingBuffer);
        buffer.putInt32(RecordDescriptor::msgLengthOffset(recordOffset), 0);
        buffer.putInt32(RecordDescriptor::msgTypeOffset(recordOffset), RecordDescriptor::PADDING_MSG_TYPE_ID);
    }
};

}}}}

#endif
