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

#ifndef INCLUDED_AERON_CONCURRENT_BROADCAST_TRANSMITTER__
#define INCLUDED_AERON_CONCURRENT_BROADCAST_TRANSMITTER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "BroadcastBufferDescriptor.h"
#include "RecordDescriptor.h"

namespace aeron { namespace concurrent { namespace broadcast {

class BroadcastTransmitter
{
public:
    BroadcastTransmitter(AtomicBuffer& buffer) :
        m_buffer(buffer),
        m_capacity(buffer.capacity() - BroadcastBufferDescriptor::TRAILER_LENGTH),
        m_mask(m_capacity - 1),
        m_maxMsgLength(RecordDescriptor::calculateMaxMessageLength(m_capacity)),
        m_tailIntentCounterIndex(m_capacity + BroadcastBufferDescriptor::TAIL_INTENT_COUNTER_OFFSET),
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

        std::int64_t currentTail = m_buffer.getInt64(m_tailCounterIndex);
        std::int32_t recordOffset = (std::int32_t) currentTail & m_mask;
        const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
        const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
        const std::int64_t newTail = currentTail + alignedRecordLength;
        const std::int32_t toEndOfBuffer = m_capacity - recordOffset;

        if (toEndOfBuffer < alignedRecordLength)
        {
            signalTailIntent(m_buffer, newTail + toEndOfBuffer);

            insertPaddingRecord(m_buffer, recordOffset, toEndOfBuffer);

            currentTail += toEndOfBuffer;
            recordOffset = 0;
        }
        else
        {
            signalTailIntent(m_buffer, newTail);
        }

        m_buffer.putInt32(RecordDescriptor::lengthOffset(recordOffset), recordLength);
        m_buffer.putInt32(RecordDescriptor::typeOffset(recordOffset), msgTypeId);

        m_buffer.putBytes(RecordDescriptor::msgOffset(recordOffset), srcBuffer, srcIndex, length);

        m_buffer.putInt64(m_latestCounterIndex, currentTail);
        m_buffer.putInt64Ordered(m_tailCounterIndex, currentTail + alignedRecordLength);
    }

private:
    AtomicBuffer& m_buffer;
    util::index_t m_capacity;
    util::index_t m_mask;
    util::index_t m_maxMsgLength;
    util::index_t m_tailIntentCounterIndex;
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

    inline void signalTailIntent(AtomicBuffer& buffer, std::int64_t newTail)
    {
        buffer.putInt64Ordered(m_tailIntentCounterIndex, newTail);
        // store fence = release()
        atomic::release();
    }

    inline static void insertPaddingRecord(AtomicBuffer& buffer, std::int32_t recordOffset, std::int32_t length)
    {
        buffer.putInt32(RecordDescriptor::lengthOffset(recordOffset), length);
        buffer.putInt32(RecordDescriptor::typeOffset(recordOffset), RecordDescriptor::PADDING_MSG_TYPE_ID);
    }
};

}}}

#endif
