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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_APPENDER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_APPENDER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"
#include "LogBuffer.h"
#include "BufferClaim.h"

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

class LogAppender : public LogBuffer
{
public:
    enum ActionStatus
    {
        SUCCESS,
        TRIPPED,
        FAILURE
    };

    LogAppender(AtomicBuffer& logBuffer, AtomicBuffer& stateBuffer,
        std::uint8_t *defaultHdr, util::index_t defaultHdrLength, util::index_t maxFrameLength)
    : LogBuffer(logBuffer, stateBuffer),
        m_defaultHdr(defaultHdr),
        m_defaultHdrLength(defaultHdrLength),
        m_maxMessageLength(FrameDescriptor::calculateMaxMessageLength(capacity())),
        m_maxFrameLength(maxFrameLength),
        m_maxPayloadLength(m_maxFrameLength - defaultHdrLength)
    {
        FrameDescriptor::checkHeaderLength(defaultHdrLength);
        FrameDescriptor::checkMaxFrameLength(maxFrameLength);
    }

    inline util::index_t maxMessageLength()
    {
        return m_maxMessageLength;
    }

    inline util::index_t maxPayloadLength()
    {
        return m_maxPayloadLength;
    }

    inline util::index_t maxFrameLength()
    {
        return m_maxFrameLength;
    }

    inline ActionStatus append(AtomicBuffer& srcBuffer, util::index_t offset, util::index_t length)
    {
        checkMessageLength(length);

        if (length <= m_maxPayloadLength)
        {
            return appendUnfragmentedMessage(srcBuffer, offset, length);
        }

        return appendFragmentedMessage(srcBuffer, offset, length);
    }

    inline ActionStatus claim(util::index_t length, BufferClaim& bufferClaim)
    {
        checkClaimLength(length);

        const util::index_t frameLength = length + m_defaultHdrLength;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const util::index_t frameOffset = getTailAndAdd(alignedLength);

        if (isBeyondLogBufferCapacity(frameOffset, alignedLength, capacity()))
        {
            if (frameOffset < capacity())
            {
                appendPaddingFrame(logBuffer(), frameOffset);
                return ActionStatus::TRIPPED;
            }
            else if (frameOffset == capacity())
            {
                return ActionStatus::TRIPPED;
            }

            return ActionStatus::FAILURE;
        }

        logBuffer().putBytes(frameOffset, m_defaultHdr, m_defaultHdrLength);
        FrameDescriptor::frameFlags(logBuffer(), frameOffset, FrameDescriptor::UNFRAGMENTED);
        FrameDescriptor::frameTermOffset(logBuffer(), frameOffset, frameOffset);

        bufferClaim.buffer(&logBuffer())
            .offset(frameOffset + m_defaultHdrLength)
            .length(length)
            .frameLengthOffset(FrameDescriptor::lengthOffset(frameOffset))
            .frameLength(frameLength);

        return ActionStatus::SUCCESS;
    }

private:
    std::uint8_t *m_defaultHdr;
    util::index_t m_defaultHdrLength;
    const util::index_t m_maxMessageLength;
    const util::index_t m_maxFrameLength;
    const util::index_t m_maxPayloadLength;

    ActionStatus appendUnfragmentedMessage(AtomicBuffer& srcBuffer, util::index_t srcOffset, util::index_t length)
    {
        const util::index_t frameLength = length + m_defaultHdrLength;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const util::index_t frameOffset = getTailAndAdd(alignedLength);

        if (isBeyondLogBufferCapacity(frameOffset, alignedLength, capacity()))
        {
            if (frameOffset < capacity())
            {
                appendPaddingFrame(logBuffer(), frameOffset);
                return ActionStatus::TRIPPED;
            }
            else if (frameOffset == capacity())
            {
                return ActionStatus::TRIPPED;
            }

            return ActionStatus::FAILURE;
        }

        logBuffer().putBytes(frameOffset, m_defaultHdr, m_defaultHdrLength);
        logBuffer().putBytes(frameOffset + m_defaultHdrLength, srcBuffer, srcOffset, length);

        FrameDescriptor::frameFlags(logBuffer(), frameOffset, FrameDescriptor::UNFRAGMENTED);
        FrameDescriptor::frameTermOffset(logBuffer(), frameOffset, frameOffset);
        FrameDescriptor::frameLengthOrdered(logBuffer(), frameOffset, frameLength);

        return ActionStatus::SUCCESS;
    }

    ActionStatus appendFragmentedMessage(AtomicBuffer& srcBuffer, util::index_t srcOffset, util::index_t length)
    {
        const int numMaxPayloads = length / m_maxPayloadLength;
        const util::index_t remainingPayload = length % m_maxPayloadLength;
        const util::index_t requiredCapacity =
            util::BitUtil::align(remainingPayload + m_defaultHdrLength, FrameDescriptor::FRAME_ALIGNMENT) +
                (numMaxPayloads * m_maxFrameLength);
        util::index_t frameOffset = getTailAndAdd(requiredCapacity);

        if (isBeyondLogBufferCapacity(frameOffset, requiredCapacity, capacity()))
        {
            if (frameOffset < capacity())
            {
                appendPaddingFrame(logBuffer(), frameOffset);
                return ActionStatus::TRIPPED;
            }
            else if (frameOffset == capacity())
            {
                return ActionStatus::TRIPPED;
            }

            return ActionStatus::FAILURE;
        }

        std::uint8_t flags = FrameDescriptor::BEGIN_FRAG;
        util::index_t remaining = length;

        do
        {
            const util::index_t bytesToWrite = std::min(remaining, m_maxPayloadLength);
            const util::index_t frameLength = bytesToWrite + m_defaultHdrLength;
            const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

            logBuffer().putBytes(frameOffset, m_defaultHdr, m_defaultHdrLength);
            logBuffer().putBytes(frameOffset + m_defaultHdrLength, srcBuffer, srcOffset + (length - remaining), bytesToWrite);

            if (remaining <= m_maxPayloadLength)
            {
                flags |= FrameDescriptor::END_FRAG;
            }

            FrameDescriptor::frameFlags(logBuffer(), frameOffset, flags);
            FrameDescriptor::frameTermOffset(logBuffer(), frameOffset, frameOffset);
            FrameDescriptor::frameLengthOrdered(logBuffer(), frameOffset, frameLength);

            flags = 0;
            frameOffset += alignedLength;
            remaining -= bytesToWrite;
        }
        while (remaining > 0);

        return ActionStatus::SUCCESS;
    }

    inline bool isBeyondLogBufferCapacity(util::index_t frameOffset, util::index_t alignedFrameLength, util::index_t capacity)
    {
        return (frameOffset + alignedFrameLength + m_defaultHdrLength) > capacity;
    }

    inline void appendPaddingFrame(AtomicBuffer& logBuffer, util::index_t frameOffset)
    {
        logBuffer.putBytes(frameOffset, m_defaultHdr, m_defaultHdrLength);

        FrameDescriptor::frameType(logBuffer, frameOffset, FrameDescriptor::PADDING_FRAME_TYPE);
        FrameDescriptor::frameFlags(logBuffer, frameOffset, FrameDescriptor::UNFRAGMENTED);
        FrameDescriptor::frameTermOffset(logBuffer, frameOffset, frameOffset);
        FrameDescriptor::frameLengthOrdered(logBuffer, frameOffset, capacity() - frameOffset);
    }

    inline std::int32_t getTailAndAdd(std::int32_t delta)
    {
        return stateBuffer().getAndAddInt32(LogBufferDescriptor::TAIL_COUNTER_OFFSET, delta);
    }

    inline void checkMessageLength(util::index_t length)
    {
        if (length > m_maxMessageLength)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("encoded message exceeds maxMessageLength of %d, length=%d", m_maxMessageLength, length), SOURCEINFO);
        }
    }

    inline void checkClaimLength(util::index_t length)
    {
        if (length > m_maxPayloadLength)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("claim exceeds maxPayloadLength of %d, length=%d", m_maxPayloadLength, length), SOURCEINFO);
        }
    }
};

}}}}

#endif
