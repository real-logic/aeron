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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "HeaderWriter.h"
#include "LogBufferDescriptor.h"
#include "LogBufferPartition.h"
#include "BufferClaim.h"
#include "DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer {

#define TERM_APPENDER_TRIPPED ((std::int32_t)-1)
#define TERM_APPENDER_FAILED ((std::int32_t)-2)

class TermAppender
{
public:
    struct Result
    {
        std::int64_t termOffset;
        std::int32_t termId;
    };

    TermAppender(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer) :
        m_termBuffer(termBuffer),
        m_metaDataBuffer(metaDataBuffer)
    {
    }

    inline AtomicBuffer& termBuffer()
    {
        return m_termBuffer;
    }

    inline AtomicBuffer& metaDataBuffer()
    {
        return m_metaDataBuffer;
    }

    inline std::int64_t rawTailVolatile() const
    {
        return m_metaDataBuffer.getInt64Volatile(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET);
    }

    inline void tailTermId(const std::int32_t termId)
    {
        m_metaDataBuffer.putInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, ((static_cast<std::int64_t>(termId)) << 32));
    }

    inline void statusOrdered(const std::int32_t status)
    {
        m_metaDataBuffer.putInt32Ordered(LogBufferDescriptor::TERM_STATUS_OFFSET, status);
    }

    inline void claim(Result& result, const HeaderWriter& header, util::index_t length, BufferClaim& bufferClaim)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;

        const std::int32_t termLength = m_termBuffer.capacity();

        result.termId = LogBufferDescriptor::termId(rawTail);
        result.termOffset = termOffset + alignedLength;
        if (result.termOffset > (termLength - DataFrameHeader::LENGTH))
        {
            handleEndOfLogCondition(result, m_termBuffer, static_cast<std::int32_t>(termOffset), header, termLength);
        }
        else
        {
            std::int32_t offset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, offset, frameLength, result.termId);
            bufferClaim.wrap(m_termBuffer, offset, frameLength);
        }
    }

    inline void appendUnfragmentedMessage(
        Result& result,
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;

        const std::int32_t termLength = m_termBuffer.capacity();

        result.termId = LogBufferDescriptor::termId(rawTail);
        result.termOffset = termOffset + alignedLength;
        if (result.termOffset > (termLength - DataFrameHeader::LENGTH))
        {
            handleEndOfLogCondition(result, m_termBuffer, static_cast<std::int32_t>(termOffset), header, termLength);
        }
        else
        {
            std::int32_t offset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, offset, frameLength, LogBufferDescriptor::termId(rawTail));
            m_termBuffer.putBytes(offset + DataFrameHeader::LENGTH, srcBuffer, srcOffset, length);
            FrameDescriptor::frameLengthOrdered(m_termBuffer, offset, frameLength);
        }
    }

    void appendFragmentedMessage(
        Result& result,
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length,
        util::index_t maxPayloadLength)
    {
        const int numMaxPayloads = length / maxPayloadLength;
        const util::index_t remainingPayload = length % maxPayloadLength;
        const util::index_t lastFrameLength = (remainingPayload > 0) ?
            util::BitUtil::align(remainingPayload + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT) : 0;
        const util::index_t requiredLength =
            (numMaxPayloads * (maxPayloadLength + DataFrameHeader::LENGTH)) + lastFrameLength;
        const std::int64_t rawTail = getAndAddRawTail(requiredLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;

        const std::int32_t termLength = m_termBuffer.capacity();

        result.termId = LogBufferDescriptor::termId(rawTail);
        result.termOffset = termOffset + requiredLength;
        if (result.termOffset > (termLength - DataFrameHeader::LENGTH))
        {
            handleEndOfLogCondition(result, m_termBuffer, static_cast<std::int32_t>(termOffset), header, termLength);
        }
        else
        {
            std::uint8_t flags = FrameDescriptor::BEGIN_FRAG;
            util::index_t remaining = length;
            std::int32_t offset = static_cast<std::int32_t>(termOffset);

            do
            {
                const util::index_t bytesToWrite = std::min(remaining, maxPayloadLength);
                const util::index_t frameLength = bytesToWrite + DataFrameHeader::LENGTH;
                const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

                header.write(m_termBuffer, offset, frameLength, result.termId);
                m_termBuffer.putBytes(
                    offset + DataFrameHeader::LENGTH,
                    srcBuffer,
                    srcOffset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= FrameDescriptor::END_FRAG;
                }

                FrameDescriptor::frameFlags(m_termBuffer, offset, flags);
                FrameDescriptor::frameLengthOrdered(m_termBuffer, offset, frameLength);

                flags = 0;
                offset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }
    }

private:
    AtomicBuffer& m_termBuffer;
    AtomicBuffer& m_metaDataBuffer;

    inline static void handleEndOfLogCondition(
        Result& result,
        AtomicBuffer& termBuffer,
        util::index_t termOffset,
        const HeaderWriter& header,
        util::index_t termLength)
    {
        result.termOffset = TERM_APPENDER_FAILED;

        if (termOffset <= (termLength - DataFrameHeader::LENGTH))
        {
            const std::int32_t paddingLength = termLength - termOffset;
            header.write(termBuffer, termOffset, paddingLength, result.termId);
            FrameDescriptor::frameType(termBuffer, termOffset, DataFrameHeader::HDR_TYPE_PAD);
            FrameDescriptor::frameLengthOrdered(termBuffer, termOffset, paddingLength);

            result.termOffset = TERM_APPENDER_TRIPPED;
        }
    }

    inline std::int64_t getAndAddRawTail(const util::index_t alignedLength)
    {
        return m_metaDataBuffer.getAndAddInt64(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, alignedLength);
    }
};

}}}

#endif
