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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER__

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "HeaderWriter.h"
#include "LogBufferDescriptor.h"
#include "BufferClaim.h"
#include "DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Supplies the reserved value field for a data frame header. The returned value will be set in the header as
 * Little Endian format.
 *
 * This will be called as the last action of encoding a data frame right before the length is set. All other fields
 * in the header plus the body of the frame will have been written at the point of supply.
 *
 * @param termBuffer for the message
 * @param termOffset of the start of the message
 * @param length of the message in bytes
 */
typedef std::function<std::int64_t(
    AtomicBuffer& termBuffer,
    util::index_t termOffset,
    util::index_t length)> on_reserved_value_supplier_t;

#define TERM_APPENDER_TRIPPED ((std::int32_t)-1)
#define TERM_APPENDER_FAILED ((std::int32_t)-2)

static const on_reserved_value_supplier_t DEFAULT_RESERVED_VALUE_SUPPLIER =
    [](AtomicBuffer&, util::index_t, util::index_t) -> std::int64_t { return 0; };

class TermAppender
{
public:
    struct Result
    {
        std::int64_t termOffset;
        std::int32_t termId;
    };

    TermAppender(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer, const int partitionIndex) :
        m_termBuffer(termBuffer),
        m_tailBuffer(metaDataBuffer),
        m_tailOffset(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)))
    {
    }

    inline AtomicBuffer& termBuffer()
    {
        return m_termBuffer;
    }

    inline std::int64_t rawTailVolatile() const
    {
        return m_tailBuffer.getInt64Volatile(m_tailOffset);
    }

    inline void tailTermId(const std::int32_t termId)
    {
        m_tailBuffer.putInt64(m_tailOffset, ((static_cast<std::int64_t>(termId)) << 32));
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
        if (result.termOffset > termLength)
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
        util::index_t length,
        const on_reserved_value_supplier_t& reservedValueSupplier)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;

        const std::int32_t termLength = m_termBuffer.capacity();

        result.termId = LogBufferDescriptor::termId(rawTail);
        result.termOffset = termOffset + alignedLength;
        if (result.termOffset > termLength)
        {
            handleEndOfLogCondition(result, m_termBuffer, static_cast<std::int32_t>(termOffset), header, termLength);
        }
        else
        {
            std::int32_t offset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, offset, frameLength, LogBufferDescriptor::termId(rawTail));
            m_termBuffer.putBytes(offset + DataFrameHeader::LENGTH, srcBuffer, srcOffset, length);

            const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, offset, frameLength);
            m_termBuffer.putInt64(offset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

            FrameDescriptor::frameLengthOrdered(m_termBuffer, offset, frameLength);
        }
    }

    void appendFragmentedMessage(
        Result& result,
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length,
        util::index_t maxPayloadLength,
        const on_reserved_value_supplier_t& reservedValueSupplier)
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
        if (result.termOffset > termLength)
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

                const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, offset, frameLength);
                m_termBuffer.putInt64(offset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

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
    AtomicBuffer& m_tailBuffer;
    const util::index_t m_tailOffset;

    inline static void handleEndOfLogCondition(
        Result& result,
        AtomicBuffer& termBuffer,
        util::index_t termOffset,
        const HeaderWriter& header,
        util::index_t termLength)
    {
        result.termOffset = TERM_APPENDER_FAILED;

        if (termOffset <= termLength)
        {
            result.termOffset = TERM_APPENDER_TRIPPED;

            if (termOffset < termLength)
            {
                const std::int32_t paddingLength = termLength - termOffset;
                header.write(termBuffer, termOffset, paddingLength, result.termId);
                FrameDescriptor::frameType(termBuffer, termOffset, DataFrameHeader::HDR_TYPE_PAD);
                FrameDescriptor::frameLengthOrdered(termBuffer, termOffset, paddingLength);
            }
        }
    }

    inline std::int64_t getAndAddRawTail(const util::index_t alignedLength)
    {
        return m_tailBuffer.getAndAddInt64(m_tailOffset, alignedLength);
    }
};

}}}

#endif
