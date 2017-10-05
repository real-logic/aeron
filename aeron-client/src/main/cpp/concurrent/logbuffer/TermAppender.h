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

    inline std::int32_t claim(
        const HeaderWriter& header,
        util::index_t length,
        BufferClaim& bufferClaim,
        std::int32_t activeTermId)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
        const std::int32_t termId = LogBufferDescriptor::termId(rawTail);

        const std::int32_t termLength = m_termBuffer.capacity();

        checkTerm(activeTermId, termId);

        std::int64_t resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(m_termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            const std::int32_t frameOffset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, frameOffset, frameLength, termId);
            bufferClaim.wrap(m_termBuffer, frameOffset, frameLength);
        }

        return static_cast<std::int32_t>(resultingOffset);
    }

    inline std::int32_t appendUnfragmentedMessage(
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length,
        const on_reserved_value_supplier_t& reservedValueSupplier,
        std::int32_t activeTermId)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
        const std::int32_t termId = LogBufferDescriptor::termId(rawTail);

        const std::int32_t termLength = m_termBuffer.capacity();

        checkTerm(activeTermId, termId);

        std::int64_t resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(m_termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            const std::int32_t frameOffset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, frameOffset, frameLength, termId);
            m_termBuffer.putBytes(frameOffset + DataFrameHeader::LENGTH, srcBuffer, srcOffset, length);

            const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, frameOffset, frameLength);
            m_termBuffer.putInt64(frameOffset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

            FrameDescriptor::frameLengthOrdered(m_termBuffer, frameOffset, frameLength);
        }

        return static_cast<std::int32_t>(resultingOffset);
    }

    template <class BufferIterator> std::int32_t appendUnfragmentedMessage(
        const HeaderWriter& header,
        BufferIterator bufferIt,
        util::index_t length,
        const on_reserved_value_supplier_t& reservedValueSupplier,
        std::int32_t activeTermId)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        const std::int64_t rawTail = getAndAddRawTail(alignedLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
        const std::int32_t termId = LogBufferDescriptor::termId(rawTail);

        const std::int32_t termLength = m_termBuffer.capacity();

        checkTerm(activeTermId, termId);

        std::int64_t resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(m_termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            const std::int32_t frameOffset = static_cast<std::int32_t>(termOffset);
            header.write(m_termBuffer, frameOffset, frameLength, termId);

            std::int32_t offset = frameOffset + DataFrameHeader::LENGTH;
            for (std::int32_t endingOffset = offset + length; offset < endingOffset; offset += bufferIt->capacity(), ++bufferIt)
            {
                m_termBuffer.putBytes(offset, *bufferIt, 0, bufferIt->capacity());
            }

            const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, frameOffset, frameLength);
            m_termBuffer.putInt64(frameOffset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

            FrameDescriptor::frameLengthOrdered(m_termBuffer, frameOffset, frameLength);
        }

        return static_cast<std::int32_t>(resultingOffset);
    }

    std::int32_t appendFragmentedMessage(
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length,
        util::index_t maxPayloadLength,
        const on_reserved_value_supplier_t& reservedValueSupplier,
        std::int32_t activeTermId)
    {
        const int numMaxPayloads = length / maxPayloadLength;
        const util::index_t remainingPayload = length % maxPayloadLength;
        const util::index_t lastFrameLength = (remainingPayload > 0) ?
            util::BitUtil::align(remainingPayload + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT) : 0;
        const util::index_t requiredLength =
            (numMaxPayloads * (maxPayloadLength + DataFrameHeader::LENGTH)) + lastFrameLength;
        const std::int64_t rawTail = getAndAddRawTail(requiredLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
        const std::int32_t termId = LogBufferDescriptor::termId(rawTail);

        const std::int32_t termLength = m_termBuffer.capacity();

        checkTerm(activeTermId, termId);

        std::int64_t resultingOffset = termOffset + requiredLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(m_termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            std::uint8_t flags = FrameDescriptor::BEGIN_FRAG;
            util::index_t remaining = length;
            std::int32_t frameOffset = static_cast<std::int32_t>(termOffset);

            do
            {
                const util::index_t bytesToWrite = std::min(remaining, maxPayloadLength);
                const util::index_t frameLength = bytesToWrite + DataFrameHeader::LENGTH;
                const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

                header.write(m_termBuffer, frameOffset, frameLength, termId);
                m_termBuffer.putBytes(
                    frameOffset + DataFrameHeader::LENGTH,
                    srcBuffer,
                    srcOffset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= FrameDescriptor::END_FRAG;
                }

                FrameDescriptor::frameFlags(m_termBuffer, frameOffset, flags);

                const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, frameOffset, frameLength);
                m_termBuffer.putInt64(frameOffset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

                FrameDescriptor::frameLengthOrdered(m_termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return static_cast<std::int32_t>(resultingOffset);
    }

    template <class BufferIterator> std::int32_t appendFragmentedMessage(
        const HeaderWriter& header,
        BufferIterator bufferIt,
        util::index_t length,
        util::index_t maxPayloadLength,
        const on_reserved_value_supplier_t& reservedValueSupplier,
        std::int32_t activeTermId)
    {
        const int numMaxPayloads = length / maxPayloadLength;
        const util::index_t remainingPayload = length % maxPayloadLength;
        const util::index_t lastFrameLength = (remainingPayload > 0) ?
            util::BitUtil::align(remainingPayload + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT) : 0;
        const util::index_t requiredLength =
            (numMaxPayloads * (maxPayloadLength + DataFrameHeader::LENGTH)) + lastFrameLength;
        const std::int64_t rawTail = getAndAddRawTail(requiredLength);
        const std::int64_t termOffset = rawTail & 0xFFFFFFFF;
        const std::int32_t termId = LogBufferDescriptor::termId(rawTail);

        const std::int32_t termLength = m_termBuffer.capacity();

        checkTerm(activeTermId, termId);

        std::int64_t resultingOffset = termOffset + requiredLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(m_termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            std::uint8_t flags = FrameDescriptor::BEGIN_FRAG;
            util::index_t remaining = length;
            std::int32_t frameOffset = static_cast<std::int32_t>(termOffset);
            util::index_t currentBufferOffset = 0;

            do
            {
                const util::index_t bytesToWrite = std::min(remaining, maxPayloadLength);
                const util::index_t frameLength = bytesToWrite + DataFrameHeader::LENGTH;
                const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

                header.write(m_termBuffer, frameOffset, frameLength, termId);

                util::index_t bytesWritten = 0;
                util::index_t payloadOffset = frameOffset + DataFrameHeader::LENGTH;
                do
                {
                    const util::index_t currentBufferRemaining = bufferIt->capacity() - currentBufferOffset;
                    const util::index_t numBytes = std::min(bytesToWrite - bytesWritten, currentBufferRemaining);

                    m_termBuffer.putBytes(payloadOffset, *bufferIt, currentBufferOffset, numBytes);

                    bytesWritten += numBytes;
                    payloadOffset += numBytes;
                    currentBufferOffset += numBytes;

                    if (currentBufferRemaining <= numBytes)
                    {
                        ++bufferIt;
                        currentBufferOffset = 0;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= FrameDescriptor::END_FRAG;
                }

                FrameDescriptor::frameFlags(m_termBuffer, frameOffset, flags);

                const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, frameOffset, frameLength);
                m_termBuffer.putInt64(frameOffset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

                FrameDescriptor::frameLengthOrdered(m_termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return static_cast<std::int32_t>(resultingOffset);
    }

private:
    AtomicBuffer& m_termBuffer;
    AtomicBuffer& m_tailBuffer;
    const util::index_t m_tailOffset;

    inline static void checkTerm(std::int32_t expectedTermId, std::int32_t termId)
    {
        if (termId != expectedTermId)
        {
            throw util::IllegalStateException(
                util::strPrintf("Action possibly delayed: expectedTermId=%d termId=%d",
                    expectedTermId, termId), SOURCEINFO);
        }
    }

    inline static std::int32_t handleEndOfLogCondition(
        AtomicBuffer& termBuffer,
        std::int64_t termOffset,
        const HeaderWriter& header,
        std::int32_t termLength,
        std::int32_t termId)
    {
        if (termOffset < termLength)
        {
            const std::int32_t offset = static_cast<std::int32_t>(termOffset);
            const std::int32_t paddingLength = termLength - offset;
            header.write(termBuffer, offset, paddingLength, termId);
            FrameDescriptor::frameType(termBuffer, offset, DataFrameHeader::HDR_TYPE_PAD);
            FrameDescriptor::frameLengthOrdered(termBuffer, offset, paddingLength);
        }

        return TERM_APPENDER_FAILED;
    }

    inline std::int64_t getAndAddRawTail(const util::index_t alignedLength)
    {
        return m_tailBuffer.getAndAddInt64(m_tailOffset, alignedLength);
    }
};

}}}

#endif
