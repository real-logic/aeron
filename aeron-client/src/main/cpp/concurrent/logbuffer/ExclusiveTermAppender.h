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

#ifndef AERON_EXCLUSIVETERMAPPENDER_H
#define AERON_EXCLUSIVETERMAPPENDER_H

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "HeaderWriter.h"
#include "LogBufferDescriptor.h"
#include "ExclusiveBufferClaim.h"
#include "DataFrameHeader.h"
#include "TermAppender.h"

namespace aeron { namespace concurrent { namespace logbuffer {

class ExclusiveTermAppender
{
public:
    struct TermInfo
    {
        std::int64_t termOffset;
        std::int32_t termId;
    };

    ExclusiveTermAppender(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer, const int partitionIndex) :
        m_termBuffer(termBuffer),
        m_tailAddr(
            reinterpret_cast<std::int64_t *>(
                metaDataBuffer.buffer() +
                LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET +
                (partitionIndex * sizeof(std::int64_t))))
    {
        metaDataBuffer.boundsCheck(
            LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)),
            sizeof(std::int64_t));
    }

    inline AtomicBuffer& termBuffer()
    {
        return m_termBuffer;
    }

    inline std::int64_t rawTail() const
    {
        return *m_tailAddr;
    }

    inline void tailTermId(const std::int32_t termId)
    {
        *m_tailAddr = static_cast<std::int64_t>(termId) << 32;
    }

    inline void claim(
        TermInfo& termInfo, const HeaderWriter& header, util::index_t length, ExclusiveBufferClaim& bufferClaim)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

        const std::int32_t termLength = m_termBuffer.capacity();
        const std::int32_t termOffset = static_cast<std::int32_t>(termInfo.termOffset);

        const std::int32_t resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termInfo.termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            handleEndOfLogCondition(termInfo, m_termBuffer, termOffset, header, termLength);
        }
        else
        {
            header.write(m_termBuffer, termOffset, frameLength, termInfo.termId);
            bufferClaim.wrap(m_termBuffer, termOffset, frameLength);
        }
    }

    inline void appendUnfragmentedMessage(
        TermInfo& termInfo,
        const HeaderWriter& header,
        AtomicBuffer& srcBuffer,
        util::index_t srcOffset,
        util::index_t length,
        const on_reserved_value_supplier_t& reservedValueSupplier)
    {
        const util::index_t frameLength = length + DataFrameHeader::LENGTH;
        const util::index_t alignedLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

        const std::int32_t termLength = m_termBuffer.capacity();
        const std::int32_t termOffset = static_cast<std::int32_t>(termInfo.termOffset);

        const std::int32_t resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termInfo.termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            handleEndOfLogCondition(termInfo, m_termBuffer, termOffset, header, termLength);
        }
        else
        {
            header.write(m_termBuffer, termOffset, frameLength, termInfo.termId);
            m_termBuffer.putBytes(termOffset + DataFrameHeader::LENGTH, srcBuffer, srcOffset, length);

            const std::int64_t reservedValue = reservedValueSupplier(m_termBuffer, termOffset, frameLength);
            m_termBuffer.putInt64(termOffset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

            FrameDescriptor::frameLengthOrdered(m_termBuffer, termOffset, frameLength);
        }
    }

    void appendFragmentedMessage(
        TermInfo& termInfo,
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

        const std::int32_t termLength = m_termBuffer.capacity();
        const std::int32_t termOffset = static_cast<std::int32_t>(termInfo.termOffset);

        const std::int32_t resultingOffset = termOffset + requiredLength;
        putRawTailOrdered(termInfo.termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            handleEndOfLogCondition(termInfo, m_termBuffer, termOffset, header, termLength);
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

                header.write(m_termBuffer, offset, frameLength, termInfo.termId);
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
    std::int64_t *const m_tailAddr;

    inline static void handleEndOfLogCondition(
        TermInfo& termInfo,
        AtomicBuffer& termBuffer,
        util::index_t termOffset,
        const HeaderWriter& header,
        util::index_t termLength)
    {
        termInfo.termOffset = TERM_APPENDER_TRIPPED;

        if (termOffset < termLength)
        {
            const std::int32_t paddingLength = termLength - termOffset;
            header.write(termBuffer, termOffset, paddingLength, termInfo.termId);
            FrameDescriptor::frameType(termBuffer, termOffset, DataFrameHeader::HDR_TYPE_PAD);
            FrameDescriptor::frameLengthOrdered(termBuffer, termOffset, paddingLength);
        }
    }

    inline void putRawTailOrdered(const std::int64_t termId, const std::int32_t termOffset)
    {
        aeron::concurrent::atomic::putInt64Ordered(m_tailAddr, ((termId << 32) + termOffset));
    }
};

}}}

#endif
