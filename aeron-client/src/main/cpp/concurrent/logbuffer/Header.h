/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_CONCURRENT_LOGBUFFER_HEADER_H
#define AERON_CONCURRENT_LOGBUFFER_HEADER_H

#include "Context.h"
#include "util/BitUtil.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
class Header
{
public:
    Header(std::int32_t initialTermId, std::int32_t positionBitsToShift, void *context) :
        m_context(context),
        m_offset(0),
        m_initialTermId(initialTermId),
        m_positionBitsToShift(positionBitsToShift),
        m_fragmentedFrameLength(aeron::NULL_VALUE)
    {
    }

    /// @cond HIDDEN_SYMBOLS
    inline void copyFrom(const Header &header)
    {
        m_context = header.m_context;
        m_initialTermId = header.m_initialTermId;
        m_positionBitsToShift = header.m_positionBitsToShift;
        ::memcpy(
            m_buffer.buffer() + m_offset,
            header.m_buffer.buffer() + header.m_offset,
            DataFrameHeader::LENGTH);
    }

    inline void fragmentedFrameLength(std::int32_t fragmentedFrameLength)
    {
        m_fragmentedFrameLength = fragmentedFrameLength;
    }
    /// @endcond

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    inline std::int32_t initialTermId() const
    {
        return m_initialTermId;
    }

    inline void initialTermId(std::int32_t initialTermId)
    {
        m_initialTermId = initialTermId;
    }

    /**
     * The offset at which the frame begins.
     *
     * @return offset at which the frame begins.
     */
    inline util::index_t offset() const
    {
        return m_offset;
    }

    inline void offset(util::index_t offset)
    {
        m_offset = offset;
    }

    /**
     * The AtomicBuffer containing the header.
     *
     * @return AtomicBuffer containing the header.
     */
    inline AtomicBuffer &buffer()
    {
        return m_buffer;
    }

    inline void buffer(AtomicBuffer &buffer)
    {
        if (&buffer != &m_buffer)
        {
            m_buffer.wrap(buffer);
        }
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    inline std::int32_t frameLength() const
    {
        return m_buffer.getInt32(m_offset);
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    inline std::int32_t sessionId() const
    {
        return m_buffer.getInt32(m_offset + DataFrameHeader::SESSION_ID_FIELD_OFFSET);
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    inline std::int32_t streamId() const
    {
        return m_buffer.getInt32(m_offset + DataFrameHeader::STREAM_ID_FIELD_OFFSET);
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    inline std::int32_t termId() const
    {
        return m_buffer.getInt32(m_offset + DataFrameHeader::TERM_ID_FIELD_OFFSET);
    }

    /**
     * The offset in the term at which the frame begins.
     *
     * @return the offset in the term at which the frame begins.
     */
    inline std::int32_t termOffset() const
    {
        return m_buffer.getInt32(m_offset + DataFrameHeader::TERM_OFFSET_FIELD_OFFSET);
    }

    /**
     * Calculates the offset of the frame immediately after this one.
     *
     * @return the offset of the next frame.
     */
    inline std::int32_t nextTermOffset() const
    {
        return BitUtil::align(termOffset() + termOccupancyLength(), FrameDescriptor::FRAME_ALIGNMENT);
    }

    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    inline std::uint16_t type() const
    {
        return m_buffer.getUInt16(m_offset + DataFrameHeader::TYPE_FIELD_OFFSET);
    }

    /**
     * The flags for this frame. Valid flags are {@link DataFrameHeader::BEGIN_FLAG}
     * and {@link DataFrameHeader::END_FLAG}. A convenience flag {@link DataFrameHeader::BEGIN_AND_END_FLAGS}
     * can be used for both flags.
     *
     * @return the flags for this frame.
     */
    inline std::uint8_t flags() const
    {
        return m_buffer.getUInt8(m_offset + DataFrameHeader::FLAGS_FIELD_OFFSET);
    }

    /**
     * Get the current position to which the Image has advanced on reading this message.
     *
     * @return the current position to which the Image has advanced on reading this message.
     */
    inline std::int64_t position() const
    {
        return LogBufferDescriptor::computePosition(termId(), nextTermOffset(), m_positionBitsToShift, m_initialTermId);
    }

    /**
     * The number of times to left shift the term count to multiply by term length.
     *
     * @return number of times to left shift the term count to multiply by term length.
     */
    inline std::int32_t positionBitsToShift() const
    {
        return m_positionBitsToShift;
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return m_buffer.getInt64(m_offset + DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET);
    }

    /**
     * Get a pointer to the context associated with this message. Only valid during poll handling. Is normally a
     * pointer to an Image instance.
     *
     * @return a pointer to the context associated with this message.
     */
    inline void *context() const
    {
        return m_context;
    }

private:
    void *m_context;
    AtomicBuffer m_buffer;
    util::index_t m_offset;
    std::int32_t m_initialTermId;
    std::int32_t m_positionBitsToShift;
    std::int32_t m_fragmentedFrameLength;

    std::int32_t termOccupancyLength() const
    {
        return aeron::NULL_VALUE == m_fragmentedFrameLength ? frameLength() : m_fragmentedFrameLength;
    }
};

}}}

#endif
