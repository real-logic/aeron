/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include "util/Index.h"
#include "util/BitUtil.h"
#include "util/Exceptions.h"
#include "concurrent/AtomicBuffer.h"
#include "DataFrameHeader.h"

#include "aeronc.h"

namespace aeron { namespace concurrent { namespace logbuffer {

using namespace aeron::util;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
class Header
{
public:
    Header(aeron_header_t *header, void *context) : m_header(header), m_context(context)
    {
        if (aeron_header_values(m_header, &m_headerValues) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    inline std::int32_t initialTermId() const
    {
        return m_headerValues.initial_term_id;
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    inline std::int32_t frameLength() const
    {
        return m_headerValues.frame.frame_length;
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    inline std::int32_t sessionId() const
    {
        return m_headerValues.frame.session_id;
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    inline std::int32_t streamId() const
    {
        return m_headerValues.frame.stream_id;
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    inline std::int32_t termId() const
    {
        return m_headerValues.frame.term_id;
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    inline std::int32_t termOffset() const
    {
        return m_headerValues.frame.term_offset;
    }

    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    inline std::uint16_t type() const
    {
        // C and Java API declare this as int16_t.
        return static_cast<uint16_t>(m_headerValues.frame.type);
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
        return m_headerValues.frame.flags;
    }

    /**
     * Get the current position to which the Image has advanced on reading this message.
     *
     * @return the current position to which the Image has advanced on reading this message.
     */
    inline std::int64_t position() const
    {
        return aeron_header_position(m_header);
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return m_headerValues.frame.reserved_value;
    }

    /**
     * Get a pointer to the context associated with this message. Only valid during poll handling. Is normally a
     * pointer to an Image instance.
     *
     * @return a pointer to the context associated with this message.
     */
    inline void* context() const
    {
        return m_context;
    }

private:
    aeron_header_t *m_header;
    aeron_header_values_t m_headerValues;
    void *m_context;
};

}}}

#endif
