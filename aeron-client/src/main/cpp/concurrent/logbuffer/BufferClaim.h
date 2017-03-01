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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_BUFFER_CLAIM__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_BUFFER_CLAIM__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/DataFrameHeader.h>

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 */
class BufferClaim
{
public:
    typedef BufferClaim this_t;

    inline BufferClaim()
    {
    }

    /// @cond HIDDEN_SYMBOLS
    inline void wrap(std::uint8_t *buffer, util::index_t length)
    {
        m_buffer.wrap(buffer, length);
    }
    /// @endcond

    /// @cond HIDDEN_SYMBOLS
    inline void wrap(AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        m_buffer.wrap(buffer.buffer() + offset, length);
    }
    /// @endcond


    /**
     * The referenced buffer to be used.
     *
     * @return the referenced buffer to be used..
     */
    inline AtomicBuffer& buffer()
    {
        return m_buffer;
    }

    /**
     * The offset in the buffer at which the claimed range begins.
     *
     * @return offset in the buffer at which the range begins.
     */
    inline util::index_t offset() const
    {
        return DataFrameHeader::LENGTH;
    }

    /**
     * The length of the claimed range in the buffer.
     *
     * @return length of the range in the buffer.
     */
    inline util::index_t length() const
    {
        return m_buffer.capacity() - DataFrameHeader::LENGTH;
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return m_buffer.getInt64(DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET);
    }

    /**
     * Write the provided value into the reserved space at the end of the data frame header.
     *
     * @param value to be stored in the reserve space at the end of a data frame header.
     * @return this for fluent API semantics.
     */
    inline this_t& reservedValue(std::int64_t value)
    {
        m_buffer.putInt64(DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, value);
        return *this;
    }

    /**
     * Commit the message to the log buffer so that is it available to subscribers.
     */
    inline void commit()
    {
        m_buffer.putInt32Ordered(0, m_buffer.capacity());
    }

    /**
     * Abort a claim of the message space to the log buffer so that log can progress ignoring this claim.
     */
    inline void abort()
    {
        m_buffer.putUInt16(DataFrameHeader::TYPE_FIELD_OFFSET, DataFrameHeader::HDR_TYPE_PAD);
        m_buffer.putInt32Ordered(0, m_buffer.capacity());
    }

private:
    AtomicBuffer m_buffer;
};

}}}

#endif
