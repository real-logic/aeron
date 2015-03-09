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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_BUFFER_CLAIM__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_BUFFER_CLAIM__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

class BufferClaim
{
public:
    typedef BufferClaim this_t;

    inline BufferClaim() :
        m_buffer(nullptr), m_offset(0), m_length(0), m_frameLengthOffset(0), m_frameLength(0)
    {
    }

    inline AtomicBuffer* buffer() const
    {
        return m_buffer;
    }

    inline this_t& buffer(AtomicBuffer* buffer)
    {
        m_buffer = buffer;
        return *this;
    }

    inline util::index_t offset() const
    {
        return m_offset;
    }

    inline this_t& offset(util::index_t offset)
    {
        m_offset = offset;
        return *this;
    }

    inline util::index_t length() const
    {
        return m_length;
    }

    inline this_t& length(util::index_t length)
    {
        m_length = length;
        return *this;
    }

    inline this_t& frameLengthOffset(util::index_t frameLengthOffset)
    {
        m_frameLengthOffset = frameLengthOffset;
        return *this;
    }

    inline this_t& frameLength(util::index_t frameLength)
    {
        m_frameLength = frameLength;
        return *this;
    }

    inline void commit()
    {
        if (nullptr == m_buffer)
        {
            throw util::IllegalStateException("buffer has not been set", SOURCEINFO);
        }

        m_buffer->putInt32Ordered(m_frameLengthOffset, m_frameLength);
    }

private:
    AtomicBuffer *m_buffer;
    util::index_t m_offset;
    util::index_t m_length;
    util::index_t m_frameLengthOffset;
    util::index_t m_frameLength;
};

}}}}

#endif
