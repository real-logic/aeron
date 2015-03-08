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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_HEADER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_HEADER__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

namespace DataHeader {

/*
 * Aeron Data Frame Header
 *
 * https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame
 */

static const util::index_t VERSION_FIELD_OFFSET = 0;
static const util::index_t FLAGS_FIELD_OFFSET = 1;
static const util::index_t TYPE_FIELD_OFFSET = 2;
static const util::index_t FRAME_LENGTH_FIELD_OFFSET = 4;
static const util::index_t TERM_OFFSET_FIELD_OFFSET = 8;
static const util::index_t SESSION_ID_FIELD_OFFSET = 12;
static const util::index_t STREAM_ID_FIELD_OFFSET = 16;
static const util::index_t TERM_ID_FIELD_OFFSET = 20;
static const util::index_t DATA_OFFSET = 24;

static const util::index_t LENGTH = DATA_OFFSET;

}

class Header
{
public:
    Header(AtomicBuffer& termBuffer) :
        m_buffer(termBuffer), m_offset(0)
    {
    }

    inline util::index_t offset()
    {
        return m_offset;
    }

    inline void offset(util::index_t offset)
    {
        m_offset = offset;
    }

    inline AtomicBuffer& buffer()
    {
        return m_buffer;
    }

    inline void buffer(AtomicBuffer& buffer)
    {
        m_buffer = buffer;
    }

    inline std::int32_t frameLength()
    {
        // TODO: add LITTLE_ENDIAN check
        return m_buffer.getInt32(m_offset + DataHeader::FRAME_LENGTH_FIELD_OFFSET);
    }

    inline std::int32_t sessionId()
    {
        // TODO: add LITTLE_ENDIAN check
        return m_buffer.getInt32(m_offset + DataHeader::SESSION_ID_FIELD_OFFSET);
    }

    inline std::int32_t streamId()
    {
        // TODO: add LITTLE_ENDIAN check
        return m_buffer.getInt32(m_offset + DataHeader::STREAM_ID_FIELD_OFFSET);
    }

    inline std::int32_t termId()
    {
        // TODO: add LITTLE_ENDIAN check
        return m_buffer.getInt32(m_offset + DataHeader::TERM_ID_FIELD_OFFSET);
    }

    inline std::int32_t termOffset()
    {
        return m_offset;
    }

    inline std::uint16_t type()
    {
        // TODO: add LITTLE_ENDIAN check
        return m_buffer.getUInt16(m_offset + DataHeader::TYPE_FIELD_OFFSET);
    }

    inline std::uint8_t flags()
    {
        return m_buffer.getUInt8(m_offset + DataHeader::FLAGS_FIELD_OFFSET);
    }

private:
    AtomicBuffer&m_buffer;
    util::index_t m_offset;
};

}}}}

#endif