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

#ifndef AERON_CONCURRENT_LOGBUFFER_HEADER_WRITER_H
#define AERON_CONCURRENT_LOGBUFFER_HEADER_WRITER_H

#include "util/Index.h"
#include "concurrent/AtomicBuffer.h"
#include "DataFrameHeader.h"
#include "FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer
{

class HeaderWriter
{
public:
    explicit HeaderWriter(AtomicBuffer defaultHdr) :
        m_sessionId(defaultHdr.getInt32(DataFrameHeader::SESSION_ID_FIELD_OFFSET)),
        m_streamId(defaultHdr.getInt32(DataFrameHeader::STREAM_ID_FIELD_OFFSET))
    {
    }

    /**
     * Write header in LITTLE_ENDIAN order
     */
    inline void write(AtomicBuffer &termBuffer, util::index_t offset, util::index_t length, std::int32_t termId) const
    {
        termBuffer.putInt32Ordered(offset, -length);
        atomic::release();

        auto *hdr = (struct DataFrameHeader::DataFrameHeaderDefn *)(termBuffer.buffer() + offset);

        hdr->version = DataFrameHeader::CURRENT_VERSION;
        hdr->flags = FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG;
        hdr->type = DataFrameHeader::HDR_TYPE_DATA;
        hdr->termOffset = offset;
        hdr->sessionId = m_sessionId;
        hdr->streamId = m_streamId;
        hdr->termId = termId;
    }

private:
    const std::int32_t m_sessionId;
    const std::int32_t m_streamId;
};

}}}

#endif
