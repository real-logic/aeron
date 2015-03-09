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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_READER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_READER__

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"
#include "LogBufferPartition.h"
#include "Header.h"

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

/** The data handler function signature */
typedef std::function<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, Header&)> handler_t;

class LogReader : public LogBufferPartition
{
public:
    LogReader(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer) :
        LogBufferPartition(termBuffer, metaDataBuffer), m_header(termBuffer), m_offset(0)
    {
    }

    inline util::index_t offset()
    {
        return m_offset;
    }

    inline void seek(util::index_t offset)
    {
        if (m_offset < 0 || offset > capacity())
        {
            throw util::OutOfBoundsException(
                util::strPrintf("Invalid offset %d: 0 - %d", offset, capacity()), SOURCEINFO);
        }

        FrameDescriptor::checkOffsetAlignment(offset);

        m_offset = offset;
    }

    inline bool isComplete()
    {
        return (m_offset >= capacity());
    }

    inline int read(const handler_t& handler, int framesCountLimit)
    {
        int framesCounter = 0;
        util::index_t offset = m_offset;

        while (offset < capacity() && framesCounter < framesCountLimit)
        {
            std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer(), offset);
            if (0 == frameLength)
            {
                break;
            }

            if (!FrameDescriptor::isPaddingFrame(termBuffer(), offset))
            {
                m_header.offset(offset);
                handler(termBuffer(), offset + DataHeader::LENGTH, frameLength - DataHeader::LENGTH, m_header);

                ++framesCounter;
            }

            offset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
            m_offset = offset;
        }

        return framesCounter;
    }

private:
    Header m_header;
    util::index_t m_offset;
};

}}}}

#endif