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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_READER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_READER__

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"
#include "LogBufferPartition.h"
#include "Header.h"

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

/** The data handler function signature */
typedef std::function<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, Header&)> data_handler_t;

class TermReader
{
public:
    TermReader(std::int32_t initialTermId, AtomicBuffer& termBuffer) :
        m_termBuffer(termBuffer), m_header(initialTermId, termBuffer), m_offset(0)
    {
        LogBufferDescriptor::checkTermBuffer(termBuffer);
    }

    inline AtomicBuffer& termBuffer() const
    {
        return m_termBuffer;
    }

    inline util::index_t offset() const
    {
        return m_offset;
    }

    inline int read(std::int32_t termOffset, const data_handler_t & handler, int framesCountLimit)
    {
        int framesCounter = 0;
        const util::index_t capacity = m_termBuffer.getCapacity();
        m_offset = termOffset;

        do
        {
            const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(m_termBuffer, termOffset);
            if (frameLength <= 0)
            {
                break;
            }

            const std::int32_t currentTermOffset = termOffset;
            termOffset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
            m_offset = termOffset;

            if (!FrameDescriptor::isPaddingFrame(m_termBuffer, currentTermOffset))
            {
                m_header.offset(currentTermOffset);
                handler(m_termBuffer, currentTermOffset + DataHeader::LENGTH, frameLength - DataHeader::LENGTH, m_header);

                ++framesCounter;
            }
        }
        while (framesCounter < framesCountLimit && termOffset < capacity);

        return framesCounter;
    }

private:
    AtomicBuffer& m_termBuffer;
    Header m_header;
    util::index_t m_offset;
};

}}}}

#endif