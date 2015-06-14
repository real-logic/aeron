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

namespace aeron { namespace concurrent { namespace logbuffer {

/** The data handler function signature */
typedef std::function<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, Header&)> fragment_handler_t;

class TermReader
{
public:
    TermReader()
    {
    }

    inline util::index_t offset() const
    {
        return m_offset;
    }

    inline int read(
        AtomicBuffer& termBuffer,
        std::int32_t termOffset,
        const fragment_handler_t & handler,
        int fragmentsLimit,
        Header& header)
    {
        int fragmentsRead = 0;
        const util::index_t capacity = termBuffer.capacity();
        m_offset = termOffset;

        do
        {
            const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, termOffset);
            if (frameLength <= 0)
            {
                break;
            }

            const std::int32_t currentTermOffset = termOffset;
            termOffset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
            m_offset = termOffset;

            if (!FrameDescriptor::isPaddingFrame(termBuffer, currentTermOffset))
            {
                header.buffer(termBuffer);
                header.offset(currentTermOffset);
                handler(termBuffer, currentTermOffset + DataFrameHeader::LENGTH, frameLength - DataFrameHeader::LENGTH, header);

                ++fragmentsRead;
            }
        }
        while (fragmentsRead < fragmentsLimit && termOffset < capacity);

        return fragmentsRead;
    }

private:
    util::index_t m_offset = 0;
};

}}}

#endif