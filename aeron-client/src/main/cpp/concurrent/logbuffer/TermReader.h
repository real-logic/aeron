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

namespace TermReader {

inline std::uint64_t readOutcome(std::int32_t offset, int fragmentsRead)
{
    return (((std::uint64_t)offset << 32) | (std::uint64_t)fragmentsRead);
}

inline std::uint64_t read(
    AtomicBuffer& termBuffer,
    std::int32_t termOffset,
    const fragment_handler_t & handler,
    int fragmentsLimit,
    Header& header)
{
    int fragmentsRead = 0;
    const util::index_t capacity = termBuffer.capacity();

    do
    {
        const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, termOffset);
        if (frameLength <= 0)
        {
            break;
        }

        const std::int32_t fragmentOffset = termOffset;
        termOffset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

        if (!FrameDescriptor::isPaddingFrame(termBuffer, fragmentOffset))
        {
            header.buffer(termBuffer);
            header.offset(fragmentOffset);
            handler(termBuffer, fragmentOffset + DataFrameHeader::LENGTH, frameLength - DataFrameHeader::LENGTH, header);

            ++fragmentsRead;
        }
    }
    while (fragmentsRead < fragmentsLimit && termOffset < capacity);

    return readOutcome(termOffset, fragmentsRead);
}

inline int fragmentsRead(std::uint64_t readOutcome)
{
    return (int)(readOutcome & 0xFFFFFFFF);
}

inline std::int32_t offset(std::uint64_t readOutcome)
{
    return (std::int32_t)((std::uint64_t)readOutcome >> 32);
}

}

}}}

#endif