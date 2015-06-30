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

#ifndef AERON_TERMBLOCKSCANNER_H
#define AERON_TERMBLOCKSCANNER_H

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"
#include "LogBufferPartition.h"
#include "Header.h"

namespace aeron { namespace concurrent { namespace logbuffer {

/** The block handler function signature */
typedef std::function<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, std::int32_t sessionId)> block_handler_t;

namespace TermBlockScanner {

inline std::int32_t scan(AtomicBuffer& termBuffer, std::int32_t offset, std::int32_t limit)
{
    do
    {
        const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, offset);
        if (frameLength <= 0)
        {
            break;
        }

        const std::int32_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        offset += alignedFrameLength;

        if (offset >= limit)
        {
            if (offset > limit)
            {
                offset -= alignedFrameLength;
            }

            break;
        }
    }
    while (true);

    return offset;
}

}

}}}

#endif //AERON_TERMBLOCKSCANNER_H
