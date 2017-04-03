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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMREBUILDER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERMREBUILDER__

#include "FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer
{

namespace TermRebuilder {

inline void insert(AtomicBuffer& termBuffer, std::int32_t termOffset, AtomicBuffer& packet, std::int32_t length)
{
    const std::int32_t firstFrameLength = packet.getInt32(0);
    packet.putInt32Ordered(0, 0);

    termBuffer.putBytes(termOffset, packet, 0, length);
    FrameDescriptor::frameLengthOrdered(termBuffer, termOffset, firstFrameLength);
}

}

}}}
#endif //AERON_TERMREBUILDER_H
