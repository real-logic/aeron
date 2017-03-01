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

#ifndef INCLUDED_AERON_CONCURRENT_RINGBUFFER_RING_BUFFER_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_RINGBUFFER_RING_BUFFER_DESCRIPTOR__

#include <functional>
#include <util/Index.h>
#include <util/BitUtil.h>
#include <util/Exceptions.h>
#include <util/StringUtil.h>

namespace aeron { namespace concurrent { namespace ringbuffer {

/** The read handler function signature */
typedef std::function<void(std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)> handler_t;

using namespace aeron::util::BitUtil;

namespace RingBufferDescriptor {

    static const util::index_t TAIL_POSITION_OFFSET = CACHE_LINE_LENGTH * 2;
    static const util::index_t HEAD_CACHE_POSITION_OFFSET = CACHE_LINE_LENGTH * 4;
    static const util::index_t HEAD_POSITION_OFFSET = CACHE_LINE_LENGTH * 6;
    static const util::index_t CORRELATION_COUNTER_OFFSET = CACHE_LINE_LENGTH * 8;
    static const util::index_t CONSUMER_HEARTBEAT_OFFSET = CACHE_LINE_LENGTH * 10;

    /** Total length of the trailer in bytes. */
    static const util::index_t TRAILER_LENGTH = CACHE_LINE_LENGTH * 12;

    inline static void checkCapacity(util::index_t capacity)
    {
        if (!util::BitUtil::isPowerOfTwo(capacity))
        {
            throw util::IllegalArgumentException(
                util::strPrintf("Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=%d", capacity), SOURCEINFO);
        }
    }
}

}}}

#endif
