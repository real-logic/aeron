/*
 * Copyright 2014 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_CONCURRENT_RINGBUFFER_RING_BUFFER__
#define INCLUDED_AERON_CONCURRENT_RINGBUFFER_RING_BUFFER__

#include <util/Index.h>

namespace aeron { namespace common { namespace concurrent { namespace ringbuffer {

class RingBuffer
{
public:

    inline util::index_t getCapacity() const
    {
        return m_length;
    }

protected:
    util::index_t m_length;
};

}}}}

#endif
