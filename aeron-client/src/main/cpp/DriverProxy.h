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

#ifndef INCLUDED_AERON_DRIVER_PROXY__
#define INCLUDED_AERON_DRIVER_PROXY__

#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>

namespace aeron {

using namespace aeron::common::concurrent::ringbuffer;

class DriverProxy
{
public:
    DriverProxy(ManyToOneRingBuffer&toDriverCommandBuffer) :
        m_toDriverCommandBuffer(toDriverCommandBuffer)
    {

    }

    std::int64_t addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId)
    {
        return 0;
    }

    std::int64_t removePublication(std::int64_t correlationId)
    {
        return 0;
    }

    std::int64_t addSubscription(const std::string& channel, std::int32_t streamId)
    {
        return 0;
    }

    std::int64_t removeSubscription(std::int64_t correlationId)
    {
        return 0;
    }

private:
    ManyToOneRingBuffer& m_toDriverCommandBuffer;
};

}

#endif