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

#include "AtomicCounter.h"
#include "CountersManager.h"

namespace aeron { namespace common { namespace concurrent {

AtomicCounter::AtomicCounter(const AtomicBuffer buffer, std::int32_t counterId, CountersManager& countersManager)
        : m_buffer(buffer)
        , m_counterId(counterId)
        , m_countersManager(countersManager)
        , m_offset (countersManager.counterOffset(counterId))
{
    m_buffer.putInt64(m_offset, 0);
}

AtomicCounter::~AtomicCounter()
{
    m_countersManager.free(m_counterId);
}

}}}

