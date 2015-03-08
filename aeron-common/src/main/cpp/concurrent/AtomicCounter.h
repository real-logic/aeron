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
#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__

#include <cstdint>
#include <memory>

#include <util/Index.h>
#include "AtomicBuffer.h"


namespace aeron { namespace common { namespace concurrent {

class CountersManager;

class AtomicCounter
{
public:
    AtomicCounter(const AtomicBuffer buffer, std::int32_t counterId, CountersManager& countersManager);
    ~AtomicCounter();

    void increment()
    {
        m_buffer.getAndAddInt64(m_offset, 1);
    }

    void orderedIncrement()
    {
        m_buffer.addInt64Ordered(m_offset, 1);
    }

    void set(std::int64_t value)
    {
        m_buffer.putInt64Atomic(m_offset, value);
    }

    void setOrdered(long value)
    {
        m_buffer.putInt64Ordered(m_offset, value);
    }

    void addOrdered(long increment)
    {
        m_buffer.addInt64Ordered(m_offset, increment);
    }

    std::int64_t get()
    {
        return m_buffer.getInt64Volatile(m_offset);
    }

    typedef std::shared_ptr<AtomicCounter> ptr_t;

private:
    AtomicBuffer m_buffer;
    std::int32_t m_counterId;
    CountersManager& m_countersManager;
    util::index_t m_offset;
};

}}}

#endif


