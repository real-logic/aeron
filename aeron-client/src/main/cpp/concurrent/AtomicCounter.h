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
#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__

#include <cstdint>
#include <memory>

#include <util/Index.h>
#include "AtomicBuffer.h"
#include "CountersManager.h"

namespace aeron { namespace concurrent {

class AtomicCounter
{
public:

    typedef std::shared_ptr<AtomicCounter> ptr_t;

    AtomicCounter(const AtomicBuffer buffer, std::int32_t counterId, CountersManager& countersManager) :
        m_buffer(buffer),
        m_counterId(counterId),
        m_countersManager(countersManager),
        m_offset(CountersManager::counterOffset(counterId))
    {
        m_buffer.putInt64(m_offset, 0);
    }

    ~AtomicCounter()
    {
        m_countersManager.free(m_counterId);
    }

    inline static AtomicCounter::ptr_t makeCounter(CountersManager& countersManager, std::string& label)
    {
        return std::make_shared<AtomicCounter>(countersManager.valuesBuffer(), countersManager.allocate(label), countersManager);
    }

    inline void increment()
    {
        m_buffer.getAndAddInt64(m_offset, 1);
    }

    inline void orderedIncrement()
    {
        m_buffer.addInt64Ordered(m_offset, 1);
    }

    inline void set(std::int64_t value)
    {
        m_buffer.putInt64Atomic(m_offset, value);
    }

    inline void setOrdered(long value)
    {
        m_buffer.putInt64Ordered(m_offset, value);
    }

    inline void addOrdered(long increment)
    {
        m_buffer.addInt64Ordered(m_offset, increment);
    }

    inline std::int64_t get()
    {
        return m_buffer.getInt64Volatile(m_offset);
    }

private:
    AtomicBuffer m_buffer;
    std::int32_t m_counterId;
    CountersManager& m_countersManager;
    util::index_t m_offset;
};

}}

#endif
