/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

#include <gtest/gtest.h>
#include <concurrent/AtomicCounter.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/Atomic64.h>
#include <status/SystemCounters.h>

using namespace testing;
using namespace aeron::concurrent;
using namespace aeron::driver::status;

static const std::int32_t VALUE_BUFFER_LENGTH = 16 * 1024;
static const std::int32_t META_BUFFER_LENGTH = 2 * VALUE_BUFFER_LENGTH;

typedef std::array<std::uint8_t, VALUE_BUFFER_LENGTH> value_buffer_t;
typedef std::array<std::uint8_t, META_BUFFER_LENGTH> meta_buffer_t;

class SystemCountersTest : public Test
{
public:
    SystemCountersTest() :
        m_meta_buffer(&m_meta[0], m_meta.size()),
        m_value_buffer(&m_value[0], m_value.size()),
        m_countersManager(m_meta_buffer, m_value_buffer),
        m_systemCounters(m_countersManager)
    {}

    AERON_DECL_ALIGNED(meta_buffer_t m_meta, 16);
    AERON_DECL_ALIGNED(value_buffer_t m_value, 16);
    AtomicBuffer m_meta_buffer;
    AtomicBuffer m_value_buffer;
    CountersManager m_countersManager;
    SystemCounters m_systemCounters;
};

static void getAndSetCounter(SystemCounters& counters, SystemCounterDescriptor descriptor)
{
    std::int64_t value = 12345;

    AtomicCounter* c1 = counters.get(descriptor);
    c1->setOrdered(value);

    AtomicCounter* c2 = counters.get(descriptor);

    std::int64_t v2 = c2->get();
    ASSERT_EQ(value, v2);
}

TEST_F(SystemCountersTest, readAndWriteSystemCounter)
{
    for (auto descriptor : SystemCounterDescriptor::VALUES)
    {
        getAndSetCounter(m_systemCounters, descriptor);
    }
}
