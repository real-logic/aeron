/*
 * Copyright 2016 Real Logic Ltd.
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

#include <array>

#include <gtest/gtest.h>

#include <thread>
#include "MockAtomicBuffer.h"
#include <concurrent/errors/DistinctErrorLog.h>

using namespace aeron::concurrent::errors;
using namespace aeron::concurrent::mock;
using namespace aeron::concurrent;
using namespace aeron;

#define CAPACITY (1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;

class DistinctErrorLogTest : public testing::Test
{
public:
    DistinctErrorLogTest() :
        m_mockBuffer(&m_buffer[0], m_buffer.size()),
        m_errorLog(m_mockBuffer, []() { return 0; })
    {
        m_buffer.fill(0);
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_buffer, 16);
    MockAtomicBuffer m_mockBuffer;
    DistinctErrorLog m_errorLog;
};

TEST_F(DistinctErrorLogTest, should)
{

}

