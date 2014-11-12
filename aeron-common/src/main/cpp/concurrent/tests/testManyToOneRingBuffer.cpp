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

#include <array>

#include <gtest/gtest.h>
#include <mintomic/mintomic.h>

#include <concurrent/ringbuffer/RingBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>

using namespace aeron::common::concurrent::ringbuffer;
using namespace aeron::common::concurrent;

#define BUFFER_SZ (1024 + RingBufferDescriptor::TRAILER_LENGTH)
#define ODD_BUFFER_SZ (1023 + RingBufferDescriptor::TRAILER_LENGTH)

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
typedef std::array<std::uint8_t, ODD_BUFFER_SZ> odd_sized_buffer_t;

class ManyToOneRingBufferTest : public testing::Test
{
public:
    static const std::int32_t MSG_TYPE_ID = 101;

    ManyToOneRingBufferTest()
        : m_ab(&m_buffer[0], m_buffer.size()), m_ringBuffer(m_ab), m_srcAb(&m_srcBuffer[0], m_srcBuffer.size())
    {
        clear();
    }

protected:
    MINT_DECL_ALIGNED(buffer_t m_buffer, 16);
    MINT_DECL_ALIGNED(buffer_t m_srcBuffer, 16);
    AtomicBuffer m_ab;
    AtomicBuffer m_srcAb;
    ManyToOneRingBuffer m_ringBuffer;

    inline void clear()
    {
        m_buffer.fill(0);
        m_srcBuffer.fill(0);
    }
};

TEST_F(ManyToOneRingBufferTest, shouldCalculateCapacityForBuffer)
{
    ASSERT_EQ(m_ab.getCapacity(), BUFFER_SZ);
    ASSERT_EQ(m_ringBuffer.capacity(), BUFFER_SZ - RingBufferDescriptor::TRAILER_LENGTH);
}

TEST_F(ManyToOneRingBufferTest, shouldThrowForCapacityNotPowerOfTwo)
{
    MINT_DECL_ALIGNED(odd_sized_buffer_t testBuffer, 16);

    testBuffer.fill(0);
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());

    ASSERT_THROW(
    {
        ManyToOneRingBuffer ringBuffer (ab);
    }, aeron::common::util::IllegalArgumentException);
}

TEST_F(ManyToOneRingBufferTest, shouldThrowWhenMaxMessageSizeExceeded)
{
    ASSERT_THROW(
    {
        m_ringBuffer.write(MSG_TYPE_ID, m_srcAb, 0, m_ringBuffer.maxMsgLength() + 1);
    }, aeron::common::util::IllegalArgumentException);
}