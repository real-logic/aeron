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

#include <thread>
#include "MockAtomicBuffer.h"
#include <concurrent/broadcast/BroadcastBufferDescriptor.h>
#include <concurrent/broadcast/BroadcastReceiver.h>

using namespace aeron::common::concurrent::broadcast;
using namespace aeron::common::concurrent::mock;
using namespace aeron::common::concurrent;
using namespace aeron::common;

#define CAPACITY (1024)
#define TOTAL_BUFFER_SIZE (CAPACITY + BroadcastBufferDescriptor::TRAILER_LENGTH)
#define MSG_TYPE_ID (7)
#define TAIL_COUNTER_INDEX (CAPACITY + BroadcastBufferDescriptor::TAIL_COUNTER_OFFSET)
#define LATEST_COUNTER_INDEX (CAPACITY + BroadcastBufferDescriptor::LATEST_COUNTER_OFFSET)

typedef std::array<std::uint8_t, TOTAL_BUFFER_SIZE> buffer_t;

class BroadcastReceiverTest : public testing::Test
{
public:
    BroadcastReceiverTest() :
        m_mockBuffer(&m_buffer[0], m_buffer.size()),
        m_broadcastReceiver(m_mockBuffer)
    {
        m_buffer.fill(0);
    }

    virtual void SetUp()
    {
        m_buffer.fill(0);
    }

protected:
    MINT_DECL_ALIGNED(buffer_t m_buffer, 16);
    MockAtomicBuffer m_mockBuffer;
    BroadcastReceiver m_broadcastReceiver;
};

TEST_F(BroadcastReceiverTest, shouldCalculateCapacityForBuffer)
{
    EXPECT_EQ(m_broadcastReceiver.capacity(), CAPACITY);
}

TEST_F(BroadcastReceiverTest, shouldThrowExceptionForCapacityThatIsNotPowerOfTwo)
{
    typedef std::array<std::uint8_t, (777 + BroadcastBufferDescriptor::TRAILER_LENGTH)> non_power_of_two_buffer_t;
    MINT_DECL_ALIGNED(non_power_of_two_buffer_t non_power_of_two_buffer, 16);
    AtomicBuffer buffer(&non_power_of_two_buffer[0], non_power_of_two_buffer.size());

    ASSERT_THROW(
    {
        BroadcastReceiver receiver(buffer);
    }, util::IllegalStateException);
}

TEST_F(BroadcastReceiverTest, shouldNotBeLappedBeforeReception)
{
    EXPECT_EQ(m_broadcastReceiver.lappedCount(), 0);
}

TEST_F(BroadcastReceiverTest, shouldNotReceiveFromEmptyBuffer)
{
    EXPECT_FALSE(m_broadcastReceiver.receiveNext());
}

TEST_F(BroadcastReceiverTest, shouldReceiveFirstMessageFromBuffer)
{
    const std::int32_t length = 8;
    const std::int32_t recordLength = util::BitUtil::align(length + RecordDescriptor::HEADER_LENGTH, RecordDescriptor::RECORD_ALIGNMENT);
    const std::int64_t tail = recordLength;
    const std::int64_t latestRecord = tail - recordLength;
    const std::int32_t recordOffset = (std::int32_t)latestRecord;

    
}
