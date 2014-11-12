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

typedef std::array<std::uint8_t, BUFFER_SZ> buffer_t;
static MINT_DECL_ALIGNED(buffer_t testBuffer, 16);

static void clearBuffer()
{
    testBuffer.fill(0);
}

TEST(manyToOneRingBufferTests, shouldCalculateCapacityForBuffer)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());

    ASSERT_EQ(ab.getCapacity(), BUFFER_SZ);
    ManyToOneRingBuffer ringBuffer (ab);

    ASSERT_EQ(ringBuffer.capacity(), 1024);
}