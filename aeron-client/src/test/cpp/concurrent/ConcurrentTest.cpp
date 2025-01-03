/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdint>
#include <array>
#include <vector>
#include <thread>

#include <gtest/gtest.h>

#include "concurrent/AtomicBuffer.h"

using namespace aeron::concurrent;
using namespace aeron::util;

typedef std::array<std::uint8_t, 1024> buffer_t;
static AERON_DECL_ALIGNED(buffer_t testBuffer, 16);

static void clearBuffer()
{
    testBuffer.fill(0);
}

#if !defined(DISABLE_BOUNDS_CHECKS)
TEST(atomicBufferTests, checkBounds)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    std::string testString("hello world!");

    ASSERT_NO_THROW({
        ab.putInt32(0, -1);
    });

    ASSERT_NO_THROW({
        ab.putInt32(convertSizeToIndex(testBuffer.size() - sizeof(std::int32_t)), -1);
    });

    ASSERT_NO_THROW({
        ab.putInt64(convertSizeToIndex(testBuffer.size() - sizeof(std::int64_t)), -1);
    });

    ASSERT_NO_THROW({
        ab.putString(convertSizeToIndex(testBuffer.size() - testString.length() - sizeof(std::int32_t)), testString);
    });

    ASSERT_THROW({
        ab.putInt32(convertSizeToIndex(testBuffer.size()), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt64(convertSizeToIndex(testBuffer.size()), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt32(convertSizeToIndex(testBuffer.size() - sizeof(std::int32_t) + 1), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt64(convertSizeToIndex(testBuffer.size() - sizeof(std::int64_t) + 1), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putString(
            convertSizeToIndex(testBuffer.size() - testString.length() - sizeof(std::int32_t) + 1), testString);
    }, OutOfBoundsException);
}

#endif

TEST(atomicBufferTests, stringStore)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    std::string testString("hello world!");

    ab.putString(256, testString);

    ASSERT_EQ(static_cast<std::size_t>(ab.getInt32(256)), testString.length());

    std::string result(reinterpret_cast<char *>(&testBuffer[256] + sizeof(std::int32_t)));

    ASSERT_EQ(testString, result);
}

TEST(atomicBufferTests, stringRead)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    std::string testString("hello world!");

    ab.putString(256, testString);

    std::string result = ab.getString(256);

    ASSERT_EQ(testString, result);
}

TEST(atomicBufferTests, getAndSet32Test)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    index_t offset = 256;
    std::int32_t valueOne = 777;
    std::int32_t valueTwo = 333;

    ab.putInt32(offset, valueOne);

    ASSERT_EQ(valueOne, ab.getInt32(offset));
    ASSERT_EQ(valueOne, ab.getAndSetInt32(offset, valueTwo));
    ASSERT_EQ(valueTwo, ab.getInt32(offset));
}

TEST(atomicBufferTests, getAndSet64Test)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    index_t offset = 256;
    std::int64_t valueOne = 777;
    std::int64_t valueTwo = 333;

    ab.putInt64(offset, valueOne);

    ASSERT_EQ(valueOne, ab.getInt64(offset));
    ASSERT_EQ(valueOne, ab.getAndSetInt64(offset, valueTwo));
    ASSERT_EQ(valueTwo, ab.getInt64(offset));
}

TEST(atomicBufferTests, concurrentTest)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    const int threadCount = 4;
    const std::size_t repetitions = 10000000;
    std::vector<std::thread> threads;

    for (int i = 0; i < threadCount; i++)
    {
        threads.push_back(std::thread(
            [&]()
            {
                for (std::size_t n = 0; n < repetitions; n++)
                {
                    ab.getAndAddInt64(0, 1);
                    ab.getAndAddInt32(256, 1);
                }
            }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    ASSERT_EQ(static_cast<std::size_t>(ab.getInt64(0)), repetitions * threads.size());
    ASSERT_EQ(static_cast<std::size_t>(ab.getInt32(256)), repetitions * threads.size());
}

#pragma pack(push)
#pragma pack(4)
struct testStruct
{
    std::int32_t f1;
    std::int32_t f2;
    std::int64_t f3;
};
#pragma pack(pop)

TEST(atomicBufferTests, checkStructOverlay)
{
    testBuffer.fill(0xff);
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    ASSERT_NO_THROW({
        ab.overlayStruct<testStruct>(ab.capacity() - sizeof(testStruct));
    });

#if !defined(DISABLE_BOUNDS_CHECKS)
    ASSERT_THROW({
        ab.overlayStruct<testStruct>(ab.capacity() - sizeof(testStruct) + 1);
    }, OutOfBoundsException);
#endif

    testStruct ts{ 1, 2, 3 };
    ASSERT_NO_THROW({
        ab.overlayStruct<testStruct>(0) = ts;
        ASSERT_EQ(ab.getInt32(0), ts.f1);
        ASSERT_EQ(ab.getInt32(4), ts.f2);
        ASSERT_EQ(ab.getInt64(8), ts.f3);
    });

    ASSERT_NO_THROW({
        ab.putInt32(16, 101);
        ab.putInt32(20, 102);
        ab.putInt64(24, 103);

        auto &s = ab.overlayStruct<testStruct>(16);

        ASSERT_EQ(s.f1, 101);
        ASSERT_EQ(s.f2, 102);
        ASSERT_EQ(s.f3, 103);
    });
}
