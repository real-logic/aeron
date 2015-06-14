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

#include <cstdint>
#include <array>
#include <vector>
#include <thread>

#include <gtest/gtest.h>

#include <concurrent/AtomicBuffer.h>
#include <util/Exceptions.h>

using namespace aeron::concurrent;
using namespace aeron::util;


typedef std::array<std::uint8_t, 1024> buffer_t;
static AERON_DECL_ALIGNED(buffer_t testBuffer, 16);

static void clearBuffer()
{
    testBuffer.fill(0);
}

TEST (atomicBufferTests, checkBounds)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());
    std::string testString ("hello world!");

    ASSERT_NO_THROW({
        ab.putInt32(0, -1);
    });

    ASSERT_NO_THROW({
        ab.putInt32(testBuffer.size() - sizeof(std::int32_t), -1);
    });

    ASSERT_NO_THROW({
        ab.putInt64(testBuffer.size() - sizeof(std::int64_t), -1);
    });

    ASSERT_NO_THROW({
        ab.putStringUtf8(testBuffer.size() - testString.length() - sizeof(std::int32_t), testString);
    });

    ASSERT_THROW({
        ab.putInt32(testBuffer.size(), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt64(testBuffer.size(), -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt32(testBuffer.size() - sizeof(std::int32_t) + 1, -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putInt64(testBuffer.size() - sizeof(std::int64_t) + 1, -1);
    }, OutOfBoundsException);

    ASSERT_THROW({
        ab.putStringUtf8(testBuffer.size() - testString.length() - sizeof(std::int32_t) + 1, testString);
    }, OutOfBoundsException);
}


TEST (atomicBufferTests, stringStore)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    std::string testString("hello world!");

    ab.putStringUtf8(256, testString);

    ASSERT_EQ((size_t)ab.getInt32(256), testString.length());

    std::string result(reinterpret_cast<char*>(&testBuffer[256] + sizeof (std::int32_t)));

    ASSERT_EQ(testString, result);
}

TEST (atomicBufferTests, stringRead)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    std::string testString("hello world!");

    ab.putStringUtf8(256, testString);

    std::string result = ab.getStringUtf8(256);

    ASSERT_EQ(testString, result);
}

TEST (atomicBufferTests, concurrentTest)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    std::vector<std::thread> threads;
    const size_t incCount = 10000000;

    for (int i = 0; i < 8; i++)
    {
        threads.push_back(std::thread([&]()
        {
            for (size_t n = 0; n < incCount; n++)
                ab.getAndAddInt64(0, 1);
        }));
    }

    for (std::thread& t: threads)
    {
        t.join();
    }

    ASSERT_EQ((size_t)ab.getInt64(0), incCount * threads.size());
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

TEST (atomicBufferTests, checkStructOveray)
{
    testBuffer.fill(0xff);
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());

    ASSERT_NO_THROW({
        ab.overlayStruct<testStruct>(ab.capacity() - sizeof(testStruct));
    });

    ASSERT_THROW({
        ab.overlayStruct<testStruct>(ab.capacity() - sizeof(testStruct) + 1);
    }, OutOfBoundsException);

    testStruct ts { 1, 2, 3 };
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

        testStruct& s = ab.overlayStruct<testStruct>(16);

        ASSERT_EQ(s.f1, 101);
        ASSERT_EQ(s.f2, 102);
        ASSERT_EQ(s.f3, 103);
    });
}
