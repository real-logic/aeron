
#include <cstdint>
#include <array>

#include <gtest/gtest.h>

#include <concurrent/AtomicBuffer.h>
#include <util/Exceptions.h>

using namespace aeron::common::concurrent;
using namespace aeron::common::util;

static std::array<std::uint8_t, 1024> testBuffer;

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

    ASSERT_EQ(ab.getInt32(256), testString.length());

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