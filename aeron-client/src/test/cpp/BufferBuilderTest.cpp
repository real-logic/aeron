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

#include "gtest/gtest.h"
#include "gmock/gmock-matchers.h"

#include "ChannelUriStringBuilder.h"
#include "BufferBuilder.h"

using namespace aeron;
using namespace aeron::protocol;

typedef std::array<std::uint8_t, BUFFER_BUILDER_INIT_MIN_CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 5 * BUFFER_BUILDER_INIT_MIN_CAPACITY> big_buffer_t;

TEST(BufferBuilderTest, shouldInitialiseToDefaultValues)
{
    BufferBuilder bufferBuilder{};
    ASSERT_EQ(0, bufferBuilder.capacity());
    ASSERT_EQ(0, bufferBuilder.limit());
    ASSERT_EQ(NULL_VALUE, bufferBuilder.nextTermOffset());

    bufferBuilder.reset();
    ASSERT_EQ(0, bufferBuilder.capacity());
    ASSERT_EQ(0, bufferBuilder.limit());
    ASSERT_EQ(NULL_VALUE, bufferBuilder.nextTermOffset());
}

TEST(BufferBuilderTest, shouldGrowDirectBuffer)
{
    BufferBuilder builder{};

    ASSERT_EQ(0, builder.limit());
    ASSERT_EQ(0, builder.capacity());

    buffer_t buf = {};
    AtomicBuffer buffer{buf};
    builder.append(buffer, 0, (util::index_t)buf.size());
    ASSERT_EQ(BUFFER_BUILDER_INIT_MIN_CAPACITY, builder.capacity());
    ASSERT_EQ(buf.size(), builder.limit());
}

TEST(BufferBuilderTest, shouldThrowIllegalArgumentExceptionIfInitialCapacityIsOutOfRange)
{
    ASSERT_THROW(BufferBuilder builder{std::numeric_limits<std::int32_t>::max()}, IllegalArgumentException);
    ASSERT_THROW(BufferBuilder builder{std::numeric_limits<std::int32_t>::max() - 7}, IllegalArgumentException);
}

TEST(BufferBuilderTest, shouldAppendNothingForZeroLength)
{
    BufferBuilder builder{};
    buffer_t buf = {};
    AtomicBuffer buffer{buf};
    builder.append(buffer, 0, 0);
    ASSERT_EQ(0, builder.limit());
    ASSERT_EQ(0, builder.limit());
}

TEST(BufferBuilderTest, shouldGrowToMultipleOfInitialCapacity)
{
    big_buffer_t buf = {};
    AtomicBuffer buffer{buf};
    BufferBuilder builder{};

    builder.append(buffer, 0, buffer.capacity());

    ASSERT_EQ(buffer.capacity(), builder.limit());
    ASSERT_GE(builder.capacity(), buffer.capacity());
}

TEST(BufferBuilderTest, shouldAppendThenReset)
{
    buffer_t buf = {};
    AtomicBuffer buffer{buf};
    BufferBuilder builder{};

    builder.append(buffer, 0, buffer.capacity());
    builder.nextTermOffset(1024);

    ASSERT_EQ(buffer.capacity(), builder.capacity());
    ASSERT_EQ(1024, builder.nextTermOffset());

    builder.reset();

    ASSERT_EQ(0, builder.limit());
    ASSERT_EQ(NULL_VALUE, builder.nextTermOffset());
}

TEST(BufferBuilderTest, shouldAppendOneBufferWithoutResizing)
{
    BufferBuilder builder{};
    builder.buffer();
    buffer_t buf = {};
    AtomicBuffer buffer{buf};
    std::string msg = "Hello World";
    std::string bufferMessage;

    buffer.putBytes(0, reinterpret_cast<const uint8_t *>(msg.c_str()), (util::index_t)msg.length());
    builder.append(buffer, 0, (util::index_t)msg.length());

    ASSERT_EQ(msg.length(), builder.limit());
    ASSERT_EQ(BUFFER_BUILDER_INIT_MIN_CAPACITY, builder.capacity());
    ASSERT_EQ(msg, bufferMessage.assign(reinterpret_cast<const char *>(builder.buffer()), static_cast<size_t>(builder.limit())));
}

TEST(BufferBuilderTest, shouldAppendTwoBufferWithoutResizing)
{
    BufferBuilder builder{};
    buffer_t buf = {};
    AtomicBuffer buffer{buf};
    std::string msg = "1111111122222222";
    std::string bufferMessage;

    buffer.putBytes(0, reinterpret_cast<const uint8_t *>(msg.c_str()), (util::index_t)msg.length());

    builder.append(buffer, 0, (util::index_t)msg.length() / 2);
    builder.append(buffer, (util::index_t)msg.length() / 2, (util::index_t)msg.length() / 2);

    ASSERT_EQ(msg.length(), builder.limit());
    ASSERT_EQ(BUFFER_BUILDER_INIT_MIN_CAPACITY, builder.capacity());
    ASSERT_EQ(msg, bufferMessage.assign(reinterpret_cast<const char *>(builder.buffer()), static_cast<size_t>(builder.limit())));
}

TEST(BufferBuilderTest, shouldFillBufferWithoutResizing)
{
    const std::int32_t length = 128;

    BufferBuilder builder{length};
    buffer_t buf = {};
    buf.fill('a');
    AtomicBuffer buffer{buf};

    const std::uint32_t initialCapacity = builder.capacity();

    builder.append(buffer, 0, length);

    ASSERT_EQ(length, builder.capacity());
    ASSERT_EQ(initialCapacity, builder.capacity());
    std::vector<char> actual{builder.buffer(), builder.buffer() + length};
    ASSERT_THAT(actual, testing::ElementsAreArray(buf.data(), buf.data() + length));
}

TEST(BufferBuilderTest, shouldResizeWhenBufferJustDoesNotFit)
{
    const std::int32_t length = 128;

    BufferBuilder builder{length};
    buffer_t buf = {};
    buf.fill('a');
    AtomicBuffer buffer{buf};

    builder.append(buffer, 0, length + 1);

    ASSERT_EQ(length + 1, builder.limit());
    ASSERT_GE(builder.capacity(), length);
    std::vector<char> actual{builder.buffer(), builder.buffer() + (length + 1)};
    ASSERT_THAT(actual, testing::ElementsAreArray(buf.data(), buf.data() + (length + 1)));
}

TEST(BufferBuilderTest, shouldAppendTwoBuffersAndResize)
{
    const std::int32_t length = 128;
    const std::int32_t firstLength = length / 2;
    const std::int32_t secondLength = length / 4;

    BufferBuilder builder{ firstLength };
    buffer_t buf = {};
    buf.fill('a');
    AtomicBuffer buffer{buf};

    builder.append(buffer, 0, firstLength);
    builder.append(buffer, firstLength, secondLength);

    ASSERT_EQ(firstLength + secondLength, builder.limit());
    ASSERT_GE(builder.capacity(), length);
    std::vector<char> actual{builder.buffer(), builder.buffer() + (firstLength + secondLength)};
    ASSERT_THAT(actual, testing::ElementsAreArray(buf.data(), buf.data() + (firstLength + secondLength)));
}

TEST(BufferBuilderTest, shouldCompactBufferToLowerLimit)
{
    const std::int32_t length = BUFFER_BUILDER_INIT_MIN_CAPACITY / 2;

    BufferBuilder builder{};
    buffer_t buf = {};
    buf.fill('a');
    AtomicBuffer buffer{buf};

    const int bufferCount = 5;
    for (int i = 0; i < bufferCount; ++i)
    {
        builder.append(buffer, 0, length);
    }

    const auto expectedLimit = (std::uint32_t)(length * bufferCount);
    const auto expandedCapacity = builder.capacity();
    ASSERT_EQ(expectedLimit, builder.limit());
    ASSERT_GT(expandedCapacity, expectedLimit);

    builder.reset();

    builder.append(buffer, 0, length);
    builder.append(buffer, 0, length);
    builder.append(buffer, 0, length);

    builder.compact();

    ASSERT_EQ(3 * length, builder.limit());
    ASSERT_LT(builder.capacity(), expandedCapacity);
    ASSERT_EQ(builder.limit(), builder.capacity());
}

TEST(BufferBuilderTest, captureFirstHeaderShouldCopyTheEntireHeader)
{
    BufferBuilder builder{};

    const std::int64_t reservedValue = std::numeric_limits<std::int64_t>::max() - 117;
    buffer_t buf = {};
    AtomicBuffer buffer{buf};

    aeron::protocol::DataHeaderFlyweight srcHeaderFlyweight{buffer, 0};
    srcHeaderFlyweight.frameLength(512);
    srcHeaderFlyweight.version(0xa);
    srcHeaderFlyweight.flags(static_cast<int8_t>(0b11100111));
    srcHeaderFlyweight.type(DataHeaderFlyweight::HDR_TYPE_DATA);
    srcHeaderFlyweight.termOffset(0);
    srcHeaderFlyweight.sessionId(-890);
    srcHeaderFlyweight.streamId(555);
    srcHeaderFlyweight.termId(42);
    srcHeaderFlyweight.reservedValue(reservedValue);

    Header header{5, 3, nullptr};
    header.buffer(buffer);

    builder.captureHeader(header);
    ASSERT_EQ(0, builder.capacity());
    ASSERT_EQ(0, builder.limit());

    ASSERT_EQ(srcHeaderFlyweight.frameLength(), builder.completeHeader().frameLength());
    ASSERT_EQ(srcHeaderFlyweight.flags(), builder.completeHeader().flags());
    ASSERT_EQ(srcHeaderFlyweight.type(), builder.completeHeader().type());
    ASSERT_EQ(srcHeaderFlyweight.termOffset(), builder.completeHeader().termOffset());
    ASSERT_EQ(srcHeaderFlyweight.sessionId(), builder.completeHeader().sessionId());
    ASSERT_EQ(srcHeaderFlyweight.streamId(), builder.completeHeader().streamId());
    ASSERT_EQ(srcHeaderFlyweight.termId(), builder.completeHeader().termId());
    ASSERT_EQ(srcHeaderFlyweight.reservedValue(), builder.completeHeader().reservedValue());
}

TEST(BufferBuilderTest, shouldPrepareCompleteHeader)
{
    buffer_t dataBuf = {};
    AtomicBuffer dataBuffer{dataBuf};
    buffer_t buf1 = {};
    AtomicBuffer buffer1{buf1};
    buffer_t buf2 = {};
    AtomicBuffer buffer2{buf2};

    BufferBuilder builder{};

    const int dataLength = 999;
    builder.append(dataBuffer, 0, dataLength);
    ASSERT_EQ(BUFFER_BUILDER_INIT_MIN_CAPACITY, builder.capacity());
    ASSERT_EQ(dataLength, builder.limit());

    const std::int32_t frameLength = 128;
    const std::int32_t termOffset = 1024;
    const std::int16_t version = 15;
    const std::int32_t type = DataHeaderFlyweight::HDR_TYPE_DATA;
    const std::int32_t sessionId = 87;
    const std::int32_t streamId = -9;
    const std::int32_t termId = 10;
    const auto reservedValue = (std::int64_t)0xCAFEBABEDEADBEEF;

    aeron::protocol::DataHeaderFlyweight headerFlyweight1{buffer1, 0};
    headerFlyweight1.frameLength(frameLength);
    headerFlyweight1.version(version);
    headerFlyweight1.flags(static_cast<int8_t>(0b10111001));
    headerFlyweight1.type(type);
    headerFlyweight1.termOffset(termOffset);
    headerFlyweight1.sessionId(sessionId);
    headerFlyweight1.streamId(streamId);
    headerFlyweight1.termId(termId);
    headerFlyweight1.reservedValue(reservedValue);

    Header header{4, 48, nullptr};
    header.buffer(buffer1);
    header.offset(0);
    builder.captureHeader(header);

    aeron::protocol::DataHeaderFlyweight headerFlyweight2{buffer2, 0};
    headerFlyweight2.frameLength(200);
    headerFlyweight2.version((short)0xC);
    headerFlyweight2.flags(static_cast<int8_t>(0b01000100));
    headerFlyweight2.type(DataHeaderFlyweight::HDR_TYPE_DATA);
    headerFlyweight2.termOffset(48);
    headerFlyweight2.sessionId(dataLength);
    headerFlyweight2.streamId(4);
    headerFlyweight2.termId(39);
    headerFlyweight2.reservedValue(std::numeric_limits<std::int64_t>::min());

    header.buffer(buffer2);
    header.offset(0);
    const Header &completeHeader = builder.completeHeader(header);

    ASSERT_EQ(BUFFER_BUILDER_INIT_MIN_CAPACITY, builder.capacity());
    ASSERT_EQ(dataLength, builder.limit());
    ASSERT_EQ(4, completeHeader.initialTermId());
    ASSERT_EQ(48, completeHeader.positionBitsToShift());
    ASSERT_EQ(DataHeaderFlyweight::headerLength() + dataLength, completeHeader.frameLength());
    ASSERT_EQ(0xFD, completeHeader.flags());
    ASSERT_EQ(type, completeHeader.type());
    ASSERT_EQ(termOffset, completeHeader.termOffset());
    ASSERT_EQ(sessionId, completeHeader.sessionId());
    ASSERT_EQ(streamId, completeHeader.streamId());
    ASSERT_EQ(reservedValue, completeHeader.reservedValue());
}

class BufferBuilderHeaderParameterisedTest : public testing::TestWithParam<std::tuple<std::int32_t, std::int32_t>>
{
};

INSTANTIATE_TEST_SUITE_P(
    BufferBuilderHeaderParameterisedTest,
    BufferBuilderHeaderParameterisedTest,
    testing::Values(
        std::make_tuple(1024, 1408),
        std::make_tuple(1024, 128),
        std::make_tuple(8192, 1408)
    ));

TEST_P(BufferBuilderHeaderParameterisedTest, shouldCalculatePositionAndFrameLengthWhenReassembling)
{
    const std::int32_t totalPayloadLength = std::get<0>(GetParam());
    const std::int32_t mtu = std::get<1>(GetParam());
    const std::int32_t maxPayloadLength = mtu - DataHeaderFlyweight::headerLength();
    const std::int32_t termOffset = 1024;
    const std::int32_t termId = 10;
    const std::int32_t initialTermId = 4;
    const std::int32_t positionBitsToShift = 16;

    const std::int32_t expectedFrameLength = DataHeaderFlyweight::headerLength() + totalPayloadLength;
    const std::int32_t fragmentedFrameLength = LogBufferDescriptor::computeFragmentedFrameLength(
        totalPayloadLength, maxPayloadLength);
    const std::int64_t startPosition = LogBufferDescriptor::computePosition(
        termId, termOffset, positionBitsToShift, initialTermId);
    const std::int64_t expectedPosition = startPosition + fragmentedFrameLength;

    big_buffer_t dataBuf = {};
    AtomicBuffer dataBuffer{dataBuf};
    buffer_t buf1 = {};
    AtomicBuffer buffer1{buf1};
    buffer_t buf2 = {};
    AtomicBuffer buffer2{buf2};

    BufferBuilder builder{};

    aeron::protocol::DataHeaderFlyweight firstHeader{buffer1, 0};
    firstHeader.frameLength(DataHeaderFlyweight::headerLength() + maxPayloadLength);
    firstHeader.version((short)15);
    firstHeader.flags((int8_t)0b10111001);
    firstHeader.type(DataHeaderFlyweight::HDR_TYPE_DATA);
    firstHeader.termOffset(termOffset);
    firstHeader.sessionId(87);
    firstHeader.streamId(-9);
    firstHeader.termId(termId);
    firstHeader.reservedValue((std::int64_t)0xCAFEBABEDEADBEEF);

    Header header{initialTermId, positionBitsToShift, nullptr};
    header.buffer(buffer1);
    header.offset(0);
    builder.captureHeader(header);

    int lastPayloadLength = 0;
    for (int position = 0; position < totalPayloadLength; position += maxPayloadLength)
    {
        int remaining = totalPayloadLength - position;
        lastPayloadLength = maxPayloadLength < remaining ? maxPayloadLength : remaining;
        builder.append(dataBuffer, position, lastPayloadLength);
    }

    ASSERT_NE(0, lastPayloadLength);

    int32_t valueIsIgnored = std::numeric_limits<int32_t>::min();

    aeron::protocol::DataHeaderFlyweight lastHeader{buffer2, 0};
    lastHeader.frameLength(DataHeaderFlyweight::headerLength() + lastPayloadLength);
    lastHeader.version((short)15);
    lastHeader.flags((int8_t)0b10111001);
    lastHeader.type(DataHeaderFlyweight::HDR_TYPE_DATA);
    lastHeader.termOffset(valueIsIgnored);
    lastHeader.sessionId(87);
    lastHeader.streamId(-9);
    lastHeader.termId(termId);
    lastHeader.reservedValue((std::int64_t)0xCAFEBABEDEADBEEF);

    header.buffer(buffer2);
    header.offset(0);

    const Header &completeHeader = builder.completeHeader(header);

    ASSERT_EQ(expectedFrameLength, completeHeader.frameLength());
    ASSERT_EQ(expectedPosition, completeHeader.position());
}
