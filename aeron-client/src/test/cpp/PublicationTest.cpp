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

#include <gtest/gtest.h>

#include "ClientConductorFixture.h"

using namespace aeron::concurrent;
using namespace aeron;

#define TERM_LENGTH (LogBufferDescriptor::TERM_MIN_LENGTH)
#define MTU_LENGTH (3072)
#define MAX_MESSAGE_LENGTH (TERM_LENGTH >> 3)
#define MAX_PAYLOAD_SIZE (MTU_LENGTH - DataFrameHeader::LENGTH)
#define PAGE_SIZE (LogBufferDescriptor::AERON_PAGE_MIN_SIZE)
#define LOG_META_DATA_LENGTH (LogBufferDescriptor::LOG_META_DATA_LENGTH)
#define SRC_BUFFER_LENGTH 1024

static_assert(LogBufferDescriptor::PARTITION_COUNT == 3, "partition count assumed to be 3 for these test");

typedef std::array<std::uint8_t, ((TERM_LENGTH * 3) + LOG_META_DATA_LENGTH)> term_buffer_t;
typedef std::array<std::uint8_t, MAX_MESSAGE_LENGTH> src_buffer_t;

static const std::string CHANNEL = "aeron:udp?endpoint=localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID = 0;

static const std::int64_t CORRELATION_ID = 100;
static const std::int64_t ORIGINAL_REGISTRATION_ID = 100;
static const std::int32_t INITIAL_TERM_ID = 13;

inline std::int64_t rawTailValue(std::int32_t termId, std::int32_t termOffset)
{
    return (static_cast<std::int64_t>(termId) << 32) | termOffset;
}

inline util::index_t termTailCounterOffset(const int index)
{
    return static_cast<util::index_t>(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET + (index * sizeof(std::int64_t)));
}

class PublicationTest : public testing::Test, public ClientConductorFixture
{
public:
    PublicationTest() :
        m_srcBuffer(m_src, 0),
        m_logBuffers(new LogBuffers(m_log.data(), static_cast<std::int64_t>(m_log.size()), TERM_LENGTH)),
        m_publicationLimit(m_counterValuesBuffer, PUBLICATION_LIMIT_COUNTER_ID),
        m_channelStatusIndicator(m_counterValuesBuffer, ChannelEndpointStatus::NO_ID_ALLOCATED)
    {
        m_log.fill(0);

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_termBuffers[i] = m_logBuffers->atomicBuffer(i);
        }

        m_logMetaDataBuffer = m_logBuffers->atomicBuffer(LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_MTU_LENGTH_OFFSET, MTU_LENGTH);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET, 0);

        m_logMetaDataBuffer.putInt64(termTailCounterOffset(0), static_cast<std::int64_t>(INITIAL_TERM_ID) << 32);
        for (int i = 1; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            const std::int32_t expectedTermId = (INITIAL_TERM_ID + i) - LogBufferDescriptor::PARTITION_COUNT;
            m_logMetaDataBuffer.putInt64(termTailCounterOffset(i), static_cast<std::int64_t>(expectedTermId) << 32);
        }

        auto *defaultHeader = (struct DataFrameHeader::DataFrameHeaderDefn *)(
            LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer).buffer());
        defaultHeader->frameLength = -1;
        defaultHeader->version = UINT8_MAX;
        defaultHeader->flags = UINT8_MAX;
        defaultHeader->type = UINT16_MAX;
        defaultHeader->termOffset = -1;
        defaultHeader->termId = -1;
        defaultHeader->reservedValue = -1;
        defaultHeader->sessionId = SESSION_ID;
        defaultHeader->streamId = STREAM_ID;

        m_publication = std::unique_ptr<Publication>(
            new Publication(
                m_conductor,
                CHANNEL,
                CORRELATION_ID,
                ORIGINAL_REGISTRATION_ID,
                STREAM_ID,
                SESSION_ID,
                m_publicationLimit,
                ChannelEndpointStatus::NO_ID_ALLOCATED,
                m_logBuffers));
    }

    void verifyHeader(
        AtomicBuffer &termBuffer,
        std::int32_t termOffset,
        std::int32_t frameLength,
        std::int32_t termId,
        std::uint16_t type,
        std::uint8_t flags,
        std::int64_t reservedValue)
    {
        const auto *hdr = (struct DataFrameHeader::DataFrameHeaderDefn *)(termBuffer.buffer() + termOffset);
        EXPECT_EQ(DataFrameHeader::CURRENT_VERSION, hdr->version);
        EXPECT_EQ(frameLength, hdr->frameLength);
        EXPECT_EQ(flags, hdr->flags);
        EXPECT_EQ(type, hdr->type);
        EXPECT_EQ(termOffset, hdr->termOffset);
        EXPECT_EQ(termId, hdr->termId);
        EXPECT_EQ(reservedValue, hdr->reservedValue);
        EXPECT_EQ(m_publication->sessionId(), hdr->sessionId);
        EXPECT_EQ(m_publication->streamId(), hdr->streamId);
    }

    static std::int64_t reserved_value_supplier(
        AtomicBuffer &termBuffer, util::index_t frameOffset, util::index_t frameLength)
    {
        return static_cast<std::int64_t>(frameOffset) * static_cast<std::int64_t>(frameLength) + frameLength;
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_log, 16) = {};
    AERON_DECL_ALIGNED(src_buffer_t m_src, 16) = {};

    AtomicBuffer m_termBuffers[3];
    AtomicBuffer m_logMetaDataBuffer;
    AtomicBuffer m_srcBuffer;

    std::shared_ptr<LogBuffers> m_logBuffers;
    UnsafeBufferPosition m_publicationLimit;
    StatusIndicatorReader m_channelStatusIndicator;
    std::unique_ptr<Publication> m_publication;
};

TEST_F(PublicationTest, shouldReportInitialPosition)
{
    EXPECT_EQ(m_publication->position(), 0);
}

TEST_F(PublicationTest, shouldReportMaxPossiblePosition)
{
    auto expectedPosition = static_cast<int64_t>(TERM_LENGTH * (UINT64_C(1) << 31u));
    EXPECT_EQ(m_publication->maxPossiblePosition(), expectedPosition);
}

TEST_F(PublicationTest, shouldReportMaxMessageLength)
{
    EXPECT_EQ(m_publication->maxMessageLength(), FrameDescriptor::computeMaxMessageLength(TERM_LENGTH));
}

TEST_F(PublicationTest, shouldReportCorrectTermBufferLength)
{
    EXPECT_EQ(m_publication->termBufferLength(), TERM_LENGTH);
}

TEST_F(PublicationTest, shouldReportThatPublicationHasNotBeenConnectedYet)
{
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, false);
    EXPECT_FALSE(m_publication->isConnected());
}

TEST_F(PublicationTest, shouldReportThatPublicationHasBeenConnectedYet)
{
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);
    EXPECT_TRUE(m_publication->isConnected());
}

TEST_F(PublicationTest, shouldEnsureThePublicationIsOpenBeforeReadingPosition)
{
    m_publication->close();
    EXPECT_EQ(m_publication->position(), PUBLICATION_CLOSED);
}

TEST_F(PublicationTest, shouldEnsureThePublicationIsOpenBeforeOffer)
{
    m_publication->close();
    EXPECT_TRUE(m_publication->isClosed());
    EXPECT_EQ(m_publication->offer(m_srcBuffer), PUBLICATION_CLOSED);
}

TEST_F(PublicationTest, shouldEnsureThePublicationIsOpenBeforeClaim)
{
    BufferClaim bufferClaim;

    m_publication->close();
    EXPECT_TRUE(m_publication->isClosed());
    EXPECT_EQ(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), PUBLICATION_CLOSED);
}

TEST_F(PublicationTest, shouldOfferAMessageUponConstruction)
{
    const std::int32_t length = 1000;
    const std::int64_t expectedPosition = 1056;
    m_publicationLimit.set(TERM_LENGTH);

    EXPECT_EQ(m_publication->offer(m_srcBuffer, 0, length, reserved_value_supplier), expectedPosition);
    EXPECT_EQ(m_publication->position(), expectedPosition);

    AtomicBuffer &termBuffer = m_termBuffers[0];
    verifyHeader(
        termBuffer,
        0,
        1032,
        INITIAL_TERM_ID,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG,
        1032);
}

TEST_F(PublicationTest, shouldFailToOfferAMessageWhenLimited)
{
    m_publicationLimit.set(0);

    EXPECT_EQ(m_publication->offer(m_srcBuffer), NOT_CONNECTED);
}

TEST_F(PublicationTest, shouldFailToOfferAMessageWithBackPressureWhenLimited)
{
    m_publicationLimit.set(0);
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);

    EXPECT_EQ(m_publication->offer(m_srcBuffer), BACK_PRESSURED);
}

TEST_F(PublicationTest, shouldFailToOfferAMessageWithMaxPositionExceededWhenLimited)
{
    const std::int32_t termCount = INT32_MAX;
    const std::int32_t termOffset = TERM_LENGTH - 1;
    const std::int32_t termId = INT32_MIN + (INITIAL_TERM_ID - 1);
    ASSERT_EQ(140737488355327l, LogBufferDescriptor::computePosition(
        termId, termOffset, m_publication->positionBitsToShift(), INITIAL_TERM_ID));
    ASSERT_EQ(140737488289792l,
        LogBufferDescriptor::computeTermBeginPosition(termId, m_publication->positionBitsToShift(), INITIAL_TERM_ID));
    const int activeIndex = LogBufferDescriptor::indexByTermCount(termCount);
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(termId, termOffset));
    LogBufferDescriptor::activeTermCountOrdered(m_logMetaDataBuffer, termCount);
    m_publicationLimit.set(1);
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);

    ASSERT_EQ(m_publication->offer(m_srcBuffer, 0, 1), MAX_POSITION_EXCEEDED);
}

TEST_F(PublicationTest, shouldFailToOfferWhenAppendFails)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(INITIAL_TERM_ID, INITIAL_TERM_ID);
    const std::int64_t initialPosition = TERM_LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(INITIAL_TERM_ID, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), ADMIN_ACTION);
}

TEST_F(PublicationTest, shouldRotateWhenAppendTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTermCount(0);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(INITIAL_TERM_ID, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), ADMIN_ACTION);

    const int nextIndex = LogBufferDescriptor::indexByTermCount(1);
    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET), 1);

    int64_t nextTermId = INITIAL_TERM_ID + 1;
    auto expectedTail = nextTermId << 32;
    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(nextIndex)), expectedTail);

    EXPECT_GT(m_publication->offer(m_srcBuffer), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
}

TEST_F(PublicationTest, shouldRotateWhenClaimTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTermCount(0);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(INITIAL_TERM_ID, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    BufferClaim bufferClaim;
    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), ADMIN_ACTION);

    const int nextIndex = LogBufferDescriptor::indexByTermCount(1);
    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET), 1);

    int64_t nextTermId = INITIAL_TERM_ID + 1;
    auto expectedTail = nextTermId << 32;
    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(nextIndex)), expectedTail);

    EXPECT_GT(
        m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim),
        initialPosition + DataFrameHeader::LENGTH + SRC_BUFFER_LENGTH);
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + SRC_BUFFER_LENGTH);
}

TEST_F(PublicationTest, shouldOfferFragmentedMessage)
{
    m_srcBuffer.setMemory(0, MAX_MESSAGE_LENGTH / 2, 'a');
    m_srcBuffer.setMemory(MAX_MESSAGE_LENGTH / 2, MAX_MESSAGE_LENGTH / 2, 'x');
    const std::int32_t termCount = 131;
    const std::int32_t termOffset = 4160;
    const std::int32_t termId = INITIAL_TERM_ID + termCount;
    const std::int32_t lastFrameOffset = termOffset + (MAX_MESSAGE_LENGTH / MAX_PAYLOAD_SIZE) * MTU_LENGTH;
    const std::int32_t remainingPayload = (MAX_MESSAGE_LENGTH % MAX_PAYLOAD_SIZE);
    const std::int32_t lastFrameLength = remainingPayload + DataFrameHeader::LENGTH;
    const int activeIndex = LogBufferDescriptor::indexByTermCount(termCount);
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(termId, termOffset));
    LogBufferDescriptor::activeTermCountOrdered(m_logMetaDataBuffer, termCount);
    m_publicationLimit.set(LONG_MAX);
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);

    ASSERT_EQ(m_publication->offer(m_srcBuffer, 0, MAX_MESSAGE_LENGTH, reserved_value_supplier), 8597664);

    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(activeIndex)),
        rawTailValue(termId, lastFrameOffset + BitUtil::align(lastFrameLength, FrameDescriptor::FRAME_ALIGNMENT)));
    AtomicBuffer &termBuffer = m_termBuffers[activeIndex];
    verifyHeader(
        termBuffer,
        termOffset,
        MTU_LENGTH,
        termId,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::BEGIN_FRAG,
        static_cast<std::int64_t>(termOffset) * static_cast<std::int64_t>(MTU_LENGTH) + MTU_LENGTH);
    EXPECT_EQ(0,
        memcmp(m_srcBuffer.buffer(), termBuffer.buffer() + termOffset + DataFrameHeader::LENGTH, MAX_PAYLOAD_SIZE));

    verifyHeader(
        termBuffer,
        lastFrameOffset,
        lastFrameLength,
        termId,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::END_FRAG,
        static_cast<std::int64_t>(lastFrameOffset) * static_cast<std::int64_t>(lastFrameLength) + lastFrameLength);
    EXPECT_EQ(0, memcmp(
        m_srcBuffer.buffer() + (MAX_MESSAGE_LENGTH - lastFrameLength - DataFrameHeader::LENGTH),
        termBuffer.buffer() + lastFrameOffset + DataFrameHeader::LENGTH,
        lastFrameLength - DataFrameHeader::LENGTH));
}

TEST_F(PublicationTest, shouldOfferFragmentedMessageViaSeveralBuffers)
{
    const std::int32_t length = 5000;
    const AtomicBuffer msgBuffers[3] = {
        AtomicBuffer(m_srcBuffer.buffer(), 100, 'a'),
        AtomicBuffer(m_srcBuffer.buffer() + 100, 200, 'b'),
        AtomicBuffer(m_srcBuffer.buffer() + 300, length - 300, 'z')};
    const std::int32_t termCount = 35;
    const std::int32_t termOffset = TERM_LENGTH / 4;
    const std::int32_t termId = INITIAL_TERM_ID + termCount;
    const std::int32_t lastFrameOffset = termOffset + (length / MAX_PAYLOAD_SIZE) * MTU_LENGTH;
    const std::int32_t remainingPayload = (length % MAX_PAYLOAD_SIZE);
    const std::int32_t lastFrameLength = remainingPayload + DataFrameHeader::LENGTH;
    const int activeIndex = LogBufferDescriptor::indexByTermCount(termCount);
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(termId, termOffset));
    LogBufferDescriptor::activeTermCountOrdered(m_logMetaDataBuffer, termCount);
    m_publicationLimit.set(LONG_MAX);
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);

    ASSERT_EQ(m_publication->offer(msgBuffers, 3, reserved_value_supplier), 2315232);

    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(activeIndex)),
        rawTailValue(termId, lastFrameOffset + BitUtil::align(lastFrameLength, FrameDescriptor::FRAME_ALIGNMENT)));
    AtomicBuffer &termBuffer = m_termBuffers[activeIndex];
    verifyHeader(
        termBuffer,
        termOffset,
        MTU_LENGTH,
        termId,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::BEGIN_FRAG,
        static_cast<std::int64_t>(termOffset) * static_cast<std::int64_t>(MTU_LENGTH) + MTU_LENGTH);
    EXPECT_EQ(0,
        memcmp(m_srcBuffer.buffer(), termBuffer.buffer() + termOffset + DataFrameHeader::LENGTH, 100));
    EXPECT_EQ(0,
        memcmp(m_srcBuffer.buffer() + 100, termBuffer.buffer() + termOffset + DataFrameHeader::LENGTH + 100, 200));

    verifyHeader(
        termBuffer,
        lastFrameOffset,
        lastFrameLength,
        termId,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::END_FRAG,
        static_cast<std::int64_t>(lastFrameOffset) * static_cast<std::int64_t>(lastFrameLength) + lastFrameLength);
    EXPECT_EQ(0, memcmp(
        m_srcBuffer.buffer() + MAX_PAYLOAD_SIZE,
        termBuffer.buffer() + lastFrameOffset + DataFrameHeader::LENGTH,
        lastFrameLength - DataFrameHeader::LENGTH));
}

TEST_F(PublicationTest, shouldTryClaimMaxMessage)
{
    BufferClaim bufferClaim;
    const std::int32_t termCount = 81;
    const std::int32_t termOffset = 1024;
    const std::int32_t termId = INITIAL_TERM_ID + termCount;
    const int activeIndex = LogBufferDescriptor::indexByTermCount(termCount);
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(termId, termOffset));
    LogBufferDescriptor::activeTermCountOrdered(m_logMetaDataBuffer, termCount);
    m_publicationLimit.set(LONG_MAX);
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);

    ASSERT_EQ(m_publication->tryClaim(MAX_PAYLOAD_SIZE, bufferClaim), 5312512);

    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(activeIndex)),
        rawTailValue(termId, termOffset + MTU_LENGTH));
    AtomicBuffer &termBuffer = m_termBuffers[activeIndex];
    verifyHeader(
        termBuffer,
        termOffset,
        -MTU_LENGTH,
        termId,
        DataFrameHeader::HDR_TYPE_DATA,
        FrameDescriptor::BEGIN_FRAG | FrameDescriptor::END_FRAG,
        0);
}
