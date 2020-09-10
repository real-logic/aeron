/*
 * Copyright 2014-2020 Real Logic Limited.
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
#define PAGE_SIZE (LogBufferDescriptor::AERON_PAGE_MIN_SIZE)
#define LOG_META_DATA_LENGTH (LogBufferDescriptor::LOG_META_DATA_LENGTH)
#define SRC_BUFFER_LENGTH 1024

static_assert(LogBufferDescriptor::PARTITION_COUNT == 3, "partition count assumed to be 3 for these test");

typedef std::array<std::uint8_t, ((TERM_LENGTH * 3) + LOG_META_DATA_LENGTH)> term_buffer_t;
typedef std::array<std::uint8_t, SRC_BUFFER_LENGTH> src_buffer_t;

static const std::string CHANNEL = "aeron:udp?endpoint=localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID = 0;

static const std::int64_t CORRELATION_ID = 100;
static const std::int32_t TERM_ID_1 = 1;

inline std::int64_t rawTailValue(std::int32_t termId, std::int64_t position)
{
    return (termId * ((INT64_C(1) << 32))) | position;
}

inline util::index_t termTailCounterOffset(const int index)
{
    return LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET + (index * sizeof(std::int64_t));
}

class ExclusivePublicationTest : public testing::Test, public ClientConductorFixture
{
public:
    ExclusivePublicationTest() :
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

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_MTU_LENGTH_OFFSET, (3 * m_srcBuffer.capacity()));
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_INITIAL_TERM_ID_OFFSET, TERM_ID_1);

        const std::int32_t index = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET, 0);

        m_logMetaDataBuffer.putInt64(termTailCounterOffset(index), static_cast<std::int64_t>(TERM_ID_1) << 32);
    }

    void createPub()
    {
        m_publication = std::unique_ptr<ExclusivePublication>(new ExclusivePublication(
            m_conductor, CHANNEL, CORRELATION_ID, STREAM_ID, SESSION_ID,
            m_publicationLimit, ChannelEndpointStatus::NO_ID_ALLOCATED, m_logBuffers));
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_log, 16);
    AERON_DECL_ALIGNED(src_buffer_t m_src, 16);

    AtomicBuffer m_termBuffers[3];
    AtomicBuffer m_logMetaDataBuffer;
    AtomicBuffer m_srcBuffer;

    std::shared_ptr<LogBuffers> m_logBuffers;
    UnsafeBufferPosition m_publicationLimit;
    StatusIndicatorReader m_channelStatusIndicator;
    std::unique_ptr<ExclusivePublication> m_publication;
};

TEST_F(ExclusivePublicationTest, shouldReportInitialPosition)
{
    createPub();
    EXPECT_EQ(m_publication->position(), 0);
}

TEST_F(ExclusivePublicationTest, shouldReportMaxMessageLength)
{
    createPub();
    EXPECT_EQ(m_publication->maxMessageLength(), FrameDescriptor::computeMaxMessageLength(TERM_LENGTH));
}

TEST_F(ExclusivePublicationTest, shouldReportMaxPossiblePosition)
{
    auto expectedPosition = static_cast<std::int64_t>(TERM_LENGTH * (UINT64_C(1) << 31u));
    createPub();
    EXPECT_EQ(m_publication->maxPossiblePosition(), expectedPosition);
}

TEST_F(ExclusivePublicationTest, shouldReportCorrectTermBufferLength)
{
    createPub();
    EXPECT_EQ(m_publication->termBufferLength(), TERM_LENGTH);
}

TEST_F(ExclusivePublicationTest, shouldReportThatPublicationHasNotBeenConnectedYet)
{
    createPub();
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, false);
    EXPECT_FALSE(m_publication->isConnected());
}

TEST_F(ExclusivePublicationTest, shouldReportThatPublicationHasBeenConnectedYet)
{
    createPub();
    LogBufferDescriptor::isConnected(m_logMetaDataBuffer, true);
    EXPECT_TRUE(m_publication->isConnected());
}

TEST_F(ExclusivePublicationTest, shouldEnsureThePublicationIsOpenBeforeReadingPosition)
{
    createPub();
    m_publication->close();
    EXPECT_EQ(m_publication->position(), PUBLICATION_CLOSED);
}

TEST_F(ExclusivePublicationTest, shouldEnsureThePublicationIsOpenBeforeOffer)
{
    createPub();
    m_publication->close();
    EXPECT_TRUE(m_publication->isClosed());
    EXPECT_EQ(m_publication->offer(m_srcBuffer), PUBLICATION_CLOSED);
}

TEST_F(ExclusivePublicationTest, shouldEnsureThePublicationIsOpenBeforeClaim)
{
    BufferClaim bufferClaim;

    createPub();
    m_publication->close();
    EXPECT_TRUE(m_publication->isClosed());
    EXPECT_EQ(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), PUBLICATION_CLOSED);
}

TEST_F(ExclusivePublicationTest, shouldOfferAMessageUponConstruction)
{
    createPub();
    const std::int64_t expectedPosition = m_srcBuffer.capacity() + DataFrameHeader::LENGTH;
    m_publicationLimit.set(2 * m_srcBuffer.capacity());

    EXPECT_EQ(m_publication->offer(m_srcBuffer), expectedPosition);
    EXPECT_EQ(m_publication->position(), expectedPosition);
}

TEST_F(ExclusivePublicationTest, shouldFailToOfferAMessageWhenLimited)
{
    createPub();
    m_publicationLimit.set(0);

    EXPECT_EQ(m_publication->offer(m_srcBuffer), NOT_CONNECTED);
}

TEST_F(ExclusivePublicationTest, shouldFailToOfferWhenAppendFails)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(TERM_ID_1, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    createPub();

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), ADMIN_ACTION);
}

TEST_F(ExclusivePublicationTest, shouldRotateWhenAppendTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(TERM_ID_1, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    createPub();

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), ADMIN_ACTION);

    const int nextIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 1);
    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET), 1);

    int64_t nextTermId = TERM_ID_1 + 1;
    auto expectedTail = nextTermId << 32;
    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(nextIndex)), expectedTail);

    EXPECT_GT(m_publication->offer(m_srcBuffer), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
}

TEST_F(ExclusivePublicationTest, shouldRotateWhenClaimTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_logMetaDataBuffer.putInt64(termTailCounterOffset(activeIndex), rawTailValue(TERM_ID_1, initialPosition));
    m_publicationLimit.set(LONG_MAX);

    createPub();

    BufferClaim bufferClaim;
    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), ADMIN_ACTION);

    const int nextIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 1);
    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_COUNT_OFFSET), 1);

    int64_t nextTermId = TERM_ID_1 + 1;
    auto expectedTail = nextTermId << 32;
    EXPECT_EQ(m_logMetaDataBuffer.getInt64(termTailCounterOffset(nextIndex)), expectedTail);

    EXPECT_GT(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim),
              initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
}
