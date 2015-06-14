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

#include <gtest/gtest.h>

#include "ClientConductorFixture.h"

using namespace aeron::concurrent;
using namespace aeron;

#define TERM_LENGTH (LogBufferDescriptor::TERM_MIN_LENGTH)
#define TERM_META_DATA_LENGTH (LogBufferDescriptor::TERM_META_DATA_LENGTH)
#define LOG_META_DATA_LENGTH (LogBufferDescriptor::LOG_META_DATA_LENGTH)
#define SRC_BUFFER_LENGTH 1024

typedef std::array<std::uint8_t, ((TERM_LENGTH * 3) + (TERM_META_DATA_LENGTH * 3) + LOG_META_DATA_LENGTH)> log_buffer_t;
typedef std::array<std::uint8_t, SRC_BUFFER_LENGTH> src_buffer_t;

static const std::string CHANNEL = "udp://localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID = 0;

static const std::int64_t CORRELATION_ID = 100;
static const std::int32_t TERM_ID_1 = 1;

class PublicationTest : public testing::Test, ClientConductorFixture
{
public:
    PublicationTest() :
        m_srcBuffer(&m_src[0], m_src.size()),
        m_publicationLimit(m_counterValuesBuffer, PUBLICATION_LIMIT_COUNTER_ID)
    {
        m_log.fill(0);

        for (int i = 0; i < 3; i++)
        {
            m_termBuffers[i].wrap(&(m_log[i * TERM_LENGTH]), TERM_LENGTH);
            m_metaDataBuffers[i].wrap(&(m_log[(TERM_LENGTH * 3) + (i * TERM_META_DATA_LENGTH)]), TERM_META_DATA_LENGTH);
        }

        m_logMetaDataBuffer.wrap(&m_log[m_log.size() - LOG_META_DATA_LENGTH], LOG_META_DATA_LENGTH);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_MTU_LENGTH_OFFSET, (3 * m_srcBuffer.capacity()));

        m_logBuffers = std::unique_ptr<LogBuffers>(new LogBuffers(&m_log[0], (index_t)m_log.size()));

        m_publication = std::unique_ptr<Publication>(new Publication(
            m_conductor, CHANNEL, CORRELATION_ID, STREAM_ID, SESSION_ID, m_publicationLimit, *(m_logBuffers.get())));
    }

    virtual void SetUp()
    {
        m_log.fill(0);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_ID_OFFSET, TERM_ID_1);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_INITIAL_TERM_ID_OFFSET, TERM_ID_1);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_MTU_LENGTH_OFFSET, (3 * m_srcBuffer.capacity()));
    }

protected:
    AERON_DECL_ALIGNED(log_buffer_t m_log, 16);
    AERON_DECL_ALIGNED(src_buffer_t m_src, 16);

    AtomicBuffer m_termBuffers[3];
    AtomicBuffer m_metaDataBuffers[3];
    AtomicBuffer m_logMetaDataBuffer;
    AtomicBuffer m_srcBuffer;

    std::unique_ptr<LogBuffers> m_logBuffers;
    std::unique_ptr<Publication> m_publication;
    UnsafeBufferPosition m_publicationLimit;
};

TEST_F(PublicationTest, shouldReportInitialPosition)
{
    EXPECT_EQ(m_publication->position(), 0);
}

TEST_F(PublicationTest, shouldReportMaxMessageLength)
{
    EXPECT_EQ(m_publication->maxMessageLength(), FrameDescriptor::computeMaxMessageLength(TERM_LENGTH));
}

TEST_F(PublicationTest, shouldOfferAMessageUponConstruction)
{
    const std::int64_t expectedPosition = m_srcBuffer.capacity() + DataFrameHeader::LENGTH;
    m_publicationLimit.set(2 * m_srcBuffer.capacity());

    EXPECT_EQ(m_publication->offer(m_srcBuffer), expectedPosition);
    EXPECT_EQ(m_publication->position(), expectedPosition);
}

TEST_F(PublicationTest, shouldFailToOfferAMessageWhenLimited)
{
    m_publicationLimit.set(0);

    EXPECT_EQ(m_publication->offer(m_srcBuffer), PUBLICATION_NOT_CONNECTED);
}

TEST_F(PublicationTest, shouldFailToOfferWhenAppendFails)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH - 1;
    m_metaDataBuffers[activeIndex].putInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, initialPosition);
    m_publicationLimit.set(LONG_MAX);

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), PUBLICATION_BACK_PRESSURE);
}

TEST_F(PublicationTest, shouldRotateWhenAppendTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_metaDataBuffers[activeIndex].putInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, initialPosition);
    m_publicationLimit.set(LONG_MAX);

    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->offer(m_srcBuffer), PUBLICATION_BACK_PRESSURE);
    EXPECT_GT(m_publication->offer(m_srcBuffer), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());

    const int cleaningIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 2);
    EXPECT_EQ(m_metaDataBuffers[cleaningIndex].getInt32(LogBufferDescriptor::TERM_STATUS_OFFSET), LogBufferDescriptor::NEEDS_CLEANING);

    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_ID_OFFSET), TERM_ID_1 + 1);

    AtomicBuffer defaultHdr;

    defaultHdr.wrap(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer, cleaningIndex), DataFrameHeader::LENGTH);
    EXPECT_EQ(defaultHdr.getInt32(DataFrameHeader::TERM_ID_FIELD_OFFSET), TERM_ID_1 + 2);

    const int newTermIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 1);
    defaultHdr.wrap(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer, newTermIndex), DataFrameHeader::LENGTH);
    EXPECT_EQ(defaultHdr.getInt32(DataFrameHeader::TERM_ID_FIELD_OFFSET), TERM_ID_1 + 1);
}

TEST_F(PublicationTest, shouldRotateWhenClaimTrips)
{
    const int activeIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1);
    const std::int64_t initialPosition = TERM_LENGTH - DataFrameHeader::LENGTH;
    m_metaDataBuffers[activeIndex].putInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET, initialPosition);
    m_publicationLimit.set(LONG_MAX);

    BufferClaim bufferClaim;
    EXPECT_EQ(m_publication->position(), initialPosition);
    EXPECT_EQ(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), PUBLICATION_BACK_PRESSURE);
    EXPECT_GT(m_publication->tryClaim(SRC_BUFFER_LENGTH, bufferClaim), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());
    EXPECT_GT(m_publication->position(), initialPosition + DataFrameHeader::LENGTH + m_srcBuffer.capacity());

    const int cleaningIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 2);
    EXPECT_EQ(m_metaDataBuffers[cleaningIndex].getInt32(LogBufferDescriptor::TERM_STATUS_OFFSET), LogBufferDescriptor::NEEDS_CLEANING);

    EXPECT_EQ(m_logMetaDataBuffer.getInt32(LogBufferDescriptor::LOG_ACTIVE_TERM_ID_OFFSET), TERM_ID_1 + 1);

    AtomicBuffer defaultHdr;

    defaultHdr.wrap(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer, cleaningIndex), DataFrameHeader::LENGTH);
    EXPECT_EQ(defaultHdr.getInt32(DataFrameHeader::TERM_ID_FIELD_OFFSET), TERM_ID_1 + 2);

    const int newTermIndex = LogBufferDescriptor::indexByTerm(TERM_ID_1, TERM_ID_1 + 1);
    defaultHdr.wrap(LogBufferDescriptor::defaultFrameHeader(m_logMetaDataBuffer, newTermIndex), DataFrameHeader::LENGTH);
    EXPECT_EQ(defaultHdr.getInt32(DataFrameHeader::TERM_ID_FIELD_OFFSET), TERM_ID_1 + 1);
}