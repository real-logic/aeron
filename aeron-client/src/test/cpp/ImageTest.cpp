/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#include <concurrent/logbuffer/DataFrameHeader.h>
#include "ClientConductorFixture.h"

using namespace aeron::concurrent;
using namespace aeron;
using namespace std::placeholders;

#define TERM_LENGTH (LogBufferDescriptor::TERM_MIN_LENGTH)
#define PAGE_SIZE (LogBufferDescriptor::PAGE_MIN_SIZE)
#define LOG_META_DATA_LENGTH (LogBufferDescriptor::LOG_META_DATA_LENGTH)
#define SRC_BUFFER_LENGTH 1024

static_assert(LogBufferDescriptor::PARTITION_COUNT==3, "partition count assumed to be 3 for these test");

typedef std::array<std::uint8_t, ((TERM_LENGTH * 3) + LOG_META_DATA_LENGTH)> term_buffer_t;
typedef std::array<std::uint8_t, SRC_BUFFER_LENGTH> src_buffer_t;

static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t SUBSCRIBER_POSITION_ID = 0;

static const std::int64_t CORRELATION_ID = 100;
static const std::int64_t SUBSCRIPTION_REGISTRATION_ID = 99;
static const std::string SOURCE_IDENTITY = "test";

static const std::array<std::uint8_t, 17> DATA = { { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 } };

static const std::int32_t INITIAL_TERM_ID = 0xFEDA;
static const std::int32_t POSITION_BITS_TO_SHIFT = BitUtil::numberOfTrailingZeroes(TERM_LENGTH);
static const util::index_t ALIGNED_FRAME_LENGTH =
    BitUtil::align(DataFrameHeader::LENGTH + (std::int32_t)DATA.size(), FrameDescriptor::FRAME_ALIGNMENT);

void exceptionHandler(const std::exception&)
{
}

class MockFragmentHandler
{
public:
    MOCK_CONST_METHOD4(onFragment, void(AtomicBuffer&, util::index_t, util::index_t, Header&));
};

class MockControlledFragmentHandler
{
public:
    MOCK_CONST_METHOD4(onFragment, ControlledPollAction(AtomicBuffer&, util::index_t, util::index_t, Header&));
};

class ImageTest : public testing::Test, ClientConductorFixture
{
public:
    ImageTest() :
        m_srcBuffer(m_src, 0),
        m_logBuffers(std::make_shared<LogBuffers>(m_log.data(), static_cast<std::int64_t>(m_log.size()), TERM_LENGTH)),
        m_subscriberPosition(m_counterValuesBuffer, SUBSCRIBER_POSITION_ID),
        m_handler(std::bind(&MockFragmentHandler::onFragment, &m_fragmentHandler, _1, _2, _3, _4)),
        m_controlledHandler(std::bind(&MockControlledFragmentHandler::onFragment, &m_controlledFragmentHandler, _1, _2, _3, _4))
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
    }

    virtual void SetUp()
    {
        m_log.fill(0);

        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID);
        m_logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_MTU_LENGTH_OFFSET, (3 * m_srcBuffer.capacity()));
    }

    void insertDataFrame(std::int32_t activeTermId, std::int32_t offset)
    {
        int termBufferIndex = LogBufferDescriptor::indexByTerm(INITIAL_TERM_ID, activeTermId);
        AtomicBuffer& buffer = m_termBuffers[termBufferIndex];
        DataFrameHeader::DataFrameHeaderDefn& frame = buffer.overlayStruct<DataFrameHeader::DataFrameHeaderDefn>(offset);
        const index_t msgLength = static_cast<index_t>(DATA.size());

        frame.frameLength = DataFrameHeader::LENGTH + msgLength;
        frame.version = DataFrameHeader::CURRENT_VERSION;
        frame.flags = FrameDescriptor::UNFRAGMENTED;
        frame.type = DataFrameHeader::HDR_TYPE_DATA;
        frame.termOffset = offset;
        frame.sessionId = SESSION_ID;
        frame.streamId = STREAM_ID;
        frame.termId = activeTermId;
        buffer.putBytes(offset + DataFrameHeader::LENGTH, DATA.data(), msgLength);
    }

    void insertPaddingFrame(std::int32_t activeTermId, std::int32_t offset)
    {
        int termBufferIndex = LogBufferDescriptor::indexByTerm(INITIAL_TERM_ID, activeTermId);
        AtomicBuffer& buffer = m_termBuffers[termBufferIndex];
        DataFrameHeader::DataFrameHeaderDefn& frame = buffer.overlayStruct<DataFrameHeader::DataFrameHeaderDefn>(offset);

        frame.frameLength = TERM_LENGTH - offset;
        frame.version = DataFrameHeader::CURRENT_VERSION;
        frame.flags = FrameDescriptor::UNFRAGMENTED;
        frame.type = DataFrameHeader::HDR_TYPE_PAD;
        frame.termOffset = offset;
        frame.sessionId = SESSION_ID;
        frame.streamId = STREAM_ID;
        frame.termId = activeTermId;
    }

    inline util::index_t offsetOfFrame(std::int32_t index)
    {
        return static_cast<util::index_t>(index * ALIGNED_FRAME_LENGTH);
    }

protected:
    AERON_DECL_ALIGNED(term_buffer_t m_log, 16);
    AERON_DECL_ALIGNED(src_buffer_t m_src, 16);

    AtomicBuffer m_termBuffers[3];
    AtomicBuffer m_logMetaDataBuffer;
    AtomicBuffer m_srcBuffer;

    std::shared_ptr<LogBuffers> m_logBuffers;
    UnsafeBufferPosition m_subscriberPosition;

    MockFragmentHandler m_fragmentHandler;
    MockControlledFragmentHandler m_controlledFragmentHandler;
    fragment_handler_t m_handler;
    controlled_poll_fragment_handler_t m_controlledHandler;
};

TEST_F(ImageTest, shouldReportCorrectInitialTermId)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(image.initialTermId(), INITIAL_TERM_ID);
}

TEST_F(ImageTest, shouldReportCorrectTermBufferLength)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(image.termBufferLength(), TERM_LENGTH);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReception)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));

    EXPECT_CALL(m_fragmentHandler, onFragment(testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1);

    const int fragments = image.poll(m_handler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId)
{
    const std::int32_t messageIndex = 5;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));

    EXPECT_CALL(m_fragmentHandler, onFragment(testing::_, initialTermOffset + DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1);

    const int fragments = image.poll(m_handler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId)
{
    const std::int32_t activeTermId = INITIAL_TERM_ID + 1;
    const std::int32_t messageIndex = 5;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(activeTermId, offsetOfFrame(messageIndex));

    EXPECT_CALL(m_fragmentHandler, onFragment(testing::_, initialTermOffset + DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1);

    const int fragments = image.poll(m_handler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldEnsureImageIsOpenBeforeReadingPosition)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    image.close();
    EXPECT_EQ(image.position(), initialPosition);
}

TEST_F(ImageTest, shouldEnsureImageIsOpenBeforePoll)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    image.close();
    EXPECT_EQ(image.poll(m_handler, INT_MAX), 0);
}

TEST_F(ImageTest, shouldPollNoFragmentsToControlledFragmentHandler)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(testing::_, testing::_, testing::_, testing::_))
        .Times(0);

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 0);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);
}

TEST_F(ImageTest, shouldPollOneFragmentToControlledFragmentHandlerOnContinue)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldNotPollOneFragmentToControlledFragmentHandlerOnAbort)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::ABORT));

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 0);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);
}

TEST_F(ImageTest, shouldPollOneFragmentToControlledFragmentHandlerOnBreak)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::BREAK));

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldPollFragmentsToControlledFragmentHandlerOnCommit)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::COMMIT));
    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, ALIGNED_FRAME_LENGTH + DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::ABORT));

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH);
}

TEST_F(ImageTest, shouldPollFragmentsToControlledFragmentHandlerOnContinue)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));
    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, ALIGNED_FRAME_LENGTH + DataFrameHeader::LENGTH, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.controlledPoll(m_controlledHandler, INT_MAX);
    EXPECT_EQ(fragments, 2);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition + ALIGNED_FRAME_LENGTH * 2);
    EXPECT_EQ(image.position(), initialPosition + ALIGNED_FRAME_LENGTH * 2);
}

TEST_F(ImageTest, shouldPollNoFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeInitialPosition)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
    const std::int64_t maxPosition = initialPosition - DataFrameHeader::LENGTH;

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, testing::_, static_cast<index_t>(DATA.size()), testing::_))
        .Times(0);

    const int fragments = image.boundedControlledPoll(m_controlledHandler, maxPosition, INT_MAX);
    EXPECT_EQ(fragments, 0);
    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithInitialOffsetNotZero)
{
    const std::int32_t messageIndex = 1;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
    const std::int64_t maxPosition = initialPosition + ALIGNED_FRAME_LENGTH;

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, testing::_, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.boundedControlledPoll(m_controlledHandler, maxPosition, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), maxPosition);
    EXPECT_EQ(image.position(), maxPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeNextMessage)
{
    const std::int32_t messageIndex = 0;
    const std::int32_t initialTermOffset = offsetOfFrame(messageIndex);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
    const std::int64_t maxPosition = initialPosition + ALIGNED_FRAME_LENGTH;

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex));
    insertDataFrame(INITIAL_TERM_ID, offsetOfFrame(messageIndex + 1));

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, testing::_, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.boundedControlledPoll(m_controlledHandler, maxPosition, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), maxPosition);
    EXPECT_EQ(image.position(), maxPosition);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAfterEndOfTerm)
{
    const std::int32_t initialOffset = TERM_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
    const std::int64_t maxPosition = initialPosition + TERM_LENGTH;

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, initialOffset);
    insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, testing::_, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.boundedControlledPoll(m_controlledHandler, maxPosition, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), TERM_LENGTH);
    EXPECT_EQ(image.position(), TERM_LENGTH);
}

TEST_F(ImageTest, shouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAboveIntMaxValue)
{
    const std::int32_t initialOffset = TERM_LENGTH - (ALIGNED_FRAME_LENGTH * 2);
    const std::int64_t initialPosition =
        LogBufferDescriptor::computePosition(INITIAL_TERM_ID, initialOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
    const std::int64_t maxPosition = static_cast<std::int64_t>(INT32_MAX) + 1000;

    m_subscriberPosition.set(initialPosition);
    Image image(
        SESSION_ID, CORRELATION_ID, SUBSCRIPTION_REGISTRATION_ID,
        SOURCE_IDENTITY, m_subscriberPosition, m_logBuffers, exceptionHandler);

    EXPECT_EQ(m_subscriberPosition.get(), initialPosition);
    EXPECT_EQ(image.position(), initialPosition);

    insertDataFrame(INITIAL_TERM_ID, initialOffset);
    insertPaddingFrame(INITIAL_TERM_ID, initialOffset + ALIGNED_FRAME_LENGTH);

    EXPECT_CALL(m_controlledFragmentHandler, onFragment(
        testing::_, testing::_, static_cast<index_t>(DATA.size()), testing::_))
        .Times(1)
        .WillOnce(testing::Return(ControlledPollAction::CONTINUE));

    const int fragments = image.boundedControlledPoll(m_controlledHandler, maxPosition, INT_MAX);
    EXPECT_EQ(fragments, 1);
    EXPECT_EQ(m_subscriberPosition.get(), TERM_LENGTH);
    EXPECT_EQ(image.position(), TERM_LENGTH);
}
