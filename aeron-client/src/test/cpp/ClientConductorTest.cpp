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
#include "util/TestUtils.h"

using namespace aeron::test;

static const std::string CHANNEL = "aeron:udp?endpoint=localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID = 0;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID_2 = 1;
static const std::int32_t CHANNEL_STATUS_INDICATOR_ID = 2;
static const std::int32_t COUNTER_ID = 3;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int32_t PAGE_SIZE = LogBufferDescriptor::AERON_PAGE_MIN_SIZE;
static const std::int32_t COUNTER_TYPE_ID = 102;
static const std::int64_t LOG_FILE_LENGTH = (TERM_LENGTH * 3) + LogBufferDescriptor::LOG_META_DATA_LENGTH;
static const std::string SOURCE_IDENTITY = "127.0.0.1:43567";
static const std::string COUNTER_LABEL = "counter label";

#ifdef _MSC_VER
#define unlink _unlink
#endif

class ClientConductorTest : public testing::Test, public ClientConductorFixture
{
public:
    ClientConductorTest() :
        m_logFileName(makeTempFileName()),
        m_logFileName2(makeTempFileName())
    {
    }

    void SetUp() override
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
        MemoryMappedFile::ptr_t logbuffer1 = MemoryMappedFile::createNew(
            m_logFileName.c_str(), 0, static_cast<size_t>(LOG_FILE_LENGTH));
        MemoryMappedFile::ptr_t logbuffer2 = MemoryMappedFile::createNew(
            m_logFileName2.c_str(), 0, static_cast<size_t>(LOG_FILE_LENGTH));
        m_manyToOneRingBuffer.consumerHeartbeatTime(m_currentTime);

        AtomicBuffer logMetaDataBuffer;

        logMetaDataBuffer.wrap(
            logbuffer1->getMemoryPtr() + (LOG_FILE_LENGTH - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
        logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
        logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);

        logMetaDataBuffer.wrap(
            logbuffer2->getMemoryPtr() + (LOG_FILE_LENGTH - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
        logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
        logMetaDataBuffer.putInt32(LogBufferDescriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);
    }

    void TearDown() override
    {
        ::unlink(m_logFileName.c_str());
        ::unlink(m_logFileName2.c_str());
    }

protected:
    std::string m_logFileName;
    std::string m_logFileName2;
};

TEST_F(ClientConductorTest, shouldReturnNullForUnknownPublication)
{
    std::shared_ptr<Publication> pub = m_conductor.findPublication(100);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForPublicationWithoutLogBuffers)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddPublicationToDriver)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);
    static std::int32_t ADD_PUBLICATION = ControlProtocolEvents::ADD_PUBLICATION;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const PublicationMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, ADD_PUBLICATION);
            EXPECT_EQ(message.correlationId(), id);
            EXPECT_EQ(message.streamId(), STREAM_ID);
            EXPECT_EQ(message.channel(), CHANNEL);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnPublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    ASSERT_TRUE(pub != nullptr);
    EXPECT_EQ(pub->registrationId(), id);
    EXPECT_EQ(pub->channel(), CHANNEL);
    EXPECT_EQ(pub->streamId(), STREAM_ID);
    EXPECT_EQ(pub->sessionId(), SESSION_ID);
}

TEST_F(ClientConductorTest, shouldReleasePublicationAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer &, util::index_t, util::index_t)
        {
        });

    m_conductor.onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    {
        std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_PUBLICATION);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<Publication> pubPost = m_conductor.findPublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnSamePublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<Publication> pub1 = m_conductor.findPublication(id);
    std::shared_ptr<Publication> pub2 = m_conductor.findPublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}

TEST_F(ClientConductorTest, shouldIgnorePublicationReadyForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(
        id + 1,
        id + 1,
        STREAM_ID,
        SESSION_ID,
        PUBLICATION_LIMIT_COUNTER_ID,
        CHANNEL_STATUS_INDICATOR_ID,
        m_logFileName);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    ASSERT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddPublicationWithoutPublicationReady)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_currentTime += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Publication> pub = m_conductor.findPublication(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddPublication)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<Publication> pub = m_conductor.findPublication(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownExclusivePublication)
{
    std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(100);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForExclusivePublicationWithoutLogBuffers)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddExclusivePublicationToDriver)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);
    static std::int32_t ADD_EXCLUSIVE_PUBLICATION = ControlProtocolEvents::ADD_EXCLUSIVE_PUBLICATION;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const PublicationMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, ADD_EXCLUSIVE_PUBLICATION);
            EXPECT_EQ(message.correlationId(), id);
            EXPECT_EQ(message.streamId(), STREAM_ID);
            EXPECT_EQ(message.channel(), CHANNEL);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnExclusivePublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_conductor.onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);

    ASSERT_TRUE(pub != nullptr);
    EXPECT_EQ(pub->registrationId(), id);
    EXPECT_EQ(pub->channel(), CHANNEL);
    EXPECT_EQ(pub->streamId(), STREAM_ID);
    EXPECT_EQ(pub->sessionId(), SESSION_ID);
}

TEST_F(ClientConductorTest, shouldReleaseExclusivePublicationAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer &, util::index_t, util::index_t)
        {
        });

    m_conductor.onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    {
        std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_PUBLICATION);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<ExclusivePublication> pubPost = m_conductor.findExclusivePublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdForDuplicateAddExclusivePublication)
{
    std::int64_t id1 = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);
    std::int64_t id2 = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameExclusivePublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_conductor.onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<ExclusivePublication> pub1 = m_conductor.findExclusivePublication(id);
    std::shared_ptr<ExclusivePublication> pub2 = m_conductor.findExclusivePublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}

TEST_F(ClientConductorTest, shouldIgnoreExclusivePublicationReadyForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_conductor.onNewExclusivePublication(
        id + 1,
        id + 1,
        STREAM_ID,
        SESSION_ID,
        PUBLICATION_LIMIT_COUNTER_ID,
        CHANNEL_STATUS_INDICATOR_ID,
        m_logFileName);

    std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);

    ASSERT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddExclusivePublicationWithoutPublicationReady)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_currentTime += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddExclusivePublication)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_conductor.onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddSubscriptionAsWarning)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onErrorResponse(id, ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE, "resource unavailable");

    try
    {
        std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
        FAIL() << "Exception should be thrown";
    }
    catch (util::RegistrationException &e)
    {
        ASSERT_EQ(ExceptionCategory::EXCEPTION_CATEGORY_WARN, e.category());
    }
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownSubscription)
{
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(100);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForSubscriptionWithoutOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddSubscriptionToDriver)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    static std::int32_t ADD_SUBSCRIPTION = ControlProtocolEvents::ADD_SUBSCRIPTION;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const SubscriptionMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, ADD_SUBSCRIPTION);
            EXPECT_EQ(message.correlationId(), id);
            EXPECT_EQ(message.streamId(), STREAM_ID);
            EXPECT_EQ(message.channel(), CHANNEL);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnSubscriptionAfterOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);
    EXPECT_EQ(sub->registrationId(), id);
    EXPECT_EQ(sub->channel(), CHANNEL);
    EXPECT_EQ(sub->streamId(), STREAM_ID);
}

TEST_F(ClientConductorTest, shouldReleaseSubscriptionAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    static std::int32_t REMOVE_SUBSCRIPTION = ControlProtocolEvents::REMOVE_SUBSCRIPTION;

    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer &, util::index_t, util::index_t)
        {
        });

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    {
        std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

        ASSERT_TRUE(sub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_SUBSCRIPTION);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<Subscription> subPost = m_conductor.findSubscription(id);
    ASSERT_TRUE(subPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdsForDuplicateAddSubscription)
{
    std::int64_t id1 = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t id2 = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameFindSubscriptionAfterOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub1 = m_conductor.findSubscription(id);
    std::shared_ptr<Subscription> sub2 = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 == sub2);
}

TEST_F(ClientConductorTest, shouldReturnDifferentSubscriptionAfterOperationSuccess)
{
    std::int64_t id1 = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t id2 = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onSubscriptionReady(id1, CHANNEL_STATUS_INDICATOR_ID);
    m_conductor.onSubscriptionReady(id2, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub1 = m_conductor.findSubscription(id1);
    std::shared_ptr<Subscription> sub2 = m_conductor.findSubscription(id2);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 != sub2);
}

TEST_F(ClientConductorTest, shouldIgnoreOperationSuccessForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onSubscriptionReady(id + 1, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddSubscriptionWithoutOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_currentTime += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddSubscription)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldCallErrorHandlerWhenInterServiceTimeoutExceeded)
{
    bool called = false;

    m_errorHandler =
        [&](const std::exception &exception)
        {
            EXPECT_EQ(typeid(ConductorServiceTimeoutException), typeid(exception));
            called = true;
        };

    m_currentTime += INTER_SERVICE_TIMEOUT_MS + 1;
    m_conductor.doWork();
    EXPECT_TRUE(called);
}

TEST_F(ClientConductorTest, shouldCallErrorHandlerWhenDriverInactiveOnIdle)
{
    bool called = false;

    m_errorHandler =
        [&](const std::exception &exception)
        {
            EXPECT_EQ(typeid(DriverTimeoutException), typeid(exception));
            called = true;
        };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);
}

TEST_F(ClientConductorTest, shouldExceptionWhenAddPublicationAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception &exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
        {
            m_conductor.addPublication(CHANNEL, STREAM_ID);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleasePublicationAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception &exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_NO_THROW(
        {
            m_conductor.releasePublication(100);
        });
}

TEST_F(ClientConductorTest, shouldExceptionWhenAddSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception &exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
        {
            m_conductor.addSubscription(CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleaseSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception &exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_NO_THROW(
        {
            m_conductor.releaseSubscription(100, nullptr, 0);
        });
}

TEST_F(ClientConductorTest, shouldCallOnNewPubAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    EXPECT_CALL(m_handlers, onNewPub(testing::StrEq(CHANNEL), STREAM_ID, SESSION_ID, id))
        .Times(1);

    m_conductor.onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);
}

TEST_F(ClientConductorTest, shouldCallOnNewSubAfterOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    EXPECT_CALL(m_handlers, onNewSub(testing::StrEq(CHANNEL), STREAM_ID, id))
        .Times(1);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
}

TEST_F(ClientConductorTest, shouldCallNewConnectionAfterOnNewConnection)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_TRUE(sub->hasImage(correlationId));
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfNoOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfUninterestingRegistrationId)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id + 1, m_logFileName, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_FALSE(sub->hasImage(correlationId));
}

TEST_F(ClientConductorTest, shouldCallInactiveConnectionAfterInactiveConnection)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(1)
        .InSequence(sequence);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);
    m_conductor.onUnavailableImage(correlationId, id);
    EXPECT_FALSE(sub->hasImage(correlationId));
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnectionIfNoOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);
    m_conductor.onUnavailableImage(correlationId, id);
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnectionIfUninterestingConnectionCorrelationId)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);
    m_conductor.onUnavailableImage(correlationId + 1, id);
    EXPECT_TRUE(sub->hasImage(correlationId));

    testing::Mock::VerifyAndClearExpectations(&m_handlers);  // avoid catching unavailable call on sub release
}

TEST_F(ClientConductorTest, shouldCallUnavailableImageIfSubscriptionReleased)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(1);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);
    EXPECT_TRUE(sub->hasImage(correlationId));
}

TEST_F(ClientConductorTest, shouldClosePublicationOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    ASSERT_TRUE(pub != nullptr);

    m_conductor.closeAllResources(m_currentTime);
    EXPECT_TRUE(pub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseExclusivePublicationOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);

    m_conductor.onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);

    std::shared_ptr<ExclusivePublication> pub = m_conductor.findExclusivePublication(id);

    ASSERT_TRUE(pub != nullptr);

    m_conductor.closeAllResources(m_currentTime);
    EXPECT_TRUE(pub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseSubscriptionOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    m_conductor.closeAllResources(m_currentTime);

    EXPECT_TRUE(sub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseAllPublicationsAndSubscriptionsOnInterServiceTimeout)
{
    std::int64_t pubId = m_conductor.addPublication(CHANNEL, STREAM_ID);
    std::int64_t exPubId = m_conductor.addExclusivePublication(CHANNEL, STREAM_ID);
    std::int64_t subId = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    m_conductor.onNewPublication(
        pubId, pubId, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, m_logFileName);
    m_conductor.onNewExclusivePublication(
        exPubId,
        exPubId,
        STREAM_ID,
        SESSION_ID,
        PUBLICATION_LIMIT_COUNTER_ID_2,
        CHANNEL_STATUS_INDICATOR_ID,
        m_logFileName2);
    m_conductor.onSubscriptionReady(subId, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(pubId);

    ASSERT_TRUE(pub != nullptr);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(subId);

    ASSERT_TRUE(sub != nullptr);

    std::shared_ptr<ExclusivePublication> exPub = m_conductor.findExclusivePublication(exPubId);

    ASSERT_TRUE(exPub != nullptr);

    m_conductor.closeAllResources(m_currentTime);
    EXPECT_TRUE(pub->isClosed());
    EXPECT_TRUE(sub->isClosed());
    EXPECT_TRUE(exPub->isClosed());
}

TEST_F(ClientConductorTest, shouldRemoveImageOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    std::int64_t correlationId = id + 1;

    m_conductor.onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    m_conductor.onAvailableImage(correlationId, SESSION_ID, 1, id, m_logFileName, SOURCE_IDENTITY);
    ASSERT_TRUE(sub->hasImage(correlationId));

    m_conductor.closeAllResources(m_currentTime);

    std::shared_ptr<Image> image = sub->imageBySessionId(SESSION_ID);

    EXPECT_TRUE(sub->isClosed());
    EXPECT_TRUE(image == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownCounter)
{
    std::shared_ptr<Counter> counter = m_conductor.findCounter(100);

    EXPECT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForCounterWithoutOnAvailableCounter)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    std::shared_ptr<Counter> counter = m_conductor.findCounter(id);

    EXPECT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddCounterToDriver)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    static std::int32_t ADD_COUNTER = ControlProtocolEvents::ADD_COUNTER;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const CounterMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, ADD_COUNTER);
            EXPECT_EQ(message.correlationId(), id);
            EXPECT_EQ(message.typeId(), COUNTER_TYPE_ID);
            EXPECT_EQ(message.keyLength(), 0);
            EXPECT_EQ(message.label(), COUNTER_LABEL);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnCounterAfterOnAvailableCounter)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id, COUNTER_ID))
        .Times(1);

    m_conductor.onAvailableCounter(id, COUNTER_ID);

    std::shared_ptr<Counter> counter = m_conductor.findCounter(id);

    ASSERT_TRUE(counter != nullptr);
    EXPECT_EQ(counter->registrationId(), id);
    EXPECT_EQ(counter->id(), COUNTER_ID);
}

TEST_F(ClientConductorTest, shouldReleaseCounterAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    static std::int32_t REMOVE_COUNTER = ControlProtocolEvents::REMOVE_COUNTER;

    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer &, util::index_t, util::index_t)
        {
        });

    m_conductor.onAvailableCounter(id, COUNTER_ID);

    {
        std::shared_ptr<Counter> counter = m_conductor.findCounter(id);

        ASSERT_TRUE(counter != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_COUNTER);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<Counter> counterPost = m_conductor.findCounter(id);
    ASSERT_TRUE(counterPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdsForDuplicateAddCounter)
{
    std::int64_t id1 = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    std::int64_t id2 = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameFindCounterAfterOnAvailableCounter)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    m_conductor.onAvailableCounter(id, COUNTER_ID);

    std::shared_ptr<Counter> counter1 = m_conductor.findCounter(id);
    std::shared_ptr<Counter> counter2 = m_conductor.findCounter(id);

    ASSERT_TRUE(counter1 != nullptr);
    ASSERT_TRUE(counter2 != nullptr);
    ASSERT_TRUE(counter1 == counter2);
}

TEST_F(ClientConductorTest, shouldReturnDifferentCounterAfterOnAvailableCounter)
{
    std::int64_t id1 = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    std::int64_t id2 = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id1, COUNTER_ID))
        .Times(1);
    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id2, COUNTER_ID))
        .Times(1);

    m_conductor.onAvailableCounter(id1, COUNTER_ID);
    m_conductor.onAvailableCounter(id2, COUNTER_ID);

    std::shared_ptr<Counter> counter1 = m_conductor.findCounter(id1);
    std::shared_ptr<Counter> counter2 = m_conductor.findCounter(id2);

    ASSERT_TRUE(counter1 != nullptr);
    ASSERT_TRUE(counter2 != nullptr);
    ASSERT_TRUE(counter1 != counter2);
}

TEST_F(ClientConductorTest, shouldNotFindCounterOnAvailableCounterForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id + 1, COUNTER_ID))
        .Times(1);

    m_conductor.onAvailableCounter(id + 1, COUNTER_ID);

    std::shared_ptr<Counter> counter = m_conductor.findCounter(id);

    ASSERT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddCounterWithoutOnAvailableCounter)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    m_currentTime += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Counter> counter = m_conductor.findCounter(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddCounter)
{
    std::int64_t id = m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    m_conductor.onErrorResponse(id, ERROR_CODE_GENERIC_ERROR, "can't add counter");

    ASSERT_THROW(
        {
            std::shared_ptr<Counter> counter = m_conductor.findCounter(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldCallOnUnavailableCounter)
{
    std::int64_t id = 101;

    EXPECT_CALL(m_handlers, onUnavailableCounter(testing::_, id, COUNTER_ID))
        .Times(1);

    m_conductor.onUnavailableCounter(id, COUNTER_ID);
}

TEST_F(ClientConductorTest, shouldThrowExceptionOnReentrantCallback)
{
    m_conductor.addAvailableCounterHandler(
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            m_conductor.addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
        });

    std::int64_t id = 101;
    std::int32_t counterId = 7;
    bool called = false;
    m_errorHandler = [&](const std::exception &exception) { called = true; };

    m_conductor.onAvailableCounter(id, counterId);

    EXPECT_TRUE(called);
}
