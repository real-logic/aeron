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
#include "util/TestUtils.h"

using namespace aeron::test;

static const std::string CHANNEL = "udp://localhost:40123";
static const std::int32_t STREAM_ID = 10;
static const std::int32_t SESSION_ID = 200;
static const std::int32_t PUBLICATION_LIMIT_COUNTER_ID = 0;
static const std::int32_t TERM_LENGTH = LogBufferDescriptor::TERM_MIN_LENGTH;
static const std::int64_t LOG_FILE_LENGTH = LogBufferDescriptor::computeLogLength(TERM_LENGTH);
static const std::string SOURCE_IDENTITY = "127.0.0.1:43567";

class ClientConductorTest : public testing::Test, public ClientConductorFixture
{
public:
    ClientConductorTest() :
        m_logFileName(makeTempFileName())
    {
    }

    virtual void SetUp()
    {
        m_toDriver.fill(0);
        m_toClients.fill(0);
        MemoryMappedFile::createNew(m_logFileName.c_str(), LOG_FILE_LENGTH);
    }

    virtual void TearDown()
    {
        ::unlink(m_logFileName.c_str());
    }

protected:
    std::string m_logFileName;
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
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
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

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id);

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

    // drain ring buffer
    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id);

    {
        std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            EXPECT_EQ(msgTypeId, REMOVE_PUBLICATION);
            EXPECT_EQ(message.registrationId(), id);
        });

    EXPECT_EQ(count, 1);

    std::shared_ptr<Publication> pubPost = m_conductor.findPublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnSameIdForDuplicateAddPublication)
{
    std::int64_t id1 = m_conductor.addPublication(CHANNEL, STREAM_ID);
    std::int64_t id2 = m_conductor.addPublication(CHANNEL, STREAM_ID);

    EXPECT_EQ(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSamePublicationAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id);

    std::shared_ptr<Publication> pub1 = m_conductor.findPublication(id);
    std::shared_ptr<Publication> pub2 = m_conductor.findPublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}

TEST_F(ClientConductorTest, shouldIgnorePublicationReadyForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id + 1);

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
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddPublication)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
    {
        std::shared_ptr<Publication> pub = m_conductor.findPublication(id);
    }, util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownSubscription)
{
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(100);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForSubscriptionWithoutOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddSubscriptionToDriver)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    static std::int32_t ADD_SUBSCRIPTION = ControlProtocolEvents::ADD_SUBSCRIPTION;

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
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
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);
    EXPECT_EQ(sub->registrationId(), id);
    EXPECT_EQ(sub->channel(), CHANNEL);
    EXPECT_EQ(sub->streamId(), STREAM_ID);
}

TEST_F(ClientConductorTest, shouldReleaseSubscriptionAfterGoingOutOfScope)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    static std::int32_t REMOVE_SUBSCRIPTION = ControlProtocolEvents::REMOVE_SUBSCRIPTION;

    // drain ring buffer
    m_manyToOneRingBuffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    m_conductor.onOperationSuccess(id);

    {
        std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

        ASSERT_TRUE(sub != nullptr);
    }

    int count = m_manyToOneRingBuffer.read(
        [&](std::int32_t msgTypeId, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
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
    std::int64_t id1 = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    std::int64_t id2 = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameFindSubscriptionAfterOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id);

    std::shared_ptr<Subscription> sub1 = m_conductor.findSubscription(id);
    std::shared_ptr<Subscription> sub2 = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 == sub2);
}

TEST_F(ClientConductorTest, shouldReturnDifferentSubscriptionAfterOperationSuccess)
{
    std::int64_t id1 = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    std::int64_t id2 = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id1);
    m_conductor.onOperationSuccess(id2);

    std::shared_ptr<Subscription> sub1 = m_conductor.findSubscription(id1);
    std::shared_ptr<Subscription> sub2 = m_conductor.findSubscription(id2);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 != sub2);
}

TEST_F(ClientConductorTest, shouldIgnoreOperationSuccessForUnknownCorrelationId)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id + 1);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddSubscriptionWithoutOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_currentTime += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
    {
        std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddSubscription)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
    {
        std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    }, util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldCallErrorHandlerWhenInterServiceTimeoutExceeded)
{
    bool called = false;

    m_errorHandler =
        [&](std::exception& exception)
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
        [&](std::exception& exception)
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
    m_errorHandler = [&](std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
    {
        m_conductor.addPublication(CHANNEL, STREAM_ID);
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleasePublicationAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
    {
        m_conductor.releasePublication(100);
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenAddSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
    {
        m_conductor.addSubscription(CHANNEL, STREAM_ID);
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleaseSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
    {
        m_conductor.releaseSubscription(100, nullptr, 0);
    }, util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldCallOnNewPubAfterLogBuffersCreated)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    EXPECT_CALL(m_handlers, onNewPub(testing::StrEq(CHANNEL), STREAM_ID, SESSION_ID, id))
        .Times(1);

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id);
}

TEST_F(ClientConductorTest, shouldCallOnNewSubAfterOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    EXPECT_CALL(m_handlers, onNewSub(testing::StrEq(CHANNEL), STREAM_ID, id))
        .Times(1);

    m_conductor.onOperationSuccess(id);
}

TEST_F(ClientConductorTest, shouldCallNewConnectionAfterOnNewConnection)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    m_conductor.onOperationSuccess(id);
    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_TRUE(sub->hasImage(SESSION_ID));
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfNoOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfUninterestingRegistrationId)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id + 1 } };

    m_conductor.onOperationSuccess(id);
    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_FALSE(sub->hasImage(SESSION_ID));
}

TEST_F(ClientConductorTest, shouldCallInactiveConnecitonAfterInactiveConnection)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    std::int64_t connectionId = id + 1;
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

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    m_conductor.onOperationSuccess(id);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    m_conductor.onAvailableImage(
        STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, connectionId);
    m_conductor.onUnavailableImage(STREAM_ID, connectionId);
    EXPECT_FALSE(sub->hasImage(SESSION_ID));
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnecitonIfNoOperationSuccess)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    std::int64_t connectionId = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    // must be able to handle newImage even if findSubscription not called
    m_conductor.onAvailableImage(
        STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, connectionId);
    m_conductor.onUnavailableImage(STREAM_ID, connectionId);
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnecitonIfUinterestingConnectionCorrelationId)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);
    std::int64_t connectionId = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    m_conductor.onOperationSuccess(id);
    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);
    m_conductor.onAvailableImage(
        STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, connectionId);
    m_conductor.onUnavailableImage(STREAM_ID, connectionId + 1);
    EXPECT_TRUE(sub->hasImage(SESSION_ID));
}

TEST_F(ClientConductorTest, shouldClosePublicationOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addPublication(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, id);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(id);

    ASSERT_TRUE(pub != nullptr);

    m_conductor.onInterServiceTimeout(m_currentTime);
    EXPECT_TRUE(pub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseSubscriptionOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    m_conductor.onInterServiceTimeout(m_currentTime);

    EXPECT_TRUE(sub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseAllPublicationsAndSubscriptionsOnInterServiceTimeout)
{
    std::int64_t pubId = m_conductor.addPublication(CHANNEL, STREAM_ID);
    std::int64_t subId = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onNewPublication(STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, m_logFileName, pubId);
    m_conductor.onOperationSuccess(subId);

    std::shared_ptr<Publication> pub = m_conductor.findPublication(pubId);

    ASSERT_TRUE(pub != nullptr);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(subId);

    ASSERT_TRUE(sub != nullptr);

    m_conductor.onInterServiceTimeout(m_currentTime);
    EXPECT_TRUE(pub->isClosed());
    EXPECT_TRUE(sub->isClosed());
}

TEST_F(ClientConductorTest, shouldRemoveImageOnInterServiceTimeout)
{
    std::int64_t id = m_conductor.addSubscription(CHANNEL, STREAM_ID);

    m_conductor.onOperationSuccess(id);

    std::shared_ptr<Subscription> sub = m_conductor.findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    ImageBuffersReadyDefn::SubscriberPosition positions[] = { { 1, id } };

    m_conductor.onAvailableImage(STREAM_ID, SESSION_ID, m_logFileName, SOURCE_IDENTITY, 1, positions, id);
    ASSERT_TRUE(sub->hasImage(SESSION_ID));

    m_conductor.onInterServiceTimeout(m_currentTime);

    std::shared_ptr<Image> image = sub->getImage(SESSION_ID);

    EXPECT_TRUE(sub->isClosed());
    EXPECT_TRUE(image == nullptr);
}
