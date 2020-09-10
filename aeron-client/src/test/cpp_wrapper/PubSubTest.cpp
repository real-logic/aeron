
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

#include <functional>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "ChannelUriStringBuilder.h"
#include "TestUtil.h"

using namespace aeron;
using testing::MockFunction;
using testing::_;

class PubSubTest : public testing::TestWithParam<std::tuple<const char *, const char *>>
{
public:
    PubSubTest()
    {
        m_driver.start();
    }

    ~PubSubTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

typedef std::array<std::uint8_t, 1024> buffer_t;
static const int dataHeaderLength = 32;

INSTANTIATE_TEST_SUITE_P(
    PubSubTest,
    PubSubTest,
    testing::Values(
        std::make_tuple("udp", "localhost:24325"),
        std::make_tuple("ipc", nullptr)
    ));

TEST_P(PubSubTest, shouldSubscribePublishAndReceiveContextCallbacks)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    std::int32_t sessionId = 908712342;
    std::int32_t imageUnavailable = 0;

    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .sessionId(sessionId)
        .build();

    std::int64_t reservedValue = INT64_C(78923648723465);
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    MockFunction<void(
        const std::string &channel,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t correlationId)> mockOnNewPublication;

    MockFunction<void(
        const std::string &channel,
        std::int32_t streamId,
        std::int64_t correlationId)> mockOnNewSubscription;

    MockFunction<void(Image &image)> mockOnAvailableImage;
    MockFunction<void(Image &image)> mockOnUnavailableImage;
    MockFunction<void()> mockClientClose;

    EXPECT_CALL(mockOnNewPublication, Call(channel, streamId, sessionId, _));
    EXPECT_CALL(mockOnNewSubscription, Call(channel, streamId, _));
    EXPECT_CALL(mockOnAvailableImage, Call(_));
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillOnce(
        [&](Image &image)
        {
            aeron::concurrent::atomic::putInt32Volatile(&imageUnavailable, 1);
        });;
    EXPECT_CALL(mockClientClose, Call());

    ctx.newPublicationHandler(mockOnNewPublication.AsStdFunction());
    ctx.newSubscriptionHandler(mockOnNewSubscription.AsStdFunction());
    ctx.availableImageHandler(mockOnAvailableImage.AsStdFunction());
    ctx.unavailableImageHandler(mockOnUnavailableImage.AsStdFunction());
    ctx.closeClientHandler(mockClientClose.AsStdFunction());

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        {
            POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
            POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

            on_reserved_value_supplier_t reservedValueSupplier = [=](
                AtomicBuffer &termBuffer,
                util::index_t termOffset,
                util::index_t length)
            {
                return reservedValue;
            };

            std::string message = "hello world!";
            int32_t length = buffer.putString(0, message);
            const std::int64_t expectedPosition = util::BitUtil::align(
                dataHeaderLength + length, FrameDescriptor::FRAME_ALIGNMENT);

            POLL_FOR(0 < pub->offer(buffer, 0, length, reservedValueSupplier), invoker);
            POLL_FOR(0 < sub->poll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    EXPECT_EQ(message, buffer.getString(offset));
                    EXPECT_EQ(reservedValue, header.reservedValue());
                    EXPECT_EQ(sessionId, header.sessionId());
                    EXPECT_EQ(streamId, header.streamId());
                    EXPECT_EQ(length, header.frameLength() - dataHeaderLength);
                    EXPECT_EQ(expectedPosition, header.position());
                }, 1), invoker);
        }

        POLL_FOR(1 == aeron::concurrent::atomic::getInt32Volatile(&imageUnavailable), invoker);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldSubscribePublishAndReceiveSubscriptionCallbacks)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder).build();
    std::int32_t imageUnavailable = 0;

    Context ctx;

    MockFunction<void(Image &image)> mockOnAvailableImage;
    MockFunction<void(Image &image)> mockOnUnavailableImage;

    EXPECT_CALL(mockOnAvailableImage, Call(_));
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillOnce(
        [&](Image &image)
        {
            aeron::concurrent::atomic::putInt32Volatile(&imageUnavailable, 1);
        });

    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(
        channel, streamId, mockOnAvailableImage.AsStdFunction(), mockOnUnavailableImage.AsStdFunction());
    std::int64_t pubId = aeron->addPublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        // Nest to trigger subscription cleanup
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        {
            // Nest to trigger images becoming unavailable
            POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
            POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

            std::string message = "hello world!";
            buffer.putString(0, message);
            POLL_FOR(0 < pub->offer(buffer), invoker);

            POLL_FOR(
                0 < sub->poll(
                    [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                    {
                        EXPECT_EQ(message, buffer.getString(offset));
                    }, 1),
                invoker);
        }

        POLL_FOR(1 == aeron::concurrent::atomic::getInt32Volatile(&imageUnavailable), invoker);
    }

    // Allow callbacks to fire to complete cleanup and prevent sanitizer errors.
    invoker.invoke();
}

TEST_P(PubSubTest, shouldSubscribeExclusivePublish)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    std::int32_t termId = 23;
    std::int32_t initialTermId = 3;
    std::int32_t termOffset = 1024;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .termId(termId)
        .termOffset(termOffset)
        .initialTermId(initialTermId)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub->offer(buffer), invoker);

        POLL_FOR(0 < sub->poll(
            [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
            {
                EXPECT_EQ(message, buffer.getString(offset));
                EXPECT_EQ(termId, header.termId());
                EXPECT_EQ(termOffset, header.termOffset());
                EXPECT_EQ(initialTermId, header.initialTermId());
            }, 1), invoker);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldBlockPollSubscription)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder).build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        AtomicBuffer buffers[]{buffer, buffer, buffer};
        POLL_FOR(0 < pub->offer(buffers, 3), invoker);

        std::int64_t bytesReceived = 0;
        std::int64_t bytesConsumed = 0;
        POLL_FOR(pub->position() <= (
            bytesConsumed += sub->blockPoll(
                [&](
                    concurrent::AtomicBuffer &buffer,
                    util::index_t offset,
                    util::index_t length,
                    std::int32_t sessionId,
                    std::int32_t termId)
                {
                    bytesReceived += length;
                    EXPECT_EQ(pub->sessionId(), sessionId);
                }, 100000)), invoker);

        EXPECT_EQ(pub->position(), bytesConsumed);
        EXPECT_EQ(pub->position(), bytesReceived);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldTryClaimAndControlledPollSubscription)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder).build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";

        BufferClaim claim;
        POLL_FOR(0 < pub->tryClaim(16, claim), invoker);
        claim.buffer().putString(claim.offset(), message);
        claim.commit();

        POLL_FOR(
            0 < sub->controlledPoll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    return ControlledPollAction::COMMIT;
                }, 1),
            invoker);
    }

    invoker.invoke();
}

