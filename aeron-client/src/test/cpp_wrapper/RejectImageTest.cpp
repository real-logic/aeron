/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "TestUtil.h"
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace aeron;

class RejectImageTest : public testing::TestWithParam<std::string>
{
public:
    RejectImageTest()
    {
        m_driver.start();
    }

    ~RejectImageTest() override
    {
        m_driver.stop();
    }

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

    std::string addressAsString(int16_t addressType, uint8_t *address)
    {
        if (AERON_RESPONSE_ADDRESS_TYPE_IPV4 == addressType)
        {
            char buffer[INET_ADDRSTRLEN] = {};
            inet_ntop(AF_INET, address, buffer, sizeof(buffer));
            return std::string{buffer};
        }
        else if (AERON_RESPONSE_ADDRESS_TYPE_IPV6 == addressType)
        {
            char buffer[INET6_ADDRSTRLEN] = {};
            inet_ntop(AF_INET6, address, buffer, sizeof(buffer));
            return "[" + std::string{buffer} + "]";
        }

        return "";
    }

protected:
    EmbeddedMediaDriver m_driver;
};

INSTANTIATE_TEST_SUITE_P(
    RejectImageTestWithParam, RejectImageTest, testing::Values("127.0.0.1", "[::1]"));

TEST_P(RejectImageTest, shouldRejectImage)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::string address = GetParam();
    std::uint16_t port = 10000;
    std::string control = address + ":" + std::to_string(10001);
    std::string endpoint = address + ":" + std::to_string(port);

    aeron::status::PublicationErrorFrame error{ nullptr };

    on_publication_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            error = errorFrame;
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t groupTag = 99999;
    std::string mdc = "aeron:udp?control-mode=dynamic|control=" + control + "|fc=tagged,g:" + std::to_string(groupTag);
    std::string channel = "aeron:udp?endpoint=" + endpoint + "|control=" + control + "|gtag=" + std::to_string(groupTag);

    std::int64_t pubId = aeron->addPublication(mdc, 10000);
    std::int64_t subId = aeron->addSubscription(channel, 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    const uint8_t *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);

    POLL_FOR(1 == sub->imageCount(), invoker);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    POLL_FOR(error.isValid(), invoker);
    ASSERT_EQ(pubId, error.registrationId());
    ASSERT_EQ(pub->sessionId(), error.sessionId());
    ASSERT_EQ(pub->streamId(), error.streamId());
    ASSERT_EQ(groupTag, error.groupTag());
    ASSERT_EQ(port, error.sourcePort());
    ASSERT_EQ(address, addressAsString(error.sourceAddressType(), error.sourceAddress()));
}

TEST_P(RejectImageTest, shouldRejectImageForExclusive)
{
    std::string address = GetParam();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::atomic<std::int32_t> errorFrameCount{0};

    on_publication_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            std::atomic_fetch_add(&errorFrameCount, 1);
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addExclusivePublication("aeron:udp?endpoint=" + address + ":10000", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:udp?endpoint=" + address + ":10000", 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    const uint8_t *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);

    POLL_FOR(1 == sub->imageCount(), invoker);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    POLL_FOR(0 < errorFrameCount, invoker);
}
