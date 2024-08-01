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

using namespace aeron;

class WrapperSystemTest : public testing::Test
{
public:
    WrapperSystemTest()
    {
        m_driver.start();
    }

    ~WrapperSystemTest() override
    {
        m_driver.stop();
    }

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(WrapperSystemTest, shouldSendReceiveDataWithRawPointer)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addPublication("aeron:ipc", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:ipc", 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    auto *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);
}

TEST_F(WrapperSystemTest, shouldSendReceiveDataWithRawPointerExclusive)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addExclusivePublication("aeron:ipc", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:ipc", 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    auto *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);
}

TEST_F(WrapperSystemTest, shouldRejectClientNameThatIsTooLong)
{
    std::string name =
        "this is a very long value that we are hoping with be reject when the value gets "
        "set on the the context without causing issues will labels";

    try
    {
        Context ctx;
        ctx.useConductorAgentInvoker(true);
        ctx.clientName(name);

        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        FAIL();
    }
    catch (IllegalArgumentException &ex)
    {
        const char *string = strstr(ex.what(), "client_name length must <= 100");
        ASSERT_NE(nullptr, string) << ex.what();
    }
}

TEST_F(WrapperSystemTest, shouldRejectImage)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::atomic<std::int32_t> errorFrameCount{0};

    on_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            std::atomic_fetch_add(&errorFrameCount, 1);
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addPublication("aeron:udp?endpoint=localhost:10000", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:udp?endpoint=localhost:10000", 10000);
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

    POLL_FOR(0 < errorFrameCount, invoker);
}

//TEST_F(WrapperSystemTest, shouldRejectImageForExclusive)
//{
//    Context ctx;
//    ctx.useConductorAgentInvoker(true);
//
//    std::atomic<std::int32_t> errorFrameCount{0};
//
//    on_error_frame_t errorFrameHandler =
//        [&](aeron::status::PublicationErrorFrame &errorFrame)
//        {
//            std::atomic_fetch_add(&errorFrameCount, 1);
//            return;
//        };
//
//    ctx.errorFrameHandler(errorFrameHandler);
//    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
//    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
//    invoker.start();
//
//    std::int64_t pubId = aeron->addExclusivePublication("aeron:udp?endpoint=localhost:10000", 10000);
//    std::int64_t subId = aeron->addSubscription("aeron:udp?endpoint=localhost:10000", 10000);
//    invoker.invoke();
//
//    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
//    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
//    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);
//
//    std::string message = "Hello World!";
//
//    const uint8_t *data = reinterpret_cast<const uint8_t *>(message.c_str());
//    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
//    POLL_FOR(0 < sub->poll(
//        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
//        {
//            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
//        },
//        1), invoker);
//
//    POLL_FOR(1 == sub->imageCount(), invoker);
//
//    const std::shared_ptr<Image> image = sub->imageByIndex(0);
//    image->reject("No Longer Valid");
//
//    POLL_FOR(0 < errorFrameCount, invoker);
//}
