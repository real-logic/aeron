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

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "ChannelUriStringBuilder.h"
#include "TestUtil.h"

using namespace aeron;

class ImageTest : public testing::TestWithParam<std::tuple<const char *, const char *>>
{
public:
    ImageTest()
    {
        m_driver.start();
    }

    ~ImageTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

typedef std::array<std::uint8_t, 1024> buffer_t;

INSTANTIATE_TEST_SUITE_P(
    ImageTest,
    ImageTest,
    testing::Values(
        std::make_tuple("udp", "localhost:24325"),
        std::make_tuple("ipc", nullptr)
    ));

TEST_P(ImageTest, shouldGetMultipleImages)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub1->offer(buffer), invoker);
        POLL_FOR(0 < pub2->offer(buffer), invoker);

        POLL_FOR(2 == sub->imageCount(), invoker);

        EXPECT_NE(nullptr, sub->imageBySessionId(pub1->sessionId()));
        EXPECT_NE(nullptr, sub->imageBySessionId(pub2->sessionId()));
        EXPECT_NE(nullptr, sub->imageByIndex(0));
        EXPECT_NE(nullptr, sub->imageByIndex(1));
        EXPECT_EQ(2U, sub->copyOfImageList()->size());
    }

    invoker.invoke();
}
