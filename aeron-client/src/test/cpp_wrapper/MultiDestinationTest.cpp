
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

#define PUB_MDC_MANUAL_URI "aeron:udp?control-mode=manual|tags=3,4"
#define SUB1_MDC_MANUAL_URI "aeron:udp?endpoint=localhost:24326|group=true"
#define SUB2_MDC_MANUAL_URI "aeron:udp?endpoint=localhost:24327|group=true"

#define UNICAST_ENDPOINT_A "localhost:24325"
#define UNICAST_ENDPOINT_B "localhost:24326"
#define SUB_URI "aeron:udp?control-mode=manual"

class MultiDestinationTest : public testing::TestWithParam<std::tuple<const char *, const char *>>
{
public:
    MultiDestinationTest()
    {
        m_driver.start();
    }

    ~MultiDestinationTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
    fragment_handler_t m_noOpHandler =
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header) {};

};

typedef std::array<std::uint8_t, 1024> buffer_t;

TEST_F(MultiDestinationTest, shouldAddRemoveDestinationFromPublication)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    auto &invoker = aeron->conductorAgentInvoker();
    std::int32_t streamId = 1001;

    {
        auto sub1RegId = aeron->addSubscription(SUB1_MDC_MANUAL_URI, streamId);
        auto sub2RegId = aeron->addSubscription(SUB2_MDC_MANUAL_URI, streamId);
        auto pubRegId = aeron->addPublication(PUB_MDC_MANUAL_URI, streamId);

        POLL_FOR_NON_NULL(sub1, aeron->findSubscription(sub1RegId), invoker);
        POLL_FOR_NON_NULL(sub2, aeron->findSubscription(sub2RegId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubRegId), invoker);

        int64_t dest1CorrelationId = pub->addDestination(SUB1_MDC_MANUAL_URI);
        int64_t dest2CorrelationId = pub->addDestination(SUB2_MDC_MANUAL_URI);

        POLL_FOR(pub->findDestinationResponse(dest1CorrelationId), invoker);
        POLL_FOR(pub->findDestinationResponse(dest2CorrelationId), invoker);

        POLL_FOR(sub1->isConnected(), invoker);
        POLL_FOR(sub2->isConnected(), invoker);

        POLL_FOR(0 < pub->offer(buffer, 0, 128), invoker);

        POLL_FOR(0 < sub1->poll(m_noOpHandler, 1), invoker);

        POLL_FOR(0 < sub2->poll(m_noOpHandler, 1), invoker);

        int64_t removeDestCorrelationId = pub->removeDestination(SUB1_MDC_MANUAL_URI);

        POLL_FOR(pub->findDestinationResponse(removeDestCorrelationId), invoker);

        POLL_FOR(0 < pub->offer(buffer, 0, 128), invoker);
        POLL_FOR(0 < sub2->poll(m_noOpHandler, 1), invoker);

        EXPECT_EQ(0, sub2->poll(m_noOpHandler, 1));
    }

    invoker.invoke();
}

TEST_F(MultiDestinationTest, shouldAddRemoveDestinationFromExclusivePublication)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    auto &invoker = aeron->conductorAgentInvoker();
    std::int32_t streamId = 1001;

    {
        auto sub1RegId = aeron->addSubscription(SUB1_MDC_MANUAL_URI, streamId);
        auto sub2RegId = aeron->addSubscription(SUB2_MDC_MANUAL_URI, streamId);
        auto pubRegId = aeron->addExclusivePublication(PUB_MDC_MANUAL_URI, streamId);

        POLL_FOR_NON_NULL(sub1, aeron->findSubscription(sub1RegId), invoker);
        POLL_FOR_NON_NULL(sub2, aeron->findSubscription(sub2RegId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubRegId), invoker);

        pub->addDestination(SUB1_MDC_MANUAL_URI);
        pub->addDestination(SUB2_MDC_MANUAL_URI);

        POLL_FOR(sub1->isConnected(), invoker);
        POLL_FOR(sub2->isConnected(), invoker);

        POLL_FOR(0 < pub->offer(buffer, 0, 128), invoker);

        POLL_FOR(0 < sub1->poll(m_noOpHandler, 1), invoker);

        POLL_FOR(0 < sub2->poll(m_noOpHandler, 1), invoker);

        pub->removeDestination(SUB1_MDC_MANUAL_URI);

        // The existing C++ API for ExclusivePublications is missing the means to track the add and removal of
        // destinations.  This is fixed in the wrapper, but the test is written for compatibility with
        // both APIs so has to take a few liberties in order to work correctly.
        sleep(1);

        POLL_FOR(0 < pub->offer(buffer, 0, 128), invoker);
        POLL_FOR(0 < sub2->poll(m_noOpHandler, 1), invoker);

        EXPECT_EQ(0, sub2->poll(m_noOpHandler, 1));
    }

    invoker.invoke();
}

TEST_F(MultiDestinationTest, shouldAddAndRemoveDestinationsFromSubscription)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    auto &invoker = aeron->conductorAgentInvoker();
    std::int32_t streamId = 1001;

    {
        std::string channel1 = ChannelUriStringBuilder()
            .media("udp")
            .endpoint(UNICAST_ENDPOINT_A)
            .build();
        std::string channel2 = ChannelUriStringBuilder()
            .media("udp")
            .endpoint(UNICAST_ENDPOINT_B)
            .build();

        auto pub1RegId = aeron->addPublication(channel1, streamId);
        auto pub2RegId = aeron->addPublication(channel2, streamId);
        auto subRegId = aeron->addSubscription(SUB_URI, streamId);

        POLL_FOR_NON_NULL(pub1, aeron->findPublication(pub1RegId), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findPublication(pub2RegId), invoker);
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subRegId), invoker);

        int64_t subDest1 = sub->addDestination(channel1);
        int64_t subDest2 = sub->addDestination(channel2);

        POLL_FOR(sub->findDestinationResponse(subDest1), invoker);
        POLL_FOR(sub->findDestinationResponse(subDest2), invoker);

//        POLL_FOR()

    }
}