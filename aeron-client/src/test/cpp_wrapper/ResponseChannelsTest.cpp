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
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "TestUtil.h"
#include "HeartbeatTimestamp.h"

#define REQUEST_STREAM_ID (10000)
#define RESPONSE_STREAM_ID (10001)
#define REQUEST_ENDPOINT "localhost:10000"
#define RESPONSE_ENDPOINT "localhost:10001"

using namespace aeron;
using testing::MockFunction;
using testing::_;

class ResponseChannelsTest: public testing::Test
{
public:
    ResponseChannelsTest()
    {
        m_driver.start();
    }

    ~ResponseChannelsTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

class MultiInvoker
{
public:
    MultiInvoker(AgentInvoker<ClientConductor> &invokerA, AgentInvoker<ClientConductor> &invokerB) :
        m_invokerA(invokerA), m_invokerB(invokerB)
    {
    }

    int invoke()
    {
        return m_invokerA.invoke() + m_invokerB.invoke();
    }

private:
    AgentInvoker<ClientConductor> &m_invokerA;
    AgentInvoker<ClientConductor> &m_invokerB;
};

TEST_F(ResponseChannelsTest, shouldConnectResponseChannelUsingConcurrent)
{
    std::string expectedMessage = "hello world!";

    std::array<std::uint8_t, 1024> buf{};
    AtomicBuffer message(buf);
    message.putString(0, expectedMessage);

    Context serverCtx;
    serverCtx.useConductorAgentInvoker(true);

    Context clientCtx;
    clientCtx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> server = Aeron::connect(serverCtx);
    AgentInvoker<ClientConductor> &serverInvoker = server->conductorAgentInvoker();

    std::shared_ptr<Aeron> client = Aeron::connect(clientCtx);
    AgentInvoker<ClientConductor> &clientInvoker = client->conductorAgentInvoker();
    MultiInvoker invoker{clientInvoker, serverInvoker};

    ChannelUriStringBuilder builder;
    builder.media("udp").endpoint(REQUEST_ENDPOINT).build();

    std::int64_t subReqId = server->addSubscription(
        builder.clear().media("udp").endpoint(REQUEST_ENDPOINT).build(),
        REQUEST_STREAM_ID);
    std::int64_t subRspId = client->addSubscription(
        builder.clear().media("udp").controlEndpoint(RESPONSE_ENDPOINT).controlMode(CONTROL_MODE_RESPONSE).build(),
        RESPONSE_STREAM_ID);

    POLL_FOR_NON_NULL(subReq, server->findSubscription(subReqId), invoker);
    POLL_FOR_NON_NULL(subRsp, client->findSubscription(subRspId), invoker);

    std::int64_t pubReqId = client->addPublication(
        builder.clear().media("udp").endpoint(REQUEST_ENDPOINT).responseCorrelationId(subRsp->registrationId()).build(),
        REQUEST_STREAM_ID);

    POLL_FOR_NON_NULL(pubReq, client->findPublication(pubReqId), invoker);
    POLL_FOR(pubReq->isConnected() && subReq->isConnected(), invoker);

    auto image = subReq->imageByIndex(0);
    std::int64_t pubRspId = server->addPublication(
        builder.clear().media("udp").controlMode(CONTROL_MODE_RESPONSE).controlEndpoint(RESPONSE_ENDPOINT).responseCorrelationId(image->correlationId()).build(),
        RESPONSE_STREAM_ID);
    POLL_FOR_NON_NULL(rspPub, server->findPublication(pubRspId), invoker);
    POLL_FOR(rspPub->isConnected() && subRsp->isConnected(), invoker);

    POLL_FOR(0 < pubReq->offer(message), invoker);
    POLL_FOR(0 < subReq->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            POLL_FOR(rspPub->offer(buffer, offset, length), invoker);
        },
        1), invoker);

    POLL_FOR(0 < subRsp->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            const std::string actualMessage = buffer.getString(offset);
            ASSERT_EQ(expectedMessage, actualMessage);
        },
        1), invoker);

}
