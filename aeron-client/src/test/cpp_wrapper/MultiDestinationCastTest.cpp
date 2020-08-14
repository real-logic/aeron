
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

class MultiDestinationCastTest : public testing::TestWithParam<std::tuple<const char *, const char *>>
{
public:
    MultiDestinationCastTest()
    {
        m_driver.start();
    }

    ~MultiDestinationCastTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

typedef std::array<std::uint8_t, 1024> buffer_t;

TEST_F(MultiDestinationCastTest, shouldAddRemoveDestinationFromPublication)
{
//    Context ctx;
//    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect();
    std::int32_t streamId = 1001;

    auto sub1RegId = aeron->addSubscription(SUB1_MDC_MANUAL_URI, streamId);
    auto sub2RegId = aeron->addSubscription(SUB2_MDC_MANUAL_URI, streamId);
//    auto pubRegId = aeron->addPublication()

    WAIT_FOR_NON_NULL(sub1, aeron->findSubscription(sub1RegId));
    WAIT_FOR_NON_NULL(sub2, aeron->findSubscription(sub2RegId));
}