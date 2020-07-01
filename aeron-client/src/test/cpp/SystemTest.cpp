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

using namespace aeron;

class SystemTest : public testing::Test
{
public:
    SystemTest()
    {
        m_driver.start();
    }

    ~SystemTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(SystemTest, shouldReclaimSubscriptionWhenOutOfScopeAndNotFound)
{
    std::shared_ptr<Aeron> aeron = Aeron::connect();

    aeron->addSubscription("aeron:udp?endpoint=localhost:24325", 10);
    const auto pub_reg_id = aeron->addPublication("aeron:udp?endpoint=localhost:24325", 10);

    auto pub = aeron->findPublication(pub_reg_id);
    while (!pub)
    {
        std::this_thread::yield();
        pub = aeron->findPublication(pub_reg_id);
    }
}
