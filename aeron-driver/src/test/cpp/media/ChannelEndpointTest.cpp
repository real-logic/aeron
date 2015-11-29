/*
 * Copyright 2015 Real Logic Ltd.
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
#include "media/ReceiveChannelEndpoint.h"
#include "media/SendChannelEndpoint.h"

using namespace aeron::driver::media;

class ChannelEndpointTest : public testing::Test
{
};

TEST_F(ChannelEndpointTest, initialisesEndpoint)
{
    const char* uri = "aeron:udp?group=224.10.9.9:40124|interface=localhost";

    ReceiveChannelEndpoint receive{std::move(UdpChannel::parse(uri))};
    SendChannelEndpoint send{std::move(UdpChannel::parse(uri))};

    receive.openDatagramChannel();
    send.openDatagramChannel();
}