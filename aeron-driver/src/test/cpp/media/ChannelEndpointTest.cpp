/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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
#include <protocol/SetupFlyweight.h>
#include "media/ReceiveChannelEndpoint.h"
#include "media/SendChannelEndpoint.h"

using namespace aeron;
using namespace aeron::driver::media;

class ChannelEndpointTest : public testing::Test
{
};

TEST_F(ChannelEndpointTest, initialisesEndpoint)
{
    const char* uri = "aeron:udp?endpoint=224.10.9.9:54326|interface=localhost";
    std::uint8_t setupBytes[protocol::SetupFlyweight::headerLength()];
    concurrent::AtomicBuffer setupBuffer{setupBytes, protocol::SetupFlyweight::headerLength()};
    protocol::SetupFlyweight setupFlyweight{setupBuffer, 0};

    ReceiveChannelEndpoint receive{std::move(UdpChannel::parse(uri))};
    SendChannelEndpoint send{std::move(UdpChannel::parse(uri))};

    receive.openDatagramChannel();
    send.openDatagramChannel();

    std::int32_t sessionId = 2;
    std::int32_t streamId = 3;
    std::int32_t initialTermId = 5;
    std::int32_t activeTermId = 7;
    std::int32_t termOffset = 0;
    std::int32_t termLength = 1 << 16;
    std::int32_t mtu = 4096;

    setupBuffer.setMemory(0, protocol::SetupFlyweight::headerLength(), 0);
    setupFlyweight
        .sessionId(sessionId)
        .streamId(streamId)
        .initialTermId(initialTermId)
        .actionTermId(activeTermId)
        .termOffset(termOffset)
        .termLength(termLength)
        .mtu(mtu)
        .version(0)
        .flags(0)
        .type(protocol::HeaderFlyweight::HDR_TYPE_SETUP)
        .frameLength(protocol::SetupFlyweight::headerLength());

    send.send(setupBuffer.buffer(), setupBuffer.capacity());
    receive.pollForData();
}