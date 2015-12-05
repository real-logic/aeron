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
#include <iostream>
#include <sys/time.h>
#include "media/InetAddress.h"
#include "media/UdpChannel.h"
#include "media/UdpChannelTransport.h"

using namespace aeron::driver::media;

class UdpChannelTransportTest : public testing::Test
{
};

std::int32_t recv(UdpChannelTransport &transport, char* buffer, std::int32_t len)
{
    timeval t0;
    timeval t1;

    gettimeofday(&t0, NULL);

    std::int32_t received;
    do
    {
        received = transport.recv(buffer, len);
        usleep(1);
        gettimeofday(&t1, NULL);
    }
    while (received == 0 && t1.tv_sec - t0.tv_sec < 5);

    return received;
}

TEST_F(UdpChannelTransportTest, sendAndReceiveMulticastIPv4)
{
    const char* message = "Hello World!";
    char receiveBuffer[512];
    memset(receiveBuffer, 0, sizeof(receiveBuffer));
    in_addr any {INADDR_ANY};
    timeval timeout{5, 0};

    std::unique_ptr<UdpChannel> channel = UdpChannel::parse("aeron:udp?group=224.0.1.3:40124|interface=localhost");

    UdpChannelTransport transport{channel, &channel->remoteData(), &channel->remoteData(), &channel->localData()};

    transport.openDatagramChannel();
    transport.send(message, (std::int32_t) strlen(message));
    std::int32_t received = recv(transport, receiveBuffer, 512);

    EXPECT_GT(received, 0);
    EXPECT_STREQ(message, receiveBuffer);
}

TEST_F(UdpChannelTransportTest, sendAndReceiveMulticastIPv6)
{
    const char* message = "Hello World!";
    char receiveBuffer[512];
    memset(receiveBuffer, 0, sizeof(receiveBuffer));
    timeval timeout{5, 0};

    std::unique_ptr<UdpChannel> channel = UdpChannel::parse("aeron:udp?group=[ff02::3]:9877|interface=localhost", PF_INET6);

    Inet6Address* bindAddress = new Inet6Address{in6addr_any, channel->remoteData().port()};
    UdpChannelTransport transport{channel, &channel->remoteData(), bindAddress, &channel->localData()};

    transport.openDatagramChannel();
    transport.setTimeout(timeout);

    transport.send(message, (std::int32_t) strlen(message));
    std::int32_t received = recv(transport, receiveBuffer, 512);

    EXPECT_GT(received, 0);
    EXPECT_STREQ(message, receiveBuffer);
}

TEST_F(UdpChannelTransportTest, sendAndReceiveUnicastIPv4)
{
    const char* message = "Hello World!";
    char receiveBuffer[512];
    memset(receiveBuffer, 0, sizeof(receiveBuffer));
    in_addr any {INADDR_ANY};
    timeval timeout{5, 0};

    std::unique_ptr<UdpChannel> channel = UdpChannel::parse("aeron:udp?local=localhost:9009|remote=localhost:9010");

    Inet4Address* bindAddress = new Inet4Address{any, channel->remoteData().port()};
    UdpChannelTransport transport{channel, &channel->remoteData(), bindAddress, &channel->localData()};

    transport.openDatagramChannel();
    transport.setTimeout(timeout);

    transport.send(message, (std::int32_t) strlen(message));
    std::int32_t received = recv(transport, receiveBuffer, 512);

    EXPECT_GT(received, 0);
    EXPECT_STREQ(message, receiveBuffer);
}

TEST_F(UdpChannelTransportTest, sendAndReceiveUnicastIPv6)
{
    const char* message = "Hello World!";
    char receiveBuffer[512];
    memset(receiveBuffer, 0, sizeof(receiveBuffer));
    timeval timeout{5, 0};

    std::unique_ptr<UdpChannel> channel = UdpChannel::parse(
        "aeron:udp?local=localhost:40123|remote=localhost:40124", PF_INET6);

    Inet6Address* bindAddress = new Inet6Address{in6addr_any, channel->remoteData().port()};
    UdpChannelTransport transport{channel, &channel->remoteData(), bindAddress, &channel->localData()};

    transport.openDatagramChannel();
    transport.setTimeout(timeout);

    transport.send(message, (std::int32_t) strlen(message));
    std::int32_t received = recv(transport, receiveBuffer, 512);

    EXPECT_GT(received, 0);
    EXPECT_STREQ(message, receiveBuffer);
}
