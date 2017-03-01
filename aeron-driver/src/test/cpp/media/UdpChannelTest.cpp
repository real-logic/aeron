/*
 * Copyright 2014-2017 Real Logic Ltd.
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
#include "media/InetAddress.h"
#include "media/UdpChannel.h"

using namespace aeron::driver::media;

class UdpChannelTest : public testing::Test
{

};

//TEST_F(UdpChannelTest, handleCanonicalFormForUnicast)
//{
//    auto udpChannel = UdpChannel::parse("aeron:udp?remote=192.168.0.1:40456");
//    auto udpChannelLocal = UdpChannel::parse("aeron:udp?local=127.0.0.1|remote=192.168.0.1:40456");
//    auto udpChannelLocalPort = UdpChannel::parse("aeron:udp?local=127.0.0.1:40455|remote=192.168.0.1:40456");
//    auto udpChannelLocalhost = UdpChannel::parse("aeron:udp?local=localhost|remote=localhost:40456");
//
//    EXPECT_EQ("UDP-00000000-0-c0a80001-40456", udpChannel->canonicalForm());
//    EXPECT_EQ("UDP-7f000001-0-c0a80001-40456", udpChannelLocal->canonicalForm());
//    EXPECT_EQ("UDP-7f000001-40455-c0a80001-40456", udpChannelLocalPort->canonicalForm());
//    EXPECT_EQ("UDP-7f000001-0-7f000001-40456", udpChannelLocalhost->canonicalForm());
//}

TEST_F(UdpChannelTest, throwsExceptionOnEvenMultcastDataAddress)
{
    EXPECT_THROW(UdpChannel::parse("aeron:udp?endpoint=224.10.9.8:40124"), InvalidChannelException);
}

TEST_F(UdpChannelTest, throwsExceptionWithMissingAddress)
{
    EXPECT_THROW(UdpChannel::parse("aeron:udp"), InvalidChannelException);
}

TEST_F(UdpChannelTest, throwsExceptionWithInvalidMedia)
{
    EXPECT_THROW(UdpChannel::parse("aeron:ipc?endpoint=224.10.9.9:40124"), InvalidChannelException);
}

TEST_F(UdpChannelTest, createValidMulticastUdpChannel)
{
    auto channel = UdpChannel::parse("aeron:udp?endpoint=224.10.9.9:40124|interface=localhost");

    EXPECT_EQ(*InetAddress::fromIPv4("224.10.9.10", 41024), channel->remoteControl());
    EXPECT_EQ(*InetAddress::fromIPv4("224.10.9.9", 41024), channel->remoteData());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localData());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localControl());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localInterface().address());
    EXPECT_TRUE(channel->isMulticast());
}

TEST_F(UdpChannelTest, createValidUnicastUdpChannel)
{
    auto channel = UdpChannel::parse("aeron:udp?endpoint=localhost:40124|interface=localhost");

    EXPECT_EQ(*InetAddress::parse("localhost:40124", AF_INET), channel->remoteControl());
    EXPECT_EQ(*InetAddress::parse("localhost:40124", AF_INET), channel->remoteData());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localData());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localControl());
    EXPECT_EQ(*InetAddress::parse("localhost", AF_INET), channel->localInterface().address());
    EXPECT_FALSE(channel->isMulticast());
}
