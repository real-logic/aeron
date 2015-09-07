//
// Created by Michael Barker on 04/09/15.
//

#include <gtest/gtest.h>
#include "media/UdpChannel.h"

using namespace aeron::driver::media;

class UdpChannelTest : public testing::Test
{

};

TEST_F(UdpChannelTest, handleCanonicalFormForUnicast)
{
    auto udpChannel = UdpChannel::parse("aeron:udp?remote=192.168.0.1:40456");
    auto udpChannelLocal = UdpChannel::parse("aeron:udp?local=127.0.0.1|remote=192.168.0.1:40456");
    auto udpChannelLocalPort = UdpChannel::parse("aeron:udp?local=127.0.0.1:40455|remote=192.168.0.1:40456");
    auto udpChannelLocalhost = UdpChannel::parse("aeron:udp?local=localhost|remote=localhost:40456");

    EXPECT_EQ("UDP-00000000-0-c0a80001-40456", udpChannel->canonicalForm());
    EXPECT_EQ("UDP-7f000001-0-c0a80001-40456", udpChannelLocal->canonicalForm());
    EXPECT_EQ("UDP-7f000001-40455-c0a80001-40456", udpChannelLocalPort->canonicalForm());
    EXPECT_EQ("UDP-7f000001-0-7f000001-40456", udpChannelLocalhost->canonicalForm());
}
