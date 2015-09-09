//
// Created by Michael Barker on 04/09/15.
//

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
    EXPECT_THROW(UdpChannel::parse("aeron:udp?group=224.10.9.8:40124"), InvalidChannelException);
}

TEST_F(UdpChannelTest, throwsExceptionWithMissingAddress)
{
    EXPECT_THROW(UdpChannel::parse("aeron:udp"), InvalidChannelException);
}

TEST_F(UdpChannelTest, throwsExceptionWithBothMulticastAndUnicastSpecified)
{
    EXPECT_THROW(
        UdpChannel::parse("aeron:udp?group=224.10.9.9:40124|local=127.0.0.1:12345"),
        InvalidChannelException);
    EXPECT_THROW(
        UdpChannel::parse("aeron:udp?interface=127.0.0.1:12345|remote=127.0.0.1:12345"),
        InvalidChannelException);
}

TEST_F(UdpChannelTest, throwsExceptionWithInvalidMedia)
{
    EXPECT_THROW(UdpChannel::parse("aeron:ipc?group=224.10.9.9:40124"), InvalidChannelException);
}

//final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|group=224.10.9.9:40124");
//
//assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
//assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
//assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
//assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
//assertThat(udpChannel.localInterface(), is(NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"))));
TEST_F(UdpChannelTest, createValidMulticastUdpChannel)
{
    auto channel = UdpChannel::parse("aeron:udp?interface=localhost|group=224.10.9.9:40124");
    auto remoteDataAddress = InetAddress::fromIpString("224.10.9.9", 41024);
    auto remoteControlAddress = InetAddress::fromIpString("224.10.9.10", 41024);
}