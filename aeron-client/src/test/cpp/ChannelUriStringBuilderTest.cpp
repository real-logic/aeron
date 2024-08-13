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

#include <cstdint>
#include "gtest/gtest.h"

#include "ChannelUriStringBuilder.h"

using namespace aeron;

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicIpcChannel)
{
    ChannelUriStringBuilder builder;

    builder.media(IPC_MEDIA);

    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999");

    ASSERT_EQ(builder.build(), "aeron:udp?endpoint=localhost:9999");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicUdpChannelSpy)
{
    ChannelUriStringBuilder builder;

    builder
        .prefix(SPY_QUALIFIER)
        .media(UDP_MEDIA)
        .endpoint("localhost:9999");

    ASSERT_EQ(builder.build(), "aeron-spy:aeron:udp?endpoint=localhost:9999");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateComplexUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .ttl(9)
        .termLength(1024 * 128);

    ASSERT_EQ(builder.build(), "aeron:udp?endpoint=localhost:9999|term-length=131072|ttl=9");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReplayUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .termLength(1024 * 128)
        .initialTermId(777)
        .termId(999)
        .termOffset(64);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-length=131072|init-term-id=777|term-id=999|term-offset=64");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateInitialPosition)
{
    ChannelUriStringBuilder builder;

    std::int32_t termLength = 1024 * 128;
    std::int64_t position = (termLength * 3) + 64;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .initialPosition(position, 777, termLength);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-length=131072|init-term-id=777|term-id=780|term-offset=64");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateSocketSndRcvbufLengths)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .socketSndbufLength(8192)
        .socketRcvbufLength(4096);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|so-sndbuf=8192|so-rcvbuf=4096");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReceiverWindowLength)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .receiverWindowLength(4096);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|rcv-wnd=4096");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateOffsets)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved")
        .channelReceiveTimestampOffset("0")
        .channelSendTimestampOffset("8");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|channel-snd-ts-offset=8");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReceiveTimestampOffset)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateMediaReceiveTimestampOffset)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateResponseCorrelationId)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .controlMode(CONTROL_MODE_RESPONSE)
        .responseCorrelationId(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|control-mode=response|response-correlation-id=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateNakDelay)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .nakDelay(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|nak-delay=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateUntetheredWindowLimitTimeout)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .untetheredWindowLimitTimeout(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|untethered-window-limit-timeout=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateUntetheredRestingTimeout)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .untetheredRestingTimeout(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|untethered-resting-timeout=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateGroupTag)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .groupTag(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|gtag=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateLinger)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .linger(INT64_C(3333333333));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|linger=3333333333");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateTtl)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .ttl(INT8_C(33));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|ttl=33");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateMtu)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mtu(UINT32_C(1408));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|mtu=1408");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateTermLength)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .termLength(INT32_C(1048576));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-length=1048576");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateInitialTermId)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .initialTermId(INT32_C(982374533));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|init-term-id=982374533");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateTermId)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .termId(INT32_C(834753));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-id=834753");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateTermOffset)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .termOffset(UINT32_C(8192));

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-offset=8192");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateTags)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .tags("1001:1002");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?tags=1001:1002|endpoint=localhost:9999");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateNetworkInterface)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .networkInterface("eno0");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|interface=eno0");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateControlEndpoint)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .controlEndpoint("localhost:9998");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|control=localhost:9998");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateControlMode)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .controlMode("response");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|control-mode=response");

    builder.clear().media("ipc");
    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldHandleMaxRetransmits)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("224.10.9.8:777")
        .maxRetransmits(123);

    const std::string uriString = builder.build();

    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uriString);
    ASSERT_NE(std::string::npos, channelUri->toString().find("retransmits-active-max=123"));
}
