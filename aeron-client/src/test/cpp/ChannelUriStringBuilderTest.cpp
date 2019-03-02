/*
 * Copyright 2014-2019 Real Logic Ltd.
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

