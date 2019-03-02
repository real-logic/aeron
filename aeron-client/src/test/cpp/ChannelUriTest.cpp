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

#include "ChannelUri.h"
#include "ChannelUriStringBuilder.h"

using namespace aeron;

TEST(ChannelUriTest, shouldParseSimpleDefaultUris)
{
    std::shared_ptr<ChannelUri> channelUri;

    channelUri = ChannelUri::parse("aeron:udp");
    ASSERT_EQ(channelUri->prefix(), "");
    ASSERT_EQ(channelUri->media(), "udp");

    channelUri = ChannelUri::parse("aeron:ipc");
    ASSERT_EQ(channelUri->prefix(), "");
    ASSERT_EQ(channelUri->media(), "ipc");

    channelUri = ChannelUri::parse("aeron:");
    ASSERT_EQ(channelUri->prefix(), "");
    ASSERT_EQ(channelUri->media(), "");

    channelUri = ChannelUri::parse("aeron-spy:aeron:udp");
    ASSERT_EQ(channelUri->prefix(), "aeron-spy");
    ASSERT_EQ(channelUri->media(), "udp");
}

TEST(ChannelUriTest, shouldRejectUriWithoutAeronPrefix)
{
    ASSERT_THROW(ChannelUri::parse(":udp"), IllegalArgumentException);
    ASSERT_THROW(ChannelUri::parse("aeron"), IllegalArgumentException);
    ASSERT_THROW(ChannelUri::parse("aron:"), IllegalArgumentException);
    ASSERT_THROW(ChannelUri::parse("eeron:"), IllegalArgumentException);
}

TEST(ChannelUriTest, shouldRejectWithOutOfPlaceColon)
{
    ASSERT_THROW(ChannelUri::parse("aeron:udp:"), IllegalArgumentException);
}

TEST(ChannelUriTest, shouldParseWithSingleParameter)
{
    std::shared_ptr<ChannelUri> channelUri;

    channelUri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8");
    ASSERT_EQ(channelUri->get("endpoint"), "224.10.9.8");

    channelUri = ChannelUri::parse("aeron:udp?add|ress=224.10.9.8");
    ASSERT_EQ(channelUri->get("add|ress"), "224.10.9.8");

    channelUri = ChannelUri::parse("aeron:udp?endpoint=224.1=0.9.8");
    ASSERT_EQ(channelUri->get("endpoint"), "224.1=0.9.8");
}

TEST(ChannelUriTest, shouldParseWithMultipleParameters)
{
    std::shared_ptr<ChannelUri> channelUri;

    channelUri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");
    ASSERT_EQ(channelUri->get("endpoint"), "224.10.9.8");
    ASSERT_EQ(channelUri->get("port"), "4567");
    ASSERT_EQ(channelUri->get("interface"), "192.168.0.3");
    ASSERT_EQ(channelUri->get("ttl"), "16");
}

TEST(ChannelUriTest, shouldAllowReturnDefaultIfParamNotSpecified)
{
    std::shared_ptr<ChannelUri> channelUri;

    channelUri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8");
    ASSERT_EQ(channelUri->get("endpoint"), "224.10.9.8");
    ASSERT_EQ(channelUri->get("interface"), "");
    ASSERT_EQ(channelUri->get("interface", "192.168.0.0"), "192.168.0.0");
}

TEST(ChannelUriTest, shouldRoundTripToString)
{
    const std::string uriString("aeron:udp?endpoint=224.10.9.8:777");
    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uriString);

    ASSERT_EQ(channelUri->toString(), uriString);
}

TEST(ChannelUriTest, shouldRoundTripToStringBuilder)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("224.10.9.8:777");

    const std::string uriString = builder.build();

    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uriString);
    ASSERT_EQ(channelUri->toString(), uriString);
}

TEST(ChannelUriTest, shouldRoundTripToStringBuilderWithPrefix)
{
    ChannelUriStringBuilder builder;

    builder
        .prefix(SPY_QUALIFIER)
        .media(UDP_MEDIA)
        .endpoint("224.10.9.8:777");

    const std::string uriString = builder.build();

    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uriString);
    ASSERT_EQ(channelUri->toString(), uriString);
}
