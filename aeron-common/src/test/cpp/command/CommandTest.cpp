/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

#include <array>
#include <gtest/gtest.h>
#include <util/Exceptions.h>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <command/ConnectionMessageFlyweight.h>
#include <command/ConnectionReadyFlyweight.h>
#include <command/RemoveMessageFlyweight.h>
#include <command/SubscriptionMessageFlyweight.h>
#include <command/PublicationMessageFlyweight.h>
#include <command/PublicationBuffersReadyFlyweight.h>

using namespace aeron::common::util;
using namespace aeron::common::command;
using namespace aeron::common::concurrent;

static std::array<std::uint8_t, 1024> testBuffer;

static void clearBuffer()
{
    testBuffer.fill(0);
}

TEST (commandTests, testInstantiateFlyweights)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 256;

    std::string channelData = "channelData";

    ASSERT_NO_THROW({
        ConnectionMessageFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        ConnectionReadyFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        RemoveMessageFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        SubscriptionMessageFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        PublicationMessageFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        PublicationBuffersReadyFlyweight cmd(ab, BASEOFFSET);
    });
}

TEST (commandTests, testConnectionMessageFlyweight)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 256;

    std::string channelData = "channelData";

    ASSERT_NO_THROW({
        ConnectionMessageFlyweight cmd (ab, BASEOFFSET);
        cmd.correlationId(1).sessionId(2).streamId(3).channel(channelData);

        ASSERT_EQ(ab.getInt64(BASEOFFSET + 0), 1);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 8), 2);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 12), 3);
        ASSERT_EQ(ab.getStringUtf8(BASEOFFSET + 16), channelData);

        ASSERT_EQ(cmd.correlationId(), 1);
        ASSERT_EQ(cmd.sessionId(), 2);
        ASSERT_EQ(cmd.streamId(), 3);
        ASSERT_EQ(cmd.channel(), channelData);

        ASSERT_EQ(cmd.length(), offsetof(ConnectionMessageDefn, channel.channelData) + channelData.length());
    });
}


TEST (commandTests, testPublicationReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 256;

    std::string channelData = "channeldata";
    std::string logFileNameData = "logfilenamedata";

    ASSERT_NO_THROW({
        PublicationBuffersReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.correlationId(-1).streamId(0x01010101).sessionId(0x02020202).positionIndicatorOffset(10);
        cmd.mtuLength(0x10101010).channel(channelData).logFileName(logFileNameData);

        ASSERT_EQ(ab.getInt64(BASEOFFSET + 0), -1);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 8), 0x02020202);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 12), 0x01010101);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 16), 10);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 20), 0x10101010);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 24), channelData.length());
        ASSERT_EQ(ab.getStringUtf8(BASEOFFSET + 24), channelData);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 28 + channelData.length()), logFileNameData.length());
        ASSERT_EQ(ab.getStringUtf8(BASEOFFSET + 28 + channelData.length()), logFileNameData);

        ASSERT_EQ(cmd.correlationId(), -1);
        ASSERT_EQ(cmd.streamId(), 0x01010101);
        ASSERT_EQ(cmd.sessionId(), 0x02020202);
        ASSERT_EQ(cmd.positionIndicatorOffset(), 10);
        ASSERT_EQ(cmd.mtuLength(), 0x10101010);
        ASSERT_EQ(cmd.channel(), channelData);
        ASSERT_EQ(cmd.logFileName(), logFileNameData);

        ASSERT_EQ(cmd.length(), offsetof(PublicationBuffersReadyDefn, channel.channelData) + channelData.length() +
            sizeof(std::int32_t) + logFileNameData.length());
    });
}

TEST (commandTests, testConnectionReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 0;

    ASSERT_NO_THROW({
        ConnectionReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.location(0, "aaaa").location(1, "bbbb").location(2, "cccc").location(3, "dddd").location(4, "eeee").location(5, "ffff");
        cmd.sourceInfo("gggg").channel("xxxx");
        cmd.positionIndicatorsCount(4);
        for (int n = 0; n < 4; n++)
            cmd.positionIndicator(n, ConnectionReadyDefn::PositionIndicator{n,n});

        ASSERT_EQ(cmd.length(), sizeof(ConnectionReadyDefn) + 8 * 4 + 4 * sizeof(ConnectionReadyDefn::PositionIndicator));
    });

    ASSERT_THROW({
        clearBuffer();
        ConnectionReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.location(1, "aaaa");
    }, IllegalStateException);

    ASSERT_THROW({
        clearBuffer();
        ConnectionReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.positionIndicator(0, ConnectionReadyDefn::PositionIndicator{1,1});
    }, IllegalStateException);
}