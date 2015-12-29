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
#include <command/ImageMessageFlyweight.h>
#include <command/ImageBuffersReadyFlyweight.h>
#include <command/RemoveMessageFlyweight.h>
#include <command/SubscriptionMessageFlyweight.h>
#include <command/PublicationMessageFlyweight.h>
#include <command/PublicationBuffersReadyFlyweight.h>

using namespace aeron::util;
using namespace aeron::command;
using namespace aeron::concurrent;

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
        ImageMessageFlyweight cmd(ab, BASEOFFSET);
    });

    ASSERT_NO_THROW({
        ImageBuffersReadyFlyweight cmd(ab, BASEOFFSET);
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

TEST (commandTests, testImageMessageFlyweight)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 256;

    std::string channelData = "channelData";

    ASSERT_NO_THROW({
        ImageMessageFlyweight cmd (ab, BASEOFFSET);
        cmd.correlationId(1).streamId(3).channel(channelData);

        ASSERT_EQ(ab.getInt64(BASEOFFSET + 0), 1);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 8), 3);
        ASSERT_EQ(ab.getStringUtf8(BASEOFFSET + 12), channelData);

        ASSERT_EQ(cmd.correlationId(), 1);
        ASSERT_EQ(cmd.streamId(), 3);
        ASSERT_EQ(cmd.channel(), channelData);

        ASSERT_EQ(cmd.length(), static_cast<int>(12 + sizeof(std::int32_t) + channelData.length()));
    });
}


TEST (commandTests, testPublicationReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 256;

    std::string logFileNameData = "logfilenamedata";

    ASSERT_NO_THROW({
        PublicationBuffersReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.correlationId(-1).streamId(0x01010101).sessionId(0x02020202).positionLimitCounterId(10);
        cmd.logFileName(logFileNameData);

        ASSERT_EQ(ab.getInt64(BASEOFFSET + 0), -1);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 8), 0x02020202);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 12), 0x01010101);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 16), 10);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 20), static_cast<int>(logFileNameData.length()));
        ASSERT_EQ(ab.getStringUtf8(BASEOFFSET + 20), logFileNameData);

        ASSERT_EQ(cmd.correlationId(), -1);
        ASSERT_EQ(cmd.streamId(), 0x01010101);
        ASSERT_EQ(cmd.sessionId(), 0x02020202);
        ASSERT_EQ(cmd.positionLimitCounterId(), 10);
        ASSERT_EQ(cmd.logFileName(), logFileNameData);

        ASSERT_EQ(cmd.length(), static_cast<int>(20 + sizeof(std::int32_t) + logFileNameData.length()));
    });
}

TEST (commandTests, testImageBuffersReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASEOFFSET = 0;

    std::string logFileNameData = "logfilenamedata";
    std::string sourceInfoData = "sourceinfodata";

    ASSERT_NO_THROW(
    {
        ImageBuffersReadyFlyweight cmd(ab, BASEOFFSET);

        cmd.correlationId(-1).streamId(0x01010101).sessionId(0x02020202).subscriberPositionCount(4);
        cmd.logFileName(logFileNameData).sourceIdentity(sourceInfoData);
        for (int n = 0; n < 4; n++)
        {
            cmd.subscriberPosition(n, ImageBuffersReadyDefn::SubscriberPosition {n, n});
        }

        ASSERT_EQ(ab.getInt64(BASEOFFSET + 0), -1);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 8), 0x02020202);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 12), 0x01010101);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 16), 12);
        ASSERT_EQ(ab.getInt32(BASEOFFSET + 20), 4);

        const index_t startOfSubscriberPositions = BASEOFFSET + 24;
        for (int n = 0; n < 4; n++)
        {
            ASSERT_EQ(
                ab.getInt32(startOfSubscriberPositions + (n * sizeof(ImageBuffersReadyDefn::SubscriberPosition))), n);
            ASSERT_EQ(
                ab.getInt32(startOfSubscriberPositions + (n * sizeof(ImageBuffersReadyDefn::SubscriberPosition)) + 4), n);
        }

        const index_t startOfLogFileName = BASEOFFSET + 24 + (4 * sizeof(ImageBuffersReadyDefn::SubscriberPosition));
        ASSERT_EQ(ab.getInt32(startOfLogFileName), static_cast<int>(logFileNameData.length()));
        ASSERT_EQ(ab.getStringUtf8(startOfLogFileName), logFileNameData);

        const index_t startOfSourceIdentity = startOfLogFileName + 4 + (index_t)logFileNameData.length();
        ASSERT_EQ(ab.getInt32(startOfSourceIdentity), static_cast<int>(sourceInfoData.length()));
        ASSERT_EQ(ab.getStringUtf8(startOfSourceIdentity), sourceInfoData);

        ASSERT_EQ(cmd.correlationId(), -1);
        ASSERT_EQ(cmd.streamId(), 0x01010101);
        ASSERT_EQ(cmd.sessionId(), 0x02020202);
        ASSERT_EQ(cmd.subscriberPositionCount(), 4);
        ASSERT_EQ(cmd.logFileName(), logFileNameData);
        ASSERT_EQ(cmd.sourceIdentity(), sourceInfoData);
        for (int n = 0; n < 4; n++)
        {
            const ImageBuffersReadyDefn::SubscriberPosition subscriberPosition = cmd.subscriberPosition(n);

            ASSERT_EQ(subscriberPosition.indicatorId, n);
            ASSERT_EQ(subscriberPosition.registrationId, n);
        }

        const ImageBuffersReadyDefn::SubscriberPosition* subscriberPositions = cmd.subscriberPositions();
        for (int n = 0; n < 4; n++)
        {
            ASSERT_EQ(subscriberPositions[n].indicatorId, n);
            ASSERT_EQ(subscriberPositions[n].registrationId, n);
        }

        ASSERT_EQ(
            cmd.length(),
            static_cast<int>(sizeof(ImageBuffersReadyDefn) +
                (4 * sizeof(ImageBuffersReadyDefn::SubscriberPosition)) +
                sizeof(std::int32_t) + logFileNameData.length() +
                sizeof(std::int32_t) + sourceInfoData.length()));
    }
    );
}