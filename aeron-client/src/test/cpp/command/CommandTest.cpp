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

#include <array>
#include <gtest/gtest.h>
#include <util/Exceptions.h>
#include <util/Index.h>
#include <util/BitUtil.h>
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
    const index_t BASE_OFFSET = 256;

    std::string channelData = "channelData";

    ASSERT_NO_THROW({
        ImageMessageFlyweight cmd(ab, BASE_OFFSET);
    });

    ASSERT_NO_THROW({
        ImageBuffersReadyFlyweight cmd(ab, BASE_OFFSET);
    });

    ASSERT_NO_THROW({
        RemoveMessageFlyweight cmd(ab, BASE_OFFSET);
    });

    ASSERT_NO_THROW({
        SubscriptionMessageFlyweight cmd(ab, BASE_OFFSET);
    });

    ASSERT_NO_THROW({
        PublicationMessageFlyweight cmd(ab, BASE_OFFSET);
    });

    ASSERT_NO_THROW({
        PublicationBuffersReadyFlyweight cmd(ab, BASE_OFFSET);
    });
}

TEST (commandTests, testImageMessageFlyweight)
{
    clearBuffer();
    AtomicBuffer ab (&testBuffer[0], testBuffer.size());
    const index_t BASE_OFFSET = 256;

    std::string channelData = "channelData";

    ASSERT_NO_THROW({
        ImageMessageFlyweight cmd (ab, BASE_OFFSET);
        cmd.correlationId(1).subscriptionRegistrationId(2).streamId(3).channel(channelData);

        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 0), 1);
        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 8), 2);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 16), 3);
        ASSERT_EQ(ab.getString(BASE_OFFSET + 20), channelData);

        ASSERT_EQ(cmd.correlationId(), 1);
        ASSERT_EQ(cmd.streamId(), 3);
        ASSERT_EQ(cmd.channel(), channelData);

        ASSERT_EQ(cmd.length(), static_cast<int>(20 + sizeof(std::int32_t) + channelData.length()));
    });
}


TEST (commandTests, testPublicationReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASE_OFFSET = 256;

    std::string logFileNameData = "logfilenamedata";

    ASSERT_NO_THROW({
        PublicationBuffersReadyFlyweight cmd(ab, BASE_OFFSET);

        cmd.correlationId(-1).registrationId(1).streamId(0x01010101).sessionId(0x02020202).positionLimitCounterId(10);
        cmd.channelStatusIndicatorId(11);
        cmd.logFileName(logFileNameData);

        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 0), -1);
        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 8), 1);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 16), 0x02020202);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 20), 0x01010101);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 24), 10);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 28), 11);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 32), static_cast<int>(logFileNameData.length()));
        ASSERT_EQ(ab.getString(BASE_OFFSET + 32), logFileNameData);

        ASSERT_EQ(cmd.correlationId(), -1);
        ASSERT_EQ(cmd.registrationId(), 1);
        ASSERT_EQ(cmd.streamId(), 0x01010101);
        ASSERT_EQ(cmd.sessionId(), 0x02020202);
        ASSERT_EQ(cmd.positionLimitCounterId(), 10);
        ASSERT_EQ(cmd.logFileName(), logFileNameData);

        ASSERT_EQ(cmd.length(), static_cast<int>(32 + sizeof(std::int32_t) + logFileNameData.length()));
    });
}

TEST (commandTests, testImageBuffersReadyFlyweight)
{
    clearBuffer();
    AtomicBuffer ab(&testBuffer[0], testBuffer.size());
    const index_t BASE_OFFSET = 0;

    std::string logFileNameData = "logfilenamedata";
    std::string sourceInfoData = "sourceinfodata";

    ASSERT_NO_THROW({
        ImageBuffersReadyFlyweight cmd(ab, BASE_OFFSET);

        cmd.correlationId(-1);

        cmd.sessionId(0x02020202)
            .streamId(0x01010101)
            .subscriberRegistrationId(2)
            .subscriberPositionId(1)
            .logFileName(logFileNameData)
            .sourceIdentity(sourceInfoData);

        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 0), -1);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 8), 0x02020202);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 12), 0x01010101);

        ASSERT_EQ(ab.getInt64(BASE_OFFSET + 16), 2);
        ASSERT_EQ(ab.getInt32(BASE_OFFSET + 24), 1);

        const index_t startOfLogFileName = BASE_OFFSET + 28;
        ASSERT_EQ(ab.getStringLength(startOfLogFileName), static_cast<int>(logFileNameData.length()));
        ASSERT_EQ(ab.getString(startOfLogFileName), logFileNameData);

        const index_t startOfSourceIdentity = startOfLogFileName + 4 + (index_t)logFileNameData.length();
        const index_t startOfSourceIdentityAligned = BitUtil::align(startOfSourceIdentity, 4);
        ASSERT_EQ(ab.getStringLength(startOfSourceIdentityAligned), static_cast<int>(sourceInfoData.length()));
        ASSERT_EQ(ab.getString(startOfSourceIdentityAligned), sourceInfoData);

        ASSERT_EQ(cmd.correlationId(), -1);
        ASSERT_EQ(cmd.sessionId(), 0x02020202);
        ASSERT_EQ(cmd.streamId(), 0x01010101);
        ASSERT_EQ(cmd.subscriberRegistrationId(), 2);
        ASSERT_EQ(cmd.subscriberPositionId(), 1);
        ASSERT_EQ(cmd.logFileName(), logFileNameData);
        ASSERT_EQ(cmd.sourceIdentity(), sourceInfoData);

        int expectedLength = static_cast<int>(
            startOfSourceIdentityAligned + sizeof(std::int32_t) + sourceInfoData.length());

        ASSERT_EQ(cmd.length(), expectedLength);
    });
}
