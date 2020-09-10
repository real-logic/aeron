/*
 * Copyright 2014-2020 Real Logic Limited.
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

#if defined(__linux__) || defined(Darwin)

#include <unistd.h>
#include <ftw.h>
#include <cstdio>
#include <spawn.h>
#include <pthread.h>
#elif defined(_WIN32)
#include <process.h>
#include <Windows.h>
typedef intptr_t pid_t;
#else
#error "must spawn Java archive per test"
#endif

#include <chrono>
#include <thread>
#include <iostream>
#include <iosfwd>
#include <vector>
#include <cstring>

#include <gtest/gtest.h>

#include "ChannelUriStringBuilder.h"
#include "client/AeronArchive.h"
#include "client/RecordingEventsAdapter.h"
#include "client/RecordingPos.h"
#include "client/ReplayMerge.h"
#include "ArchiveTestUtil.h"

using namespace aeron;
using namespace aeron::util;
using namespace aeron::archive::client;

#ifdef _WIN32
int aeron_delete_directory(const char *dir)
{
    SHFILEOPSTRUCT file_op =
    {
        nullptr,
        FO_DELETE,
        dir,
        "",
        FOF_NOCONFIRMATION | FOF_NOERRORUI | FOF_SILENT,
        false,
        nullptr,
        ""
    };

    return SHFileOperation(&file_op);
}

#else

static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
{
    if (remove(path) != 0)
    {
        perror("remove");
    }

    return 0;
}

int aeron_delete_directory(const char *dirname)
{
    return nftw(dirname, unlink_func, 64, FTW_DEPTH | FTW_PHYS);
}

#endif

class AeronArchiveTest : public testing::Test
{
public:
    ~AeronArchiveTest() override
    {
        if (m_debug)
        {
            std::cout << m_stream.str();
        }
    }

    void SetUp() final
    {
        std::string archiveDirArg = "-Daeron.archive.dir=" + m_archiveDir;
        char const * const argv[] =
        {
            "java",
#if JAVA_MAJOR_VERSION >= 9
            "--add-opens",
            "java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.net=ALL-UNNAMED",
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
#endif
            "-Daeron.dir.delete.on.start=true",
            "-Daeron.dir.delete.on.shutdown=true",
            "-Daeron.archive.dir.delete.on.start=true",
            "-Daeron.archive.max.catalog.entries=1024",
            "-Daeron.threading.mode=INVOKER",
            "-Daeron.archive.threading.mode=SHARED",
            "-Daeron.archive.recording.events.enabled=false",
            "-Daeron.term.buffer.sparse.file=true",
            "-Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator",
            "-Daeron.term.buffer.length=64k",
            "-Daeron.archive.authenticator.supplier=io.aeron.samples.archive.SampleAuthenticatorSupplier",
            archiveDirArg.c_str(),
            "-cp",
            m_aeronAllJar.c_str(),
            "io.aeron.archive.ArchivingMediaDriver",
            nullptr
        };
 
        #if defined(_WIN32)
        m_pid = spawnv(P_NOWAIT, m_java.c_str(), &argv[0]);
        #else
        m_pid = -1;
        if (0 != posix_spawn(&m_pid, m_java.c_str(), nullptr, nullptr, (char * const *)&argv[0], nullptr))
        {
            perror("spawn");
            ::exit(EXIT_FAILURE);
        }
        #endif

        if (m_pid < 0)
        {
            perror("spawn");
            ::exit(EXIT_FAILURE);
        }

        auto onEncodedCredentials =
            []() -> std::pair<const char *, std::uint32_t>
            {
                std::string credentials("admin:admin");

                char *arr = new char[credentials.length() + 1];
                std::memcpy(arr, credentials.data(), credentials.length());
                arr[credentials.length()] = '\0';

                return { arr, (std::uint32_t)credentials.length() };
            };

        m_context.credentialsSupplier(CredentialsSupplier(onEncodedCredentials));

        m_stream << "ArchivingMediaDriver PID " << std::to_string(m_pid) << std::endl;
    }

    void TearDown() final
    {
        if (0 != m_pid)
        {
            m_stream << "Shutting down PID " << m_pid << std::endl;

            const std::string cncFilename = m_context.aeron()->context().cncFileName();
            const std::string aeronPath = aeron::Context::defaultAeronPath();
            m_context.aeron(nullptr);

            if (aeron::Context::requestDriverTermination(aeronPath, nullptr, 0))
            {
                m_stream << "Waiting for driver termination" << std::endl;

                const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_1(1);
                while (test::fileExists(cncFilename.c_str()))
                {
                    std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
                }
            }
            else
            {
                m_stream << "Failed to send driver terminate command" << std::endl;
            }

            m_stream << "Deleting " << m_archiveDir << std::endl;
            aeron_delete_directory(m_archiveDir.c_str());
        }
    }

    static std::shared_ptr<Publication> addPublication(Aeron &aeron, const std::string &channel, std::int32_t streamId)
    {
        std::int64_t publicationId = aeron.addPublication(channel, streamId);
        std::shared_ptr<Publication> publication = aeron.findPublication(publicationId);
        aeron::concurrent::YieldingIdleStrategy idle;
        while (!publication)
        {
            idle.idle();
            publication = aeron.findPublication(publicationId);
        }

        return publication;
    }

    static std::shared_ptr<Subscription> addSubscription(
        Aeron &aeron, const std::string &channel, std::int32_t streamId)
    {
        std::int64_t subscriptionId = aeron.addSubscription(channel, streamId);
        std::shared_ptr<Subscription> subscription = aeron.findSubscription(subscriptionId);
        aeron::concurrent::YieldingIdleStrategy idleStrategy;
        while (!subscription)
        {
            idleStrategy.idle();
            subscription = aeron.findSubscription(subscriptionId);
        }

        return subscription;
    }

    static std::int32_t getRecordingCounterId(std::int32_t sessionId, CountersReader &countersReader)
    {
        std::int32_t counterId;
        while (CountersReader::NULL_COUNTER_ID ==
            (counterId = RecordingPos::findCounterIdBySessionId(countersReader, sessionId)))
        {
            std::this_thread::yield();
        }

        return counterId;
    }

    static void offerMessages(Publication &publication, std::size_t messageCount, const std::string &messagePrefix)
    {
        BufferClaim bufferClaim;
        aeron::concurrent::YieldingIdleStrategy idleStrategy;

        for (std::size_t i = 0; i < messageCount; i++)
        {
            const std::string message = messagePrefix + std::to_string(i);
            while (publication.tryClaim(static_cast<util::index_t>(message.length()), bufferClaim) < 0)
            {
                idleStrategy.idle();
            }

            bufferClaim.buffer().putStringWithoutLength(bufferClaim.offset(), message);
            bufferClaim.commit();
        }
    }

    void consumeMessages(Subscription &subscription, std::size_t messageCount, const std::string &messagePrefix) const
    {
        std::size_t received = 0;
        aeron::concurrent::YieldingIdleStrategy idleStrategy;

        fragment_handler_t handler =
            [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
            {
                const std::string expected = messagePrefix + std::to_string(received);
                const std::string actual = buffer.getStringWithoutLength(offset, static_cast<std::size_t>(length));

                EXPECT_EQ(expected, actual);

                received++;
            };

        while (received < messageCount)
        {
            if (0 == subscription.poll(handler, m_fragmentLimit))
            {
                idleStrategy.idle();
            }
        }

        ASSERT_EQ(received, messageCount);
    }

    bool attemptReplayMerge(
        std::shared_ptr<AeronArchive> aeronArchive,
        std::shared_ptr<Publication> publication,
        fragment_handler_t &handler,
        const std::string &messagePrefix,
        ChannelUriStringBuilder &subscriptionChannel,
        ChannelUriStringBuilder &replayChannel,
        ChannelUriStringBuilder &replayDestination,
        ChannelUriStringBuilder &liveDestination,
        std::int64_t recordingId,
        std::size_t totalMessageCount,
        std::size_t &messagesPublished,
        std::size_t &receivedMessageCount,
        std::int64_t receivedPosition)
    {
        aeron::concurrent::YieldingIdleStrategy idleStrategy;
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), subscriptionChannel.build(), m_recordingStreamId);

        ReplayMerge replayMerge(
            subscription,
            aeronArchive,
            replayChannel.build(),
            replayDestination.build(),
            liveDestination.build(),
            recordingId,
            receivedPosition);

        for (std::size_t i = messagesPublished; i < totalMessageCount; i++)
        {
            BufferClaim bufferClaim;
            const std::string message = messagePrefix + std::to_string(i);

            idleStrategy.reset();
            while (publication->tryClaim(static_cast<util::index_t>(message.length()), bufferClaim) < 0)
            {
                idleStrategy.idle();
                int fragments = replayMerge.poll(handler, m_fragmentLimit);
                if (0 == fragments && replayMerge.hasFailed())
                {
                    return false;
                }
            }

            bufferClaim.buffer().putStringWithoutLength(bufferClaim.offset(), message);
            bufferClaim.commit();
            ++messagesPublished;

            int fragments = replayMerge.poll(handler, m_fragmentLimit);
            if (0 == fragments && replayMerge.hasFailed())
            {
                return false;
            }
        }

        while (!replayMerge.isMerged())
        {
            int fragments = replayMerge.poll(handler, m_fragmentLimit);
            if (0 == fragments && replayMerge.hasFailed())
            {
                return false;
            }
            idleStrategy.idle(fragments);
        }

        Image &image = *replayMerge.image();
        while (receivedMessageCount < totalMessageCount)
        {
            int fragments = image.poll(handler, m_fragmentLimit);
            if (0 == fragments && image.isClosed())
            {
                return false;
            }
            idleStrategy.idle(fragments);
        }
        
        return true;
    }

protected:
    const std::string m_java = JAVA_EXECUTABLE;
    const std::string m_aeronAllJar = AERON_ALL_JAR;
    const std::string m_archiveDir = ARCHIVE_DIR;

    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333|term-length=64k";
    const std::int32_t m_recordingStreamId = 33;
    const std::string m_replayChannel = "aeron:udp?endpoint=localhost:6666";
    const std::int32_t m_replayStreamId = 66;

    const int m_fragmentLimit = 10;
    AeronArchive::Context_t m_context;
    pid_t m_pid = -1;
    std::ostringstream m_stream;
    bool m_debug = false;
};

TEST_F(AeronArchiveTest, shouldAsyncConnectToArchive)
{
    std::shared_ptr<AeronArchive::AsyncConnect> asyncConnect = AeronArchive::asyncConnect(m_context);
    aeron::concurrent::YieldingIdleStrategy idle;
    std::uint8_t previousStep = asyncConnect->step();

    std::shared_ptr<AeronArchive> aeronArchive = asyncConnect->poll();
    while (!aeronArchive)
    {
        if (asyncConnect->step() == previousStep)
        {
            idle.idle();
        }
        else
        {
            idle.reset();
            previousStep = asyncConnect->step();
        }

        aeronArchive = asyncConnect->poll();
    }

    aeronArchive->checkForErrorResponse();
}

TEST_F(AeronArchiveTest, shouldConnectToArchive)
{
    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);
    aeronArchive->checkForErrorResponse();
}

TEST_F(AeronArchiveTest, shouldRecordPublicationAndFindRecording)
{
    const std::string messagePrefix = "Message ";
    const std::size_t messageCount = 10;
    std::int32_t sessionId;
    std::int64_t recordingIdFromCounter;
    std::int64_t stopPosition;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    const std::int64_t subscriptionId = aeronArchive->startRecording(
        m_recordingChannel, m_recordingStreamId, AeronArchive::SourceLocation::LOCAL);

    {
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);
        std::shared_ptr<Publication> publication = addPublication(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);

        sessionId = publication->sessionId();

        CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
        const std::int32_t counterId = getRecordingCounterId(sessionId, countersReader);
        recordingIdFromCounter = RecordingPos::getRecordingId(countersReader, counterId);

        offerMessages(*publication, messageCount, messagePrefix);
        consumeMessages(*subscription, messageCount, messagePrefix);

        stopPosition = publication->position();

        aeron::concurrent::YieldingIdleStrategy idle;
        while (countersReader.getCounterValue(counterId) < stopPosition)
        {
            idle.idle();
        }

        EXPECT_EQ(aeronArchive->getRecordingPosition(recordingIdFromCounter), stopPosition);
        EXPECT_EQ(aeronArchive->getStopPosition(recordingIdFromCounter), aeron::NULL_VALUE);
    }

    aeronArchive->stopRecording(subscriptionId);

    const std::int64_t recordingId = aeronArchive->findLastMatchingRecording(
        0, "endpoint=localhost:3333", m_recordingStreamId, sessionId);

    EXPECT_EQ(recordingIdFromCounter, recordingId);
    EXPECT_EQ(aeronArchive->getStopPosition(recordingIdFromCounter), stopPosition);

    const std::int32_t count = aeronArchive->listRecording(
        recordingId,
        [&](std::int64_t controlSessionId,
            std::int64_t correlationId,
            std::int64_t recordingId1,
            std::int64_t startTimestamp,
            std::int64_t stopTimestamp,
            std::int64_t startPosition,
            std::int64_t newStopPosition,
            std::int32_t initialTermId,
            std::int32_t segmentFileLength,
            std::int32_t termBufferLength,
            std::int32_t mtuLength,
            std::int32_t sessionId1,
            std::int32_t streamId,
            const std::string &strippedChannel,
            const std::string &originalChannel,
            const std::string &sourceIdentity)
        {
            EXPECT_EQ(recordingId, recordingId1);
            EXPECT_EQ(streamId, m_recordingStreamId);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(AeronArchiveTest, shouldRecordThenReplay)
{
    const std::string messagePrefix = "Message ";
    const std::size_t messageCount = 10;
    std::int32_t sessionId;
    std::int64_t recordingIdFromCounter;
    std::int64_t stopPosition;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    const std::int64_t subscriptionId = aeronArchive->startRecording(
        m_recordingChannel, m_recordingStreamId, AeronArchive::SourceLocation::LOCAL);

    {
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);
        std::shared_ptr<Publication> publication = addPublication(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);

        sessionId = publication->sessionId();

        CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
        const std::int32_t counterId = getRecordingCounterId(sessionId, countersReader);
        recordingIdFromCounter = RecordingPos::getRecordingId(countersReader, counterId);

        offerMessages(*publication, messageCount, messagePrefix);
        consumeMessages(*subscription, messageCount, messagePrefix);

        stopPosition = publication->position();

        aeron::concurrent::YieldingIdleStrategy idle;
        while (countersReader.getCounterValue(counterId) < stopPosition)
        {
            idle.idle();
        }

        EXPECT_EQ(aeronArchive->getRecordingPosition(recordingIdFromCounter), stopPosition);
    }

    aeronArchive->stopRecording(subscriptionId);

    EXPECT_EQ(aeronArchive->getStopPosition(recordingIdFromCounter), stopPosition);

    {
        const std::int64_t position = 0L;
        const std::int64_t length = stopPosition - position;
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_replayChannel, m_replayStreamId);

        aeronArchive->startReplay(recordingIdFromCounter, position, length, m_replayChannel, m_replayStreamId);

        consumeMessages(*subscription, messageCount, messagePrefix);
        EXPECT_EQ(stopPosition, subscription->imageByIndex(0)->position());
    }
}

TEST_F(AeronArchiveTest, shouldRecordThenReplayThenTruncate)
{
    const std::string messagePrefix = "Message ";
    const std::size_t messageCount = 10;
    std::int32_t sessionId;
    std::int64_t recordingIdFromCounter;
    std::int64_t stopPosition;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    const std::int64_t subscriptionId = aeronArchive->startRecording(
        m_recordingChannel, m_recordingStreamId, AeronArchive::SourceLocation::LOCAL);

    {
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);
        std::shared_ptr<Publication> publication = addPublication(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);

        sessionId = publication->sessionId();

        CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
        const std::int32_t counterId = getRecordingCounterId(sessionId, countersReader);
        recordingIdFromCounter = RecordingPos::getRecordingId(countersReader, counterId);

        offerMessages(*publication, messageCount, messagePrefix);
        consumeMessages(*subscription, messageCount, messagePrefix);

        stopPosition = publication->position();

        aeron::concurrent::YieldingIdleStrategy idle;
        while (countersReader.getCounterValue(counterId) < stopPosition)
        {
            idle.idle();
        }

        EXPECT_EQ(aeronArchive->getRecordingPosition(recordingIdFromCounter), stopPosition);
        EXPECT_EQ(aeronArchive->getStopPosition(recordingIdFromCounter), aeron::NULL_VALUE);
    }

    aeronArchive->stopRecording(subscriptionId);

    const std::int64_t recordingId = aeronArchive->findLastMatchingRecording(
        0, "endpoint=localhost:3333", m_recordingStreamId, sessionId);

    EXPECT_EQ(recordingIdFromCounter, recordingId);
    EXPECT_EQ(aeronArchive->getStopPosition(recordingIdFromCounter), stopPosition);

    const std::int64_t position = 0L;

    {
        const std::int64_t length = stopPosition - position;
        std::shared_ptr<Subscription> subscription = aeronArchive->replay(
            recordingId, position, length, m_replayChannel, m_replayStreamId);

        consumeMessages(*subscription, messageCount, messagePrefix);
        EXPECT_EQ(stopPosition, subscription->imageByIndex(0)->position());
    }

    aeronArchive->truncateRecording(recordingId, position);

    const std::int32_t count = aeronArchive->listRecording(
        recordingId,
        [&](std::int64_t controlSessionId,
            std::int64_t correlationId,
            std::int64_t recordingId1,
            std::int64_t startTimestamp,
            std::int64_t stopTimestamp,
            std::int64_t startPosition,
            std::int64_t newStopPosition,
            std::int32_t initialTermId,
            std::int32_t segmentFileLength,
            std::int32_t termBufferLength,
            std::int32_t mtuLength,
            std::int32_t sessionId1,
            std::int32_t streamId,
            const std::string &strippedChannel,
            const std::string &originalChannel,
            const std::string &sourceIdentity)
        {
            EXPECT_EQ(startPosition, newStopPosition);
        });

    EXPECT_EQ(count, 1);
}

TEST_F(AeronArchiveTest, shouldRecordAndCancelReplayEarly)
{
    const std::string messagePrefix = "Message ";
    const std::size_t messageCount = 10;
    std::int64_t recordingId;
    std::int64_t stopPosition;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    {
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);
        std::shared_ptr<Publication> publication = aeronArchive->addRecordedPublication(
            m_recordingChannel, m_recordingStreamId);

        CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
        const std::int32_t counterId = getRecordingCounterId(publication->sessionId(), countersReader);
        recordingId = RecordingPos::getRecordingId(countersReader, counterId);

        offerMessages(*publication, messageCount, messagePrefix);
        consumeMessages(*subscription, messageCount, messagePrefix);

        stopPosition = publication->position();

        aeron::concurrent::YieldingIdleStrategy idle;
        while (countersReader.getCounterValue(counterId) < stopPosition)
        {
            idle.idle();
        }

        EXPECT_EQ(aeronArchive->getRecordingPosition(recordingId), stopPosition);

        aeronArchive->stopRecording(publication);

        while (NULL_POSITION != aeronArchive->getRecordingPosition(recordingId))
        {
            idle.idle();
        }
    }

    const std::int64_t position = 0L;
    const std::int64_t length = stopPosition - position;

    const std::int64_t replaySessionId = aeronArchive->startReplay(
        recordingId, position, length, m_replayChannel, m_replayStreamId);

    aeronArchive->stopReplay(replaySessionId);
}

TEST_F(AeronArchiveTest, shouldReplayRecordingFromLateJoinPosition)
{
    const std::string messagePrefix = "Message ";
    const std::size_t messageCount = 10;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    aeronArchive->startRecording(
        m_recordingChannel, m_recordingStreamId, AeronArchive::SourceLocation::LOCAL, true);

    {
        std::shared_ptr<Subscription> subscription = addSubscription(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);
        std::shared_ptr<Publication> publication = addPublication(
            *aeronArchive->context().aeron(), m_recordingChannel, m_recordingStreamId);

        CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
        const std::int32_t counterId = getRecordingCounterId(publication->sessionId(), countersReader);
        const std::int64_t recordingId = RecordingPos::getRecordingId(countersReader, counterId);

        offerMessages(*publication, messageCount, messagePrefix);
        consumeMessages(*subscription, messageCount, messagePrefix);

        const std::int64_t currentPosition = publication->position();

        aeron::concurrent::YieldingIdleStrategy idle;
        while (countersReader.getCounterValue(counterId) < currentPosition)
        {
            idle.idle();
        }

        {
            std::shared_ptr<Subscription> replaySubscription = aeronArchive->replay(
                recordingId, currentPosition, NULL_LENGTH, m_replayChannel, m_replayStreamId);

            offerMessages(*publication, messageCount, messagePrefix);
            consumeMessages(*subscription, messageCount, messagePrefix);
            consumeMessages(*replaySubscription, messageCount, messagePrefix);

            const std::int64_t endPosition = publication->position();
            EXPECT_EQ(endPosition, replaySubscription->imageByIndex(0)->position());
        }
    }
}

struct SubscriptionDescriptor
{
    const std::int64_t m_controlSessionId;
    const std::int64_t m_correlationId;
    const std::int64_t m_subscriptionId;
    const std::int32_t m_streamId;
    const std::string m_strippedChannel;

    SubscriptionDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t subscriptionId,
        std::int32_t streamId,
        const std::string &strippedChannel) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_subscriptionId(subscriptionId),
        m_streamId(streamId),
        m_strippedChannel(strippedChannel)
    {
    }
};

TEST_F(AeronArchiveTest, shouldListRegisteredRecordingSubscriptions)
{
    std::vector<SubscriptionDescriptor> descriptors;
    recording_subscription_descriptor_consumer_t consumer =
        [&](std::int64_t controlSessionId,
            std::int64_t correlationId,
            std::int64_t subscriptionId,
            std::int32_t streamId,
            const std::string &strippedChannel)
        {
            descriptors.emplace_back(controlSessionId, correlationId, subscriptionId, streamId, strippedChannel);
        };

    const std::int32_t expectedStreamId = 7;
    const std::string channelOne = "aeron:ipc";
    const std::string channelTwo = "aeron:udp?endpoint=localhost:5678";
    const std::string channelThree = "aeron:udp?endpoint=localhost:4321";

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);

    const std::int64_t subIdOne = aeronArchive->startRecording(
        channelOne, expectedStreamId, AeronArchive::SourceLocation::LOCAL);
    const std::int64_t subIdTwo = aeronArchive->startRecording(
        channelTwo, expectedStreamId + 1, AeronArchive::SourceLocation::LOCAL);
    const std::int64_t subIdThree = aeronArchive->startRecording(
        channelThree, expectedStreamId + 2, AeronArchive::SourceLocation::LOCAL);

    const std::int32_t countOne = aeronArchive->listRecordingSubscriptions(
        0, 5, "ipc", expectedStreamId, true, consumer);

    EXPECT_EQ(1uL, descriptors.size());
    EXPECT_EQ(1L, countOne);

    descriptors.clear();

    const std::int32_t countTwo = aeronArchive->listRecordingSubscriptions(
        0, 5, "", expectedStreamId, false, consumer);

    EXPECT_EQ(3uL, descriptors.size());
    EXPECT_EQ(3L, countTwo);

    aeronArchive->stopRecording(subIdTwo);
    descriptors.clear();

    const std::int32_t countThree = aeronArchive->listRecordingSubscriptions(
        0, 5, "", expectedStreamId, false, consumer);

    EXPECT_EQ(2uL, descriptors.size());
    EXPECT_EQ(2L, countThree);

    EXPECT_EQ(1L, std::count_if(
        descriptors.begin(),
        descriptors.end(),
        [=](SubscriptionDescriptor s){ return s.m_subscriptionId == subIdOne;}));

    EXPECT_EQ(1L, std::count_if(
        descriptors.begin(),
        descriptors.end(),
        [=](SubscriptionDescriptor s){ return s.m_subscriptionId == subIdThree;}));
}

TEST_F(AeronArchiveTest, shouldMergeFromReplayToLive)
{
    const std::size_t termLength = 64 * 1024;
    const std::string messagePrefix = "Message ";
    const std::size_t minMessagesPerTerm = termLength / (messagePrefix.length() + DataFrameHeader::LENGTH);
    const std::string controlEndpoint = "localhost:23265";
    const std::string recordingEndpoint = "localhost:23266";
    const std::string liveEndpoint = "localhost:23267";
    const std::string replayEndpoint = "localhost:23268";

    ChannelUriStringBuilder publicationChannel, recordingChannel, subscriptionChannel;
    ChannelUriStringBuilder liveDestination, replayDestination, replayChannel;

    publicationChannel
        .media(UDP_MEDIA)
        .tags("1,2")
        .controlEndpoint(controlEndpoint)
        .controlMode(MDC_CONTROL_MODE_DYNAMIC)
        .flowControl("tagged,g:99901/1,t:5s")
        .termLength(termLength);

    recordingChannel
        .media(UDP_MEDIA)
        .groupTag(99901)
        .endpoint(recordingEndpoint)
        .controlEndpoint(controlEndpoint);

    subscriptionChannel
        .media(UDP_MEDIA)
        .controlMode(MDC_CONTROL_MODE_MANUAL);

    liveDestination
        .media(UDP_MEDIA)
        .endpoint(liveEndpoint)
        .controlEndpoint(controlEndpoint);

    replayDestination
        .media(UDP_MEDIA)
        .endpoint(replayEndpoint);

    replayChannel
        .media(UDP_MEDIA)
        .isSessionIdTagged(true)
        .sessionId(2)
        .endpoint(replayEndpoint);

    const std::size_t initialMessageCount = minMessagesPerTerm * 3;
    const std::size_t subsequentMessageCount = minMessagesPerTerm * 3;
    const std::size_t totalMessageCount = initialMessageCount + subsequentMessageCount;
    aeron::concurrent::YieldingIdleStrategy idleStrategy;

    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);
    std::shared_ptr<Publication> publication = addPublication(
        *aeronArchive->context().aeron(), publicationChannel.build(), m_recordingStreamId);

    const std::int32_t sessionId = publication->sessionId();
    recordingChannel.sessionId(sessionId);
    subscriptionChannel.sessionId(sessionId);

    aeronArchive->startRecording(
        recordingChannel.build(), m_recordingStreamId, AeronArchive::SourceLocation::REMOTE, true);

    CountersReader &countersReader = aeronArchive->context().aeron()->countersReader();
    const std::int32_t counterId = getRecordingCounterId(sessionId, countersReader);
    const std::int64_t recordingId = RecordingPos::getRecordingId(countersReader, counterId);

    offerMessages(*publication, initialMessageCount, messagePrefix);
    while (countersReader.getCounterValue(counterId) < publication->position())
    {
        idleStrategy.idle();
    }

    std::size_t messagesPublished = initialMessageCount;
    std::size_t receivedMessageCount = 0;
    std::int64_t receivedPosition = 0;

    fragment_handler_t handler =
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            const std::string expected = messagePrefix + std::to_string(receivedMessageCount);
            const std::string actual = buffer.getStringWithoutLength(offset, static_cast<std::size_t>(length));

            EXPECT_EQ(expected, actual);

            receivedMessageCount++;
            receivedPosition = header.position();
        };

    while (!attemptReplayMerge(
        aeronArchive,
        publication,
        handler,
        messagePrefix,
        subscriptionChannel,
        replayChannel,
        replayDestination,
        liveDestination,
        recordingId,
        totalMessageCount,
        messagesPublished,
        receivedMessageCount,
        receivedPosition))
    {
        idleStrategy.idle();
    }

    EXPECT_EQ(receivedMessageCount, totalMessageCount);
    EXPECT_EQ(receivedPosition, publication->position());
}

TEST_F(AeronArchiveTest, shouldExceptionForIncorrectInitialCredentials)
{
    auto onEncodedCredentials =
        []() -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:NotAdmin");

            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';

            return { arr, (std::uint32_t)credentials.length() };
        };

    m_context.credentialsSupplier(CredentialsSupplier(onEncodedCredentials));

    ASSERT_THROW(
        {
            std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);
        },
        ArchiveException);
}

TEST_F(AeronArchiveTest, shouldBeAbleToHandleBeingChallenged)
{
    auto onEncodedCredentials =
        []() -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:adminC");

            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';

            return { arr, (std::uint32_t)credentials.length() };
        };

    auto onChallenge =
        [](std::pair<const char *, std::uint32_t> encodedChallenge) -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:CSadmin");

            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';

            return { arr, (std::uint32_t)credentials.length() };
        };

    m_context.credentialsSupplier(CredentialsSupplier(onEncodedCredentials, onChallenge));

    ASSERT_NO_THROW(
        {
            std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);
        });
}

TEST_F(AeronArchiveTest, shouldExceptionForIncorrectChallengeCredentials)
{
    auto onEncodedCredentials =
        []() -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:adminC");

            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';

            return { arr, (std::uint32_t)credentials.length() };
        };

    auto onChallenge =
        [](std::pair<const char *, std::uint32_t> encodedChallenge) -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:adminNoCS");

            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';

            return { arr, (std::uint32_t)credentials.length() };
        };

    m_context.credentialsSupplier(CredentialsSupplier(onEncodedCredentials, onChallenge));

    ASSERT_THROW(
        {
            std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect(m_context);
        },
        ArchiveException);
}
