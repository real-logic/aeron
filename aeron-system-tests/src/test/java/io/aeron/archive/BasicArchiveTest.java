/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.ArrayList;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class BasicArchiveTest
{
    private static final String RECORDED_CHANNEL_ALIAS = "named-log";
    private static final int RECORDED_STREAM_ID = 33;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(ArchiveSystemTests.TERM_LENGTH)
        .alias(RECORDED_CHANNEL_ALIAS)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private File archiveDir;

    @BeforeEach
    public void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        final Archive.Context archiveCtx = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try
        {
            driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
            archive = Archive.launch(archiveCtx);
        }
        finally
        {
            systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
            systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());
        }

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldRecordThenReplayThenTruncate()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, sessionId);
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
        }

        aeronArchive.stopRecording(subscriptionId);

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);

        assertFalse(aeronArchive.tryStopRecordingByIdentity(recordingId));

        assertEquals(recordingIdFromCounter, recordingId);
        assertEquals(stopPosition, aeronArchive.getStopPosition(recordingIdFromCounter));

        final long position = 0L;
        final long length = stopPosition - position;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, messageCount, messagePrefix);
            assertEquals(stopPosition, subscription.imageAtIndex(0).position());
        }

        aeronArchive.truncateRecording(recordingId, position);

        final int count = aeronArchive.listRecording(
            recordingId,
            (controlSessionId,
            correlationId,
            recordingId1,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId1,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> assertEquals(startPosition, newStopPosition));

        assertEquals(1, count);
    }

    @Test
    @InterruptAfter(10)
    void jumboRecordingDescriptorEndToEndTest()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final String recordedChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .termLength(TERM_LENGTH)
            .alias(Tests.generateStringWithSuffix("alias", "X", 2000))
            .build();

        final long subscriptionId = aeronArchive.startRecording(recordedChannel, RECORDED_STREAM_ID, LOCAL);
        final long recordingId;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(recordedChannel, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(recordedChannel, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, sessionId);
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingId));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingId));
        }

        aeronArchive.stopRecording(subscriptionId);

        final int count = aeronArchive.listRecording(
            recordingId,
            (controlSessionId,
            correlationId,
            recordingId1,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId1,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> assertEquals(recordedChannel, originalChannel));

        assertEquals(1, count);
    }

    @Test
    @InterruptAfter(10)
    void purgeRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, sessionId);
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
        }

        aeronArchive.stopRecording(subscriptionId);

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);
        assertEquals(recordingIdFromCounter, recordingId);

        assertFalse(aeronArchive.tryStopRecordingByIdentity(recordingId));

        assertEquals(recordingIdFromCounter, recordingId);
        assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

        final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);

        assertNotEquals(0, segmentFiles.size());

        aeronArchive.purgeRecording(recordingId);

        final int count = aeronArchive.listRecording(
            recordingId,
            (controlSessionId,
            correlationId,
            recordingId1,
            startTimestamp,
            stopTimestamp,
            startPosition,
            newStopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId1,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> fail("Recording was not purged!"));

        assertEquals(0, count);
        Tests.await(() -> Catalog.listSegmentFiles(archiveDir, recordingId).isEmpty());

        for (final String segmentFile : segmentFiles)
        {
            assertFalse(new File(archiveDir, segmentFile).exists());
        }
    }

    @Test
    @InterruptAfter(10)
    void purgeRecordingFailsIfRecordingIsActive()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        try
        {
            final long recordingIdFromCounter;
            final int sessionId;

            try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
                Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
            {
                sessionId = publication.sessionId();

                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounterId(counters, sessionId);
                recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

                assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

                offer(publication, messageCount, messagePrefix);
                consume(subscription, messageCount, messagePrefix);

                stopPosition = publication.position();
                awaitPosition(counters, counterId, stopPosition);

                final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
                assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
                assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
                assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));

                final long recordingId = aeronArchive.findLastMatchingRecording(
                    0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);
                assertEquals(recordingIdFromCounter, recordingId);

                final ArchiveException exception = assertThrows(
                    ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
                assertThat(exception.getMessage(), endsWith("error: cannot purge active recording " + recordingId));

                final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
                assertNotEquals(0, segmentFiles.size());

                for (final String segmentFile : segmentFiles)
                {
                    assertTrue(new File(archiveDir, segmentFile).exists());
                }
            }
        }
        finally
        {
            aeronArchive.stopRecording(subscriptionId);
        }
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void purgeRecordingFailsIfThereAreActiveReplays()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long stopPosition;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, sessionId);
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
        }

        aeronArchive.stopRecording(subscriptionId);

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);
        assertEquals(recordingIdFromCounter, recordingId);
        assertEquals(stopPosition, aeronArchive.getStopPosition(recordingIdFromCounter));

        final long position = 0L;
        final long length = stopPosition - position;

        try (Subscription ignore = aeronArchive.replay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            final ArchiveException exception = assertThrows(
                ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
            assertThat(exception.getMessage(),
                endsWith("error: cannot purge recording with active replay " + recordingId));

            final ArrayList<String> segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
            assertNotEquals(0, segmentFiles.size());

            for (final String segmentFile : segmentFiles)
            {
                assertTrue(new File(archiveDir, segmentFile).exists());
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRecordReplayAndCancelReplayEarly()
    {
        final String messagePrefix = "Message-Prefix-";
        final long stopPosition;
        final int messageCount = 10;
        final long recordingId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeronArchive.addRecordedPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = ArchiveSystemTests.awaitRecordingCounterId(counters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingId));

            assertTrue(aeronArchive.tryStopRecordingByIdentity(recordingId));

            while (NULL_POSITION != aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.yield();
            }
        }

        final long position = 0L;
        final long length = stopPosition - position;

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        aeronArchive.stopReplay(replaySessionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplayRecordingFromLateJoinPosition()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = ArchiveSystemTests.awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            awaitPosition(counters, counterId, currentPosition);

            try (Subscription replaySubscription = aeronArchive.replay(
                recordingId, currentPosition, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, REPLAY_STREAM_ID))
            {
                offer(publication, messageCount, messagePrefix);
                consume(subscription, messageCount, messagePrefix);
                consume(replaySubscription, messageCount, messagePrefix);

                final long endPosition = publication.position();
                assertEquals(endPosition, replaySubscription.imageAtIndex(0).position());
            }
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldFindLastMatchingRecordingIdWithFullUri()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = ArchiveSystemTests.awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            awaitPosition(counters, counterId, currentPosition);

            final long lastMatchingRecording = aeronArchive.findLastMatchingRecording(
                0, RECORDED_CHANNEL, RECORDED_STREAM_ID, publication.sessionId());

            assertEquals(lastMatchingRecording, recordingId);
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @Test
    @InterruptAfter(10)
    void shouldReturnNullValueWithFindLastMatchingRecordingIdDoesNotFindTheRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = ArchiveSystemTests.awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            assertNotEquals(RecordingPos.NULL_RECORDING_ID, recordingId);

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            final long currentPosition = publication.position();
            awaitPosition(counters, counterId, currentPosition);

            final long lastMatchingRecording = aeronArchive.findLastMatchingRecording(
                0, RECORDED_CHANNEL, RECORDED_STREAM_ID, publication.sessionId() + 1);

            assertEquals(Aeron.NULL_VALUE, lastMatchingRecording);
        }

        aeronArchive.stopRecording(subscriptionId);
    }

    @SuppressWarnings("checkstyle:MethodLength")
    @Test
    @SlowTest
    @InterruptAfter(20)
    public void shouldRecordThenBoundReplayWithCounter()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 100;
        final long stopPosition;
        final int timeout = 3_000;

        final long subscriptionId = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;
        final int sessionId;

        try (Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            sessionId = publication.sessionId();

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, sessionId);
            recordingIdFromCounter = RecordingPos.getRecordingId(counters, counterId);

            assertEquals(CommonContext.IPC_CHANNEL, RecordingPos.getSourceIdentity(counters, counterId));

            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            stopPosition = publication.position();
            awaitPosition(counters, counterId, stopPosition);

            final long joinPosition = subscription.imageBySessionId(sessionId).joinPosition();
            assertEquals(joinPosition, aeronArchive.getStartPosition(recordingIdFromCounter));
            assertEquals(stopPosition, aeronArchive.getRecordingPosition(recordingIdFromCounter));
            assertEquals(NULL_VALUE, aeronArchive.getStopPosition(recordingIdFromCounter));
        }

        aeronArchive.stopRecording(subscriptionId);

        final long recordingId = aeronArchive.findLastMatchingRecording(
            0, "alias=" + RECORDED_CHANNEL_ALIAS, RECORDED_STREAM_ID, sessionId);

        final Counter boundingCounter = aeron.addCounter(
            AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID, "bounding counter");

        final RecordingDescriptorCollector recordingDescriptorCollector = new RecordingDescriptorCollector(1);
        assertEquals(1, aeronArchive.listRecording(recordingId, recordingDescriptorCollector.reset()));
        final RecordingDescriptor recordingDescriptor = recordingDescriptorCollector.descriptors().get(0);

        assertNotEquals(-1, recordingDescriptor.stopPosition());
        final long halfLength = (recordingDescriptor.stopPosition() - recordingDescriptor.startPosition()) / 2;
        final long halfPosition = recordingDescriptor.startPosition() + halfLength;
        final long tqPosition = recordingDescriptor.startPosition() + halfLength + (halfLength / 2);

        boundingCounter.setOrdered(0);

        final long replaySessionId = aeronArchive.startReplay(
            recordingId,
            recordingDescriptor.startPosition(),
            Long.MAX_VALUE,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            new ReplayParams().boundingLimitCounterId(boundingCounter.id()));

        final String channel = new ChannelUriStringBuilder(REPLAY_CHANNEL).sessionId((int)replaySessionId).build();

        long subscriptionPosition = 0;
        final MutableReference<Image> replayImage = new MutableReference<>();
        try (Subscription replaySubscription = aeron.addSubscription(
            channel, REPLAY_STREAM_ID, replayImage::set, image -> {}))
        {
            boundingCounter.setOrdered(halfPosition);

            final long halfPollDeadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < halfPollDeadline)
            {
                if (0 < replaySubscription.poll((buffer, offset, length, header) -> {}, 20))
                {
                    if (null != replayImage.get())
                    {
                        subscriptionPosition = replayImage.get().position();
                    }
                }

                assertThat(subscriptionPosition, Matchers.lessThanOrEqualTo(halfPosition));
            }

            assertThat(subscriptionPosition, Matchers.greaterThan(0L));

            boundingCounter.setOrdered(tqPosition);

            final long tqPollDeadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < tqPollDeadline)
            {
                if (0 < replaySubscription.poll((buffer, offset, length, header) -> {}, 20))
                {
                    if (null != replayImage.get())
                    {
                        subscriptionPosition = replayImage.get().position();
                    }
                }

                assertThat(subscriptionPosition, Matchers.lessThanOrEqualTo(tqPosition));
            }

            assertThat(subscriptionPosition, Matchers.greaterThan(halfPosition));
        }
    }
}
