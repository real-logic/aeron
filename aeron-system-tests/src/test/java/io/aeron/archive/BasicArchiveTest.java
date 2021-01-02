/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class BasicArchiveTest
{
    private static final int RECORDED_STREAM_ID = 33;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(ArchiveSystemTests.TERM_LENGTH)
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
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();
    private File archiveDir;

    @BeforeEach
    public void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            testWatcher);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archive = Archive.launch(
            new Archive.Context()
                .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
                .aeronDirectoryName(aeronDirectoryName)
                .errorHandler(Tests::onError)
                .deleteArchiveOnStart(true)
                .archiveDir(archiveDir)
                .fileSyncLevel(0)
                .errorHandler(Tests::onError)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);

        archive.context().deleteDirectory();
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldRecordThenReplayThenTruncate()
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
            0, "endpoint=localhost:3333", RECORDED_STREAM_ID, sessionId);

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
    @Timeout(10)
    public void jumboRecordingDescriptorEndToEndTest()
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
    @Timeout(10)
    public void purgeRecording()
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
            0, "endpoint=localhost:3333", RECORDED_STREAM_ID, sessionId);
        assertEquals(recordingIdFromCounter, recordingId);

        assertFalse(aeronArchive.tryStopRecordingByIdentity(recordingId));

        assertEquals(recordingIdFromCounter, recordingId);
        assertEquals(stopPosition, aeronArchive.getStopPosition(recordingId));

        final String[] segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
        assertNotNull(segmentFiles);
        assertNotEquals(0, segmentFiles.length);

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

        assertArrayEquals(new String[0], Catalog.listSegmentFiles(archiveDir, recordingId));

        for (final String segmentFile : segmentFiles)
        {
            assertFalse(new File(archiveDir, segmentFile).exists());
        }
    }

    @Test
    @Timeout(10)
    public void purgeRecordingFailsIfRecordingIsActive()
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
                    0, "endpoint=localhost:3333", RECORDED_STREAM_ID, sessionId);
                assertEquals(recordingIdFromCounter, recordingId);

                final ArchiveException exception = assertThrows(
                    ArchiveException.class, () -> aeronArchive.purgeRecording(recordingId));
                assertThat(exception.getMessage(), endsWith("error: cannot purge active recording " + recordingId));

                final String[] segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
                assertNotNull(segmentFiles);
                assertNotEquals(0, segmentFiles.length);

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
    @Timeout(10)
    public void purgeRecordingFailsIfThereAreActiveReplays()
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
            0, "endpoint=localhost:3333", RECORDED_STREAM_ID, sessionId);
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

            final String[] segmentFiles = Catalog.listSegmentFiles(archiveDir, recordingId);
            assertNotNull(segmentFiles);
            assertNotEquals(0, segmentFiles.length);

            for (final String segmentFile : segmentFiles)
            {
                assertTrue(new File(archiveDir, segmentFile).exists());
            }
        }
    }

    @Test
    @Timeout(10)
    public void shouldRecordReplayAndCancelReplayEarly()
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
    @Timeout(10)
    public void shouldReplayRecordingFromLateJoinPosition()
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
}
