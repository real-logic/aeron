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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.MediaDriverTestWatcher;
import io.aeron.test.TestMediaDriver;
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

        archive = Archive.launch(
            new Archive.Context()
                .maxCatalogEntries(ArchiveSystemTests.MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(aeronDirectoryName)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "archive"))
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
