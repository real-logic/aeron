/*
 * Copyright 2014-2018 Real Logic Limited.
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
import io.aeron.archive.client.ReplayMerge;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;

import static io.aeron.archive.Common.*;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.junit.jupiter.api.Assertions.*;

public class ReplayMergeTest
{
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final int MIN_MESSAGES_PER_TERM =
        TERM_LENGTH / (MESSAGE_PREFIX.length() + DataHeaderFlyweight.HEADER_LENGTH);

    private static final int PUBLICATION_TAG = 2;
    private static final int STREAM_ID = 1033;

    private static final String CONTROL_ENDPOINT = "localhost:23265";
    private static final String RECORDING_ENDPOINT = "localhost:23266";
    private static final String LIVE_ENDPOINT = "localhost:23267";
    private static final String REPLAY_ENDPOINT = "localhost:23268";

    private final ChannelUriStringBuilder publicationChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .tags("1," + PUBLICATION_TAG)
        .controlEndpoint(CONTROL_ENDPOINT)
        .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
        .minFlowControl(null, "5s")
        .termLength(TERM_LENGTH);

    private ChannelUriStringBuilder recordingChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .endpoint(RECORDING_ENDPOINT)
        .controlEndpoint(CONTROL_ENDPOINT);

    private ChannelUriStringBuilder subscriptionChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL);

    private final ChannelUriStringBuilder liveDestination = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .endpoint(LIVE_ENDPOINT)
        .controlEndpoint(CONTROL_ENDPOINT);

    private final ChannelUriStringBuilder replayDestination = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .endpoint(REPLAY_ENDPOINT);

    private final ChannelUriStringBuilder replayChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .isSessionIdTagged(true)
        .sessionId(PUBLICATION_TAG)
        .endpoint(REPLAY_ENDPOINT);

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final MutableInteger received = new MutableInteger();
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();

    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private int messagesPublished = 0;

    @BeforeEach
    public void before()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archivingMediaDriver = ArchivingMediaDriver.launch(
            mediaDriverContext
                .termBufferSparseFile(true)
                .publicationTermBufferLength(TERM_LENGTH)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .errorHandler(Throwable::printStackTrace)
                .archiveDir(archiveDir)
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true));

        aeron = Aeron.connect(
            new Aeron.Context()
                .errorHandler(Throwable::printStackTrace)
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName()));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .errorHandler(Throwable::printStackTrace)
                .aeron(aeron));
    }

    @AfterEach
    public void after()
    {
        if (received.get() != MIN_MESSAGES_PER_TERM * 6)
        {
            System.out.println(
                "received " + received.get() + ", sent " + messagesPublished +
                ", total " + (MIN_MESSAGES_PER_TERM * 6));
        }

        CloseHelper.closeAll(aeronArchive, aeron, archivingMediaDriver);

        archivingMediaDriver.archive().context().deleteDirectory();
        archivingMediaDriver.mediaDriver().context().deleteDirectory();
    }

    @SuppressWarnings("methodlength")
    @Test
    @Timeout(5)
    public void shouldMergeFromReplayToLive()
    {
        final int initialMessageCount = MIN_MESSAGES_PER_TERM * 3;
        final int subsequentMessageCount = MIN_MESSAGES_PER_TERM * 3;
        final int totalMessageCount = initialMessageCount + subsequentMessageCount;

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = MESSAGE_PREFIX + received.get();
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);
                received.incrementAndGet();
            });

        try (Publication publication = aeron.addPublication(publicationChannel.build(), STREAM_ID))
        {
            final int sessionId = publication.sessionId();
            final String recordingChannel = this.recordingChannel.sessionId(sessionId).build();
            final String subscriptionChannel = this.subscriptionChannel.sessionId(sessionId).build();

            aeronArchive.startRecording(recordingChannel, STREAM_ID, REMOTE);

            final CountersReader counters = aeron.countersReader();
            final int recordingCounterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);

            publishMessages(publication, initialMessageCount);
            messagesPublished += initialMessageCount;
            awaitPosition(counters, recordingCounterId, publication.position());

            try (Subscription subscription = aeron.addSubscription(subscriptionChannel, STREAM_ID);
                ReplayMerge replayMerge = new ReplayMerge(
                    subscription,
                    aeronArchive,
                    replayChannel.build(),
                    replayDestination.build(),
                    liveDestination.build(),
                    recordingId,
                    0))
            {
                for (int i = initialMessageCount; i < totalMessageCount; i++)
                {
                    long position;
                    while ((position = offerMessage(publication, i)) <= 0)
                    {
                        if (Publication.BACK_PRESSURED == position)
                        {
                            awaitPositionChange(counters, recordingCounterId, publication.position());
                        }
                        else
                        {
                            Tests.yieldingWait(
                                "i=%d < totalMessageCount=%d, lastPosition=%d", i, totalMessageCount, position);
                        }
                    }
                    messagesPublished++;

                    if (0 == replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT))
                    {
                        Tests.yieldingWait("i=%d < totalMessageCount=%d", i, totalMessageCount);
                    }
                }

                while (!replayMerge.isMerged())
                {
                    if (0 == replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT))
                    {
                        assertFalse(replayMerge.hasFailed(), "failed to merge");

                        Tests.yieldingWait("replay did not merge");
                    }
                }

                final Image image = replayMerge.image();
                while (received.get() < totalMessageCount)
                {
                    if (0 == image.poll(fragmentHandler, FRAGMENT_LIMIT))
                    {
                        assertFalse(replayMerge.hasFailed(), "image closed unexpectedly");

                        Tests.yieldingWait(
                            "received.get()=%d < totalMessageCount=%d", received.get(), totalMessageCount);
                    }
                }

                assertTrue(replayMerge.isMerged());
                assertTrue(replayMerge.isLiveAdded());
                assertFalse(replayMerge.hasFailed());
                assertEquals(totalMessageCount, received.get());
            }
            finally
            {
                aeronArchive.stopRecording(recordingChannel, STREAM_ID);
            }
        }
    }

    static void awaitPositionChange(final CountersReader counters, final int counterId, final long position)
    {
        if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
        {
            throw new IllegalStateException("count not active: " + counterId);
        }

        final long initialTimestampNs = System.nanoTime();
        final long currentPosition = counters.getCounterValue(counterId);
        do
        {
            Tests.yieldingWait(
                "publication position: %d, current position: %d, time since last change: %.6f ms",
                position, currentPosition, (System.nanoTime() - initialTimestampNs) / 1_000_000.0);
        }
        while (currentPosition == counters.getCounterValue(counterId) && currentPosition < position);
    }

    private long offerMessage(final Publication publication, final int index)
    {
        final int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX + index);
        return publication.offer(buffer, 0, length);
    }

    private void publishMessages(final Publication publication, final int count)
    {
        for (int i = 0; i < count; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX + i);

            long position;
            while ((position = publication.offer(buffer, 0, length)) <= 0)
            {
                Tests.yieldingWait("i=%d < count=%d, lastPosition=%d", i, count, position);
            }
        }
    }
}
