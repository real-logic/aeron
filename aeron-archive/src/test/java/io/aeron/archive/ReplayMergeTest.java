/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class ReplayMergeTest
{
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int MIN_MESSAGES_PER_TERM =
        TERM_BUFFER_LENGTH / (MESSAGE_PREFIX.length() + DataHeaderFlyweight.HEADER_LENGTH);

    private static final int PUBLICATION_TAG = 2;
    private static final int STREAM_ID = 33;

    private static final String CONTROL_ENDPOINT = "localhost:43265";
    private static final String RECORDING_ENDPOINT = "localhost:43266";
    private static final String LIVE_ENDPOINT = "localhost:43267";
    private static final String REPLAY_ENDPOINT = "localhost:43268";

    private final ChannelUriStringBuilder publicationChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .tags("1," + PUBLICATION_TAG)
        .controlEndpoint(CONTROL_ENDPOINT)
        .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
        .termLength(TERM_BUFFER_LENGTH);

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
    private final MutableInteger received = new MutableInteger(0);
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();

    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @Before
    public void before()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archivingMediaDriver = ArchivingMediaDriver.launch(
            mediaDriverContext
                .termBufferSparseFile(true)
                .publicationTermBufferLength(TERM_BUFFER_LENGTH)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
                .archiveDir(archiveDir)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName()));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @After
    public void after()
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(archivingMediaDriver);

        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
        archivingMediaDriver.archive().context().deleteArchiveDirectory();
    }

    @Test(timeout = 20_000)
    public void shouldMergeFromReplayToLive()
    {
        final int initialMessageCount = MIN_MESSAGES_PER_TERM * 3;
        final int subsequentMessageCount = MIN_MESSAGES_PER_TERM * 3;
        final int totalMessageCount = initialMessageCount + subsequentMessageCount;

        try (Publication publication = aeron.addPublication(publicationChannel.build(), STREAM_ID))
        {
            final int sessionId = publication.sessionId();
            final String recordingChannel = this.recordingChannel.sessionId(sessionId).build();
            final String subscriptionChannel = this.subscriptionChannel.sessionId(sessionId).build();

            aeronArchive.startRecording(recordingChannel, STREAM_ID, REMOTE);

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offerMessages(publication, 0, initialMessageCount, MESSAGE_PREFIX);
            awaitPosition(counters, counterId, publication.position());

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
                final FragmentHandler fragmentHandler = new FragmentAssembler(
                    (buffer, offset, length, header) ->
                    {
                        final String expected = MESSAGE_PREFIX + received.value;
                        final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                        assertEquals(expected, actual);

                        received.value++;
                    });

                for (int i = initialMessageCount; i < totalMessageCount; i++)
                {
                    offer(publication, i, MESSAGE_PREFIX);

                    if (0 == replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT))
                    {
                        checkInterruptedStatus();
                        Thread.yield();
                    }
                }

                while (received.get() < totalMessageCount || !replayMerge.isMerged())
                {
                    if (0 == replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT))
                    {
                        checkInterruptedStatus();
                        Thread.yield();
                    }
                }

                assertThat(received.get(), is(totalMessageCount));
                assertTrue(replayMerge.isMerged());
                assertEquals(ReplayMerge.State.MERGED, replayMerge.state());
            }
            finally
            {
                aeronArchive.stopRecording(recordingChannel, STREAM_ID);
            }
        }
    }

    private void offer(final Publication publication, final int index, final String prefix)
    {
        final int length = buffer.putStringWithoutLengthAscii(0, prefix + index);

        while (publication.offer(buffer, 0, length) <= 0)
        {
            checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void offerMessages(
        final Publication publication, final int startIndex, final int count, final String prefix)
    {
        for (int i = startIndex; i < (startIndex + count); i++)
        {
            offer(publication, i, prefix);
        }
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            fail("Unexpected interrupt - Test likely to have timed out");
        }
    }

    private static int awaitCounterId(final CountersReader counters, final int sessionId)
    {
        int counterId;

        while (NULL_COUNTER_ID == (counterId = RecordingPos.findCounterIdBySession(counters, sessionId)))
        {
            checkInterruptedStatus();
            Thread.yield();
        }

        return counterId;
    }

    private static void awaitPosition(final CountersReader counters, final int counterId, final long position)
    {
        while (counters.getCounterValue(counterId) < position)
        {
            checkInterruptedStatus();
            Thread.yield();
        }
    }
}
