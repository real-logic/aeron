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
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ExtendRecordingTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int MTU_LENGTH = Configuration.MTU_LENGTH;

    private static final int RECORDING_STREAM_ID = 33;
    private static final String RECORDING_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(TERM_BUFFER_LENGTH)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;
    private File archiveDir;
    private AeronArchive aeronArchive;
    public static final String MESSAGE_PREFIX = "Message-Prefix-";

    @Before
    public void before()
    {
        launchAeronAndArchive();
    }

    @After
    public void after()
    {
        closeDownAndCleanMediaDriver();
        archivingMediaDriver.archive().context().deleteArchiveDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldExtendRecordingAndReplay()
    {
        final int messageCount = 10;
        final long firstStopPosition;

        final long recordingId;
        final int initialTermId;

        try (Publication publication = aeron.addPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID);
            Subscription subscription = aeron.addSubscription(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            initialTermId = publication.initialTermId();

            aeronArchive.startRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID, LOCAL);

            try
            {
                offer(publication, 0, messageCount, MESSAGE_PREFIX);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                consume(subscription, 0, messageCount, MESSAGE_PREFIX);

                firstStopPosition = publication.position();
                while (counters.getCounterValue(counterId) < firstStopPosition)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }
            finally
            {
                aeronArchive.stopRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID);
            }
        }

        closeDownAndCleanMediaDriver();
        launchAeronAndArchive();

        final String publicationExtendChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .initialPosition(firstStopPosition, initialTermId, TERM_BUFFER_LENGTH)
            .mtu(MTU_LENGTH)
            .build();

        final String subscriptionExtendChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .build();

        final long secondStopPosition;

        try (Publication publication = aeron.addExclusivePublication(publicationExtendChannel, RECORDING_STREAM_ID);
            Subscription subscription = aeron.addSubscription(subscriptionExtendChannel, RECORDING_STREAM_ID))
        {
            aeronArchive.extendRecording(recordingId, subscriptionExtendChannel, RECORDING_STREAM_ID, LOCAL);

            try
            {
                offer(publication, messageCount, messageCount, MESSAGE_PREFIX);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

                consume(subscription, messageCount, messageCount, MESSAGE_PREFIX);

                secondStopPosition = publication.position();
                while (counters.getCounterValue(counterId) < secondStopPosition)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionExtendChannel, RECORDING_STREAM_ID);
            }
        }

        final long fromPosition = 0L;
        final long length = secondStopPosition - fromPosition;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, 0, messageCount * 2, MESSAGE_PREFIX);
            assertEquals(secondStopPosition, subscription.imageAtIndex(0).position());
        }
    }

    private static void offer(
        final Publication publication, final int startIndex, final int count, final String prefix)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = startIndex; i < (startIndex + count); i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, prefix + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }

    private static void consume(
        final Subscription subscription, final int startIndex, final int count, final String prefix)
    {
        final MutableInteger received = new MutableInteger(startIndex);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = prefix + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < (startIndex + count))
        {
            if (0 == subscription.poll(fragmentHandler, FRAGMENT_LIMIT))
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }

        assertThat(received.get(), is(startIndex + count));
    }

    private void closeDownAndCleanMediaDriver()
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(archivingMediaDriver);

        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
        // do not clean out archive directory
    }

    private void launchAeronAndArchive()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        if (null == archiveDir)
        {
            archiveDir = new File(IoUtil.tmpDirName(), "archive");
        }

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(aeronDirectoryName)
                .archiveDir(archiveDir)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }
}
