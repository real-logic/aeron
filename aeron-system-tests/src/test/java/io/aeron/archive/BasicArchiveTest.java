/*
 * Copyright 2017 Real Logic Ltd.
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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BasicArchiveTest
{
    private static final int FRAGMENT_LIMIT = 10;

    private static final int RECORDING_STREAM_ID = 33;
    private static final String RECORDING_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(64 * 1024)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @Before
    public void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .archiveDir(TestUtil.makeTempDir())
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));

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

        archivingMediaDriver.archive().context().deleteArchiveDirectory();
        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void shouldRecordAndReplay()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long length;

        final long correlationId = aeronArchive.startRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID, LOCAL);
        final long recordingIdFromCounter;

        try (Publication publication = aeron.addPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID);
             Subscription subscription = aeron.addSubscription(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            offer(publication, messageCount, messagePrefix);

            recordingIdFromCounter = RecordingPos.findActiveRecordingId(
                aeron.countersReader(), aeronArchive.controlSessionId(), correlationId, publication.sessionId());

            consume(subscription, messageCount, messagePrefix);

            length = publication.position();
        }

        aeronArchive.stopRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID);

        final long recordingId = findRecordingId(RECORDING_CHANNEL, RECORDING_STREAM_ID, length);
        assertEquals(recordingIdFromCounter, recordingId);

        final long fromPosition = 0L;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, messageCount, messagePrefix);
            assertEquals(length, subscription.imageAtIndex(0).position());
        }
    }

    @Test(timeout = 10000)
    public void shouldRecordReplayAndCancelReplayEarly()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long length;

        try (Publication publication = aeronArchive.addRecordedPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID);
             Subscription subscription = aeron.addSubscription(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            length = publication.position();
        }

        aeronArchive.stopRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID);

        final long recordingId = findRecordingId(RECORDING_CHANNEL, RECORDING_STREAM_ID, length);
        final long fromPosition = 0L;

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        aeronArchive.stopReplay(replaySessionId);
    }

    private long findRecordingId(final String expectedChannel, final int expectedStreamId, final long expectedPosition)
    {
        final MutableLong foundRecordingId = new MutableLong();

        final int recordingsFound = aeronArchive.listRecordingsForUri(
            0L,
            10,
            expectedChannel,
            expectedStreamId,
            (
                controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity
            ) ->
            {
                foundRecordingId.set(recordingId);

                assertEquals(0L, startPosition);
                assertEquals(expectedPosition, stopPosition);
                assertEquals(expectedStreamId, streamId);
                assertEquals(expectedChannel, originalChannel);
            });

        assertThat(recordingsFound, greaterThan(0));

        return foundRecordingId.get();
    }

    private static void offer(final Publication publication, final int count, final String prefix)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = 0; i < count; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, prefix + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Thread.yield();
            }
        }
    }

    private static void consume(final Subscription subscription, final int count, final String prefix)
    {
        final MutableInteger received = new MutableInteger(0);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = prefix + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < count)
        {
            if (0 == subscription.poll(fragmentHandler, FRAGMENT_LIMIT))
            {
                Thread.yield();
            }
        }

        assertThat(received.get(), is(count));
    }
}
