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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Ignore
public class ArchiveTest
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int STREAM_ID = 33;
    private static final String CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("127.0.0.1:7777")
        .termLength(64 * 1024)
        .build();

    private MediaDriver driver;
    private Archive archive;
    private Aeron aeronClient;
    private AeronArchive archiveClient;

    @Before
    public void before() throws Exception
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .dirsDeleteOnStart(true)
                .useConcurrentCounterManager(true));

        archive = Archive.launch(
            new Archive.Context()
                .fileSyncLevel(0)
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .archiveDir(TestUtil.makeTempDir())
                .threadingMode(ArchiveThreadingMode.SHARED)
                .countersManager(driver.context().countersManager())
                .errorHandler(driver.context().errorHandler()));

        aeronClient = Aeron.connect();

        archiveClient = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeronClient));
    }

    @After
    public void after() throws Exception
    {
        CloseHelper.close(aeronClient);
        CloseHelper.close(archive);
        CloseHelper.close(driver);

        archive.context().deleteArchiveDirectory();
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void shouldRecordAndReplay()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long bytesPublished;

        try (Publication publication = archiveClient.addRecordedPublication(CHANNEL, STREAM_ID);
             Subscription subscription = aeronClient.addSubscription(CHANNEL, STREAM_ID))
        {
            offer(publication, messageCount, messagePrefix);
            consume(subscription, messageCount, messagePrefix);

            bytesPublished = publication.position();
        }

        final MutableLong foundRecordingId = new MutableLong();
        final int recordingsFound = archiveClient.listRecordingsForUri(
            0L,
            10,
            CHANNEL,
            STREAM_ID,
            (
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
                foundRecordingId.value = recordingId;

                assertEquals(0L, startPosition);
                assertEquals(bytesPublished, stopPosition);
                assertEquals(STREAM_ID, streamId);
                assertEquals(CHANNEL, originalChannel);
            });

        assertEquals(1, recordingsFound);
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

        while (received.value < count && subscription.poll(fragmentHandler, FRAGMENT_LIMIT) == 0)
        {
            Thread.yield();
        }

        assertEquals(count, received.value);
    }
}
