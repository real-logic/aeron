/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import io.aeron.driver.*;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.*;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class ArchiverLoadTest
{
    private static final int TIMEOUT = 5000;
    private static final boolean DEBUG = false;
    private static final String PUBLISH_URI = "aeron:ipc?endpoint=127.0.0.1:54325";
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();
    private Aeron publishingClient;
    private Archiver archiver;
    private MediaDriver driver;
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private File archiveFolder;
    private int recordingId;
    private String source;
    private int[] fragmentLength;
    private long totalDataLength;
    private long totalArchiveLength;
    private long archived;
    private volatile int lastTermId = -1;
    private Throwable trackerError;
    private Random rnd = new Random();
    private long seed;

    @Rule
    public TestWatcher ruleExample = new TestWatcher()
    {
        protected void failed(final Throwable t, final Description description)
        {
            System.err.println(
                "ArchiveAndReplaySystemTest failed with random seed: " + ArchiverLoadTest.this.seed);
        }
    };

    @Before
    public void setUp() throws Exception
    {
        seed = System.nanoTime();
        rnd.setSeed(seed);

        driverCtx
            .termBufferSparseFile(false)
            .threadingMode(ThreadingMode.DEDICATED)
            .errorHandler(LangUtil::rethrowUnchecked)
            .dirsDeleteOnStart(true);

        driver = MediaDriver.launch(driverCtx);
        archiveFolder = TestUtil.makeTempFolder();
        archiverCtx.archiveFolder(archiveFolder);
        archiver = Archiver.launch(archiverCtx);
        println("Archiver started, folder: " + archiverCtx.archiveFolder().getAbsolutePath());
        publishingClient = Aeron.connect();
    }

    @After
    public void closeEverything() throws Exception
    {
        CloseHelper.quietClose(publishingClient);
        CloseHelper.quietClose(archiver);
        CloseHelper.quietClose(driver);

        if (null != archiveFolder)
        {
            IoUtil.delete(archiveFolder, false);
        }

        driverCtx.deleteAeronDirectory();
    }

    @Test
    public void archive() throws IOException, InterruptedException
    {
        try (Publication archiverServiceRequest = publishingClient.addPublication(
                archiverCtx.serviceRequestChannel(), archiverCtx.serviceRequestStreamId());
             Subscription archiverNotifications = publishingClient.addSubscription(
                archiverCtx.archiverNotificationsChannel(), archiverCtx.archiverNotificationsStreamId()))
        {
            awaitPublicationIsConnected(archiverServiceRequest, TIMEOUT);
            awaitSubscriptionIsConnected(archiverNotifications, TIMEOUT);
            println("Archive service connected");

            requestArchive(archiverServiceRequest, PUBLISH_URI, PUBLISH_STREAM_ID);

            println("Archive requested");

            final Publication publication = publishingClient.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
            awaitPublicationIsConnected(publication, TIMEOUT);

            awaitArchiveForPublicationStartedNotification(archiverNotifications, publication);

            for (int i = 0; i < 100; i++)
            {
                prepAndSendMessages(archiverNotifications, publication, 200000);
                System.out.printf("Sent %d : %d %n", i, totalArchiveLength);
            }

            assertNull(trackerError);
            println("All data arrived");
        }
    }

    private void prepAndSendMessages(
        final Subscription archiverNotifications,
        final Publication publication,
        final int messageCount)
        throws InterruptedException
    {
        fragmentLength = new int[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - DataHeaderFlyweight.HEADER_LENGTH;
            fragmentLength[i] = messageLength + DataHeaderFlyweight.HEADER_LENGTH;
            totalDataLength += fragmentLength[i];
        }

        final CountDownLatch waitForData = new CountDownLatch(1);
        printf("Sending %d messages, total length=%d %n", messageCount, totalDataLength);

        lastTermId = -1;
        trackArchiveProgress(publication, archiverNotifications, waitForData);
        publishDataToBeArchived(publication, messageCount);
        waitForData.await();

    }


    private void awaitArchiveForPublicationStartedNotification(
        final Subscription archiverNotifications, final Publication publication)
    {
        // the archiver has subscribed to the publication, now we wait for the archive start message
        poll(
            archiverNotifications,
            (buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                assertThat(hDecoder.templateId(), is(RecordingStartedDecoder.TEMPLATE_ID));

                final RecordingStartedDecoder decoder = new RecordingStartedDecoder()
                    .wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());

                recordingId = decoder.recordingId();
                assertThat(decoder.streamId(), is(PUBLISH_STREAM_ID));
                assertThat(decoder.sessionId(), is(publication.sessionId()));

                source = decoder.source();
                assertThat(decoder.channel(), is(PUBLISH_URI));
                println("Archive started. source: " + source);
            },
            TIMEOUT);
    }

    private void publishDataToBeArchived(final Publication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());

        // clear out the buffer we write
        for (int i = 0; i < 1024; i++)
        {
            buffer.putByte(i, (byte)'z');
        }
        buffer.putStringAscii(32, "TEST");
        final long initialPosition = publication.position();
        final int startTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            initialPosition, positionBitsToShift);
        final int startTermIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            initialPosition, positionBitsToShift, publication.initialTermId());
        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = fragmentLength[i] - DataHeaderFlyweight.HEADER_LENGTH;
            buffer.putInt(0, i);
            printf("Sending: index=%d length=%d %n", i, dataLength);
            offer(publication, buffer, 0, dataLength, TIMEOUT);
        }

        final long lastPosition = publication.position();
        final int lastTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            lastPosition, positionBitsToShift);
        final int lastTermIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            lastPosition, positionBitsToShift, publication.initialTermId());
        totalArchiveLength =
            (lastTermIdFromPosition - startTermIdFromPosition) * publication.termBufferLength() +
            (lastTermOffset - startTermOffset);

        assertThat(lastPosition - initialPosition, is(totalArchiveLength));
        lastTermId = lastTermIdFromPosition;
    }

    private void trackArchiveProgress(
        final Publication publication,
        final Subscription archiverNotifications,
        final CountDownLatch waitForData)
    {
        final Thread t = new Thread(
            () ->
            {
                try
                {
                    long start = System.currentTimeMillis();
                    long startBytes = archived;
                    final long initialArchived = archived;
                    // each message is fragmentLength[fragmentCount]
                    while (lastTermId == -1 || archived < initialArchived + totalArchiveLength)
                    {
                        poll(
                            archiverNotifications,
                            (buffer, offset, length, header) ->
                            {
                                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                                assertThat(hDecoder.templateId(), is(RecordingProgressDecoder.TEMPLATE_ID));

                                final RecordingProgressDecoder mDecoder =
                                    new RecordingProgressDecoder().wrap(
                                        buffer,
                                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        hDecoder.blockLength(),
                                        hDecoder.version());
                                assertThat(mDecoder.recordingId(), is(recordingId));

                                println(mDecoder.toString());
                                archived = publication.termBufferLength() *
                                    (mDecoder.termId() - mDecoder.initialTermId()) +
                                    (mDecoder.termOffset() - mDecoder.initialTermOffset());
                                System.out.printf("a=%d total=%d %n", archived, totalArchiveLength);
                            },
                            50000);

                        final long end = System.currentTimeMillis();
                        final long deltaTime = end - start;
                        if (deltaTime > 1000)
                        {
                            start = end;
                            final long deltaBytes = archived - startBytes;
                            startBytes = archived;
                            final double mbps = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                            System.out.printf("Archive reported speed: %f MB/s %n", mbps);
                        }
                    }
                    final long end = System.currentTimeMillis();
                    final long deltaTime = end - start;

                    final long deltaBytes = archived - startBytes;
                    final double mbps = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                    System.out.printf("-Archive reported speed: %f MB/s %n", mbps);
                }
                catch (final Throwable throwable)
                {
                    throwable.printStackTrace();
                    trackerError = throwable;
                }

                waitForData.countDown();
            });

        t.setDaemon(true);
        t.start();
    }

    private void poll(final Subscription subscription, final FragmentHandler handler, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (0 >= subscription.poll(handler, 1))
        {
            LockSupport.parkNanos(TIMEOUT);
            if (limit < System.currentTimeMillis())
            {
                fail("Poll has timed out");
            }
        }
    }

    private long offer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int offset,
        final int length,
        final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        final long newPosition;

        if ((newPosition = publication.offer(buffer, offset, length)) < 0)
        {
            return slowOffer(publication, buffer, offset, length, limit);
        }

        return newPosition;
    }

    private long slowOffer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int offset,
        final int length,
        final long limit)
    {
        long newPosition;
        for (int i = 0; i < 100; i++)
        {
            if ((newPosition = publication.offer(buffer, offset, length)) > 0)
            {
                return newPosition;
            }
        }
        for (int i = 0; i < 100; i++)
        {
            if ((newPosition = publication.offer(buffer, offset, length)) > 0)
            {
                return newPosition;
            }
            Thread.yield();
        }
        while ((newPosition = publication.offer(buffer, offset, length)) < 0)
        {
            LockSupport.parkNanos(TIMEOUT);
            if (limit < System.currentTimeMillis())
            {
                fail("Offer has timed out");
            }
        }
        return newPosition;
    }

    private void awaitSubscriptionIsConnected(final Subscription subscription, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (subscription.imageCount() == 0)
        {
            LockSupport.parkNanos(TIMEOUT);
            if (limit < System.currentTimeMillis())
            {
                fail("awaitSubscriptionIsConnected has timed out");
            }
        }
    }

    private void awaitPublicationIsConnected(final Publication publication, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (!publication.isConnected())
        {
            LockSupport.parkNanos(TIMEOUT);
            if (limit < System.currentTimeMillis())
            {
                fail("awaitPublicationIsConnected has timed out");
            }
        }
    }

    private void printf(final String s, final Object... args)
    {
        if (DEBUG)
        {
            System.out.printf(s, args);
        }
    }

    private void println(final String s)
    {
        if (DEBUG)
        {
            System.out.println(s);
        }
    }

    private void requestArchive(final Publication archiverServiceRequest, final String channel, final int streamId)
    {
        new MessageHeaderEncoder()
            .wrap(buffer, 0)
            .templateId(StartRecordingRequestEncoder.TEMPLATE_ID)
            .blockLength(StartRecordingRequestEncoder.BLOCK_LENGTH)
            .schemaId(StartRecordingRequestEncoder.SCHEMA_ID)
            .version(StartRecordingRequestEncoder.SCHEMA_VERSION);

        final StartRecordingRequestEncoder encoder = new StartRecordingRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .channel(channel)
            .streamId(streamId);

        offer(
            archiverServiceRequest,
            buffer,
            0,
            encoder.encodedLength() + MessageHeaderEncoder.ENCODED_LENGTH,
            TIMEOUT);
    }
}
