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

package io.aeron.archiver.workloads;

import io.aeron.*;
import io.aeron.archiver.*;
import io.aeron.archiver.client.ArchiveClient;
import io.aeron.archiver.codecs.*;
import io.aeron.driver.*;
import io.aeron.logbuffer.LogBufferDescriptor;
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

import static io.aeron.archiver.TestUtil.*;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.REPLY_STREAM_ID;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.REPLY_URI;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class ArchiveRecordingLoadTest
{
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
    private File archiveDir;
    private long recordingId;
    private int[] fragmentLength;
    private String sourceIdentity;
    private long totalDataLength;
    private long totalRecordingLength;
    private long recorded;
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
                "ArchiveAndReplaySystemTest failed with random seed: " + ArchiveRecordingLoadTest.this.seed);
        }
    };
    private Subscription reply;
    private long correlationId;

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
        archiveDir = TestUtil.makeTempDir();
        archiverCtx.archiveDir(archiveDir);
        archiver = Archiver.launch(archiverCtx);
        println("Archiver started, dir: " + archiverCtx.archiveDir().getAbsolutePath());
        publishingClient = Aeron.connect();
    }

    @After
    public void closeEverything() throws Exception
    {
        CloseHelper.quietClose(publishingClient);
        CloseHelper.quietClose(archiver);
        CloseHelper.quietClose(driver);

        if (null != archiveDir)
        {
            IoUtil.delete(archiveDir, false);
        }

        driverCtx.deleteAeronDirectory();
    }

    @Test
    public void archive() throws IOException, InterruptedException
    {
        try (Publication control = publishingClient.addPublication(
            archiverCtx.controlRequestChannel(), archiverCtx.controlRequestStreamId());
             Subscription recordingEvents = publishingClient.addSubscription(
                 archiverCtx.recordingEventsChannel(), archiverCtx.recordingEventsStreamId()))
        {
            final ArchiveClient client = new ArchiveClient(control, recordingEvents);

            TestUtil.awaitPublicationIsConnected(control);
            TestUtil.awaitSubscriptionIsConnected(recordingEvents);
            println("Archive service connected");

            reply = publishingClient.addSubscription(REPLY_URI, REPLY_STREAM_ID);
            client.connect(REPLY_URI, REPLY_STREAM_ID);
            TestUtil.awaitSubscriptionIsConnected(reply);
            println("Client connected");

            final long startRecordingCorrelationId = this.correlationId++;
            waitFor(() -> client.startRecording(PUBLISH_URI, PUBLISH_STREAM_ID, startRecordingCorrelationId));
            println("Recording requested");
            waitForOk(client, reply, startRecordingCorrelationId);

            final Publication publication = publishingClient.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
            awaitPublicationIsConnected(publication);

            awaitStartedRecordingNotification(recordingEvents, publication);

            for (int i = 0; i < 100; i++)
            {
                prepAndSendMessages(recordingEvents, publication, 200000);
                System.out.printf("Sent %d : %d %n", i, totalRecordingLength);
            }

            assertNull(trackerError);
            println("All data arrived");
        }
    }

    private void prepAndSendMessages(
        final Subscription recordingEvents,
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
        trackRecordingProgress(recordingEvents, waitForData);
        publishDataToBeRecorded(publication, messageCount);
        waitForData.await();
    }

    private void awaitStartedRecordingNotification(
        final Subscription recordingEvents, final Publication publication)
    {
        // the archiver has subscribed to the publication, now we wait for the recording start message
        poll(
            recordingEvents,
            (buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                assertThat(hDecoder.templateId(), is(RecordingStartedDecoder.TEMPLATE_ID));

                final RecordingStartedDecoder decoder = new RecordingStartedDecoder().wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    hDecoder.blockLength(),
                    hDecoder.version());

                recordingId = decoder.recordingId();
                assertThat(decoder.streamId(), is(PUBLISH_STREAM_ID));
                assertThat(decoder.sessionId(), is(publication.sessionId()));

                assertThat(decoder.channel(), is(PUBLISH_URI));
                sourceIdentity = decoder.sourceIdentity();
                println("Recording started. sourceIdentity: " + sourceIdentity);
            }
        );
    }

    private void publishDataToBeRecorded(final Publication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());

        // clear out the buffer we write
        for (int i = 0; i < 1024; i++)
        {
            buffer.putByte(i, (byte)'z');
        }
        buffer.putStringAscii(32, "TEST");
        final long joiningPosition = publication.position();
        final int startTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            joiningPosition, positionBitsToShift);
        final int startTermIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            joiningPosition, positionBitsToShift, publication.initialTermId());
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
        totalRecordingLength =
            (lastTermIdFromPosition - startTermIdFromPosition) * publication.termBufferLength() +
                (lastTermOffset - startTermOffset);

        assertThat(lastPosition - joiningPosition, is(totalRecordingLength));
        lastTermId = lastTermIdFromPosition;
    }

    private void trackRecordingProgress(
        final Subscription recordingEvents,
        final CountDownLatch waitForData)
    {
        final Thread t = new Thread(
            () ->
            {
                try
                {
                    long start = System.currentTimeMillis();
                    final long initialRecorded = recorded;
                    long startBytes = recorded;
                    // each message is fragmentLength[fragmentCount]
                    while (lastTermId == -1 || (recorded - initialRecorded) < totalRecordingLength)
                    {
                        if (recordingEvents.poll(
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
                                recorded = mDecoder.currentPosition() - mDecoder.joiningPosition();
                                System.out.printf("a=%d total=%d %n", (recorded - initialRecorded),
                                    totalRecordingLength);
                            }, 1) == 0)
                        {
                            LockSupport.parkNanos(1000000);
                        }

                        final long end = System.currentTimeMillis();
                        final long deltaTime = end - start;

                        if (deltaTime > 1000)
                        {
                            start = end;
                            final long deltaBytes = recorded - startBytes;
                            startBytes = recorded;
                            final double mbps = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                            System.out.printf("Archive reported speed: %f MB/s %n", mbps);
                        }
                    }
                    final long end = System.currentTimeMillis();
                    final long deltaTime = end - start;

                    final long deltaBytes = recorded - startBytes;
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
}
