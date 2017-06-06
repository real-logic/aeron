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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.aeron.archiver.TestUtil.*;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.REPLY_STREAM_ID;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.REPLY_URI;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Ignore
public class ArchiveRecordingLoadTest
{
    private static final String PUBLISH_URI = "aeron:ipc?endpoint=127.0.0.1:54325";
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private static final int MESSAGE_COUNT = 2000000;
    private static final int TEST_DURATION_SEC = 30;
    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();
    private Aeron publishingClient;
    private Archiver archiver;
    private MediaDriver driver;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private File archiveDir;
    private long recordingId;
    private int[] fragmentLength;
    private long totalDataLength;
    private long totalRecordingLength;
    private long recorded;
    private boolean doneRecording;
    private final Random rnd = new Random();
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
    private long correlationId;
    private BooleanSupplier recordingStartedIndicator;
    private BooleanSupplier recordingEndIndicator;

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
        archiverCtx
            .forceMetadataUpdates(false)
            .forceWrites(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiverThreadingMode.DEDICATED);

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
            initRecordingStartIndicator(client);
            initRecordingEndIndicator(client);
            TestUtil.awaitPublicationIsConnected(control);
            TestUtil.awaitSubscriptionIsConnected(recordingEvents);
            println("Archive service connected");

            final Subscription reply = publishingClient.addSubscription(REPLY_URI, REPLY_STREAM_ID);
            client.connect(REPLY_URI, REPLY_STREAM_ID);
            TestUtil.awaitSubscriptionIsConnected(reply);
            println("Client connected");

            long start;
            final long duration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TEST_DURATION_SEC);
            while (System.currentTimeMillis() < duration)
            {
                final long startRecordingCorrelationId = this.correlationId++;
                waitFor(() -> client.startRecording(PUBLISH_URI, PUBLISH_STREAM_ID, startRecordingCorrelationId));
                waitForOk(client, reply, startRecordingCorrelationId);
                println("Recording requested");

                try (Publication publication = publishingClient.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID))
                {
                    awaitPublicationIsConnected(publication);
                    waitFor(recordingStartedIndicator);
                    start = System.currentTimeMillis();

                    prepAndSendMessages(publication);
                }

                while (!doneRecording)
                {
                    waitFor(recordingEndIndicator);
                }

                doneRecording = false;
                assertThat(totalRecordingLength, is(recorded));
                final long time = System.currentTimeMillis() - start;
                final double recordedMbps = (totalRecordingLength * 1000.0 / time) / MEGABYTE;
                final double recordedMb = totalRecordingLength / MEGABYTE;
                System.out.printf("%d : sent=%f MB recorded=%f MBps %n", recordingId, recordedMb, recordedMbps);
            }

            println("All data arrived");
        }
    }

    private void initRecordingStartIndicator(final ArchiveClient client)
    {
        recordingStartedIndicator = () -> client.pollEvents(new FailRecordingEventsListener()
        {
            public void onStart(
                final long recordingId0,
                final int sessionId,
                final int streamId,
                final String channel,
                final String sourceIdentity)
            {
                recordingId = recordingId0;
                assertThat(streamId, is(PUBLISH_STREAM_ID));
                assertThat(channel, is(PUBLISH_URI));
            }
        }, 1) != 0;
    }

    private void initRecordingEndIndicator(final ArchiveClient client)
    {
        recordingEndIndicator =
            () -> client.pollEvents(new FailRecordingEventsListener()
            {
                public void onProgress(
                    final long recordingId0,
                    final long joiningPosition,
                    final long currentPosition)
                {
                    assertThat(recordingId0, is(recordingId));
                    recorded = currentPosition - joiningPosition;
                }

                public void onStop(final long recordingId0)
                {
                    doneRecording = true;
                    assertThat(recordingId0, is(recordingId));
                }
            }, 1) != 0;
    }

    private void prepAndSendMessages(final Publication publication)
    {
        fragmentLength = new int[ArchiveRecordingLoadTest.MESSAGE_COUNT];
        for (int i = 0; i < ArchiveRecordingLoadTest.MESSAGE_COUNT; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - DataHeaderFlyweight.HEADER_LENGTH;
            fragmentLength[i] = messageLength + DataHeaderFlyweight.HEADER_LENGTH;
            totalDataLength += fragmentLength[i];
        }

        printf("Sending %d messages, total length=%d %n", ArchiveRecordingLoadTest.MESSAGE_COUNT, totalDataLength);

        publishDataToBeRecorded(publication, ArchiveRecordingLoadTest.MESSAGE_COUNT);
    }

    private void publishDataToBeRecorded(final Publication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());

        buffer.setMemory(0, 1024, (byte)'z');
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
            //printf("Sending: index=%d length=%d %n", i, dataLength);
            offer(publication, buffer, dataLength);
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
    }

    private void offer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int length)
    {
        final long limit = System.currentTimeMillis() + (long)TestUtil.TIMEOUT;
        if (publication.offer(buffer, 0, length) < 0)
        {
            slowOffer(publication, buffer, length, limit);
        }
    }

    private void slowOffer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int length,
        final long limit)
    {
        for (int i = 0; i < 100; i++)
        {
            if (publication.offer(buffer, 0, length) > 0)
            {
                return;
            }
        }

        for (int i = 0; i < 100; i++)
        {
            if (publication.offer(buffer, 0, length) > 0)
            {
                return;
            }
            Thread.yield();
        }

        while (publication.offer(buffer, 0, length) < 0)
        {
            LockSupport.parkNanos(TIMEOUT);
            if (limit < System.currentTimeMillis())
            {
                fail("Offer has timed out");
            }
        }
    }
}
