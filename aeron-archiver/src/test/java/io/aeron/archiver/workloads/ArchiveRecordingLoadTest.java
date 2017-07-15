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
import io.aeron.archiver.client.ArchiveProxy;
import io.aeron.archiver.client.RecordingEventsPoller;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.aeron.archiver.ArchiverSystemTest.recordingUri;
import static io.aeron.archiver.ArchiverSystemTest.startChannelDrainingSubscription;
import static io.aeron.archiver.TestUtil.*;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.CONTROL_STREAM_ID;
import static io.aeron.archiver.workloads.ArchiveReplayLoadTest.CONTROL_URI;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Ignore
public class ArchiveRecordingLoadTest
{
    private static final String PUBLISH_URI = "aeron:ipc";
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private static final int MESSAGE_COUNT = 2000000;
    private static final int TEST_DURATION_SEC = 20;
    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();
    private Aeron aeron;
    private Archiver archiver;
    private MediaDriver driver;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private File archiveDir = TestUtil.makeTempDir();
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
                ArchiveRecordingLoadTest.class.getName() +
                    " failed with random seed: " + ArchiveRecordingLoadTest.this.seed);
        }
    };

    private long correlationId;
    private BooleanSupplier recordingStartedIndicator;
    private BooleanSupplier recordingEndIndicator;

    @Before
    public void before() throws Exception
    {
        seed = System.nanoTime();
        rnd.setSeed(seed);

        driverCtx
            .termBufferSparseFile(false)
            .threadingMode(ThreadingMode.DEDICATED)
            .useConcurrentCounterManager(true)
            .errorHandler(Throwable::printStackTrace)
            .dirsDeleteOnStart(true);

        driver = MediaDriver.launch(driverCtx);

        archiverCtx
            .fileSyncLevel(2)
            .archiveDir(archiveDir)
            .threadingMode(ArchiverThreadingMode.DEDICATED)
            .countersManager(driverCtx.countersManager())
            .errorHandler(driverCtx.errorHandler());

        archiver = Archiver.launch(archiverCtx);
        println("Archiver started, dir: " + archiverCtx.archiveDir().getAbsolutePath());

        aeron = Aeron.connect();
    }

    @After
    public void after() throws Exception
    {
        CloseHelper.quietClose(aeron);
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
        try (Publication controlRequest = aeron.addPublication(
                archiverCtx.controlChannel(), archiverCtx.controlStreamId());
             Subscription recordingEvents = aeron.addSubscription(
                archiverCtx.recordingEventsChannel(), archiverCtx.recordingEventsStreamId()))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlRequest);
            initRecordingStartIndicator(recordingEvents);
            initRecordingEndIndicator(recordingEvents);
            TestUtil.awaitPublicationIsConnected(controlRequest);
            TestUtil.awaitSubscriptionIsConnected(recordingEvents);
            println("Archive service connected");

            final Subscription controlResponse = aeron.addSubscription(CONTROL_URI, CONTROL_STREAM_ID);
            assertTrue(archiveProxy.connect(CONTROL_URI, CONTROL_STREAM_ID));
            TestUtil.awaitSubscriptionIsConnected(controlResponse);
            println("Client connected");

            long start;
            final long duration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TEST_DURATION_SEC);
            startChannelDrainingSubscription(aeron, PUBLISH_URI, PUBLISH_STREAM_ID);
            final String channel = recordingUri(PUBLISH_URI);

            while (System.currentTimeMillis() < duration)
            {
                final long startRecordingCorrelationId = this.correlationId++;
                waitFor(() -> archiveProxy.startRecording(channel, PUBLISH_STREAM_ID, startRecordingCorrelationId));
                waitForOk(controlResponse, startRecordingCorrelationId);
                println("Recording requested");

                try (ExclusivePublication publication = aeron.addExclusivePublication(PUBLISH_URI, PUBLISH_STREAM_ID))
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
                final double rate = (totalRecordingLength * 1000.0 / time) / MEGABYTE;
                final double recordedMb = totalRecordingLength / MEGABYTE;
                System.out.printf("%d : sent %.02f MB, recorded %.02f MB/s %n", recordingId, recordedMb, rate);
                final long stopRecordingCorrelationId = this.correlationId++;
                waitFor(() -> archiveProxy.stopRecording(channel, PUBLISH_STREAM_ID, stopRecordingCorrelationId));
                waitForOk(controlResponse, stopRecordingCorrelationId);
            }

            println("All data arrived");
        }
    }

    private void initRecordingStartIndicator(final Subscription recordingEvents)
    {
        final RecordingEventsPoller recordingEventsPoller = new RecordingEventsPoller(
            new FailRecordingEventsListener()
            {
                public void onStart(
                    final long recordingId0,
                    final long joinPosition,
                    final int sessionId,
                    final int streamId,
                    final String channel,
                    final String sourceIdentity)
                {
                    recordingId = recordingId0;
                    assertThat(streamId, is(PUBLISH_STREAM_ID));
                }
            },
            recordingEvents,
            1);

        recordingStartedIndicator = () -> recordingEventsPoller.poll() != 0;
    }

    private void initRecordingEndIndicator(final Subscription recordingEvents)
    {
        final RecordingEventsPoller recordingEventsPoller = new RecordingEventsPoller(
            new FailRecordingEventsListener()
            {
                public void onProgress(
                    final long recordingId0,
                    final long joinPosition,
                    final long position)
                {
                    assertThat(recordingId0, is(recordingId));
                    recorded = position - joinPosition;
                }

                public void onStop(final long recordingId0, final long joinPosition, final long endPosition)
                {
                    doneRecording = true;
                    assertThat(recordingId0, is(recordingId));
                }
            },
            recordingEvents,
            1);

        recordingEndIndicator = () -> recordingEventsPoller.poll() != 0;
    }

    private void prepAndSendMessages(final ExclusivePublication publication)
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

    private void publishDataToBeRecorded(final ExclusivePublication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");
        final long joinPosition = publication.position();
        final int startTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            joinPosition, positionBitsToShift);
        final int startTermIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            joinPosition, positionBitsToShift, publication.initialTermId());

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = fragmentLength[i] - DataHeaderFlyweight.HEADER_LENGTH;
            buffer.putInt(0, i);
            offer(publication, buffer, dataLength);
        }

        final long position = publication.position();
        final int lastTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(position, positionBitsToShift);
        final int lastTermIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            position, positionBitsToShift, publication.initialTermId());
        totalRecordingLength =
            (lastTermIdFromPosition - startTermIdFromPosition) * publication.termBufferLength() +
                (lastTermOffset - startTermOffset);

        assertThat(position - joinPosition, is(totalRecordingLength));
    }

    private void offer(final ExclusivePublication publication, final UnsafeBuffer buffer, final int length)
    {
        final long limit = System.currentTimeMillis() + TestUtil.TIMEOUT;
        if (publication.offer(buffer, 0, length) < 0)
        {
            slowOffer(publication, buffer, length, limit);
        }
    }

    private void slowOffer(
        final ExclusivePublication publication,
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
