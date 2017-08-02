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

package io.aeron.archive.workloads;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.NoOpRecordingEventsListener;
import io.aeron.archive.TestUtil;
import io.aeron.archive.client.ArchiveProxy;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TestWatcher;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.TestUtil.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static junit.framework.TestCase.assertTrue;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@Ignore
public class ArchiveReplayLoadTest
{
    static final String CONTROL_URI = "aeron:udp?endpoint=127.0.0.1:54327";
    static final int CONTROL_STREAM_ID = 100;
    static final int TEST_DURATION_SEC = 30;

    private static final String REPLAY_URI = "aeron:udp?endpoint=127.0.0.1:54326";

    private static final String PUBLISH_URI = new ChannelUriStringBuilder()
        .media("ipc")
        .mtu(16 * 1024)
        .termLength(128 * 1024 * 1024)
        .build();

    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private static final int MESSAGE_COUNT = 2000000;
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT));
    private final Random rnd = new Random();
    private final long seed = System.nanoTime();

    @Rule
    public final TestWatcher testWatcher = TestUtil.newWatcher(ArchiveReplayLoadTest.class, seed);

    private Aeron aeron;
    private Archive archive;
    private MediaDriver driver;
    private final long recordingId = 0;
    private long remaining;
    private int fragmentCount;
    private long totalPayloadLength;
    private long totalRecordingLength;
    private long recorded;
    private volatile int lastTermId = -1;
    private Throwable trackerError;

    private long correlationId;
    private long startPosition;
    private FragmentHandler validateFragmentHandler = this::validateFragment;

    @Before
    public void before() throws Exception
    {
        rnd.setSeed(seed);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .useConcurrentCounterManager(true)
            .errorHandler(Throwable::printStackTrace)
            .dirsDeleteOnStart(true);

        driver = MediaDriver.launch(driverCtx);

        final Archive.Context archiverCtx = new Archive.Context()
            .archiveDir(TestUtil.makeTempDir())
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.DEDICATED)
            .countersManager(driverCtx.countersManager())
            .errorHandler(driverCtx.errorHandler());

        archive = Archive.launch(archiverCtx);
        aeron = Aeron.connect();
    }

    @After
    public void after() throws Exception
    {
        CloseHelper.close(aeron);
        CloseHelper.close(archive);
        CloseHelper.close(driver);

        archive.context().deleteArchiveDirectory();
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 180000)
    public void replay() throws IOException, InterruptedException
    {
        try (Publication controlRequest = aeron.addPublication(
                archive.context().controlChannel(), archive.context().controlStreamId());
             Subscription recordingEvents = aeron.addSubscription(
                archive.context().recordingEventsChannel(), archive.context().recordingEventsStreamId()))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlRequest);

            awaitPublicationIsConnected(controlRequest);
            awaitSubscriptionIsConnected(recordingEvents);
            println("Archive service connected");

            final Subscription controlResponse = aeron.addSubscription(CONTROL_URI, CONTROL_STREAM_ID);
            assertTrue(archiveProxy.connect(CONTROL_URI, CONTROL_STREAM_ID));
            awaitSubscriptionIsConnected(controlResponse);
            println("Client connected");

            final long startRecordingCorrelationId = this.correlationId++;
            final String recordingUri = PUBLISH_URI;
            waitFor(() -> archiveProxy.startRecording(
                recordingUri, PUBLISH_STREAM_ID, SourceLocation.LOCAL, startRecordingCorrelationId));
            println("Recording requested");
            waitForOk(controlResponse, startRecordingCorrelationId);

            final Publication publication = aeron.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
            awaitPublicationIsConnected(publication);
            startChannelDrainingSubscription(aeron, PUBLISH_URI, PUBLISH_STREAM_ID);

            final int messageCount = prepAndSendMessages(recordingEvents, publication);
            publication.close();

            assertNull(trackerError);
            println("All data arrived");

            println("Request stop recording");
            final long requestStopCorrelationId = this.correlationId++;
            waitFor(() -> archiveProxy.stopRecording(recordingUri, PUBLISH_STREAM_ID, requestStopCorrelationId));
            waitForOk(controlResponse, requestStopCorrelationId);

            final long duration = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TEST_DURATION_SEC);
            int i = 0;

            while (System.currentTimeMillis() < duration)
            {
                final long start = System.currentTimeMillis();
                validateReplay(archiveProxy, messageCount);

                printScore(++i, System.currentTimeMillis() - start);
            }
        }
    }

    private void printScore(final int i, final long time)
    {
        final double rate = (totalRecordingLength * 1000.0d / time) / MEGABYTE;
        final double receivedMb = totalRecordingLength / MEGABYTE;
        System.out.printf("%d : received %.02f MB, replayed @ %.02f MB/s %n", i, receivedMb, rate);
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
        throws InterruptedException
    {
        System.out.printf("Sending %,d messages%n", MESSAGE_COUNT);

        final CountDownLatch waitForData = new CountDownLatch(1);

        trackRecordingProgress(recordingEvents, waitForData);
        publishDataToRecorded(publication, MESSAGE_COUNT);

        waitForData.await();

        return MESSAGE_COUNT;
    }

    private void publishDataToRecorded(final Publication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());
        startPosition = publication.position();
        final int initialTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            startPosition, positionBitsToShift);

        buffer.setMemory(0, 1024, (byte)'z');

        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + (rnd.nextInt((MAX_FRAGMENT_SIZE - 64) / 4) * 4);

            totalPayloadLength += messageLength;
            buffer.putInt(0, i, LITTLE_ENDIAN);
            buffer.putInt(messageLength - 4, i, LITTLE_ENDIAN);

            offer(publication, buffer, messageLength);
        }

        final int lastTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            publication.position(), positionBitsToShift);
        final int termIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            publication.position(), positionBitsToShift, publication.initialTermId());

        totalRecordingLength =
            (termIdFromPosition - publication.initialTermId()) * publication.termBufferLength() +
                (lastTermOffset - initialTermOffset);

        assertThat(publication.position() - startPosition, is(totalRecordingLength));

        lastTermId = termIdFromPosition;
    }

    private void validateReplay(final ArchiveProxy archiveProxy, final int messageCount)
    {
        final int replayStreamId = (int)correlationId;

        try (Subscription replay = aeron.addSubscription(REPLAY_URI, replayStreamId))
        {
            final long correlationId = this.correlationId++;

            TestUtil.waitFor(() -> archiveProxy.replay(
                recordingId,
                startPosition,
                totalRecordingLength,
                REPLAY_URI,
                replayStreamId,
                correlationId));

            awaitSubscriptionIsConnected(replay);

            fragmentCount = 0;
            remaining = totalPayloadLength;

            while (remaining > 0)
            {
                final int fragments = replay.poll(validateFragmentHandler, 128);
                if (0 == fragments && replay.hasNoImages() && remaining > 0)
                {
                    System.err.println("Unexpected close of image: remaining=" + remaining);
                    break;
                }
            }

            assertThat(fragmentCount, is(messageCount));
            assertThat(remaining, is(0L));
        }
    }

    private void validateFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        @SuppressWarnings("unused") final Header header)
    {
        assertThat(buffer.getInt(offset, LITTLE_ENDIAN), is(fragmentCount));
        assertThat(buffer.getInt(offset + (length - 4), LITTLE_ENDIAN), is(fragmentCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= length;
        fragmentCount++;
    }

    private void trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch waitForData)
    {
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new NoOpRecordingEventsListener()
            {
                public void onProgress(
                    final long recordingId0,
                    final long startPosition,
                    final long position)
                {
                    assertThat(recordingId0, is(recordingId));
                    recorded = position - startPosition;
                    printf("a=%d total=%d %n", recorded, totalRecordingLength);
                }
            },
            recordingEvents,
            1);

        final Thread t = new Thread(
            () ->
            {
                try
                {
                    recorded = 0;
                    final long start = System.currentTimeMillis();
                    final long startBytes = remaining;

                    while (lastTermId == -1)
                    {
                        TestUtil.waitFor(() -> recordingEventsAdapter.poll() != 0);
                    }

                    final long deltaTime = System.currentTimeMillis() - start;
                    final long deltaBytes = remaining - startBytes;
                    final double rate = ((deltaBytes * 1000.0d) / deltaTime) / MEGABYTE;
                    printf("Archive reported rate: %.02f MB/s %n", rate);
                }
                catch (final Throwable throwable)
                {
                    trackerError = throwable;
                }

                waitForData.countDown();
            });

        t.setDaemon(true);
        t.start();
    }
}
