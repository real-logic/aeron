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
import io.aeron.archive.TestUtil;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.RecordingEventsPoller;
import io.aeron.archive.codecs.*;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TestWatcher;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.TestUtil.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class ArchiveReplayLoadTest
{
    private static final String CONTROL_RESPONSE_URI = "aeron:udp?endpoint=localhost:54327";
    private static final int CONTROL_RESPONSE_STREAM_ID = 100;

    private static final int TEST_DURATION_SEC = 60;
    private static final String REPLAY_URI = "aeron:udp?endpoint=localhost:54326";

    private static final String PUBLISH_URI = new ChannelUriStringBuilder()
        .media("ipc")
        .mtu(16 * 1024)
        .termLength(32 * 1024 * 1024)
        .build();

    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private static final int MESSAGE_COUNT = 2_000_000;

    static
    {
        System.setProperty(Configuration.SOCKET_RCVBUF_LENGTH_PROP_NAME, Integer.toString(2 * 1024 * 1024));
        System.setProperty(Configuration.SOCKET_SNDBUF_LENGTH_PROP_NAME, Integer.toString(2 * 1024 * 1024));
        System.setProperty(Configuration.INITIAL_WINDOW_LENGTH_PROP_NAME, Integer.toString(2 * 1024 * 1024));
    }

    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT));
    private final Random rnd = new Random();
    private final long seed = System.nanoTime();

    @Rule
    public final TestWatcher testWatcher = TestUtil.newWatcher(this.getClass(), seed);

    private Aeron aeron;
    private Archive archive;
    private MediaDriver driver;
    private AeronArchive aeronArchive;
    private long startPosition;
    private long recordingId = -1L;
    private long totalPayloadLength;
    private volatile long expectedRecordingLength;
    private long recordedLength = 0;
    private Throwable trackerError;
    private long receivedPosition = 0;
    private long remaining;
    private int fragmentCount;
    private FragmentHandler validatingFragmentHandler = this::validateFragment;

    @Before
    public void before() throws Exception
    {
        rnd.setSeed(seed);

        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .useConcurrentCounterManager(true)
                .errorHandler(Throwable::printStackTrace)
                .dirsDeleteOnStart(true));

        archive = Archive.launch(
            new Archive.Context()
                .archiveDir(TestUtil.makeTempDir())
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.DEDICATED)
                .countersManager(driver.context().countersManager())
                .errorHandler(driver.context().errorHandler()));

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .controlResponseChannel(CONTROL_RESPONSE_URI)
                .controlResponseStreamId(CONTROL_RESPONSE_STREAM_ID)
                .aeron(aeron));
    }

    @After
    public void after() throws Exception
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(archive);
        CloseHelper.close(driver);

        archive.context().deleteArchiveDirectory();
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = TEST_DURATION_SEC * 2000)
    public void replay() throws IOException, InterruptedException
    {
        try (Publication publication = aeron.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
             Subscription recordingEvents = aeron.addSubscription(
                 archive.context().recordingEventsChannel(), archive.context().recordingEventsStreamId()))
        {
            awaitConnected(recordingEvents);
            aeronArchive.startRecording(PUBLISH_URI, PUBLISH_STREAM_ID, SourceLocation.LOCAL);

            startDrainingSubscriber(aeron, PUBLISH_URI, PUBLISH_STREAM_ID);
            awaitConnected(publication);

            final CountDownLatch recordingStopped = prepAndSendMessages(recordingEvents, publication);

            assertNull(trackerError);

            recordingStopped.await();
            aeronArchive.stopRecording(PUBLISH_URI, PUBLISH_STREAM_ID);

            assertNull(trackerError);
            assertNotEquals(-1L, recordingId);
            assertEquals(expectedRecordingLength, recordedLength);
        }

        final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TEST_DURATION_SEC);
        int i = 0;

        while (System.currentTimeMillis() < deadlineMs)
        {
            final long start = System.currentTimeMillis();
            replay(i);

            printScore(++i, System.currentTimeMillis() - start);
        }
    }

    private void printScore(final int i, final long time)
    {
        final double rate = (expectedRecordingLength * 1000.0d / time) / MEGABYTE;
        final double receivedMb = expectedRecordingLength / MEGABYTE;
        System.out.printf("%d : received %.02f MB, replayed @ %.02f MB/s %n", i, receivedMb, rate);
    }

    private CountDownLatch prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
    {
        System.out.printf("Sending %,d messages%n", MESSAGE_COUNT);

        final CountDownLatch recordingStopped = new CountDownLatch(1);

        trackRecordingProgress(recordingEvents, recordingStopped);
        publishDataToRecorded(publication, MESSAGE_COUNT);

        return recordingStopped;
    }

    private void publishDataToRecorded(final Publication publication, final int messageCount)
    {
        startPosition = publication.position();
        buffer.setMemory(0, 1024, (byte)'z');

        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + (rnd.nextInt((MAX_FRAGMENT_SIZE - 64) / 4) * 4);

            totalPayloadLength += messageLength;
            buffer.putInt(0, i, LITTLE_ENDIAN);
            buffer.putInt(messageLength - 4, i, LITTLE_ENDIAN);

            offer(publication, buffer, messageLength);
        }

        expectedRecordingLength = publication.position() - startPosition;
    }

    private void replay(final int iteration)
    {
        try (Subscription replay = aeronArchive.replay(
            recordingId, startPosition, expectedRecordingLength, REPLAY_URI, iteration))
        {
            while (replay.hasNoImages())
            {
                Thread.yield();
            }

            fragmentCount = 0;
            remaining = totalPayloadLength;

            while (remaining > 0)
            {
                final int fragments = replay.poll(validatingFragmentHandler, 128);
                if (0 == fragments && replay.hasNoImages() && remaining > 0)
                {
                    System.err.println("Unexpected close of image: remaining=" + remaining);
                    System.err.println("Image position=" + receivedPosition + " expected=" + expectedRecordingLength);

                    pollForError(aeronArchive.controlResponsePoller());
                    break;
                }
            }

            assertThat(fragmentCount, is(MESSAGE_COUNT));
            assertThat(remaining, is(0L));
        }
    }

    private void pollForError(final ControlResponsePoller controlResponsePoller)
    {
        if (controlResponsePoller.poll() != 0 && controlResponsePoller.isPollComplete())
        {
            if (controlResponsePoller.templateId() == ControlResponseDecoder.TEMPLATE_ID &&
                controlResponsePoller.controlResponseDecoder().code() == ControlResponseCode.ERROR)
            {
                System.out.println(controlResponsePoller.controlResponseDecoder().errorMessage());
            }
        }
    }

    private void validateFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        int actual = buffer.getInt(offset, LITTLE_ENDIAN);
        if (fragmentCount != actual)
        {
            throw new IllegalStateException("expected=" + fragmentCount + " actual=" + actual);
        }

        actual = buffer.getInt(offset + (length - 4), LITTLE_ENDIAN);
        if (fragmentCount != actual)
        {
            throw new IllegalStateException("expected=" + fragmentCount + " actual=" + actual);
        }

        remaining -= length;
        fragmentCount++;
        receivedPosition = header.position();
    }

    private void trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch recordingStopped)
    {
        final Thread t = new Thread(
            () ->
            {
                try
                {
                    recordedLength = 0;
                    final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);
                    final RecordingEventsPoller poller = new RecordingEventsPoller(recordingEvents);

                    while (0 == expectedRecordingLength || recordedLength < expectedRecordingLength)
                    {
                        idleStrategy.reset();

                        while (poller.poll() <= 0 && !poller.isPollComplete())
                        {
                            idleStrategy.idle();
                        }

                        switch (poller.templateId())
                        {
                            case RecordingStartedDecoder.TEMPLATE_ID:
                            {
                                final RecordingStartedDecoder decoder = poller.recordingStartedDecoder();
                                recordingId = decoder.recordingId();

                                if (0L != decoder.startPosition())
                                {
                                    throw new IllegalStateException("expected=0 actual=" + decoder.startPosition());
                                }

                                printf("Recording started %d %n", recordingId);
                                break;
                            }

                            case RecordingProgressDecoder.TEMPLATE_ID:
                            {
                                final RecordingProgressDecoder decoder = poller.recordingProgressDecoder();
                                recordedLength = decoder.position() - decoder.startPosition();
                                printf("Recording progress %d %n", recordedLength);
                                break;
                            }
                        }
                    }
                }
                catch (final Throwable throwable)
                {
                    trackerError = throwable;
                }
                finally
                {
                    recordingStopped.countDown();
                }
            });

        t.setDaemon(true);
        t.start();
    }
}
