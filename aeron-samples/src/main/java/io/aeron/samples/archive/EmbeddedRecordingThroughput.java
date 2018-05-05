/*
 *  Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.SampleConfiguration;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;

import java.io.File;

import static io.aeron.archive.Archive.Configuration.ARCHIVE_DIR_DEFAULT;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.samples.archive.TestUtil.MEGABYTE;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.loadPropertiesFiles;

public class EmbeddedRecordingThroughput implements AutoCloseable, RecordingEventsListener
{
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(MESSAGE_LENGTH, FRAME_ALIGNMENT));
    private final Thread recordingEventsThread;
    private volatile long recordingStartTimeMs;
    private volatile long stopPosition;
    private volatile boolean isRunning = true;
    private volatile boolean isRecording;

    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        try (EmbeddedRecordingThroughput test = new EmbeddedRecordingThroughput())
        {
            test.startRecording();

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");
            do
            {
                test.streamMessagesForRecording();
            }
            while (barrier.await());

            test.stop();
        }
    }

    public EmbeddedRecordingThroughput()
    {
        final String archiveDirName = Archive.Configuration.archiveDirName();
        final File archiveDir =  ARCHIVE_DIR_DEFAULT.equals(archiveDirName) ?
            TestUtil.createTempDir() : new File(archiveDirName);

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .spiesSimulateConnection(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .deleteArchiveOnStart(true)
                .archiveDir(archiveDir));

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));

        recordingEventsThread = new Thread(this::runRecordingEventPoller);
        recordingEventsThread.setName("recording-events-poller");
        recordingEventsThread.start();
    }

    public void close()
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(archivingMediaDriver);

        archivingMediaDriver.archive().context().deleteArchiveDirectory();
        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
    }

    public void onStart(
        final long recordingId,
        final long startPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        System.out.println("Recording started for id: " + recordingId);
    }

    public void onProgress(final long recordingId, final long startPosition, final long position)
    {
        if (position >= stopPosition)
        {
            final long durationMs = System.currentTimeMillis() - recordingStartTimeMs;
            final long recordingLength = position - startPosition;
            final double dataRate = (recordingLength * 1000.0d / durationMs) / MEGABYTE;
            final double recordingMb = recordingLength / MEGABYTE;
            final long msgRate = (NUMBER_OF_MESSAGES / durationMs) * 1000L;

            System.out.printf(
                "Recorded %.02f MB @ %.02f MB/s - %,d msg/sec - %d byte payload + 32 byte header%n",
                recordingMb, dataRate, msgRate, MESSAGE_LENGTH);

            isRecording = false;
        }
    }

    public void onStop(final long recordingId, final long startPosition, final long stopPosition)
    {
        isRecording = false;
        //System.out.println("Recording stopped for id: " + recordingId + " @ " + stopPosition);
    }

    public void streamMessagesForRecording() throws Exception
    {
        try (ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            isRecording = true;
            stopPosition = Long.MAX_VALUE;
            recordingStartTimeMs = System.currentTimeMillis();

            for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                buffer.putInt(0, i);
                while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
                {
                    Thread.yield();
                }
            }

            stopPosition = publication.position();
        }

        while (isRecording)
        {
            Thread.sleep(1);
        }
    }

    public void stop() throws InterruptedException
    {
        isRunning = false;
        recordingEventsThread.join();
    }

    public void startRecording()
    {
        aeronArchive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
    }

    private void runRecordingEventPoller()
    {
        try (Subscription subscription = aeron.addSubscription(
            AeronArchive.Configuration.recordingEventsChannel(),
            AeronArchive.Configuration.recordingEventsStreamId()))
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(10, 100, 1, 1);
            final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
                this, subscription, FRAGMENT_COUNT_LIMIT);

            while (isRunning)
            {
                idleStrategy.idle(recordingEventsAdapter.poll());
            }
        }
    }
}
