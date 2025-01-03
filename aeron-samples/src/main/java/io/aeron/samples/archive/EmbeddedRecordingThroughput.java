/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.SampleConfiguration;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.console.ContinueBarrier;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.Archive.Configuration.ARCHIVE_DIR_DEFAULT;
import static io.aeron.samples.archive.Samples.MEGABYTE;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Tests the throughput when recording a stream of messages.
 */
public class EmbeddedRecordingThroughput implements AutoCloseable
{
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(MESSAGE_LENGTH, CACHE_LINE_LENGTH));
    private final RecordingSignalCapture recordingSignalCapture;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (EmbeddedRecordingThroughput test = new EmbeddedRecordingThroughput())
        {
            test.startRecording();
            long previousRecordingId = Aeron.NULL_VALUE;

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");
            do
            {
                if (Aeron.NULL_VALUE != previousRecordingId)
                {
                    test.truncateRecording(previousRecordingId);
                }

                previousRecordingId = test.streamMessagesForRecording();
            }
            while (barrier.await());
        }
    }

    EmbeddedRecordingThroughput()
    {
        final String archiveDirName = Archive.Configuration.archiveDirName();
        final File archiveDir = ARCHIVE_DIR_DEFAULT.equals(archiveDirName) ?
            Samples.createTempDir() : new File(archiveDirName);

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .spiesSimulateConnection(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .recordingEventsEnabled(false)
                .archiveDir(archiveDir));

        aeron = Aeron.connect();

        recordingSignalCapture = new RecordingSignalCapture();

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron)
                .recordingSignalConsumer(recordingSignalCapture));
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(
            aeronArchive,
            aeron,
            archivingMediaDriver,
            () -> archivingMediaDriver.archive().context().deleteDirectory(),
            () -> archivingMediaDriver.mediaDriver().context().deleteDirectory());
    }

    private long streamMessagesForRecording()
    {
        try (ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
            while (!publication.isConnected())
            {
                idleStrategy.idle();
            }

            final long startNs = System.nanoTime();
            final UnsafeBuffer buffer = this.buffer;

            for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                buffer.putLong(0, i);

                idleStrategy.reset();
                while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0)
                {
                    idleStrategy.idle();
                }
            }

            final long stopPosition = publication.position();
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                RecordingPos.findCounterIdBySession(counters, publication.sessionId(), aeronArchive.archiveId());

            idleStrategy.reset();
            while (counters.getCounterValue(counterId) < stopPosition)
            {
                idleStrategy.idle();
            }

            final long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            final double dataRate = (stopPosition * 1000.0d / durationMs) / MEGABYTE;
            final double recordingMb = stopPosition / MEGABYTE;
            final long msgRate = (NUMBER_OF_MESSAGES / durationMs) * 1000L;

            System.out.printf(
                "Recorded %.02f MB @ %.02f MB/s - %,d msg/sec - %d byte payload + 32 byte header%n",
                recordingMb, dataRate, msgRate, MESSAGE_LENGTH);

            return RecordingPos.getRecordingId(counters, counterId);
        }
    }

    private void startRecording()
    {
        aeronArchive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
    }

    private void truncateRecording(final long previousRecordingId)
    {
        recordingSignalCapture.reset();
        aeronArchive.truncateRecording(previousRecordingId, 0L);

        recordingSignalCapture.awaitSignalForRecordingId(aeronArchive, previousRecordingId, RecordingSignal.DELETE);
    }
}
