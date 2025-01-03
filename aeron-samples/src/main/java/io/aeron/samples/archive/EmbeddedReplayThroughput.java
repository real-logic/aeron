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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import io.aeron.samples.SampleConfiguration;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.console.ContinueBarrier;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.Archive.Configuration.ARCHIVE_DIR_DEFAULT;
import static io.aeron.samples.archive.Samples.MEGABYTE;
import static io.aeron.samples.archive.Samples.NOOP_FRAGMENT_HANDLER;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.loadPropertiesFiles;

abstract class EmbeddedReplayThroughputLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class EmbeddedReplayThroughputValue extends EmbeddedReplayThroughputLhsPadding
{
    long messageCount;
}

abstract class EmbeddedReplayThroughputRhsPadding extends EmbeddedReplayThroughputValue
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * Tests the throughput when replaying a recorded stream of messages.
 */
public class EmbeddedReplayThroughput extends EmbeddedReplayThroughputRhsPadding implements AutoCloseable
{
    private static final int REPLAY_STREAM_ID = 101;
    private static final String REPLAY_URI = "aeron:ipc";
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(MESSAGE_LENGTH, CACHE_LINE_LENGTH));
    private int publicationSessionId;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (EmbeddedReplayThroughput test = new EmbeddedReplayThroughput())
        {
            System.out.println("Making a recording for playback...");
            final long recordingLength = test.makeRecording();

            System.out.println("Finding the recording...");
            final long recordingId = test.findRecordingId(ChannelUri.addSessionId(CHANNEL, test.publicationSessionId));
            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                System.out.printf("Replaying %,d messages%n", NUMBER_OF_MESSAGES);
                final long startNs = System.nanoTime();

                test.replayRecording(recordingLength, recordingId);

                final long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
                final double dataRate = (recordingLength * 1000.0d / durationMs) / MEGABYTE;
                final double recordingMb = recordingLength / MEGABYTE;
                final long msgRate = (NUMBER_OF_MESSAGES / durationMs) * 1000L;

                System.out.println("Performance inclusive of replay request and connection setup:");
                System.out.printf(
                    "Replayed %.02f MB @ %.02f MB/s - %,d msg/sec - %d byte payload + 32 byte header%n",
                    recordingMb, dataRate, msgRate, MESSAGE_LENGTH);
            }
            while (barrier.await());
        }
    }

    EmbeddedReplayThroughput()
    {
        final String archiveDirName = Archive.Configuration.archiveDirName();
        final File archiveDir = ARCHIVE_DIR_DEFAULT.equals(archiveDirName) ?
            Samples.createTempDir() : new File(archiveDirName);

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .dirDeleteOnStart(true),
            new Archive.Context()
                .archiveDir(archiveDir)
                .recordingEventsEnabled(false));

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
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

    void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long count = buffer.getLong(offset);
        if (count != messageCount)
        {
            throw new IllegalStateException("invalid message count=" + count + " @ " + messageCount);
        }

        messageCount++;
    }

    private long makeRecording()
    {
        try (Publication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            publicationSessionId = publication.sessionId();
            final String channel = ChannelUri.addSessionId(CHANNEL, publicationSessionId);
            final long subscriptionId = aeronArchive.startRecording(channel, STREAM_ID, SourceLocation.LOCAL);
            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;

            final CountersReader countersReader = aeron.countersReader();
            final long archiveId = aeronArchive.archiveId();
            int recordingCounterId;
            while (Aeron.NULL_VALUE == (recordingCounterId = RecordingPos.findCounterIdBySession(
                countersReader, publicationSessionId, archiveId)))
            {
                idleStrategy.idle();
            }

            try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
            {
                idleStrategy.reset();
                while (!subscription.isConnected())
                {
                    idleStrategy.idle();
                }

                final Image image = subscription.imageBySessionId(publicationSessionId);

                long i = 0;
                while (i < NUMBER_OF_MESSAGES)
                {
                    int workCount = 0;
                    buffer.putLong(0, i);

                    if (publication.offer(buffer, 0, MESSAGE_LENGTH) > 0)
                    {
                        i++;
                        workCount += 1;
                    }

                    final int fragments = image.poll(NOOP_FRAGMENT_HANDLER, 10);
                    if (0 == fragments && image.isClosed())
                    {
                        throw new IllegalStateException("image closed unexpectedly");
                    }

                    workCount += fragments;
                    idleStrategy.idle(workCount);
                }

                final long position = publication.position();
                while (image.position() < position)
                {
                    final int fragments = image.poll(NOOP_FRAGMENT_HANDLER, 10);
                    if (0 == fragments && image.isClosed())
                    {
                        throw new IllegalStateException("image closed unexpectedly");
                    }
                    idleStrategy.idle(fragments);
                }

                awaitRecordingComplete(recordingCounterId, position, idleStrategy);

                return position;
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionId);
            }
        }
    }

    private void awaitRecordingComplete(final int counterId, final long position, final IdleStrategy idleStrategy)
    {
        final CountersReader counters = aeron.countersReader();
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idleStrategy.idle();
        }
    }

    private void replayRecording(final long recordingLength, final long recordingId)
    {
        try (Subscription subscription = aeronArchive.replay(
            recordingId, 0L, recordingLength, REPLAY_URI, REPLAY_STREAM_ID))
        {
            final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();
            while (!subscription.isConnected())
            {
                idleStrategy.idle();
            }

            messageCount = 0;
            final Image image = subscription.imageAtIndex(0);
            final ImageFragmentAssembler fragmentAssembler = new ImageFragmentAssembler(this::onMessage);

            while (messageCount < NUMBER_OF_MESSAGES)
            {
                final int fragments = image.poll(fragmentAssembler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments && image.isClosed())
                {
                    System.out.println("\n*** unexpected end of stream at message count: " + messageCount);
                    break;
                }

                idleStrategy.idle(fragments);
            }
        }
    }

    private long findRecordingId(final String expectedChannel)
    {
        final MutableLong foundRecordingId = new MutableLong();

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
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
            sourceIdentity) -> foundRecordingId.set(recordingId);

        final int recordingsFound = aeronArchive.listRecordingsForUri(
            0L, 10, expectedChannel, STREAM_ID, consumer);

        if (1 != recordingsFound)
        {
            throw new IllegalStateException("should have been only one recording");
        }

        return foundRecordingId.get();
    }
}
