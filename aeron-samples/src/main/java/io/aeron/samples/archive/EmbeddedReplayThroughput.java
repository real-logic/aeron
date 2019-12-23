/*
 *  Copyright 2014-2019 Real Logic Ltd.
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
import static io.aeron.samples.archive.TestUtil.MEGABYTE;
import static io.aeron.samples.archive.TestUtil.NOOP_FRAGMENT_HANDLER;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Tests the throughput when replaying a recorded stream of messages.
 */
public class EmbeddedReplayThroughput implements AutoCloseable
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
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onMessage);
    private long messageCount;
    private int publicationSessionId;

    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        try (EmbeddedReplayThroughput test = new EmbeddedReplayThroughput())
        {
            System.out.println("Making a recording for playback...");
            final long recordingLength = test.makeRecording();
            Thread.sleep(10);

            System.out.println("Finding the recording...");
            final long recordingId = test.findRecordingId(ChannelUri.addSessionId(CHANNEL, test.publicationSessionId));
            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                System.out.printf("Replaying %,d messages%n", NUMBER_OF_MESSAGES);
                final long start = System.nanoTime();

                test.replayRecording(recordingLength, recordingId);

                final long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
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

    public EmbeddedReplayThroughput()
    {
        final String archiveDirName = Archive.Configuration.archiveDirName();
        final File archiveDir = ARCHIVE_DIR_DEFAULT.equals(archiveDirName) ?
            TestUtil.createTempDir() : new File(archiveDirName);

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

    public void close()
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(archivingMediaDriver);

        archivingMediaDriver.archive().context().deleteArchiveDirectory();
        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
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

                awaitRecordingComplete(position, idleStrategy);

                return position;
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionId);
            }
        }
    }

    private void awaitRecordingComplete(final long position, final IdleStrategy idleStrategy)
    {
        final CountersReader counters = aeron.countersReader();
        final int counterId = RecordingPos.findCounterIdBySession(counters, publicationSessionId);

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
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(10, 10, 1000, 1000);
            while (!subscription.isConnected())
            {
                idleStrategy.idle();
            }

            messageCount = 0;
            final FragmentAssembler fragmentAssembler = this.fragmentAssembler;
            fragmentAssembler.clear();

            while (messageCount < NUMBER_OF_MESSAGES)
            {
                final int fragments = subscription.poll(fragmentAssembler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    if (!subscription.isConnected())
                    {
                        System.out.println("unexpected end of stream at message count: " + messageCount);
                        break;
                    }
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
            throw new IllegalStateException("should have been one recording");
        }

        return foundRecordingId.get();
    }
}
