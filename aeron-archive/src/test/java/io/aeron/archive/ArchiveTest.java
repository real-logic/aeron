/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.Archive.Context;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSubscriptionDescriptorConsumer;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static io.aeron.archive.ArchiveThreadingMode.DEDICATED;
import static io.aeron.archive.ArchiveThreadingMode.SHARED;
import static io.aeron.archive.Catalog.MIN_CAPACITY;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(10)
public class ArchiveTest
{
    @Test
    public void shouldGenerateRecordingName()
    {
        final long recordingId = 1L;
        final long segmentPosition = 2 * 64 * 1024;
        final String expected = "1-" + (2 * 64 * 1024) + ".rec";

        final String actual = Archive.segmentFileName(recordingId, segmentPosition);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldCalculateCorrectSegmentFilePosition()
    {
        final int termLength = LogBufferDescriptor.TERM_MIN_LENGTH;
        final int segmentLength = termLength * 8;

        long startPosition = 0;
        long position = 0;
        assertEquals(0L, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = 0;
        position = termLength * 2;
        assertEquals(0L, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = 0;
        position = segmentLength;
        assertEquals(segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength;
        assertEquals(startPosition, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength * 4;
        position = termLength * 4;
        assertEquals(startPosition, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength + segmentLength;
        assertEquals(
            termLength + segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength;
        position = termLength + segmentLength - FrameDescriptor.FRAME_ALIGNMENT;
        assertEquals(termLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));

        startPosition = termLength + FrameDescriptor.FRAME_ALIGNMENT;
        position = termLength + segmentLength;
        assertEquals(
            termLength + segmentLength, segmentFileBasePosition(startPosition, position, termLength, segmentLength));
    }

    @Test
    public void shouldAllowMultipleConnectionsInParallel() throws InterruptedException
    {
        final int numberOfArchiveClients = 5;
        final long connectTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        final CountDownLatch latch = new CountDownLatch(numberOfArchiveClients);
        final ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(numberOfArchiveClients);
        final ManyToOneConcurrentLinkedQueue<AeronArchive> archiveClientQueue = new ManyToOneConcurrentLinkedQueue<>();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .clientLivenessTimeoutNs(connectTimeoutNs)
            .dirDeleteOnStart(true)
            .publicationUnblockTimeoutNs(connectTimeoutNs * 2)
            .threadingMode(ThreadingMode.SHARED);
        final Context archiveCtx = new Context()
            .threadingMode(SHARED)
            .connectTimeoutNs(connectTimeoutNs);
        executor.prestartAllCoreThreads();

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            for (int i = 0; i < numberOfArchiveClients; i++)
            {
                executor.execute(
                    () ->
                    {
                        final AeronArchive.Context ctx = new AeronArchive.Context().messageTimeoutNs(connectTimeoutNs);
                        final AeronArchive archive = AeronArchive.connect(ctx);
                        archiveClientQueue.add(archive);
                        latch.countDown();
                    });
            }

            assertTrue(latch.await(driver.archive().context().connectTimeoutNs() * 2, TimeUnit.NANOSECONDS));

            AeronArchive archiveClient;
            while (null != (archiveClient = archiveClientQueue.poll()))
            {
                archiveClient.close();
            }
        }
        finally
        {
            executor.shutdownNow();
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    public void shouldRecoverRecordingWithNonZeroStartPosition()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Context archiveCtx = new Context().threadingMode(SHARED);

        long resultingPosition;
        final int initialPosition = DataHeaderFlyweight.HEADER_LENGTH * 9;
        final long recordingId;

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx.clone(), archiveCtx.clone());
            AeronArchive archive = AeronArchive.connect())
        {
            final int termLength = 128 * 1024;
            final int initialTermId = 29;

            final String channel = new ChannelUriStringBuilder()
                .media(CommonContext.IPC_MEDIA)
                .initialPosition(initialPosition, initialTermId, termLength)
                .build();

            final Publication publication = archive.addRecordedExclusivePublication(channel, 1);
            final DirectBuffer buffer = new UnsafeBuffer("Hello World".getBytes(StandardCharsets.US_ASCII));

            while ((resultingPosition = publication.offer(buffer)) <= 0)
            {
                Tests.yield();
            }

            final Aeron aeron = archive.context().aeron();

            int counterId;
            final int sessionId = publication.sessionId();
            final CountersReader countersReader = aeron.countersReader();
            while (Aeron.NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(countersReader, sessionId)))
            {
                Tests.yield();
            }

            recordingId = RecordingPos.getRecordingId(countersReader, counterId);

            while (countersReader.getCounterValue(counterId) < resultingPosition)
            {
                Tests.yield();
            }
        }

        try (Catalog catalog = openCatalog(archiveCtx))
        {
            final Catalog.CatalogEntryProcessor catalogEntryProcessor =
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                descriptorEncoder.stopPosition(Aeron.NULL_VALUE);

            assertTrue(catalog.forEntry(recordingId, catalogEntryProcessor));
        }

        final Context archiveCtxClone = archiveCtx.clone();
        final MediaDriver.Context driverCtxClone = driverCtx.clone();
        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtxClone, archiveCtxClone);
            AeronArchive archive = AeronArchive.connect())
        {
            assertEquals(initialPosition, archive.getStartPosition(recordingId));
            assertEquals(resultingPosition, archive.getStopPosition(recordingId));
        }
        finally
        {
            archiveCtxClone.deleteDirectory();
            driverCtxClone.deleteDirectory();
        }
    }

    private static Catalog openCatalog(final Context archiveCtx)
    {
        final IntConsumer intConsumer = (version) -> {};
        return new Catalog(
            new File(archiveCtx.archiveDirectoryName()),
            new SystemEpochClock(),
            MIN_CAPACITY,
            true,
            null,
            intConsumer);
    }

    @Test
    @Timeout(10)
    public void shouldListRegisteredRecordingSubscriptions()
    {
        final int expectedStreamId = 7;
        final String channelOne = "aeron:ipc";
        final String channelTwo = "aeron:udp?endpoint=localhost:5678";
        final String channelThree = "aeron:udp?endpoint=localhost:4321";

        final ArrayList<SubscriptionDescriptor> descriptors = new ArrayList<>();
        @SuppressWarnings("Indentation") final RecordingSubscriptionDescriptorConsumer consumer =
            (controlSessionId, correlationId, subscriptionId, streamId, strippedChannel) ->
                descriptors.add(new SubscriptionDescriptor(
                    controlSessionId, correlationId, subscriptionId, streamId, strippedChannel));

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Context archiveCtx = new Context().threadingMode(SHARED);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect())
        {
            final long subIdOne = archive.startRecording(channelOne, expectedStreamId, LOCAL);
            final long subIdTwo = archive.startRecording(channelTwo, expectedStreamId + 1, LOCAL);
            final long subOdThree = archive.startRecording(channelThree, expectedStreamId + 2, LOCAL);

            final int countOne = archive.listRecordingSubscriptions(
                0, 5, "ipc", expectedStreamId, true, consumer);

            assertEquals(1, descriptors.size());
            assertEquals(1, countOne);

            descriptors.clear();
            final int countTwo = archive.listRecordingSubscriptions(
                0, 5, "", expectedStreamId, false, consumer);

            assertEquals(3, descriptors.size());
            assertEquals(3, countTwo);

            archive.stopRecording(subIdTwo);

            descriptors.clear();
            final int countThree = archive.listRecordingSubscriptions(
                0, 5, "", expectedStreamId, false, consumer);

            assertEquals(2, descriptors.size());
            assertEquals(2, countThree);

            assertEquals(1L, descriptors.stream().filter((sd) -> sd.subscriptionId == subIdOne).count());
            assertEquals(1L, descriptors.stream().filter((sd) -> sd.subscriptionId == subOdThree).count());
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @Timeout(10)
    public void shouldErrorOnLowSpace() throws IOException
    {
        final int streamId = 7;
        final String channel = "aeron:ipc";
        final long usableSpace = 100;
        final long threshold = 101;
        final FileStore fileStore = mock(FileStore.class);
        when(fileStore.getUsableSpace()).thenReturn(usableSpace);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Context archiveCtx = new Context()
            .archiveFileStore(fileStore)
            .lowStorageSpaceThreshold(threshold)
            .deleteArchiveOnStart(true)
            .threadingMode(SHARED);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect())
        {
            try
            {
                archive.startRecording(channel, streamId, LOCAL);
            }
            catch (final ArchiveException ex)
            {
                assertEquals(ArchiveException.STORAGE_SPACE, ex.errorCode());
                return;
            }

            fail("Expected exception");
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    @Test
    void dataBufferIsAllocatedOnDemand()
    {
        final Context context = new Context();

        final UnsafeBuffer buffer = context.dataBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.dataBuffer());
    }

    @Test
    void dataBufferReturnsValueAssigned()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Context context = new Context();
        context.dataBuffer(buffer);

        assertSame(buffer, context.dataBuffer());
    }

    @Test
    void replayBufferIsAllocatedOnDemandIfThreadingModeIsDEDICATED()
    {
        final Context context = new Context().threadingMode(DEDICATED);

        final UnsafeBuffer buffer = context.replayBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.replayBuffer());
        assertNotSame(context.dataBuffer(), buffer);
    }

    @Test
    void replayBufferReturnsValueAssignedIfThreadingModeIsDEDICATED()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Context context = new Context().threadingMode(DEDICATED);
        context.replayBuffer(buffer);

        assertSame(buffer, context.replayBuffer());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveThreadingMode.class, mode = EXCLUDE, names = "DEDICATED")
    void replayBufferReturnsDataBufferIfThreadingModeIsNotDEDICATED(final ArchiveThreadingMode threadingMode)
    {
        final Context context = new Context().threadingMode(threadingMode);

        final UnsafeBuffer buffer = context.replayBuffer();

        assertSame(context.dataBuffer(), buffer);
    }

    @Test
    void recordChecksumBufferReturnsNullIfRecordChecksumIsNull()
    {
        final Context context = new Context();
        assertNull(context.recordChecksumBuffer());
    }

    @Test
    void recordChecksumBufferIsAllocatedOnDemandIfThreadingModeIsDEDICATED()
    {
        final Checksum recordChecksum = mock(Checksum.class);
        final Context context = new Context().recordChecksum(recordChecksum).threadingMode(DEDICATED);

        final UnsafeBuffer buffer = context.recordChecksumBuffer();

        assertNotNull(buffer);
        assertEquals(context.fileIoMaxLength(), buffer.capacity());
        assertSame(buffer, context.recordChecksumBuffer());
        assertNotSame(context.dataBuffer(), buffer);
    }

    @Test
    void recordChecksumBufferReturnsValueAssignedIfThreadingModeIsDEDICATED()
    {
        final UnsafeBuffer buffer = mock(UnsafeBuffer.class);
        final Checksum recordChecksum = mock(Checksum.class);
        final Context context = new Context().recordChecksum(recordChecksum).threadingMode(DEDICATED);
        context.recordChecksumBuffer(buffer);

        assertSame(buffer, context.recordChecksumBuffer());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveThreadingMode.class, mode = EXCLUDE, names = "DEDICATED")
    void recordChecksumBufferReturnsDataBufferIfThreadingModeIsNotDEDICATED(final ArchiveThreadingMode threadingMode)
    {
        final Checksum recordChecksum = mock(Checksum.class);
        final Context context = new Context().recordChecksum(recordChecksum).threadingMode(threadingMode);

        final UnsafeBuffer buffer = context.recordChecksumBuffer();

        assertSame(context.dataBuffer(), buffer);
    }

    @ParameterizedTest
    @ValueSource(strings = { "localhost:0", "localhost:8888" })
    public void shouldResolveControlResponseEndpointAddress(final String endpoint)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);
        final Context archiveCtx = new Context().threadingMode(SHARED);
        final String controlResponseChannel = "aeron:udp?endpoint=" + endpoint;
        final AeronArchive.Context clientContext = new AeronArchive.Context()
            .controlResponseChannel(controlResponseChannel);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive archive = AeronArchive.connect(clientContext))
        {
            final int count = archive.listRecordings(0, 10,
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
                sourceIdentity) -> {});

            assertEquals(0, count);
        }
        finally
        {
            archiveCtx.deleteDirectory();
            driverCtx.deleteDirectory();
        }
    }

    static final class SubscriptionDescriptor
    {
        final long controlSessionId;
        final long correlationId;
        final long subscriptionId;
        final int streamId;
        final String strippedChannel;

        private SubscriptionDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long subscriptionId,
            final int streamId,
            final String strippedChannel)
        {
            this.controlSessionId = controlSessionId;
            this.correlationId = correlationId;
            this.subscriptionId = subscriptionId;
            this.streamId = streamId;
            this.strippedChannel = strippedChannel;
        }

        public String toString()
        {
            return "SubscriptionDescriptor{" +
                "controlSessionId=" + controlSessionId +
                ", correlationId=" + correlationId +
                ", subscriptionId=" + subscriptionId +
                ", streamId=" + streamId +
                ", strippedChannel='" + strippedChannel + '\'' +
                '}';
        }
    }
}
