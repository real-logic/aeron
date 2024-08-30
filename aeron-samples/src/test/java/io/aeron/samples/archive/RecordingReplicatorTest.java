/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class RecordingReplicatorTest
{
    private static final String SRC_ARCHIVE_CONTROL_CHANNEL =
        "aeron:udp?alias=src-request-channel|endpoint=localhost:8091";
    private static final int SRC_ARCHIVE_CONTROL_STREAM_ID = 1001;
    private static final String SRC_ARCHIVE_REPLICATION_CHANNEL =
        "aeron:udp?alias=dst-replication-channel|endpoint=localhost:9998";
    private static final String DST_ARCHIVE_CONTROL_CHANNEL =
        "aeron:udp?alias=dst-request-channel|endpoint=localhost:8092";
    private static final int DST_ARCHIVE_CONTROL_STREAM_ID = 2002;
    private static final String DST_ARCHIVE_REPLICATION_CHANNEL =
        "aeron:udp?alias=dst-replication-channel|endpoint=localhost:9999";
    private static final int TERM_BUFFER_LENGTH = 128 * 1024;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver srcMediaDriver;
    private TestMediaDriver dstMediaDriver;
    private Archive srcArchive;
    private Archive dstArchive;
    private AeronArchive srcAeronArchive;
    private AeronArchive dstAeronArchive;

    @BeforeEach
    void setup(@TempDir final Path tempDir)
    {
        srcMediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("src-driver").toString())
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true),
            testWatcher);
        testWatcher.dataCollector().add(srcMediaDriver.context().aeronDirectory());

        dstMediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
            .spiesSimulateConnection(true)
            .aeronDirectoryName(tempDir.resolve("dst-driver").toString())
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true),
            testWatcher);
        testWatcher.dataCollector().add(dstMediaDriver.context().aeronDirectory());

        srcArchive = Archive.launch(new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .recordingEventsEnabled(false)
            .aeronDirectoryName(srcMediaDriver.aeronDirectoryName())
            .archiveDir(tempDir.resolve("src-archive").toFile())
            .controlChannel(SRC_ARCHIVE_CONTROL_CHANNEL)
            .controlStreamId(SRC_ARCHIVE_CONTROL_STREAM_ID)
            .replicationChannel(SRC_ARCHIVE_REPLICATION_CHANNEL));
        testWatcher.dataCollector().add(srcArchive.context().archiveDir());

        dstArchive = Archive.launch(new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .recordingEventsEnabled(false)
            .aeronDirectoryName(dstMediaDriver.aeronDirectoryName())
            .archiveDir(tempDir.resolve("dst-archive").toFile())
            .controlChannel(DST_ARCHIVE_CONTROL_CHANNEL)
            .controlStreamId(DST_ARCHIVE_CONTROL_STREAM_ID)
            .replicationChannel(DST_ARCHIVE_REPLICATION_CHANNEL));
        testWatcher.dataCollector().add(dstArchive.context().archiveDir());

        srcAeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(srcMediaDriver.aeronDirectoryName())
            .controlRequestChannel(SRC_ARCHIVE_CONTROL_CHANNEL)
            .controlRequestStreamId(SRC_ARCHIVE_CONTROL_STREAM_ID)
            .controlResponseChannel("aeron:udp?alias=src-archive-response|endpoint=localhost:0")
            .recordingSignalConsumer(new RecordingSignalCapture()));

        dstAeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(dstMediaDriver.aeronDirectoryName())
            .controlRequestChannel(DST_ARCHIVE_CONTROL_CHANNEL)
            .controlRequestStreamId(DST_ARCHIVE_CONTROL_STREAM_ID)
            .controlResponseChannel("aeron:udp?alias=dst-archive-response|endpoint=localhost:0")
            .recordingSignalConsumer(new RecordingSignalCapture()));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(dstAeronArchive, srcAeronArchive, dstArchive, srcArchive, dstMediaDriver, srcMediaDriver);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 9 })
    @InterruptAfter(30)
    void replicateAsNewRecording(final int srcMessageCount)
    {
        createRecording(srcAeronArchive, IPC_CHANNEL, 555, 3);
        createRecording(srcAeronArchive, "aeron:udp?endpoint=localhost:8108", 666, 1);
        final long srcRecordingId = createRecording(
            srcAeronArchive, IPC_CHANNEL + "?alias=third", 1010101010, srcMessageCount);

        final RecordingReplicator recordingReplicator = new RecordingReplicator(
            dstAeronArchive,
            srcRecordingId,
            NULL_VALUE,
            SRC_ARCHIVE_CONTROL_CHANNEL,
            SRC_ARCHIVE_CONTROL_STREAM_ID,
            null);
        final long replicatedRecordingId = recordingReplicator.replicate();

        verifyRecordingReplicated(srcRecordingId, replicatedRecordingId);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 7, 21 })
    @InterruptAfter(30)
    void replicateOverAnExistingRecording(final int srcMessageCount)
    {
        createRecording(srcAeronArchive, IPC_CHANNEL, 555, 1);
        createRecording(srcAeronArchive, "aeron:udp?endpoint=localhost:8108", 666, 1);
        final long srcRecordingId = createRecording(
            srcAeronArchive, IPC_CHANNEL + "?alias=third", 1010101010, srcMessageCount);

        createRecording(dstAeronArchive, IPC_CHANNEL + "?alias=one", 111, 3);
        final long dstRecordingId = createRecording(
            dstAeronArchive,
            "aeron:udp?endpoint=localhost:8114|init-term-id=13|term-id=27|term-offset=1024|term-length=64K",
            444, 19);

        try (AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(dstMediaDriver.aeronDirectoryName())
            .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
            .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
            .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
            .recordingSignalConsumer(new RecordingSignalCapture())))
        {
            final RecordingReplicator recordingReplicator = new RecordingReplicator(
                aeronArchive,
                srcRecordingId,
                dstRecordingId,
                SRC_ARCHIVE_CONTROL_CHANNEL,
                SRC_ARCHIVE_CONTROL_STREAM_ID,
                "aeron:udp?alias=test-channel|endpoint=localhost:9393");
            final long replicatedRecordingId = recordingReplicator.replicate();
            assertEquals(dstRecordingId, replicatedRecordingId);

            verifyRecordingReplicated(srcRecordingId, replicatedRecordingId);
        }
    }

    private long createRecording(
        final AeronArchive aeronArchive,
        final String channel,
        final int streamId,
        final int numMessages)
    {
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            final CountersReader counters = aeronArchive.context().aeron().countersReader();
            final int counterId = Tests.awaitRecordingCounterId(
                counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            final BufferClaim bufferClaim = new BufferClaim();

            for (int i = 0; i < numMessages; i++)
            {
                final int messageSize = ThreadLocalRandom.current().nextInt(8, 500);
                long position;
                while ((position = publication.tryClaim(messageSize, bufferClaim)) < 0)
                {
                    if (position == Publication.CLOSED ||
                        position == Publication.NOT_CONNECTED ||
                        position == Publication.MAX_POSITION_EXCEEDED)
                    {
                        fail("tryClaim failed: " + Publication.errorString(position));
                    }
                    Tests.yield();
                }

                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
                buffer.putInt(offset, ThreadLocalRandom.current().nextInt(), LITTLE_ENDIAN);
                buffer.putInt(
                    offset + (messageSize - SIZE_OF_INT), ThreadLocalRandom.current().nextInt(), LITTLE_ENDIAN);
                bufferClaim.commit();
            }

            Tests.awaitPosition(counters, counterId, publication.position());

            final RecordingSignalCapture signalCapture =
                (RecordingSignalCapture)aeronArchive.context().recordingSignalConsumer();
            signalCapture.reset();
            aeronArchive.stopRecording(publication);
            signalCapture.awaitSignalForRecordingId(aeronArchive, recordingId, RecordingSignal.STOP);

            if (numMessages > 0)
            {
                final long startPosition = aeronArchive.getStartPosition(recordingId);
                final long stopPosition = aeronArchive.getStopPosition(recordingId);
                assertNotEquals(startPosition, stopPosition);
            }

            return recordingId;
        }
    }

    private void verifyRecordingReplicated(final long srcRecordingId, final long dstRecordingId)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);

        assertEquals(1, srcAeronArchive.listRecording(srcRecordingId, collector.reset()));
        final RecordingDescriptor srcRecording = collector.descriptors().get(0).retain();

        while (dstAeronArchive.getStopPosition(dstRecordingId) == AeronArchive.NULL_POSITION)
        {
            Tests.yield();
        }

        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor dstRecording = collector.descriptors().get(0).retain();

        assertNotEquals(srcRecording.recordingId(), dstRecording.recordingId());
        assertEquals(srcRecording.startTimestamp(), dstRecording.startTimestamp());
        assertEquals(srcRecording.startPosition(), dstRecording.startPosition());
        assertEquals(srcRecording.stopPosition(), dstRecording.stopPosition());
        assertEquals(srcRecording.initialTermId(), dstRecording.initialTermId());
        assertEquals(srcRecording.segmentFileLength(), dstRecording.segmentFileLength());
        assertEquals(srcRecording.termBufferLength(), dstRecording.termBufferLength());
        assertEquals(srcRecording.mtuLength(), dstRecording.mtuLength());
        assertEquals(srcRecording.sessionId(), dstRecording.sessionId());
        assertEquals(srcRecording.streamId(), dstRecording.streamId());
        assertEquals(srcRecording.strippedChannel(), dstRecording.strippedChannel());
        assertEquals(srcRecording.originalChannel(), dstRecording.originalChannel());
        assertEquals(srcRecording.sourceIdentity(), dstRecording.sourceIdentity());
        assertEquals(dstRecording.controlSessionId(), dstRecording.controlSessionId());
    }
}
