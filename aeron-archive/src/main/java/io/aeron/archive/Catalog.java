/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.BiConsumer;

import static io.aeron.archive.ArchiveUtil.recordingDataFileName;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 4KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file.
 * <p>
 * Catalog file format:
 * <pre>
 * # |---------------- 32b --------------|
 * 0 |desc-length 4b|------24b-unused----|
 * 1 |RecordingDescriptor (length < 4064)|
 * 2 |...continues...                    |
 *128|------------- repeat --------------|
 * </pre>
 * <p>
 */
class Catalog implements AutoCloseable
{
    private static final String CATALOG_INDEX_FILE_NAME = "archive.catalog";

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = HEADER_LENGTH;
    static final int MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH =
        (RECORD_LENGTH - (CATALOG_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12));
    static final int NULL_RECORD_ID = -1;
    static final int MAX_CATALOG_SIZE = Integer.MAX_VALUE - (RECORD_LENGTH - 1);
    static final int MAX_RECORDING_ID = MAX_CATALOG_SIZE / RECORD_LENGTH;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final UnsafeBuffer indexUBuffer;
    private final MappedByteBuffer indexMappedBBuffer;

    private final File archiveDir;
    private final int fileSyncLevel;
    private final EpochClock epochClock;
    private long nextRecordingId = 0;

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final EpochClock epochClock)
    {
        this.archiveDir = archiveDir;
        this.fileSyncLevel = fileSyncLevel;
        this.epochClock = epochClock;
        final File indexFile = new File(archiveDir, CATALOG_INDEX_FILE_NAME);
        final boolean indexPreExists = indexFile.exists();

        try (FileChannel channel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            indexMappedBBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_CATALOG_SIZE);
            indexUBuffer = new UnsafeBuffer(indexMappedBBuffer);

            if (!indexPreExists && archiveDirChannel != null && fileSyncLevel > 0)
            {
                archiveDirChannel.force(fileSyncLevel > 1);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        refreshCatalog();
    }

    public void close()
    {
        IoUtil.unmap(indexMappedBBuffer);
    }

    long addNewRecording(
        final long startPosition,
        final long startTimestamp,
        final int imageInitialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        if (nextRecordingId > MAX_RECORDING_ID)
        {
            throw new IllegalStateException("Catalog is full, max recordings reached: " + MAX_RECORDING_ID);
        }

        if (strippedChannel.length() + sourceIdentity.length() + originalChannel.length() >
            MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH)
        {
            throw new IllegalArgumentException("Combined length of channel:'" + strippedChannel +
                "' and sourceIdentity:'" + sourceIdentity +
                "' and originalChannel:'" + originalChannel +
                "' exceeds max allowed:" + MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH);
        }

        final long newRecordingId = nextRecordingId;

        indexUBuffer.wrap(indexMappedBBuffer, (int) (newRecordingId * RECORD_LENGTH), RECORD_LENGTH);
        recordingDescriptorEncoder.wrap(indexUBuffer, CATALOG_FRAME_LENGTH);
        initDescriptor(
            recordingDescriptorEncoder,
            newRecordingId,
            startTimestamp,
            startPosition,
            imageInitialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        indexUBuffer.putIntOrdered(0, recordingDescriptorEncoder.encodedLength());
        nextRecordingId++;

        if (fileSyncLevel > 0)
        {
            indexMappedBBuffer.force();
        }

        return newRecordingId;
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId >= nextRecordingId)
        {
            return false;
        }

        buffer.wrap(indexMappedBBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);

        return true;
    }

    UnsafeBuffer wrapDescriptor(final long recordingId)
    {
        if (recordingId < 0 || recordingId >= nextRecordingId)
        {
            return null;
        }

        return new UnsafeBuffer(indexMappedBBuffer, (int) (recordingId * RECORD_LENGTH), RECORD_LENGTH);
    }

    long nextRecordingId()
    {
        return nextRecordingId;
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     */
    private void refreshCatalog()
    {
        forEach(this::refreshDescriptor, 0, MAX_CATALOG_SIZE / RECORD_LENGTH);
    }

    void forEach(final BiConsumer<RecordingDescriptorEncoder, RecordingDescriptorDecoder> consumer)
    {
        forEach(consumer, 0, nextRecordingId);
    }

    void forEach(
        final BiConsumer<RecordingDescriptorEncoder, RecordingDescriptorDecoder> consumer,
        final long fromId,
        final long toId)
    {
        long nextRecordingId = fromId;
        while (nextRecordingId < toId && consumeDescriptor(nextRecordingId, consumer))
        {
            ++nextRecordingId;
        }
    }

    void forEntry(
        final long recordingId,
        final BiConsumer<RecordingDescriptorEncoder, RecordingDescriptorDecoder> consumer)
    {
        if (recordingId < 0 || recordingId >= nextRecordingId)
        {
            throw new IllegalArgumentException("recordingId:" + recordingId + " not found.");
        }

        if (!consumeDescriptor(recordingId, consumer))
        {
            throw new IllegalArgumentException("recordingId:" + recordingId + " not valid.");
        }
    }

    private boolean consumeDescriptor(
        final long recordingId,
        final BiConsumer<RecordingDescriptorEncoder, RecordingDescriptorDecoder> consumer)
    {
        final int offset = (int) (recordingId * RECORD_LENGTH);
        if (offset >= MAX_CATALOG_SIZE)
        {
            return false;
        }

        indexUBuffer.wrap(indexMappedBBuffer, offset, RECORD_LENGTH);
        if (indexUBuffer.getInt(0) == 0)
        {
            return false;
        }
        recordingDescriptorDecoder.wrap(
            indexUBuffer,
            CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        recordingDescriptorEncoder.wrap(indexUBuffer, CATALOG_FRAME_LENGTH);
        consumer.accept(recordingDescriptorEncoder, recordingDescriptorDecoder);

        return true;
    }

    private void refreshDescriptor(final RecordingDescriptorEncoder encoder, final RecordingDescriptorDecoder decoder)
    {
        // clean shutdown of recordings will fill in the stopTimestamp, check for it
        if (decoder.stopTimestamp() == NULL_TIME)
        {
            final long stopPosition = decoder.stopPosition();
            final long recordingLength = stopPosition - decoder.startPosition();
            final int segmentFileLength = decoder.segmentFileLength();
            final int segmentIndex = (int) (recordingLength / segmentFileLength);
            final long stoppedSegmentOffset =
                ((decoder.startPosition() % decoder.termBufferLength()) + recordingLength) % segmentFileLength;

            final File segmentFile = new File(archiveDir, recordingDataFileName(decoder.recordingId(), segmentIndex));
            if (!segmentFile.exists())
            {
                if (recordingLength == 0L || stoppedSegmentOffset == 0)
                {
                    // the process failed before writing to a new file
                }
                else
                {
                    // ???
                    throw new IllegalStateException();
                }
            }
            else
            {
                try (FileChannel segment = FileChannel.open(segmentFile.toPath(), READ))
                {
                    final ByteBuffer headerBB = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
                    final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(headerBB);
                    long lastFragmentSegmentOffset;
                    long nextFragmentSegmentOffset = stoppedSegmentOffset;
                    do
                    {
                        lastFragmentSegmentOffset = nextFragmentSegmentOffset;
                        headerBB.clear();
                        if (HEADER_LENGTH != segment.read(headerBB, nextFragmentSegmentOffset))
                        {
                            throw new IllegalStateException();
                        }
                        nextFragmentSegmentOffset = lastFragmentSegmentOffset +
                            align(headerFlyweight.frameLength(), FRAME_ALIGNMENT);
                    }
                    while (headerFlyweight.frameLength() != 0 && nextFragmentSegmentOffset != segmentFileLength);
                    // since we know descriptor buffers are forced on file rollover we don't need to handle rollover
                    // beyond segment boundary (see RecordingWriter#onFileRollover)

                    if (lastFragmentSegmentOffset != stoppedSegmentOffset)
                    {
                        // process has failed between transfering the data to updating th stop position, we cant trust
                        // the last fragment, so take the position of the previous fragment as the stop position
                        encoder.stopPosition(stopPosition + (lastFragmentSegmentOffset - stoppedSegmentOffset));
                    }
                }
                catch (final Exception e)
                {
                    LangUtil.rethrowUnchecked(e);
                }
            }
            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = decoder.recordingId() + 1;
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
        final long startTimestamp,
        final long startPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .startTimestamp(startTimestamp)
            .stopTimestamp(NULL_TIME)
            .startPosition(startPosition)
            .stopPosition(startPosition)
            .initialTermId(initialTermId)
            .segmentFileLength(segmentFileLength)
            .termBufferLength(termBufferLength)
            .mtuLength(mtuLength)
            .sessionId(sessionId)
            .streamId(streamId)
            .strippedChannel(strippedChannel)
            .originalChannel(originalChannel)
            .sourceIdentity(sourceIdentity);
    }
}
