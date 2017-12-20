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

import io.aeron.archive.codecs.*;
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

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.codecs.RecordingDescriptorDecoder.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 1KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file.
 * <p>
 * @see RecordingDescriptorHeaderDecoder
 * @see RecordingDescriptorDecoder
 * Catalog file format:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                      Descriptor Length                        |
 *  +---------------+-----------------------------------------------+
 *  |     valid     |                  Reserved                     |
 *  +---------------+-----------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                          Reserved                             |
 *  +---------------------------------------------------------------+
 *  |                Recording Descriptor (< 1024)                  |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                          Repeats...                           |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
class Catalog implements AutoCloseable
{
    @FunctionalInterface
    interface CatalogEntryProcessor
    {
        void accept(
            RecordingDescriptorHeaderEncoder headerEncoder,
            RecordingDescriptorHeaderDecoder headerDecoder,
            RecordingDescriptorEncoder descriptorEncoder,
            RecordingDescriptorDecoder descriptorDecoder);
    }

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    static final int PAGE_SIZE = 4096;
    static final int NULL_RECORD_ID = -1;

    static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
    static final int DEFAULT_RECORD_LENGTH = 1024;
    static final byte VALID = 1;
    static final byte INVALID = 0;

    private final RecordingDescriptorHeaderDecoder descriptorHeaderDecoder = new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorHeaderEncoder descriptorHeaderEncoder = new RecordingDescriptorHeaderEncoder();

    private final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

    private final MappedByteBuffer indexMappedBBuffer;
    private final UnsafeBuffer indexUBuffer;
    private final UnsafeBuffer fieldAccessBuffer;

    private final int recordLength;
    private final int maxDescriptorStringsCombinedLength;
    private final int maxRecordingId;
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
        this(archiveDir, archiveDirChannel, fileSyncLevel, epochClock, true);
    }

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final EpochClock epochClock,
        final boolean fixOnRefresh)
    {
        this.archiveDir = archiveDir;
        this.fileSyncLevel = fileSyncLevel;
        this.epochClock = epochClock;
        final File indexFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
        final boolean indexPreExists = indexFile.exists();

        try (FileChannel channel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            indexMappedBBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            indexUBuffer = new UnsafeBuffer(indexMappedBBuffer);
            fieldAccessBuffer = new UnsafeBuffer(indexMappedBBuffer);

            if (!indexPreExists && archiveDirChannel != null && fileSyncLevel > 0)
            {
                archiveDirChannel.force(fileSyncLevel > 1);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        try
        {
            if (indexPreExists)
            {
                final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder()
                    .wrap(indexUBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

                if (catalogHeaderDecoder.version() != CatalogHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("Catalog file version" + catalogHeaderDecoder.version() +
                        " does not match software:" + CatalogHeaderDecoder.SCHEMA_VERSION);
                }

                recordLength = catalogHeaderDecoder.entryLength();
            }
            else
            {
                new CatalogHeaderEncoder()
                    .wrap(indexUBuffer, 0)
                    .version(CatalogHeaderEncoder.SCHEMA_VERSION)
                    .entryLength(DEFAULT_RECORD_LENGTH);

                recordLength = DEFAULT_RECORD_LENGTH;
            }

            maxDescriptorStringsCombinedLength =
                recordLength - (DESCRIPTOR_HEADER_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12);
            maxRecordingId = (Integer.MAX_VALUE - (2 * recordLength - 1)) / recordLength;

            refreshCatalog(fixOnRefresh);
        }
        catch (final Throwable ex)
        {
            close();
            throw ex;
        }
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
        if (nextRecordingId > maxRecordingId)
        {
            throw new IllegalStateException("Catalog is full, max recordings reached: " + maxRecordingId);
        }

        final int combinedStringsLen = strippedChannel.length() + sourceIdentity.length() + originalChannel.length();
        if (combinedStringsLen > maxDescriptorStringsCombinedLength)
        {
            throw new IllegalArgumentException("Combined length of channel:'" + strippedChannel +
                "' and sourceIdentity:'" + sourceIdentity +
                "' and originalChannel:'" + originalChannel +
                "' exceeds max allowed:" + maxDescriptorStringsCombinedLength);
        }

        final long newRecordingId = nextRecordingId;

        indexUBuffer.wrap(indexMappedBBuffer, recordingDescriptorOffset(newRecordingId), recordLength);
        descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);

        initDescriptor(
            descriptorEncoder,
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

        descriptorHeaderEncoder
            .wrap(indexUBuffer, 0)
            .length(descriptorEncoder.encodedLength())
            .valid(VALID);

        nextRecordingId++;

        if (fileSyncLevel > 0)
        {
            indexMappedBBuffer.force();
        }

        return newRecordingId;
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId >= maxRecordingId)
        {
            return false;
        }

        buffer.wrap(indexMappedBBuffer, recordingDescriptorOffset(recordingId), recordLength);

        return descriptorLength(buffer) > 0;
    }

    boolean wrapAndValidateDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId >= maxRecordingId)
        {
            return false;
        }

        buffer.wrap(indexMappedBBuffer, recordingDescriptorOffset(recordingId), recordLength);

        return descriptorLength(buffer) > 0 && isValidDescriptor(buffer);
    }

    boolean hasRecording(final long recordingId)
    {
        return recordingId >= 0 && recordingId < nextRecordingId &&
            fieldAccessBuffer.getInt(
                recordingDescriptorOffset(recordingId) +
                    RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), LITTLE_ENDIAN) > 0;
    }

    void forEach(final CatalogEntryProcessor consumer)
    {
        long recordingId = 0L;
        while (recordingId < maxRecordingId && wrapDescriptor(recordingId, indexUBuffer))
        {
            descriptorHeaderDecoder.wrap(
                indexUBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(indexUBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, indexUBuffer);
            descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);
            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);
            ++recordingId;
        }
    }

    boolean forEntry(final CatalogEntryProcessor consumer, final long recordingId)
    {
        if (wrapDescriptor(recordingId, indexUBuffer))
        {
            descriptorHeaderDecoder.wrap(
                indexUBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(indexUBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, indexUBuffer);
            descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);
            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);

            return true;
        }

        return false;
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
            .stopPosition(NULL_POSITION)
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

    static void wrapDescriptorDecoder(final RecordingDescriptorDecoder decoder, final UnsafeBuffer descriptorBuffer)
    {
        decoder.wrap(
            descriptorBuffer,
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
    }

    //
    // Methods for access specify record fields by recordingId.
    // Note: These methods are thread safe.
    /////////////////////////////////////////////////////////////

    void recordingStopped(final long recordingId, final long position, final long timestamp)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

        fieldAccessBuffer.putLong(offset + stopPositionEncodingOffset(), position, LITTLE_ENDIAN);
        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), timestamp, LITTLE_ENDIAN);
    }

    long stopPosition(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH +
            startPositionEncodingOffset();

        return fieldAccessBuffer.getLong(offset, LITTLE_ENDIAN);
    }

    RecordingSummary recordingSummary(final long recordingId, final RecordingSummary summary)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

        summary.recordingId = recordingId;
        summary.startPosition = fieldAccessBuffer.getLong(offset + startPositionEncodingOffset(), LITTLE_ENDIAN);
        summary.stopPosition = fieldAccessBuffer.getLong(offset + stopPositionEncodingOffset(), LITTLE_ENDIAN);
        summary.initialTermId = fieldAccessBuffer.getInt(offset + initialTermIdEncodingOffset(), LITTLE_ENDIAN);
        summary.segmentFileLength = fieldAccessBuffer.getInt(offset + segmentFileLengthEncodingOffset(), LITTLE_ENDIAN);
        summary.termBufferLength = fieldAccessBuffer.getInt(offset + termBufferLengthEncodingOffset(), LITTLE_ENDIAN);
        summary.mtuLength = fieldAccessBuffer.getInt(offset + mtuLengthEncodingOffset(), LITTLE_ENDIAN);
        summary.streamId = fieldAccessBuffer.getInt(offset + streamIdEncodingOffset(), LITTLE_ENDIAN);
        summary.sessionId = fieldAccessBuffer.getInt(offset + sessionIdEncodingOffset(), LITTLE_ENDIAN);

        return summary;
    }

    static int descriptorLength(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getInt(RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), LITTLE_ENDIAN);
    }

    static boolean isValidDescriptor(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getByte(RecordingDescriptorHeaderDecoder.validEncodingOffset()) == VALID;
    }

    int recordingDescriptorOffset(final long recordingId)
    {
        return (int)(recordingId * recordLength) + recordLength;
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     */
    private void refreshCatalog(final boolean fixOnRefresh)
    {
        if (fixOnRefresh)
        {
            forEach(this::refreshAndFixDescriptor);
        }
        else
        {
            forEach(((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) -> nextRecordingId++));
        }
    }

    private void refreshAndFixDescriptor(
        @SuppressWarnings("unused") final RecordingDescriptorHeaderEncoder unused,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        if (headerDecoder.valid() == VALID && decoder.stopTimestamp() == NULL_TIME)
        {
            int segmentIndex = 0;
            File segmentFile = new File(archiveDir, segmentFileName(recordingId, segmentIndex));
            final long startPosition = decoder.startPosition();

            if (!segmentFile.exists())
            {
                encoder.stopPosition(startPosition);
            }
            else
            {
                File nextSegmentFile = new File(archiveDir, segmentFileName(recordingId, segmentIndex + 1));
                while (nextSegmentFile.exists())
                {
                    segmentIndex++;
                    segmentFile = nextSegmentFile;
                    nextSegmentFile = new File(archiveDir, segmentFileName(recordingId, segmentIndex + 1));
                }
                final int segmentFileLength = decoder.segmentFileLength();
                final long stopOffset = recoverStopOffset(segmentFile, segmentFileLength);
                final int termBufferLength = decoder.termBufferLength();
                final long recordingLength =
                    (startPosition % termBufferLength) + (segmentIndex * segmentFileLength) + stopOffset;
                encoder.stopPosition(startPosition + recordingLength);
            }

            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = recordingId + 1;
    }

    private long recoverStopOffset(final File segmentFile, final int segmentFileLength)
    {
        long lastFragmentSegmentOffset = 0;
        try (FileChannel segment = FileChannel.open(segmentFile.toPath(), READ))
        {
            final ByteBuffer headerBB = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(headerBB);
            long nextFragmentSegmentOffset = 0;
            do
            {
                headerBB.clear();
                if (HEADER_LENGTH != segment.read(headerBB, nextFragmentSegmentOffset))
                {
                    throw new IllegalStateException("Unexpected read failure from file: " +
                        segmentFile.getAbsolutePath() + " at position:" + nextFragmentSegmentOffset);
                }

                if (headerFlyweight.frameLength() == 0)
                {
                    break;
                }

                lastFragmentSegmentOffset = nextFragmentSegmentOffset;
                nextFragmentSegmentOffset += align(headerFlyweight.frameLength(), FRAME_ALIGNMENT);
            }
            while (nextFragmentSegmentOffset != segmentFileLength);

            if ((nextFragmentSegmentOffset / PAGE_SIZE) == (lastFragmentSegmentOffset / PAGE_SIZE))
            {
                // if last fragment does not straddle page boundaries we need not drop it
                lastFragmentSegmentOffset = nextFragmentSegmentOffset;
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return lastFragmentSegmentOffset;
    }
}
