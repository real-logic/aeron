/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import org.agrona.*;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingDescriptorDecoder.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.BitUtil.align;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 1KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file. The first record contains the catalog header.
 * <p>
 *
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
 *  |             Recording Descriptor (less than 1024)             |
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
    public interface CatalogEntryProcessor
    {
        void accept(
            RecordingDescriptorHeaderEncoder headerEncoder,
            RecordingDescriptorHeaderDecoder headerDecoder,
            RecordingDescriptorEncoder descriptorEncoder,
            RecordingDescriptorDecoder descriptorDecoder);
    }

    static final int PAGE_SIZE = 4096;
    static final int NULL_RECORD_ID = Aeron.NULL_VALUE;

    static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
    static final int DEFAULT_RECORD_LENGTH = 1024;
    static final long MAX_ENTRIES = calculateMaxEntries(Integer.MAX_VALUE, DEFAULT_RECORD_LENGTH);
    static final long DEFAULT_MAX_ENTRIES = 8 * 1024;
    static final byte VALID = 1;
    static final byte INVALID = 0;

    private final RecordingDescriptorHeaderDecoder descriptorHeaderDecoder = new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorHeaderEncoder descriptorHeaderEncoder = new RecordingDescriptorHeaderEncoder();

    private final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

    private final MappedByteBuffer catalogByteBuffer;
    private final UnsafeBuffer catalogBuffer;
    private final UnsafeBuffer fieldAccessBuffer;

    private final int recordLength;
    private final int maxDescriptorStringsCombinedLength;
    private final int maxRecordingId;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private boolean isClosed;
    private final File archiveDir;
    private final EpochClock epochClock;
    private final FileChannel catalogChannel;
    private long nextRecordingId = 0;

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final long maxNumEntries,
        final EpochClock epochClock)
    {
        this.archiveDir = archiveDir;
        this.forceWrites = fileSyncLevel > 0;
        this.forceMetadata = fileSyncLevel > 1;
        this.epochClock = epochClock;

        validateMaxEntries(maxNumEntries);

        try
        {
            final File catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
            final boolean catalogPreExists = catalogFile.exists();
            final MappedByteBuffer catalogMappedByteBuffer;
            FileChannel catalogFileChannel = null;
            final long catalogLength;

            try
            {
                catalogFileChannel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, SPARSE);
                if (catalogPreExists)
                {
                    catalogLength = max(catalogFileChannel.size(), calculateCatalogLength(maxNumEntries));
                }
                else
                {
                    catalogLength = calculateCatalogLength(maxNumEntries);
                }

                catalogMappedByteBuffer = catalogFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                CloseHelper.close(catalogFileChannel);
                throw new RuntimeException(ex);
            }

            catalogChannel = catalogFileChannel;
            catalogByteBuffer = catalogMappedByteBuffer;
            catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
            fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);

            final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder();
            catalogHeaderDecoder.wrap(
                catalogBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

            if (catalogPreExists)
            {
                final int version = catalogHeaderDecoder.version();
                if (SemanticVersion.major(version) != ArchiveMarkFile.MAJOR_VERSION)
                {
                    throw new ArchiveException("invalid version " + SemanticVersion.toString(version) +
                        ", archive is " +
                        SemanticVersion.toString(ArchiveMarkFile.SEMANTIC_VERSION));
                }

                recordLength = catalogHeaderDecoder.entryLength();
            }
            else
            {
                forceWrites(archiveDirChannel, forceWrites, forceMetadata);
                recordLength = DEFAULT_RECORD_LENGTH;

                new CatalogHeaderEncoder()
                    .wrap(catalogBuffer, 0)
                    .entryLength(DEFAULT_RECORD_LENGTH)
                    .version(ArchiveMarkFile.SEMANTIC_VERSION);
            }

            maxDescriptorStringsCombinedLength =
                recordLength - (DESCRIPTOR_HEADER_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12);
            maxRecordingId = (int)calculateMaxEntries(catalogLength, recordLength) - 1;

            refreshCatalog(true);
        }
        catch (final Throwable ex)
        {
            close();
            throw ex;
        }
    }

    Catalog(final File archiveDir, final EpochClock epochClock)
    {
        this(archiveDir, epochClock, false, null);
    }

    Catalog(final File archiveDir, final EpochClock epochClock, final boolean writable, final IntConsumer versionCheck)
    {
        this.archiveDir = archiveDir;
        this.forceWrites = false;
        this.forceMetadata = false;
        this.epochClock = epochClock;
        this.catalogChannel = null;

        try
        {
            final File catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
            final MappedByteBuffer catalogMappedByteBuffer;
            final long catalogLength;

            final StandardOpenOption[] openOptions =
                writable ? new StandardOpenOption[]{ READ, WRITE, SPARSE } : new StandardOpenOption[]{ READ };
            try (FileChannel channel = FileChannel.open(catalogFile.toPath(), openOptions))
            {
                catalogLength = channel.size();
                catalogMappedByteBuffer = channel.map(
                    writable ? FileChannel.MapMode.READ_WRITE : FileChannel.MapMode.READ_ONLY, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }

            catalogByteBuffer = catalogMappedByteBuffer;
            catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
            fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);

            final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder();
            catalogHeaderDecoder.wrap(
                catalogBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

            final int version = catalogHeaderDecoder.version();
            if (null == versionCheck)
            {
                if (SemanticVersion.major(version) != ArchiveMarkFile.MAJOR_VERSION)
                {
                    throw new ArchiveException("invalid version " + SemanticVersion.toString(version) +
                        ", archive is " +
                        SemanticVersion.toString(ArchiveMarkFile.SEMANTIC_VERSION));
                }
            }
            else
            {
                versionCheck.accept(version);
            }

            recordLength = catalogHeaderDecoder.entryLength();
            maxDescriptorStringsCombinedLength =
                recordLength - (DESCRIPTOR_HEADER_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12);
            maxRecordingId = (int)calculateMaxEntries(catalogLength, recordLength) - 1;

            refreshCatalog(false);
        }
        catch (final Throwable ex)
        {
            close();
            throw ex;
        }
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.quietClose(catalogChannel); // Ignore error so that the rest can be closed
            IoUtil.unmap(catalogByteBuffer);
        }
    }

    int maxEntries()
    {
        return maxRecordingId + 1;
    }

    int countEntries()
    {
        return (int)nextRecordingId;
    }

    int version()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(catalogByteBuffer);
        final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder()
            .wrap(buffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

        return catalogHeaderDecoder.version();
    }

    void updateVersion(final int version)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(catalogByteBuffer);
        new CatalogHeaderEncoder().wrap(buffer, 0).version(version);
    }

    long addNewRecording(
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimestamp,
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
            throw new ArchiveException("catalog is full, max recordings reached: " + maxEntries());
        }

        final int combinedStringsLen = strippedChannel.length() + sourceIdentity.length() + originalChannel.length();
        if (combinedStringsLen > maxDescriptorStringsCombinedLength)
        {
            throw new ArchiveException("combined length of channel:'" + strippedChannel +
                "' and sourceIdentity:'" + sourceIdentity +
                "' and originalChannel:'" + originalChannel +
                "' exceeds max allowed:" + maxDescriptorStringsCombinedLength);
        }

        final long recordingId = nextRecordingId++;

        catalogBuffer.wrap(catalogByteBuffer, recordingDescriptorOffset(recordingId), recordLength);
        descriptorEncoder
            .wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH)
            .recordingId(recordingId)
            .startTimestamp(startTimestamp)
            .stopTimestamp(stopTimestamp)
            .startPosition(startPosition)
            .stopPosition(stopPosition)
            .initialTermId(imageInitialTermId)
            .segmentFileLength(segmentFileLength)
            .termBufferLength(termBufferLength)
            .mtuLength(mtuLength)
            .sessionId(sessionId)
            .streamId(streamId)
            .strippedChannel(strippedChannel)
            .originalChannel(originalChannel)
            .sourceIdentity(sourceIdentity);

        descriptorHeaderEncoder
            .wrap(catalogBuffer, 0)
            .length(descriptorEncoder.encodedLength())
            .valid(VALID);

        forceWrites(catalogChannel, forceWrites, forceMetadata);

        return recordingId;
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
        return addNewRecording(
            startPosition,
            NULL_POSITION,
            startTimestamp,
            NULL_TIMESTAMP,
            imageInitialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId > maxRecordingId)
        {
            return false;
        }

        buffer.wrap(catalogByteBuffer, recordingDescriptorOffset(recordingId), recordLength);

        return descriptorLength(buffer) > 0;
    }

    boolean wrapAndValidateDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId > maxRecordingId)
        {
            return false;
        }

        buffer.wrap(catalogByteBuffer, recordingDescriptorOffset(recordingId), recordLength);

        return descriptorLength(buffer) > 0 && isValidDescriptor(buffer);
    }

    boolean hasRecording(final long recordingId)
    {
        return recordingId >= 0 && recordingId < nextRecordingId &&
            fieldAccessBuffer.getByte(
                recordingDescriptorOffset(recordingId) +
                    RecordingDescriptorHeaderDecoder.validEncodingOffset()) == VALID;
    }

    int forEach(final CatalogEntryProcessor consumer)
    {
        int count = 0;
        long recordingId = 0L;
        while (wrapDescriptor(recordingId, catalogBuffer))
        {
            descriptorHeaderDecoder.wrap(
                catalogBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

            descriptorHeaderEncoder.wrap(catalogBuffer, 0);

            descriptorDecoder.wrap(
                catalogBuffer,
                DESCRIPTOR_HEADER_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);

            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);
            ++recordingId;
            ++count;
        }

        return count;
    }

    boolean forEntry(final long recordingId, final CatalogEntryProcessor consumer)
    {
        if (wrapDescriptor(recordingId, catalogBuffer))
        {
            descriptorHeaderDecoder.wrap(
                catalogBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

            descriptorHeaderEncoder.wrap(catalogBuffer, 0);

            descriptorDecoder.wrap(
                catalogBuffer,
                DESCRIPTOR_HEADER_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);

            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);

            return true;
        }

        return false;
    }

    long findLast(final long minRecordingId, final int sessionId, final int streamId, final byte[] channelFragment)
    {
        long recordingId = nextRecordingId;
        while (--recordingId >= minRecordingId)
        {
            catalogBuffer.wrap(catalogByteBuffer, recordingDescriptorOffset(recordingId), recordLength);

            if (isValidDescriptor(catalogBuffer))
            {
                descriptorDecoder.wrap(
                    catalogBuffer,
                    DESCRIPTOR_HEADER_LENGTH,
                    RecordingDescriptorDecoder.BLOCK_LENGTH,
                    RecordingDescriptorDecoder.SCHEMA_VERSION);

                if (sessionId == descriptorDecoder.sessionId() &&
                    streamId == descriptorDecoder.streamId() &&
                    originalChannelContains(descriptorDecoder, channelFragment))
                {
                    return recordingId;
                }
            }
        }

        return NULL_RECORD_ID;
    }

    //
    // Methods for access specific record fields by recordingId.
    // Note: These methods are thread safe.
    /////////////////////////////////////////////////////////////

    static boolean originalChannelContains(
        final RecordingDescriptorDecoder descriptorDecoder, final byte[] channelFragment)
    {
        final int fragmentLength = channelFragment.length;
        if (0 == fragmentLength)
        {
            return true;
        }

        final int limit = descriptorDecoder.limit();
        final int strippedChannelLength = descriptorDecoder.strippedChannelLength();
        final int originalChannelOffset =
            limit + RecordingDescriptorDecoder.strippedChannelHeaderLength() + strippedChannelLength;

        descriptorDecoder.limit(originalChannelOffset);
        final int channelLength = descriptorDecoder.originalChannelLength();
        descriptorDecoder.limit(limit);

        final DirectBuffer buffer = descriptorDecoder.buffer();
        int offset = descriptorDecoder.offset() + descriptorDecoder.sbeBlockLength() +
            RecordingDescriptorDecoder.strippedChannelHeaderLength() + strippedChannelLength +
            RecordingDescriptorDecoder.originalChannelHeaderLength();

        nextChar:
        for (int end = offset + (channelLength - fragmentLength); offset <= end; offset++)
        {
            for (int i = 0; i < fragmentLength; i++)
            {
                if (buffer.getByte(offset + i) != channelFragment[i])
                {
                    continue nextChar;
                }
            }

            return true;
        }

        return false;
    }

    void recordingStopped(final long recordingId, final long position, final long timestampMs)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), timestampMs, BYTE_ORDER);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        forceWrites(catalogChannel, forceWrites, forceMetadata);
    }

    void stopPosition(final long recordingId, final long position)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        forceWrites(catalogChannel, forceWrites, forceMetadata);
    }

    void extendRecording(
        final long recordingId, final long controlSessionId, final long correlationId, final int sessionId)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? NULL_POSITION : Long.reverseBytes(NULL_POSITION);

        fieldAccessBuffer.putLong(offset + controlSessionIdEncodingOffset(), controlSessionId, BYTE_ORDER);
        fieldAccessBuffer.putLong(offset + correlationIdEncodingOffset(), correlationId, BYTE_ORDER);
        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), NULL_TIMESTAMP, BYTE_ORDER);
        fieldAccessBuffer.putInt(offset + sessionIdEncodingOffset(), sessionId, BYTE_ORDER);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        forceWrites(catalogChannel, forceWrites, forceMetadata);
    }

    long startPosition(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH +
            startPositionEncodingOffset();

        final long startPosition = fieldAccessBuffer.getLongVolatile(offset);

        return nativeOrder() == BYTE_ORDER ? startPosition : Long.reverseBytes(startPosition);
    }

    void startPosition(final long recordingId, final long position)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH +
            startPositionEncodingOffset();

        fieldAccessBuffer.putLong(offset, position, BYTE_ORDER);
        forceWrites(catalogChannel, forceWrites, forceMetadata);
    }

    long stopPosition(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) +
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH +
            stopPositionEncodingOffset();

        final long stopPosition = fieldAccessBuffer.getLongVolatile(offset);

        return nativeOrder() == BYTE_ORDER ? stopPosition : Long.reverseBytes(stopPosition);
    }

    RecordingSummary recordingSummary(final long recordingId, final RecordingSummary summary)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

        summary.recordingId = recordingId;
        summary.startPosition = fieldAccessBuffer.getLong(offset + startPositionEncodingOffset(), BYTE_ORDER);
        summary.stopPosition = fieldAccessBuffer.getLong(offset + stopPositionEncodingOffset(), BYTE_ORDER);
        summary.initialTermId = fieldAccessBuffer.getInt(offset + initialTermIdEncodingOffset(), BYTE_ORDER);
        summary.segmentFileLength = fieldAccessBuffer.getInt(offset + segmentFileLengthEncodingOffset(), BYTE_ORDER);
        summary.termBufferLength = fieldAccessBuffer.getInt(offset + termBufferLengthEncodingOffset(), BYTE_ORDER);
        summary.mtuLength = fieldAccessBuffer.getInt(offset + mtuLengthEncodingOffset(), BYTE_ORDER);
        summary.streamId = fieldAccessBuffer.getInt(offset + streamIdEncodingOffset(), BYTE_ORDER);
        summary.sessionId = fieldAccessBuffer.getInt(offset + sessionIdEncodingOffset(), BYTE_ORDER);

        return summary;
    }

    static int descriptorLength(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getInt(RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), BYTE_ORDER);
    }

    static boolean isValidDescriptor(final UnsafeBuffer descriptorBuffer)
    {
        return descriptorBuffer.getByte(RecordingDescriptorHeaderDecoder.validEncodingOffset()) == VALID;
    }

    static long calculateCatalogLength(final long maxEntries)
    {
        return min((maxEntries * DEFAULT_RECORD_LENGTH) + DEFAULT_RECORD_LENGTH, Integer.MAX_VALUE);
    }

    static long calculateMaxEntries(final long catalogLength, final long recordLength)
    {
        if (Integer.MAX_VALUE == catalogLength)
        {
            return (Integer.MAX_VALUE - (recordLength - 1)) / recordLength;
        }

        return (catalogLength / recordLength) - 1;
    }

    int recordingDescriptorOffset(final long recordingId)
    {
        return (int)(recordingId * recordLength) + recordLength;
    }

    static void validateMaxEntries(final long maxEntries)
    {
        if (maxEntries < 1 || maxEntries > MAX_ENTRIES)
        {
            throw new ArchiveException(
                "Catalog max entries must be between 1 and " + MAX_ENTRIES + ": maxEntries=" + maxEntries);
        }
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     *
     * @param fixOnRefresh set if the catalog should have its entries fixed.
     */
    private void refreshCatalog(final boolean fixOnRefresh)
    {
        if (fixOnRefresh)
        {
            forEach(this::refreshAndFixDescriptor);
        }
        else
        {
            forEach((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                nextRecordingId = descriptorDecoder.recordingId() + 1);
        }
    }

    private void refreshAndFixDescriptor(
        @SuppressWarnings("unused") final RecordingDescriptorHeaderEncoder unused,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        if (VALID == headerDecoder.valid() && NULL_POSITION == decoder.stopPosition())
        {
            final String[] segmentFiles = listSegmentFiles(archiveDir, recordingId);
            final String maxSegmentFile = findSegmentFileWithHighestPosition(segmentFiles);

            encoder.stopPosition(computeStopPosition(
                archiveDir,
                maxSegmentFile,
                decoder.startPosition(),
                decoder.termBufferLength(),
                decoder.segmentFileLength(),
                (segmentFile) ->
                {
                    throw new ArchiveException(String.format("Found potentially incomplete last fragment in the " +
                            "file: %s.%nPlease run `ArchiveTool verify` to run the " +
                            "corrective action!",
                        segmentFile.getAbsolutePath()));
                }));

            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = recordingId + 1;
    }

    private void forceWrites(final FileChannel channel, final boolean forceWrites, final boolean forceMetadata)
    {
        if (null != channel && forceWrites)
        {
            try
            {
                channel.force(forceMetadata);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    static String[] listSegmentFiles(final File archiveDir, final long recordingId)
    {
        final String prefix = recordingId + "-";
        return archiveDir.list((dir, name) -> name.startsWith(prefix) && name.endsWith(RECORDING_SEGMENT_SUFFIX));
    }

    static String findSegmentFileWithHighestPosition(final String[] segmentFiles)
    {
        if (null == segmentFiles || 0 == segmentFiles.length)
        {
            return null;
        }

        long maxSegmentPosition = NULL_POSITION;
        String maxFileName = null;

        for (final String filename : segmentFiles)
        {
            final long filePosition = parseSegmentFilePosition(filename);
            if (filePosition < 0)
            {
                throw new ArchiveException("negative position encoded in the file name: " + filename);
            }

            if (filePosition > maxSegmentPosition)
            {
                maxSegmentPosition = filePosition;
                maxFileName = filename;
            }
        }

        return maxFileName;
    }

    static long parseSegmentFilePosition(final String filename)
    {
        final int dashOffset = filename.indexOf('-');
        if (-1 == dashOffset)
        {
            throw new ArchiveException("invalid filename format: " + filename);
        }

        final int positionOffset = dashOffset + 1;
        final int positionLength = filename.length() - positionOffset - RECORDING_SEGMENT_SUFFIX.length();
        if (0 >= positionLength)
        {
            throw new ArchiveException("no position encoded in the segment file: " + filename);
        }

        return parseLongAscii(filename, positionOffset, positionLength);
    }

    static long computeStopPosition(
        final File archiveDir,
        final String maxSegmentFile,
        final long startPosition,
        final int termLength,
        final int segmentLength,
        final Predicate<File> truncateFileOnPageStraddle)
    {
        if (null == maxSegmentFile)
        {
            return startPosition;
        }
        else
        {
            final long startTermOffset = startPosition & (termLength - 1);
            final long startTermBasePosition = startPosition - startTermOffset;
            final long segmentFileBasePosition = parseSegmentFilePosition(maxSegmentFile);
            final long fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;
            final long segmentStopOffset = recoverStopOffset(
                archiveDir, maxSegmentFile, fileOffset, segmentLength, truncateFileOnPageStraddle);

            return max(segmentFileBasePosition + segmentStopOffset, startPosition);
        }
    }

    private static long recoverStopOffset(
        final File archiveDir,
        final String segmentFile,
        final long offset,
        final int segmentFileLength,
        final Predicate<File> truncateFileOnPageStraddle)
    {
        final File file = new File(archiveDir, segmentFile);
        try (FileChannel segment = FileChannel.open(file.toPath(), READ, WRITE))
        {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(HEADER_LENGTH);
            buffer.order(BYTE_ORDER);

            long lastFragmentOffset = offset;
            long nextFragmentOffset = offset;
            long lastFrameLength = 0;
            final long offsetLimit = min(segmentFileLength, segment.size());

            do
            {
                buffer.clear();
                if (HEADER_LENGTH != segment.read(buffer, nextFragmentOffset))
                {
                    throw new ArchiveException("unexpected read failure from file: " +
                        file.getAbsolutePath() + " at position:" + nextFragmentOffset);
                }

                final int frameLength = buffer.getInt(FRAME_LENGTH_FIELD_OFFSET);
                if (frameLength <= 0)
                {
                    break;
                }

                lastFrameLength = frameLength;
                lastFragmentOffset = nextFragmentOffset;
                nextFragmentOffset += align(frameLength, FRAME_ALIGNMENT);
            }
            while (nextFragmentOffset < offsetLimit);

            if (fragmentStraddlesPageBoundary(lastFragmentOffset, lastFrameLength) &&
                truncateFileOnPageStraddle.test(file))
            {
                segment.truncate(lastFragmentOffset);
                buffer.put(0, (byte)0).limit(1).position(0);
                segment.write(buffer, segmentFileLength - 1);

                return lastFragmentOffset;
            }
            else
            {
                return nextFragmentOffset;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return Aeron.NULL_VALUE;
        }
    }

    static boolean fragmentStraddlesPageBoundary(final long fragmentOffset, final long fragmentLength)
    {
        return fragmentOffset / PAGE_SIZE != (fragmentOffset + (fragmentLength - 1)) / PAGE_SIZE;
    }
}
