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
import io.aeron.archive.checksum.Checksum;
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

import static io.aeron.archive.Archive.Configuration.FILE_IO_MAX_LENGTH_DEFAULT;
import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingDescriptorDecoder.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.BitUtil.SIZE_OF_LONG;
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
    static final long MAX_CATALOG_LENGTH = Integer.MAX_VALUE;
    static final long MAX_ENTRIES = calculateMaxEntries(MAX_CATALOG_LENGTH, DEFAULT_RECORD_LENGTH);
    static final long DEFAULT_MAX_ENTRIES = 8 * 1024;
    static final byte VALID = 1;
    static final byte INVALID = 0;

    private final RecordingDescriptorHeaderDecoder descriptorHeaderDecoder = new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorHeaderEncoder descriptorHeaderEncoder = new RecordingDescriptorHeaderEncoder();

    private final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();


    private final int recordLength;
    private final int maxDescriptorStringsCombinedLength;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private boolean isClosed;
    private final File catalogFile;
    private final File archiveDir;
    private final EpochClock epochClock;

    private FileChannel catalogChannel;
    private MappedByteBuffer catalogByteBuffer;
    private UnsafeBuffer catalogBuffer;
    private UnsafeBuffer fieldAccessBuffer;
    private int maxRecordingId;
    private long nextRecordingId = 0;

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final long maxNumEntries,
        final EpochClock epochClock,
        final Checksum checksum,
        final UnsafeBuffer buffer)
    {
        this.archiveDir = archiveDir;
        this.forceWrites = fileSyncLevel > 0;
        this.forceMetadata = fileSyncLevel > 1;
        this.epochClock = epochClock;

        validateMaxEntries(maxNumEntries);

        catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
        try
        {
            final boolean catalogPreExists = catalogFile.exists();
            MappedByteBuffer catalogMappedByteBuffer = null;
            FileChannel catalogFileChannel = null;
            long catalogLength = -1;

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

                catalogMappedByteBuffer = catalogFileChannel.map(READ_WRITE, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                CloseHelper.close(catalogFileChannel);
                LangUtil.rethrowUnchecked(ex);
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
                forceWrites(archiveDirChannel);
                recordLength = DEFAULT_RECORD_LENGTH;

                new CatalogHeaderEncoder()
                    .wrap(catalogBuffer, 0)
                    .entryLength(DEFAULT_RECORD_LENGTH)
                    .version(ArchiveMarkFile.SEMANTIC_VERSION);
            }

            maxDescriptorStringsCombinedLength =
                recordLength - (DESCRIPTOR_HEADER_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12);
            maxRecordingId = (int)calculateMaxEntries(catalogLength, recordLength) - 1;

            refreshCatalog(true, checksum, buffer);
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
        catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);

        try
        {
            MappedByteBuffer catalogMappedByteBuffer = null;
            long catalogLength = -1;

            final StandardOpenOption[] openOptions = writable ?
                new StandardOpenOption[]{ READ, WRITE, SPARSE } : new StandardOpenOption[]{ READ };
            try (FileChannel channel = FileChannel.open(catalogFile.toPath(), openOptions))
            {
                catalogLength = channel.size();
                catalogMappedByteBuffer = channel.map(writable ? READ_WRITE : READ_ONLY, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
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

            refreshCatalog(false, null, null);
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
            unmapAndCloseChannel();
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
            growCatalog(MAX_CATALOG_LENGTH);
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

        forceWrites(catalogChannel);

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
    // Methods for accessing specific record fields by recordingId.
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
        forceWrites(catalogChannel);
    }

    void stopPosition(final long recordingId, final long position)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);
        forceWrites(catalogChannel);
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
        forceWrites(catalogChannel);
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
        forceWrites(catalogChannel);
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
        return min((maxEntries * DEFAULT_RECORD_LENGTH) + DEFAULT_RECORD_LENGTH, MAX_CATALOG_LENGTH);
    }

    static long calculateMaxEntries(final long catalogLength, final long recordLength)
    {
        if (MAX_CATALOG_LENGTH == catalogLength)
        {
            return (MAX_CATALOG_LENGTH - (recordLength - 1)) / recordLength;
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

    void growCatalog(final long maxCatalogLength)
    {
        final long catalogLength = catalogByteBuffer.capacity();
        final long newCatalogLength = Math.min(catalogLength + (catalogLength >> 1), maxCatalogLength);
        if ((newCatalogLength - catalogLength) < recordLength)
        {
            throw new ArchiveException("catalog is full, max length reached: " + maxCatalogLength);
        }

        try
        {
            unmapAndCloseChannel();
            catalogChannel = FileChannel.open(catalogFile.toPath(), READ, WRITE, SPARSE);
            catalogByteBuffer = catalogChannel.map(READ_WRITE, 0, newCatalogLength);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }

        catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
        fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);

        final int oldMaxEntries = maxEntries();
        final int newMaxEntries = (int)calculateMaxEntries(newCatalogLength, recordLength);
        maxRecordingId = newMaxEntries - 1;

        catalogResized(oldMaxEntries, catalogLength, newMaxEntries, newCatalogLength);
    }

    void catalogResized(
        final int maxEntries, final long catalogLength, final int newMaxEntries, final long newCatalogLength)
    {
//        System.out.println("Catalog size changed: " + maxEntries + " entries (" + catalogLength + " bytes) => " +
//            newMaxEntries + " entries ( " + newCatalogLength + " bytes)");
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     *
     * @param fixOnRefresh set if the catalog should have its entries fixed.
     * @param checksum     to validate the last fragment upon the page straddle.
     * @param buffer       to recover the stop position.
     */
    private void refreshCatalog(final boolean fixOnRefresh, final Checksum checksum, final UnsafeBuffer buffer)
    {
        if (fixOnRefresh)
        {
            final UnsafeBuffer tmpBuffer = null != buffer ?
                buffer : new UnsafeBuffer(ByteBuffer.allocateDirect(FILE_IO_MAX_LENGTH_DEFAULT));
            forEach((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                refreshAndFixDescriptor(headerDecoder, descriptorEncoder, descriptorDecoder, checksum, tmpBuffer));
        }
        else
        {
            forEach((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                nextRecordingId = descriptorDecoder.recordingId() + 1);
        }
    }

    private void refreshAndFixDescriptor(
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder,
        final Checksum checksum,
        final UnsafeBuffer buffer)
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
                checksum,
                buffer,
                (segmentFile) ->
                {
                    throw new ArchiveException(
                        "Found potentially incomplete last fragment straddling page boundary in file: " +
                        segmentFile.getAbsolutePath() +
                        "\nRun `ArchiveTool verify` for corrective action!");
                }));

            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = recordingId + 1;
    }

    private void forceWrites(final FileChannel channel)
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
        final Checksum checksum,
        final UnsafeBuffer buffer,
        final Predicate<File> truncateOnPageStraddle)
    {
        if (null == maxSegmentFile)
        {
            return startPosition;
        }
        else
        {
            final int startTermOffset = (int)(startPosition & (termLength - 1));
            final long startTermBasePosition = startPosition - startTermOffset;
            final long segmentFileBasePosition = parseSegmentFilePosition(maxSegmentFile);
            final int fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;
            final int segmentStopOffset = recoverStopOffset(
                archiveDir, maxSegmentFile, fileOffset, segmentLength, truncateOnPageStraddle, checksum, buffer);

            return max(segmentFileBasePosition + segmentStopOffset, startPosition);
        }
    }

    static boolean fragmentStraddlesPageBoundary(final int fragmentOffset, final int fragmentLength)
    {
        return (fragmentOffset / PAGE_SIZE) != ((fragmentOffset + (fragmentLength - 1)) / PAGE_SIZE);
    }

    private void unmapAndCloseChannel()
    {
        final MappedByteBuffer buffer = this.catalogByteBuffer;
        IoUtil.unmap(buffer);
        this.catalogByteBuffer = null;
        CloseHelper.close(catalogChannel);
    }

    private static int recoverStopOffset(
        final File archiveDir,
        final String segmentFile,
        final int offset,
        final int segmentFileLength,
        final Predicate<File> truncateOnPageStraddle,
        final Checksum checksum,
        final UnsafeBuffer buffer)
    {
        final File file = new File(archiveDir, segmentFile);
        try (FileChannel segment = FileChannel.open(file.toPath(), READ, WRITE))
        {
            final int offsetLimit = (int)min(segmentFileLength, segment.size());
            final ByteBuffer byteBuffer = buffer.byteBuffer();

            int nextFragmentOffset = offset;
            int lastFragmentLength = 0;
            int bufferOffset = 0;

            out:
            while (nextFragmentOffset < offsetLimit)
            {
                final int bytesRead = readNextChunk(segment, byteBuffer, nextFragmentOffset, offsetLimit);

                bufferOffset = 0;
                while (bufferOffset < bytesRead)
                {
                    final int frameLength = frameLength(buffer, bufferOffset);
                    if (frameLength <= 0)
                    {
                        break out;
                    }

                    lastFragmentLength = align(frameLength, FRAME_ALIGNMENT);
                    nextFragmentOffset += lastFragmentLength;
                    bufferOffset += lastFragmentLength;
                }
            }

            final int lastFragmentOffset = nextFragmentOffset - lastFragmentLength;
            if (fragmentStraddlesPageBoundary(lastFragmentOffset, lastFragmentLength) &&
                !isValidFragment(buffer, bufferOffset - lastFragmentLength, lastFragmentLength, checksum) &&
                truncateOnPageStraddle.test(file))
            {
                segment.truncate(lastFragmentOffset);
                byteBuffer.put(0, (byte)0).limit(1).position(0);
                segment.write(byteBuffer, segmentFileLength - 1);

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

    private static int readNextChunk(
        final FileChannel segment, final ByteBuffer byteBuffer, final int offset, final int limit) throws IOException
    {
        int position = offset;
        byteBuffer.clear().limit(min(byteBuffer.capacity(), limit - position));
        do
        {
            final int bytesRead = segment.read(byteBuffer, position);
            if (bytesRead < 0)
            {
                break;
            }
            position += bytesRead;
        }
        while (byteBuffer.remaining() > 0);

        return position - offset;
    }

    private static boolean isValidFragment(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength, final Checksum checksum)
    {
        return null != checksum && hasValidChecksum(buffer, fragmentOffset, alignedFragmentLength, checksum) ||
            hasDataInAllPagesAfterStraddle(buffer, fragmentOffset, alignedFragmentLength);
    }

    private static boolean hasValidChecksum(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength, final Checksum checksum)
    {
        final int computedChecksum = checksum.compute(
            buffer.addressOffset(),
            fragmentOffset + HEADER_LENGTH,
            alignedFragmentLength - HEADER_LENGTH);
        final int recordedChecksum = frameSessionId(buffer, fragmentOffset);

        return recordedChecksum == computedChecksum;
    }

    private static boolean hasDataInAllPagesAfterStraddle(
        final UnsafeBuffer buffer, final int fragmentOffset, final int alignedFragmentLength)
    {
        int straddleOffset = (fragmentOffset / PAGE_SIZE + 1) * PAGE_SIZE;
        final int endOffset = fragmentOffset + alignedFragmentLength;
        while (straddleOffset < endOffset)
        {
            if (isEmptyPage(buffer, straddleOffset, endOffset))
            {
                return false;
            }
            straddleOffset += PAGE_SIZE;
        }

        return true;
    }

    private static boolean isEmptyPage(final UnsafeBuffer buffer, final int pageStart, final int endOffset)
    {
        for (int i = pageStart, pageEnd = min(pageStart + PAGE_SIZE, endOffset); i < pageEnd; i += SIZE_OF_LONG)
        {
            if (0L != buffer.getLong(i))
            {
                return false;
            }
        }

        return true;
    }
}
