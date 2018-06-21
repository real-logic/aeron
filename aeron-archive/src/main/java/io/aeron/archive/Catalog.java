/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.AsciiEncoding;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_POSTFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingDescriptorDecoder.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;

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

    private final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder();

    private final MappedByteBuffer catalogByteBuffer;
    private final UnsafeBuffer catalogBuffer;
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
        final long maxNumEntries,
        final EpochClock epochClock)
    {
        this.archiveDir = archiveDir;
        this.fileSyncLevel = fileSyncLevel;
        this.epochClock = epochClock;

        validateMaxEntries(maxNumEntries);

        try
        {
            final File catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
            final boolean catalogPreExists = catalogFile.exists();
            MappedByteBuffer catalogMappedByteBuffer = null;
            final long catalogLength;

            try (FileChannel channel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, SPARSE))
            {
                if (catalogPreExists)
                {
                    catalogLength = Math.max(channel.size(), calculateCatalogLength(maxNumEntries));
                }
                else
                {
                    catalogLength = calculateCatalogLength(maxNumEntries);
                }

                catalogMappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                if (null != catalogMappedByteBuffer)
                {
                    IoUtil.unmap(catalogMappedByteBuffer);
                }

                throw new RuntimeException(ex);
            }

            catalogByteBuffer = catalogMappedByteBuffer;
            catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
            fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);

            final UnsafeBuffer catalogHeaderBuffer = new UnsafeBuffer(catalogByteBuffer);

            catalogHeaderDecoder.wrap(
                catalogHeaderBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

            final CatalogHeaderEncoder catalogHeaderEncoder = new CatalogHeaderEncoder();
            catalogHeaderEncoder.wrap(catalogHeaderBuffer, 0);

            if (catalogPreExists)
            {
                if (catalogHeaderDecoder.version() != CatalogHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("catalog file version " + catalogHeaderDecoder.version() +
                        " does not match software:" + CatalogHeaderDecoder.SCHEMA_VERSION);
                }

                recordLength = catalogHeaderDecoder.entryLength();
            }
            else
            {
                if (null != archiveDirChannel && fileSyncLevel > 0)
                {
                    try
                    {
                        archiveDirChannel.force(fileSyncLevel > 1);
                    }
                    catch (final IOException ex)
                    {
                        LangUtil.rethrowUnchecked(ex);
                    }
                }

                catalogHeaderEncoder.entryLength(DEFAULT_RECORD_LENGTH);
                catalogHeaderEncoder.version(CatalogHeaderEncoder.SCHEMA_VERSION);

                recordLength = DEFAULT_RECORD_LENGTH;
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
        this.archiveDir = archiveDir;
        this.fileSyncLevel = 0;
        this.epochClock = epochClock;

        try
        {
            final File catalogFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
            MappedByteBuffer catalogMappedByteBuffer = null;
            final long catalogLength;

            try (FileChannel channel = FileChannel.open(catalogFile.toPath(), READ, WRITE, SPARSE))
            {
                catalogLength = channel.size();
                catalogMappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, catalogLength);
            }
            catch (final Exception ex)
            {
                if (null != catalogMappedByteBuffer)
                {
                    IoUtil.unmap(catalogMappedByteBuffer);
                }

                throw new RuntimeException(ex);
            }

            catalogByteBuffer = catalogMappedByteBuffer;
            catalogBuffer = new UnsafeBuffer(catalogByteBuffer);
            fieldAccessBuffer = new UnsafeBuffer(catalogByteBuffer);

            final UnsafeBuffer catalogHeaderBuffer = new UnsafeBuffer(catalogByteBuffer);

            catalogHeaderDecoder.wrap(
                catalogHeaderBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, CatalogHeaderDecoder.SCHEMA_VERSION);

            if (catalogHeaderDecoder.version() != CatalogHeaderDecoder.SCHEMA_VERSION)
            {
                throw new IllegalArgumentException("catalog file version " + catalogHeaderDecoder.version() +
                    " does not match software:" + CatalogHeaderDecoder.SCHEMA_VERSION);
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
        IoUtil.unmap(catalogByteBuffer);
    }

    public int maxEntries()
    {
        return maxRecordingId + 1;
    }

    public int countEntries()
    {
        return (int)nextRecordingId;
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

        final long newRecordingId = nextRecordingId;

        catalogBuffer.wrap(catalogByteBuffer, recordingDescriptorOffset(newRecordingId), recordLength);
        descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);

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
            .wrap(catalogBuffer, 0)
            .length(descriptorEncoder.encodedLength())
            .valid(VALID);

        nextRecordingId++;

        if (fileSyncLevel > 0)
        {
            catalogByteBuffer.force();
        }

        return newRecordingId;
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
            fieldAccessBuffer.getInt(
                recordingDescriptorOffset(recordingId) +
                    RecordingDescriptorHeaderDecoder.lengthEncodingOffset(), LITTLE_ENDIAN) > 0;
    }

    void forEach(final CatalogEntryProcessor consumer)
    {
        long recordingId = 0L;
        while (wrapDescriptor(recordingId, catalogBuffer))
        {
            descriptorHeaderDecoder.wrap(
                catalogBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(catalogBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, catalogBuffer);
            descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);
            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);
            ++recordingId;
        }
    }

    boolean forEntry(final CatalogEntryProcessor consumer, final long recordingId)
    {
        if (wrapDescriptor(recordingId, catalogBuffer))
        {
            descriptorHeaderDecoder.wrap(
                catalogBuffer, 0, DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(catalogBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, catalogBuffer);
            descriptorEncoder.wrap(catalogBuffer, DESCRIPTOR_HEADER_LENGTH);
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
            .stopTimestamp(NULL_TIMESTAMP)
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

        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);

        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), timestamp, BYTE_ORDER);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);

        if (fileSyncLevel > 0)
        {
            catalogByteBuffer.force();
        }
    }

    void recordingStopped(final long recordingId, final long position)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
        final long stopPosition = nativeOrder() == BYTE_ORDER ? position : Long.reverseBytes(position);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);

        if (fileSyncLevel > 0)
        {
            catalogByteBuffer.force();
        }
    }

    void extendRecording(final long recordingId)
    {
        final int offset = recordingDescriptorOffset(recordingId) + RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

        final long stopPosition = nativeOrder() == BYTE_ORDER ? NULL_POSITION : Long.reverseBytes(NULL_POSITION);

        fieldAccessBuffer.putLong(offset + stopTimestampEncodingOffset(), NULL_TIMESTAMP);
        fieldAccessBuffer.putLongVolatile(offset + stopPositionEncodingOffset(), stopPosition);

        if (fileSyncLevel > 0)
        {
            catalogByteBuffer.force();
        }
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

    public static long calculateCatalogLength(final long maxEntries)
    {
        return Math.min((maxEntries * DEFAULT_RECORD_LENGTH) + DEFAULT_RECORD_LENGTH, Integer.MAX_VALUE);
    }

    public static long calculateMaxEntries(final long catalogLength, final long recordLength)
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

    public static void validateMaxEntries(final long maxEntries)
    {
        if (maxEntries < 1 || maxEntries > MAX_ENTRIES)
        {
            throw new IllegalArgumentException(
                "Catalog max entries must be between 1 and " + MAX_ENTRIES + ": maxEntries=" + maxEntries);
        }
    }

    public static long recoverStopOffset(final File segmentFile, final int segmentFileLength)
    {
        long lastFragmentOffset = 0;
        try (FileChannel segment = FileChannel.open(segmentFile.toPath(), READ))
        {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(HEADER_LENGTH);
            buffer.order(BYTE_ORDER);

            long nextFragmentOffset = 0;
            do
            {
                buffer.clear();
                if (HEADER_LENGTH != segment.read(buffer, nextFragmentOffset))
                {
                    throw new ArchiveException("unexpected read failure from file: " +
                        segmentFile.getAbsolutePath() + " at position:" + nextFragmentOffset);
                }

                final int frameLength = buffer.getInt(DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET);
                if (frameLength == 0)
                {
                    break;
                }

                lastFragmentOffset = nextFragmentOffset;
                nextFragmentOffset += align(frameLength, FRAME_ALIGNMENT);
            }
            while (nextFragmentOffset != segmentFileLength);

            if ((nextFragmentOffset / PAGE_SIZE) == (lastFragmentOffset / PAGE_SIZE))
            {
                // if last fragment does not straddle page boundaries we need not drop it
                lastFragmentOffset = nextFragmentOffset;
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return lastFragmentOffset;
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
        if (headerDecoder.valid() == VALID && decoder.stopPosition() == NULL_POSITION)
        {
            final String prefix = recordingId + "-";
            String[] segmentFiles = archiveDir.list(
                (dir, name) -> name.startsWith(prefix) && name.endsWith(RECORDING_SEGMENT_POSTFIX));

            if (null == segmentFiles)
            {
                segmentFiles = ArrayUtil.EMPTY_STRING_ARRAY;
            }

            int maxSegmentIndex = -1;
            for (final String filename : segmentFiles)
            {
                final int length = filename.length();
                final int offset = prefix.length();
                final int remaining = length - offset - RECORDING_SEGMENT_POSTFIX.length();

                if (remaining > 0)
                {
                    try
                    {
                        maxSegmentIndex = Math.max(
                            AsciiEncoding.parseIntAscii(filename, offset, remaining),
                            maxSegmentIndex);
                    }
                    catch (final Exception ignore)
                    {
                    }
                }
            }

            if (maxSegmentIndex < 0)
            {
                encoder.stopPosition(decoder.startPosition());
            }
            else
            {
                final File maxSegmentFile = new File(archiveDir, segmentFileName(recordingId, maxSegmentIndex));
                final int segmentFileLength = decoder.segmentFileLength();
                final long stopOffset = recoverStopOffset(maxSegmentFile, segmentFileLength);
                final int termBufferLength = decoder.termBufferLength();
                final long startPosition = decoder.startPosition();
                final long recordingLength =
                    (startPosition & (termBufferLength - 1)) + (maxSegmentIndex * segmentFileLength) + stopOffset;
                encoder.stopPosition(startPosition + recordingLength);
            }

            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = recordingId + 1;
    }
}
