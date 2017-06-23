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
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import io.aeron.archiver.codecs.RecordingDescriptorEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.*;
import static io.aeron.archiver.RecordingWriter.NULL_TIME;
import static io.aeron.archiver.RecordingWriter.initDescriptor;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 4KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file.
 */
class Catalog implements AutoCloseable
{
    private static final String CATALOG_FILE_NAME = "archive.cat";
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    static final int NULL_RECORD_ID = -1;

    private static final int PAGE_SIZE = 4096;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();

    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer unsafeBuffer;
    private final FileChannel catalogFileChannel;
    private final File archiveDir;
    private long nextRecordingId = 0;

    Catalog(final File archiveDir)
    {
        this.archiveDir = archiveDir;
        byteBuffer = allocateDirectAligned(RECORD_LENGTH, PAGE_SIZE);
        unsafeBuffer = new UnsafeBuffer(byteBuffer);
        recordingDescriptorEncoder.wrap(unsafeBuffer, CATALOG_FRAME_LENGTH);

        final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);
        FileChannel channel = null;
        try
        {
            channel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, DSYNC);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            catalogFileChannel = channel;
        }

        sanitizeCatalog();
    }

    public void close()
    {
        CloseHelper.close(catalogFileChannel);
    }

    long addNewRecording(
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity,
        final int termBufferLength,
        final int mtuLength,
        final int imageInitialTermId,
        final long joinPosition,
        final int segmentFileLength)
    {
        final long newRecordingId = nextRecordingId;

        recordingDescriptorEncoder.limit(CATALOG_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH);
        initDescriptor(
            recordingDescriptorEncoder,
            newRecordingId,
            termBufferLength,
            segmentFileLength,
            mtuLength,
            imageInitialTermId,
            joinPosition,
            sessionId,
            streamId,
            channel,
            sourceIdentity);

        final int encodedLength = recordingDescriptorEncoder.encodedLength();
        unsafeBuffer.putInt(0, encodedLength);

        try
        {
            byteBuffer.clear();
            final int written = catalogFileChannel.write(byteBuffer, newRecordingId * RECORD_LENGTH);
            if (written != RECORD_LENGTH)
            {
                throw new IllegalStateException();
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        nextRecordingId++;

        return newRecordingId;
    }

    boolean readDescriptor(final long recordingId, final ByteBuffer buffer)
        throws IOException
    {
        if (buffer.remaining() != RECORD_LENGTH)
        {
            throw new IllegalArgumentException("buffer must have exactly RECORD_LENGTH remaining to read into");
        }

        final int read = catalogFileChannel.read(buffer, recordingId * RECORD_LENGTH);
        if (read == 0 || read == -1)
        {
            return false;
        }

        if (read != RECORD_LENGTH)
        {
            throw new IllegalStateException("Wrong read size:" + read);
        }

        return true;
    }

    void updateRecordingMetaDataInCatalog(
        final long recordingId,
        final long endPosition,
        final long joinTimestamp,
        final long endTimestamp) throws IOException
    {
        byteBuffer.clear();
        if (!readDescriptor(recordingId, byteBuffer))
        {
            throw new IllegalArgumentException("Invalid recording id : " + recordingId);
        }

        recordingDescriptorEncoder
            .wrap(unsafeBuffer, CATALOG_FRAME_LENGTH)
            .endPosition(endPosition)
            .joinTimestamp(joinTimestamp)
            .endTimestamp(endTimestamp);

        byteBuffer.clear();
        catalogFileChannel.write(byteBuffer, recordingId * RECORD_LENGTH);
    }

    long nextRecordingId()
    {
        return nextRecordingId;
    }

    private void sanitizeCatalog()
    {
        try
        {
            final RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();

            while (catalogFileChannel.read(byteBuffer, nextRecordingId * RECORD_LENGTH) != -1)
            {
                byteBuffer.flip();
                if (byteBuffer.remaining() == 0)
                {
                    break;
                }

                if (byteBuffer.remaining() != RECORD_LENGTH)
                {
                    throw new IllegalStateException();
                }
                byteBuffer.clear();
                decoder.wrap(
                    unsafeBuffer,
                    CATALOG_FRAME_LENGTH,
                    RecordingDescriptorDecoder.BLOCK_LENGTH,
                    RecordingDescriptorDecoder.SCHEMA_VERSION);

                sanitizeCatalogEntry(decoder, nextRecordingId);
                nextRecordingId++;
                byteBuffer.clear();
            }
        }
        catch (final IOException ex)
        {
            CloseHelper.quietClose(catalogFileChannel);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void sanitizeCatalogEntry(
        final RecordingDescriptorDecoder catalogRecordingDescriptor,
        final long recordingId) throws IOException
    {
        if (recordingId != catalogRecordingDescriptor.recordingId())
        {
            throw new IllegalStateException("Expecting recordingId: " + recordingId +
                " but found: " + catalogRecordingDescriptor.recordingId());
        }

        if (catalogRecordingDescriptor.endTimestamp() != NULL_TIME)
        {
            // TODO: do we want to confirm all files for an entry exist? are valid?
            return;
        }

        // On load from a clean shutdown all catalog entries should have a valid end time. If we see a NULL_TIME we
        // need to patch up the entry or declare the index unusable
        if (catalogRecordingDescriptor.endTimestamp() == NULL_TIME)
        {
            final File metaFile = new File(archiveDir, recordingMetaFileName(recordingId));
            final RecordingDescriptorDecoder fileRecordingDescriptor = loadRecordingDescriptor(metaFile);
            if (fileRecordingDescriptor.endTimestamp() != NULL_TIME)
            {
                // metafile has an end time -> it concluded recording, update catalog entry
                updateRecordingMetaDataInCatalog(recordingId,
                    fileRecordingDescriptor.endPosition(),
                    fileRecordingDescriptor.joinTimestamp(),
                    fileRecordingDescriptor.endTimestamp());
                return;
            }
            recoverIncompleteMetaData(recordingId, metaFile, fileRecordingDescriptor);
        }
    }

    private void recoverIncompleteMetaData(
        final long recordingId,
        final File metaFile,
        final RecordingDescriptorDecoder fileRecordingDescriptor) throws IOException
    {
        // last metadata update failed -> look in the recording files for end position
        final int maxSegment = findRecordingLastSegment(recordingId);

        // there are no segments
        final long endPosition;
        final long joinTimestamp;
        final long endTimestamp;

        if (maxSegment == -1)
        {
            // no segments found, no data written, update catalog and meta file with the last modified TS
            endPosition = fileRecordingDescriptor.joinPosition();
            joinTimestamp = fileRecordingDescriptor.joinTimestamp();
            endTimestamp = metaFile.lastModified();
        }
        else
        {
            if (fileRecordingDescriptor.joinTimestamp() == NULL_TIME)
            {
                throw new IllegalStateException("joinTimestamp is NULL_TIME, but 1 or more segment files found");
            }
            else
            {
                joinTimestamp = fileRecordingDescriptor.joinTimestamp();
            }

            final File segmentFile = new File(archiveDir, recordingDataFileName(recordingId, maxSegment));
            final ByteBuffer headerBuffer =
                allocateDirectAligned(DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(headerBuffer);

            try (FileChannel segmentFileChannel = FileChannel.open(segmentFile.toPath(), READ))
            {
                final long joinPosition = fileRecordingDescriptor.joinPosition();
                // validate initial padding frame in first file
                if (joinPosition != 0 && maxSegment == 0)
                {
                    validateFirstWritePreamble(
                        recordingId,
                        headerBuffer,
                        headerFlyweight,
                        segmentFileChannel,
                        joinPosition);
                }

                // chase frames to END_OF_DATA/RECORDING
                endPosition = readToEndPosition(
                    headerBuffer,
                    headerFlyweight,
                    segmentFileChannel
                );

                // correct recording final terminal marker if not found
                if (headerFlyweight.frameLength() != RecordingWriter.END_OF_RECORDING_INDICATOR)
                {
                    headerFlyweight.frameLength(RecordingWriter.END_OF_RECORDING_INDICATOR);
                    segmentFileChannel.write(headerBuffer, endPosition);
                }
            }

            endTimestamp = segmentFile.lastModified();
        }
        updateRecordingMetaDataInCatalog(recordingId, endPosition, joinTimestamp, endTimestamp);

        updateRecordingMetaDataFile(metaFile, endPosition, joinTimestamp, endTimestamp);
    }

    private void updateRecordingMetaDataFile(
        final File metaDataFile,
        final long endPosition,
        final long joinTimestamp,
        final long endTimestamp) throws IOException
    {
        try (FileChannel metaDataFileChannel = FileChannel.open(metaDataFile.toPath(), WRITE, READ))
        {
            byteBuffer.clear();
            metaDataFileChannel.read(byteBuffer);
            recordingDescriptorEncoder
                .wrap(unsafeBuffer, CATALOG_FRAME_LENGTH)
                .endPosition(endPosition)
                .joinTimestamp(joinTimestamp)
                .endTimestamp(endTimestamp);

            byteBuffer.clear();
            metaDataFileChannel.write(byteBuffer, 0);
        }
    }

    private void validateFirstWritePreamble(
        final long recordingId,
        final ByteBuffer headerBuffer,
        final DataHeaderFlyweight headerFlyweight,
        final FileChannel segmentFileChannel,
        final long joinPosition) throws IOException
    {
        segmentFileChannel.read(headerBuffer, 0);
        headerBuffer.clear();

        final int headerType = headerFlyweight.headerType();
        if (headerType != DataHeaderFlyweight.HDR_TYPE_PAD)
        {
            throw new IllegalStateException("Recording : " + recordingId + " segment 0 is corrupt" +
                ". Expected a padding frame as join position is non-zero, but type is:" +
                headerType);
        }
        final int frameLength = headerFlyweight.frameLength();
        if (frameLength != joinPosition)
        {
            throw new IllegalStateException("Recording : " + recordingId + " segment 0 is corrupt" +
                ". Expected a padding frame with a length equal to join position, but length is:" +
                headerType);
        }
    }

    private long readToEndPosition(
        final ByteBuffer headerBuffer,
        final DataHeaderFlyweight headerFlyweight,
        final FileChannel segmentFileChannel) throws IOException
    {
        long endPosition = 0;
        // first read required before examining the flyweight value
        segmentFileChannel.read(headerBuffer, endPosition);
        headerBuffer.clear();

        while (headerFlyweight.frameLength() > 0)
        {
            endPosition += align(headerFlyweight.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
            segmentFileChannel.read(headerBuffer, endPosition);
            headerBuffer.clear();
        }
        return endPosition;
    }

    private int findRecordingLastSegment(final long recordingId)
    {
        int maxSegment = -1;
        final String[] recordingSegments = ArchiveUtil.listRecordingSegments(archiveDir, recordingId);
        for (final String segmentName : recordingSegments)
        {
            final int segmentIndex = ArchiveUtil.segmentIndexFromFileName(segmentName);
            maxSegment = Math.max(maxSegment, segmentIndex);
        }
        return maxSegment;
    }
}
