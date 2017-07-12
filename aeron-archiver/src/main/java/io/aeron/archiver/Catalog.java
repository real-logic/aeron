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
import org.agrona.BufferUtil;
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
 * Catalog descriptors may legitimately differ from recording descriptors while recordings are in flight. Once a
 * recording is closed the 2 sources should match. To verify a match between catalog contents and archive folder
 * contents the file contents are scanned on startup. Minor discrepancies are fixed on startup, but more severe
 * issues may prevent a catalog from loading.
 */
class Catalog implements AutoCloseable
{
    private static final String CATALOG_FILE_NAME = "archive.cat";
    static final int PAGE_SIZE = 4096;
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    static final int MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH =
        (RECORD_LENGTH - (CATALOG_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 8));
    static final int NULL_RECORD_ID = -1;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(RECORD_LENGTH, PAGE_SIZE);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final FileChannel catalogFileChannel;
    private final File archiveDir;

    private long nextRecordingId = 0;

    Catalog(final File archiveDir)
    {
        this.archiveDir = archiveDir;
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

        refreshCatalog();
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
        if (channel.length() + sourceIdentity.length() > MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH)
        {
            throw new IllegalArgumentException("Combined length of channel:'" + channel + "' and sourceIdentity:'" +
                sourceIdentity + "' exceeds max allowed:" + MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH);
        }
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
            // because writes to the catalog are at absolute position based on newRecordingId, a failure here does not
            // leave the file in an inconsistent state. A partial write followed by a shutdown however may leave the
            // file in a bad size. The refreshCatalog method handles this eventuality.
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

        final int bytesRead = catalogFileChannel.read(buffer, recordingId * RECORD_LENGTH);
        if (bytesRead == 0 || bytesRead == -1)
        {
            return false;
        }

        if (bytesRead != RECORD_LENGTH)
        {
            throw new IllegalStateException("Wrong read length: " + bytesRead);
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

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for endPosition/endTimestamp. This operation may be
     * expensive for large catalogs.
     */
    private void refreshCatalog()
    {
        try
        {
            catalogFileChannel.truncate((catalogFileChannel.size() / RECORD_LENGTH) * RECORD_LENGTH);

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
                    throw new IllegalStateException("Catalog read should be of " + RECORD_LENGTH +
                        " but was:" + byteBuffer.remaining());
                }

                byteBuffer.clear();
                decoder.wrap(
                    unsafeBuffer,
                    CATALOG_FRAME_LENGTH,
                    RecordingDescriptorDecoder.BLOCK_LENGTH,
                    RecordingDescriptorDecoder.SCHEMA_VERSION);

                refreshCatalogEntry(decoder, nextRecordingId);
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

    private void refreshCatalogEntry(
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

        if (catalogRecordingDescriptor.endTimestamp() == NULL_TIME)
        {
            final File descriptorFile = new File(archiveDir, recordingDescriptorFileName(recordingId));
            final RecordingDescriptorDecoder fileRecordingDescriptor =
                loadRecordingDescriptor(descriptorFile, byteBuffer);
            if (fileRecordingDescriptor.endTimestamp() != NULL_TIME)
            {
                updateRecordingMetaDataInCatalog(recordingId,
                    fileRecordingDescriptor.endPosition(),
                    fileRecordingDescriptor.joinTimestamp(),
                    fileRecordingDescriptor.endTimestamp());
            }
            else
            {
                recoverIncompleteMetaData(recordingId, descriptorFile, fileRecordingDescriptor);
            }
        }
    }

    private void recoverIncompleteMetaData(
        final long recordingId,
        final File descriptorFile,
        final RecordingDescriptorDecoder fileRecordingDescriptor) throws IOException
    {
        final int maxSegment = findLastRecordingSegment(recordingId);
        final long endPosition;
        final long joinTimestamp;
        final long endTimestamp;

        if (maxSegment == -1)
        {
            endPosition = fileRecordingDescriptor.joinPosition();
            joinTimestamp = fileRecordingDescriptor.joinTimestamp();
            endTimestamp = descriptorFile.lastModified();
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
            final ByteBuffer headerBuffer = allocateDirectAligned(
                DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(headerBuffer);

            try (FileChannel segmentFileChannel = FileChannel.open(segmentFile.toPath(), READ))
            {
                final long joinPosition = fileRecordingDescriptor.joinPosition();

                if (joinPosition != 0 && maxSegment == 0)
                {
                    validateFirstWritePreamble(
                        recordingId,
                        headerBuffer,
                        headerFlyweight,
                        segmentFileChannel,
                        joinPosition);
                }

                endPosition = readToEndPosition(
                    headerBuffer,
                    headerFlyweight,
                    segmentFileChannel);
            }

            endTimestamp = segmentFile.lastModified();
        }

        updateRecordingMetaDataInCatalog(recordingId, endPosition, joinTimestamp, endTimestamp);
        updateRecordingMetaDataFile(descriptorFile, endPosition, joinTimestamp, endTimestamp);
    }

    private void updateRecordingMetaDataFile(
        final File descriptorFile,
        final long endPosition,
        final long joinTimestamp,
        final long endTimestamp) throws IOException
    {
        try (FileChannel descriptorFileChannel = FileChannel.open(descriptorFile.toPath(), WRITE, READ))
        {
            byteBuffer.clear();
            descriptorFileChannel.read(byteBuffer);
            recordingDescriptorEncoder
                .wrap(unsafeBuffer, CATALOG_FRAME_LENGTH)
                .endPosition(endPosition)
                .joinTimestamp(joinTimestamp)
                .endTimestamp(endTimestamp);

            byteBuffer.clear();
            descriptorFileChannel.write(byteBuffer, 0);
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

    private int findLastRecordingSegment(final long recordingId)
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
