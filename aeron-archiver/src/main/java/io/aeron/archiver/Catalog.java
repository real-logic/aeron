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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.*;

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
    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    private static final String CATALOG_FILE_NAME = "archive.cat";
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    static final int MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH =
        (RECORD_LENGTH - (CATALOG_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 8));
    static final int NULL_RECORD_ID = -1;
    static final int MAX_CATALOG_SIZE = Integer.MAX_VALUE - (RECORD_LENGTH - 1);
    static final int MAX_RECORDING_ID = MAX_CATALOG_SIZE / RECORD_LENGTH;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final UnsafeBuffer unsafeBuffer;
    private final File archiveDir;
    private final MappedByteBuffer mappedByteBuffer;

    private long nextRecordingId = 0;

    Catalog(final File archiveDir)
    {
        this.archiveDir = archiveDir;

        final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);

        try (FileChannel channel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_CATALOG_SIZE);
            unsafeBuffer = new UnsafeBuffer(mappedByteBuffer);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }

        refreshCatalog();
    }

    public void close()
    {
        IoUtil.unmap(mappedByteBuffer);
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
        if (nextRecordingId > MAX_RECORDING_ID)
        {
            throw new IllegalStateException("Catalog is full, max recordings reached: " + MAX_RECORDING_ID);
        }

        if (channel.length() + sourceIdentity.length() > MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH)
        {
            throw new IllegalArgumentException("Combined length of channel:'" + channel + "' and sourceIdentity:'" +
                sourceIdentity + "' exceeds max allowed:" + MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH);
        }

        final long newRecordingId = nextRecordingId;

        unsafeBuffer.wrap(mappedByteBuffer, (int)(newRecordingId * RECORD_LENGTH), RECORD_LENGTH);
        recordingDescriptorEncoder.wrap(unsafeBuffer, CATALOG_FRAME_LENGTH);
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

        unsafeBuffer.putInt(0, recordingDescriptorEncoder.encodedLength());
        mappedByteBuffer.force();
        nextRecordingId++;

        return newRecordingId;
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId > nextRecordingId)
        {
            return false;
        }

        buffer.wrap(mappedByteBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);
        return true;
    }

    UnsafeBuffer wrapDescriptor(final long recordingId)
    {
        if (recordingId < 0 || recordingId > nextRecordingId)
        {
            return null;
        }
        return new UnsafeBuffer(mappedByteBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);
    }

    void updateRecordingMetaDataInCatalog(
        final long recordingId,
        final long endPosition,
        final long joinTimestamp,
        final long endTimestamp) throws IOException
    {

        unsafeBuffer.wrap(mappedByteBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);

        recordingDescriptorEncoder
            .wrap(unsafeBuffer, CATALOG_FRAME_LENGTH)
            .endPosition(endPosition)
            .joinTimestamp(joinTimestamp)
            .endTimestamp(endTimestamp);
        mappedByteBuffer.force();
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
        final RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();
        unsafeBuffer.wrap(mappedByteBuffer, (int)(nextRecordingId * RECORD_LENGTH), RECORD_LENGTH);

        while (unsafeBuffer.getInt(0) != 0)
        {
            decoder.wrap(
                unsafeBuffer,
                CATALOG_FRAME_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            validateDescriptor(decoder);
            ++nextRecordingId;
            unsafeBuffer.wrap(mappedByteBuffer, (int)(nextRecordingId * RECORD_LENGTH), RECORD_LENGTH);
        }
    }

    private void validateDescriptor(final RecordingDescriptorDecoder decoder)
    {
        // TODO:
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
        final int termBufferLength,
        final int segmentFileLength,
        final int mtuLength,
        final int initialTermId,
        final long joinPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .termBufferLength(termBufferLength)
            .joinTimestamp(NULL_TIME)
            .joinPosition(joinPosition)
            .endPosition(joinPosition)
            .endTimestamp(NULL_TIME)
            .mtuLength(mtuLength)
            .initialTermId(initialTermId)
            .sessionId(sessionId)
            .streamId(streamId)
            .segmentFileLength(segmentFileLength)
            .channel(channel)
            .sourceIdentity(sourceIdentity);
    }
}
