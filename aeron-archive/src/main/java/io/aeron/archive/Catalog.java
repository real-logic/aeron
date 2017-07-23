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
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.BiConsumer;

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
    private static final String CATALOG_FILE_NAME = "archive.cat";

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    static final int MAX_DESCRIPTOR_STRINGS_COMBINED_LENGTH =
        (RECORD_LENGTH - (CATALOG_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12));
    static final int NULL_RECORD_ID = -1;
    static final int MAX_CATALOG_SIZE = Integer.MAX_VALUE - (RECORD_LENGTH - 1);
    static final int MAX_RECORDING_ID = MAX_CATALOG_SIZE / RECORD_LENGTH;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final UnsafeBuffer unsafeBuffer;
    private final MappedByteBuffer mappedByteBuffer;

    private final int fileSyncLevel;
    private long nextRecordingId = 0;

    Catalog(final File archiveDir, final FileChannel archiveDirChannel, final int fileSyncLevel)
    {
        this.fileSyncLevel = fileSyncLevel;
        final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);
        final boolean filePreExists = catalogFile.exists();

        try (FileChannel channel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_CATALOG_SIZE);
            unsafeBuffer = new UnsafeBuffer(mappedByteBuffer);

            if (!filePreExists && archiveDirChannel != null && fileSyncLevel > 0)
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
        IoUtil.unmap(mappedByteBuffer);
    }

    long addNewRecording(
        final long startPosition,
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

        unsafeBuffer.wrap(mappedByteBuffer, (int)(newRecordingId * RECORD_LENGTH), RECORD_LENGTH);
        recordingDescriptorEncoder.wrap(unsafeBuffer, CATALOG_FRAME_LENGTH);
        initDescriptor(
            recordingDescriptorEncoder,
            newRecordingId,
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

        unsafeBuffer.putInt(0, recordingDescriptorEncoder.encodedLength());
        nextRecordingId++;

        if (fileSyncLevel > 0)
        {
            mappedByteBuffer.force();
        }

        return newRecordingId;
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId >= nextRecordingId)
        {
            return false;
        }

        buffer.wrap(mappedByteBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);

        return true;
    }

    UnsafeBuffer wrapDescriptor(final long recordingId)
    {
        if (recordingId < 0 || recordingId >= nextRecordingId)
        {
            return null;
        }

        return new UnsafeBuffer(mappedByteBuffer, (int)(recordingId * RECORD_LENGTH), RECORD_LENGTH);
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

        unsafeBuffer.wrap(mappedByteBuffer, offset, RECORD_LENGTH);
        if (unsafeBuffer.getInt(0) == 0)
        {
            return false;
        }
        recordingDescriptorDecoder.wrap(
            unsafeBuffer,
            CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        recordingDescriptorEncoder.wrap(unsafeBuffer, CATALOG_FRAME_LENGTH);
        consumer.accept(recordingDescriptorEncoder, recordingDescriptorDecoder);

        return true;
    }

    private void refreshDescriptor(final RecordingDescriptorEncoder encoder, final RecordingDescriptorDecoder decoder)
    {
        // TODO:
        nextRecordingId = decoder.recordingId() + 1;
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
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
            .startTimestamp(NULL_TIME)
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
