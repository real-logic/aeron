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
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.RecordingWriter.initDescriptor;
import static java.nio.file.StandardOpenOption.*;

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
    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(RECORD_LENGTH, PAGE_SIZE);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final FileChannel catalogFileChannel;

    private long nextRecordingId = 0;

    Catalog(final File archiveDir)
    {
        FileChannel channel = null;
        try
        {
            final File catalogFile = new File(archiveDir, CATALOG_FILE_NAME);
            channel = FileChannel.open(catalogFile.toPath(), CREATE, READ, WRITE);
            final RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();

            while (channel.read(byteBuffer) != -1)
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

                loadIntoCatalog(byteBuffer, unsafeBuffer, decoder);
                byteBuffer.clear();
            }

            recordingDescriptorEncoder.wrap(unsafeBuffer, CATALOG_FRAME_LENGTH);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            catalogFileChannel = channel;
        }
    }

    private void loadIntoCatalog(
        final ByteBuffer dst, final UnsafeBuffer unsafeBuffer, final RecordingDescriptorDecoder decoder)
    {
        if (dst.remaining() == 0)
        {
            return;
        }

        decoder.wrap(
            unsafeBuffer,
            CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        final long recordingId = decoder.recordingId();

        // TODO: verify catalog reflects last position from metadata file, and equally that the files are aligned
        // TODO: with last writes.
        nextRecordingId = Math.max(recordingId + 1, nextRecordingId);
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
            final int written = catalogFileChannel.write(byteBuffer);
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

    public void close()
    {
        CloseHelper.close(catalogFileChannel);
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

    void updateCatalogFromMeta(
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

        catalogFileChannel.write(byteBuffer, recordingId * RECORD_LENGTH);
    }

    long nextRecordingId()
    {
        return nextRecordingId;
    }
}
