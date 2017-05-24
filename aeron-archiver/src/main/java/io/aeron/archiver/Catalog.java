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

import io.aeron.archiver.codecs.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.Recorder.initDescriptor;
import static java.nio.file.StandardOpenOption.*;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 4KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file.
 */
class Catalog implements AutoCloseable
{
    static final String CATALOG_FILE_NAME = "archive.cat";
    static final int RECORD_LENGTH = 4096;
    static final int CATALOG_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    static final int NULL_RECORD_ID = -1;

    private static final int PAGE_SIZE = 4096;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();

    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer unsafeBuffer;
    private final FileChannel catalogFileChannel;
    private long nextRecordingId = 0;

    Catalog(final File archiveDir)
    {
        byteBuffer = BufferUtil.allocateDirectAligned(RECORD_LENGTH, PAGE_SIZE);
        unsafeBuffer = new UnsafeBuffer(byteBuffer);

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

    private int loadIntoCatalog(
        final ByteBuffer dst,
        final UnsafeBuffer unsafeBuffer,
        final RecordingDescriptorDecoder decoder)
    {
        if (dst.remaining() == 0)
        {
            return 0;
        }

        // Note: prep for some lookup method construction
        final int length = unsafeBuffer.getInt(0);

        decoder.wrap(
            unsafeBuffer,
            CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        final long recordingId = decoder.recordingId();

        nextRecordingId = Math.max(recordingId + 1, nextRecordingId);

        return length + CATALOG_FRAME_LENGTH;
    }

    long addNewRecording(
        final String source,
        final int sessionId,
        final String channel,
        final int streamId,
        final int termBufferLength,
        final int mtuLength,
        final int imageInitialTermId,
        final long joiningPosition,
        final RecordingSession session,
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
            joiningPosition,
            source,
            sessionId,
            channel,
            streamId);

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
        recordingSessionByIdMap.put(newRecordingId, session);
        return newRecordingId;
    }

    public void close()
    {
        CloseHelper.close(catalogFileChannel);

        if (!recordingSessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected empty recordingSessionByIdMap");
        }
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

    void updateCatalogFromMeta(final long recordingId, final ByteBuffer metaDataBuffer) throws IOException
    {
        catalogFileChannel.write(metaDataBuffer, recordingId * RECORD_LENGTH);
    }

    long nextRecordingId()
    {
        return nextRecordingId;
    }

    RecordingSession getRecordingSession(final long recordingId)
    {
        return recordingSessionByIdMap.get(recordingId);
    }

    void removeRecordingSession(final long recordingId)
    {
        recordingSessionByIdMap.remove(recordingId);
    }
}
