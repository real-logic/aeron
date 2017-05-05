/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import io.aeron.archiver.codecs.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ImageRecorder.initDescriptor;
import static java.nio.file.StandardOpenOption.*;

/**
 * Index file for the archive keeps an archive of recorded images, past and present, and used to lookup
 * details. The index format is simple, allocating a fixed 4KB record for each archive descriptor. This allows offset
 * based look up of a descriptor in the file.
 */
class ArchiveIndex implements AutoCloseable
{
    public static final String INDEX_FILE_NAME = "archive.idx";
    static final int INDEX_RECORD_LENGTH = 4096;
    static final int INDEX_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    private static final int PAGE_SIZE = 4096;
    static final int NULL_STREAM_INDEX = -1;

    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final Int2ObjectHashMap<RecordingSession> recordSessionByIdMap = new Int2ObjectHashMap<>();

    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer unsafeBuffer;
    private final FileChannel indexFileChannel;
    private int recordingIdSeq = 0;

    ArchiveIndex(final File archiveFolder)
    {
        byteBuffer = BufferUtil.allocateDirectAligned(INDEX_RECORD_LENGTH, PAGE_SIZE);
        unsafeBuffer = new UnsafeBuffer(byteBuffer);

        FileChannel channel = null;
        try
        {
            final File indexFile = new File(archiveFolder, INDEX_FILE_NAME);
            channel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE);
            final RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();

            while (channel.read(byteBuffer) != -1)
            {
                byteBuffer.flip();
                if (byteBuffer.remaining() == 0)
                {
                    break;
                }

                if (byteBuffer.remaining() != INDEX_RECORD_LENGTH)
                {
                    throw new IllegalStateException();
                }

                loadIntoIndex(byteBuffer, unsafeBuffer, decoder);
                byteBuffer.clear();
            }

            recordingDescriptorEncoder.wrap(unsafeBuffer, INDEX_FRAME_LENGTH);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            indexFileChannel = channel;
        }
    }

    // TODO: prep for some lookup method construction
    private int loadIntoIndex(
        final ByteBuffer dst, final UnsafeBuffer unsafeBuffer, final RecordingDescriptorDecoder decoder)
    {
        if (dst.remaining() == 0)
        {
            return 0;
        }

        // frame
        final int length = unsafeBuffer.getInt(0);

        decoder.wrap(
            unsafeBuffer,
            INDEX_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        final int recordingId = decoder.recordingId();
//        final int sessionId = decoder.sessionId();
//        final int streamId = decoder.streamId();
//        final String source = decoder.source();
//        final String channel = decoder.channel();

        recordingIdSeq = Math.max(recordingId + 1, recordingIdSeq);

        return length + INDEX_FRAME_LENGTH;
    }

    int addNewRecording(
        final String source,
        final int sessionId,
        final String channel,
        final int streamId,
        final int termBufferLength,
        final int imageInitialTermId,
        final RecordingSession session,
        final int archiveFileSize)
    {
        final int newRecordingId = recordingIdSeq;

        recordingDescriptorEncoder.limit(INDEX_FRAME_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH);
        initDescriptor(
            recordingDescriptorEncoder,
            newRecordingId,
            termBufferLength,
            archiveFileSize,
            imageInitialTermId,
            source,
            sessionId,
            channel,
            streamId);

        final int encodedLength = recordingDescriptorEncoder.encodedLength();
        unsafeBuffer.putInt(0, encodedLength);

        try
        {
            byteBuffer.clear();
            final int written = indexFileChannel.write(byteBuffer);
            if (written != INDEX_RECORD_LENGTH)
            {
                throw new IllegalStateException();
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        recordingIdSeq++;
        recordSessionByIdMap.put(newRecordingId, session);
        return newRecordingId;
    }

    public void close()
    {
        CloseHelper.close(indexFileChannel);

        if (!recordSessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected empty recordSessionByIdMap");
        }
    }

    boolean readDescriptor(final int recordingId, final ByteBuffer buffer)
        throws IOException
    {
        if (buffer.remaining() != INDEX_RECORD_LENGTH)
        {
            throw new IllegalArgumentException("buffer must have exactly INDEX_RECORD_LENGTH remaining to read into");
        }

        final int read = indexFileChannel.read(buffer, recordingId * INDEX_RECORD_LENGTH);
        if (read == 0 || read == -1)
        {
            return false;
        }

        if (read != INDEX_RECORD_LENGTH)
        {
            throw new IllegalStateException("Wrong read size:" + read);
        }

        return true;
    }

    void updateIndexFromMeta(final int recordingId, final ByteBuffer metaDataBuffer) throws IOException
    {
        indexFileChannel.write(metaDataBuffer, recordingId * INDEX_RECORD_LENGTH);
    }

    int maxRecordingId()
    {
        return recordingIdSeq;
    }

    RecordingSession getRecordingSession(final int recordingId)
    {
        return recordSessionByIdMap.get(recordingId);
    }

    void removeRecordingSession(final int recordingId)
    {
        recordSessionByIdMap.remove(recordingId);
    }
}
