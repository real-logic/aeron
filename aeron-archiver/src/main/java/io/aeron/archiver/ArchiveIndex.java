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

import io.aeron.archiver.messages.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveStreamWriter.initDescriptor;
import static java.nio.file.StandardOpenOption.*;

/**
 * Index file for the archiving service provides a persisted log of archives, past and present, and used to lookup
 * details. The index format is simple, allocating a fixed 4KB record for each archive descriptor. This allows offset
 * based look up of a descriptor in the file.
 */
class ArchiveIndex implements AutoCloseable
{
    // TODO: Make DSYNC optional via configuration.

    public static final String INDEX_FILE_NAME = "archive.idx";
    static final int INDEX_RECORD_SIZE = 4096;
    static final int INDEX_FRAME_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    private static final int PAGE_SIZE = 4096;

    private final ArchiveDescriptorEncoder archiveDescriptorEncoder = new ArchiveDescriptorEncoder();

    private final ByteBuffer byteBuffer;
    private final UnsafeBuffer unsafeBuffer;
    private final FileChannel archiveIndexFileChannel;
    private int streamInstanceIdSeq = 0;

    ArchiveIndex(final File archiveFolder)
    {
        byteBuffer = BufferUtil.allocateDirectAligned(INDEX_RECORD_SIZE, PAGE_SIZE);
        unsafeBuffer = new UnsafeBuffer(byteBuffer);

        FileChannel channel = null;
        try
        {
            final File indexFile = new File(archiveFolder, INDEX_FILE_NAME);
            channel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE, DSYNC);
            final ArchiveDescriptorDecoder decoder = new ArchiveDescriptorDecoder();

            while (channel.read(byteBuffer) != -1)
            {
                byteBuffer.flip();
                if (byteBuffer.remaining() == 0)
                {
                    break;
                }

                if (byteBuffer.remaining() != INDEX_RECORD_SIZE)
                {
                    throw new IllegalStateException();
                }

                loadIntoIndex(byteBuffer, unsafeBuffer, decoder);
                byteBuffer.clear();
            }

            archiveDescriptorEncoder.wrap(unsafeBuffer, INDEX_FRAME_LENGTH);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            archiveIndexFileChannel = channel;
        }
    }

    private int loadIntoIndex(
        final ByteBuffer dst, final UnsafeBuffer unsafeBuffer, final ArchiveDescriptorDecoder decoder)
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
            ArchiveDescriptorDecoder.BLOCK_LENGTH,
            ArchiveDescriptorDecoder.SCHEMA_VERSION);

        final int streamInstanceId = decoder.streamInstanceId();
        final int sessionId = decoder.sessionId();
        final int streamId = decoder.streamId();
        final String source = decoder.source();
        final String channel = decoder.channel();

        streamInstanceIdSeq = Math.max(streamInstanceId + 1, streamInstanceIdSeq);

        return length + INDEX_FRAME_LENGTH;
    }

    int addNewStreamInstance(
        final StreamKey newStreamKey,
        final int termBufferLength,
        final int imageInitialTermId)
    {
        final int newStreamInstanceId = streamInstanceIdSeq;

        archiveDescriptorEncoder.limit(INDEX_FRAME_LENGTH + ArchiveDescriptorEncoder.BLOCK_LENGTH);
        initDescriptor(
            archiveDescriptorEncoder,
            newStreamInstanceId,
            termBufferLength,
            imageInitialTermId,
            newStreamKey);

        final int encodedLength = archiveDescriptorEncoder.encodedLength();
        unsafeBuffer.putInt(0, encodedLength);

        try
        {
            byteBuffer.clear();
            final int written = archiveIndexFileChannel.write(byteBuffer);
            if (written != INDEX_RECORD_SIZE)
            {
                throw new IllegalStateException();
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        streamInstanceIdSeq++;

        return newStreamInstanceId;
    }

    public void close()
    {
        CloseHelper.close(archiveIndexFileChannel);
    }

    boolean readArchiveDescriptor(final int streamInstanceId, final ByteBuffer buffer)
        throws IOException
    {
        if (buffer.remaining() != INDEX_RECORD_SIZE)
        {
            throw new IllegalArgumentException("buffer must have exactly INDEX_RECORD_SIZE remaining to read into");
        }

        final int read = archiveIndexFileChannel.read(buffer, streamInstanceId * INDEX_RECORD_SIZE);
        if (read == 0 || read == -1)
        {
            return false;
        }

        if (read != INDEX_RECORD_SIZE)
        {
            throw new IllegalStateException("Wrong read size:" + read);
        }

        return true;
    }

    void updateIndexFromMeta(final int streamInstanceId, final ByteBuffer metaDataBuffer) throws IOException
    {
        archiveIndexFileChannel.write(metaDataBuffer, streamInstanceId * INDEX_RECORD_SIZE);
    }

    int maxStreamInstanceId()
    {
        return streamInstanceIdSeq;
    }
}
