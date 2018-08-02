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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;

import static io.aeron.samples.FileReceiver.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Sends a large file in chunks to a {@link FileReceiver}.
 * <p>
 * Protocol is to send a {@code file-create} followed by 1 or more {@code file-chunk} messages that are all
 * linked via the correlation id. Messages are encoded in {@link java.nio.ByteOrder#LITTLE_ENDIAN}.
 * <p>
 * The chunk size if best determined by {@link io.aeron.Publication#maxPayloadLength()} minus header for the chunk.
 *
 * <b>file-create</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Version                              |
 *  +---------------------------------------------------------------+
 *  |                      Message Type = 1                         |
 *  +---------------------------------------------------------------+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        File Length                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      File Name Length                         |
 *  +---------------------------------------------------------------+
 *  |                         File Name                            ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 * <b>file-chunk</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Version                              |
 *  +---------------------------------------------------------------+
 *  |                      Message Type = 2                         |
 *  +---------------------------------------------------------------+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Chunk Offset                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Chunk Length                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Chunk Payload                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class FileSender
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 1)
        {
            System.out.println("Filename to be sent must be supplied as a command line argument");
            System.exit(1);
        }

        try (Aeron aeron = Aeron.connect();
            Publication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            while (!publication.isConnected())
            {
                Thread.sleep(1);
            }

            final File file = new File(args[0]);
            final UnsafeBuffer buffer = new UnsafeBuffer(IoUtil.mapExistingFile(file, "sending"));
            final long correlationId = aeron.nextCorrelationId();

            sendFileCreate(publication, correlationId, buffer.capacity(), file.getName());
            streamChunks(publication, correlationId, buffer);
        }
    }

    private static void sendFileCreate(
        final Publication publication, final long correlationId, final int length, final String filename)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        buffer.putInt(VERSION_OFFSET, VERSION, LITTLE_ENDIAN);
        buffer.putInt(TYPE_OFFSET, FILE_CREATE_TYPE, LITTLE_ENDIAN);
        buffer.putLong(CORRELATION_ID_OFFSET, correlationId, LITTLE_ENDIAN);
        buffer.putLong(FILE_LENGTH_OFFSET, length, LITTLE_ENDIAN);

        final int msgLength = FILE_NAME_OFFSET + buffer.putStringUtf8(FILE_NAME_OFFSET, filename);

        long result;
        while ((result = publication.offer(buffer, 0, msgLength)) < 0)
        {
            checkResult(result);
            Thread.yield();
        }
    }

    private static void streamChunks(final Publication publication, final long correlationId, final UnsafeBuffer buffer)
    {
        final BufferClaim bufferClaim = new BufferClaim();
        final int fileLength = buffer.capacity();
        final int maxChunkLength = publication.maxPayloadLength() - CHUNK_PAYLOAD_OFFSET;
        int chunkOffset = 0;

        while (chunkOffset < fileLength)
        {
            final int chunkLength = Math.min(maxChunkLength, fileLength - chunkOffset);
            sendChunk(publication, bufferClaim, correlationId, buffer, chunkOffset, chunkLength);
            chunkOffset += chunkLength;
        }
    }

    private static void sendChunk(
        final Publication publication,
        final BufferClaim bufferClaim,
        final long correlationId,
        final UnsafeBuffer fileBuffer,
        final int chunkOffset,
        final int chunkLength)
    {
        long result;
        while ((result = publication.tryClaim(CHUNK_PAYLOAD_OFFSET + chunkLength, bufferClaim)) < 0)
        {
            checkResult(result);
            Thread.yield();
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        buffer.putInt(offset + VERSION_OFFSET, VERSION, LITTLE_ENDIAN);
        buffer.putInt(offset + TYPE_OFFSET, FILE_CHUNK_TYPE, LITTLE_ENDIAN);
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId, LITTLE_ENDIAN);
        buffer.putLong(offset + CHUNK_OFFSET_OFFSET, chunkOffset, LITTLE_ENDIAN);
        buffer.putLong(offset + CHUNK_LENGTH_OFFSET, chunkLength, LITTLE_ENDIAN);
        buffer.putBytes(offset + CHUNK_PAYLOAD_OFFSET, fileBuffer, chunkOffset, chunkLength);

        bufferClaim.commit();
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED)
        {
            throw new IllegalStateException("Connection has been closed");
        }

        if (result == Publication.NOT_CONNECTED)
        {
            throw new IllegalStateException("Connection is no longer available");
        }

        if (result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Publication failed due to max position being reached");
        }
    }
}
