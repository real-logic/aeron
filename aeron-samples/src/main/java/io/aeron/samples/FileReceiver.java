/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Receives files in chunks and saves them in a directory provided as the first command line option or the
 * temporary directory if no command line arguments are provided.
 * <p>
 * Protocol is to receive a {@code file-create} followed by 1 or more {@code file-chunk} messages that are all
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
 * </pre> * @see FileSender
 */
public class FileReceiver
{
    public static final int VERSION = 0;
    public static final int FILE_CREATE_TYPE = 1;
    public static final int FILE_CHUNK_TYPE = 2;

    public static final int VERSION_OFFSET = 0;
    public static final int TYPE_OFFSET = VERSION_OFFSET + SIZE_OF_INT;
    public static final int CORRELATION_ID_OFFSET = TYPE_OFFSET + SIZE_OF_INT;
    public static final int FILE_LENGTH_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;

    public static final int FILE_NAME_OFFSET = FILE_LENGTH_OFFSET + SIZE_OF_LONG;

    public static final int CHUNK_OFFSET_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    public static final int CHUNK_LENGTH_OFFSET = CHUNK_OFFSET_OFFSET + SIZE_OF_LONG;
    public static final int CHUNK_PAYLOAD_OFFSET = CHUNK_LENGTH_OFFSET + SIZE_OF_LONG;

    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_LIMIT = 10;

    private final File storageDir;
    private final Subscription subscription;
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
    private final Long2ObjectHashMap<UnsafeBuffer> fileSessionByIdMap = new Long2ObjectHashMap<>();

    public FileReceiver(final File storageDir, final Subscription subscription)
    {
        this.storageDir = storageDir;
        this.subscription = subscription;
    }

    public static void main(final String[] args)
    {
        final File storageDir;
        if (args.length > 1)
        {
            storageDir = new File(args[0]);
            if (!storageDir.isDirectory())
            {
                System.out.println(args[0] + " is not a directory");
                System.exit(1);
            }
        }
        else
        {
            storageDir = new File(IoUtil.tmpDirName());
        }

        System.out.println("Files stored to " + storageDir.getAbsolutePath());

        final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (MediaDriver ignore = MediaDriver.launch();
            Aeron aeron = Aeron.connect();
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            System.out.println("Receiving from " + CHANNEL + " on stream Id " + STREAM_ID);
            final FileReceiver fileReceiver = new FileReceiver(storageDir, subscription);

            while (running.get())
            {
                idleStrategy.idle(fileReceiver.doWork());
            }
        }
    }

    @SuppressWarnings("unused")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int version = buffer.getInt(offset + VERSION_OFFSET, LITTLE_ENDIAN);
        if (VERSION != version)
        {
            throw new IllegalArgumentException("unsupported version " + version + " expected " + VERSION);
        }

        final int messageType = buffer.getInt(offset + TYPE_OFFSET, LITTLE_ENDIAN);
        switch (messageType)
        {
            case FILE_CREATE_TYPE:
                createFile(
                    buffer.getLong(offset + CORRELATION_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(offset + FILE_LENGTH_OFFSET, LITTLE_ENDIAN),
                    buffer.getStringUtf8(offset + FILE_NAME_OFFSET, LITTLE_ENDIAN));
                break;

            case FILE_CHUNK_TYPE:
                fileChunk(
                    buffer.getLong(offset + CORRELATION_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(offset + CHUNK_OFFSET_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(offset + CHUNK_LENGTH_OFFSET, LITTLE_ENDIAN),
                    buffer,
                    offset);
                break;

            default:
                throw new IllegalArgumentException("unknown message type: " + messageType);
        }
    }

    private void createFile(final long correlationId, final long length, final String filename)
    {
        if (fileSessionByIdMap.containsKey(correlationId))
        {
            throw new IllegalStateException("correlationId is in use: " + correlationId);
        }

        final File file = new File(storageDir, filename);
        if (file.exists() && !file.delete())
        {
            throw new IllegalStateException("failed to delete existing file: " + file);
        }

        if (length == 0)
        {
            try
            {
                if (!file.createNewFile())
                {
                    throw new IllegalStateException("failed to create " + filename);
                }
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else
        {
            fileSessionByIdMap.put(correlationId, new UnsafeBuffer(IoUtil.mapNewFile(file, length, false)));
        }
    }

    private void fileChunk(
        final long correlationId,
        final long chunkOffset,
        final long chunkLength,
        final DirectBuffer buffer,
        final int offset)
    {
        final UnsafeBuffer fileBuffer = fileSessionByIdMap.get(correlationId);
        buffer.getBytes(offset + CHUNK_PAYLOAD_OFFSET, fileBuffer, (int)chunkOffset, (int)chunkLength);

        if ((chunkOffset + chunkLength) >= fileBuffer.capacity())
        {
            fileSessionByIdMap.remove(correlationId);
            IoUtil.unmap(fileBuffer.byteBuffer());
        }
    }

    private int doWork()
    {
        return subscription.poll(assembler, FRAGMENT_LIMIT);
    }
}
