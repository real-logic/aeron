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

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Receives files in chunks and saves them in the temporary directory.
 *
 * @see FileSender
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
    public static final int FILE_NAME_OFFSET = FILE_LENGTH_OFFSET + SIZE_OF_INT;

    public static final int CHUNK_OFFSET_OFFSET = FILE_LENGTH_OFFSET + SIZE_OF_LONG;
    public static final int CHUNK_LENGTH_OFFSET = CHUNK_OFFSET_OFFSET + SIZE_OF_LONG;
    public static final int CHUNK_PAYLOAD_OFFSET = CHUNK_LENGTH_OFFSET + SIZE_OF_LONG;

    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_LIMIT = 10;

    private final File parentDirectory = new File(IoUtil.tmpDirName());
    private final Subscription subscription;
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
    private final Long2ObjectHashMap<MappedByteBuffer> fileSessionByIdMap = new Long2ObjectHashMap<>();

    public FileReceiver(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    public static void main(final String[] args)
    {
        System.out.println("Receiving from " + CHANNEL + " on stream Id " + STREAM_ID);

        final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (MediaDriver ignore = MediaDriver.launch();
            Aeron aeron = Aeron.connect();
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final FileReceiver fileReceiver = new FileReceiver(subscription);

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
            throw new IllegalArgumentException("Unsupported version " + version + " expected " + VERSION);
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
                    buffer.getLong(offset + FILE_LENGTH_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(offset + CHUNK_OFFSET_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(offset + CHUNK_LENGTH_OFFSET, LITTLE_ENDIAN),
                    buffer,
                    offset);
                break;

            default:
                throw new IllegalArgumentException("Unknown message type: " + messageType);
        }
    }

    private void fileChunk(
        final long correlationId,
        final long fileLength,
        final long chunkOffset,
        final long chunkLength,
        final DirectBuffer buffer,
        final int offset)
    {
        final MappedByteBuffer byteBuffer = fileSessionByIdMap.get(correlationId);
        buffer.getBytes(
            offset + CHUNK_PAYLOAD_OFFSET,
            byteBuffer,
            (int)chunkOffset,
            (int)chunkLength);

        if ((chunkOffset + chunkLength) >= fileLength)
        {
            fileSessionByIdMap.remove(correlationId);
            IoUtil.unmap(byteBuffer);
        }
    }

    private void createFile(final long correlationId, final long length, final String filename)
    {
        if (fileSessionByIdMap.containsKey(correlationId))
        {
            throw new IllegalStateException("correlationId is in use: " + correlationId);
        }

        final File file = new File(parentDirectory, filename);
        if (file.exists() && !file.delete())
        {
            throw new IllegalStateException("Failed to delete existing file: " + file);
        }

        try (FileChannel channel = FileChannel.open(file.toPath(), CREATE_NEW, WRITE))
        {
            final MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, length);
            fileSessionByIdMap.put(correlationId, byteBuffer);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private int doWork()
    {
        return subscription.poll(assembler, FRAGMENT_LIMIT);
    }
}
