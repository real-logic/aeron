/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron;

import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Takes a log file name and maps the file into memory and wraps it with {@link UnsafeBuffer}s as appropriate.
 *
 * @see io.aeron.logbuffer.LogBufferDescriptor
 */
public class LogBuffers implements AutoCloseable
{
    private final int termLength;
    private final FileChannel fileChannel;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final MappedByteBuffer[] mappedByteBuffers;

    public LogBuffers(final String logFileName, final FileChannel.MapMode mapMode)
    {
        try
        {
            fileChannel = FileChannel.open(Paths.get(logFileName), READ, WRITE);

            final long logLength = fileChannel.size();
            final int termLength = computeTermLength(logLength);

            checkTermLength(termLength);
            this.termLength = termLength;

            if (logLength < Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = fileChannel.map(mapMode, 0, logLength);
                mappedByteBuffers = new MappedByteBuffer[]{ mappedBuffer };

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    termBuffers[i] = new UnsafeBuffer(mappedBuffer, i * termLength, termLength);
                }

                logMetaDataBuffer = new UnsafeBuffer(
                    mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
            }
            else
            {
                mappedByteBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    mappedByteBuffers[i] = fileChannel.map(mapMode, termLength * (long)i, termLength);
                    termBuffers[i] = new UnsafeBuffer(mappedByteBuffers[i]);
                }

                final MappedByteBuffer metaDataMappedBuffer = fileChannel.map(
                    mapMode, logLength - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
                mappedByteBuffers[mappedByteBuffers.length - 1] = metaDataMappedBuffer;
                logMetaDataBuffer = new UnsafeBuffer(metaDataMappedBuffer);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        for (final UnsafeBuffer buffer : termBuffers)
        {
            buffer.verifyAlignment();
        }

        logMetaDataBuffer.verifyAlignment();
    }

    public UnsafeBuffer[] termBuffers()
    {
        return termBuffers;
    }

    public UnsafeBuffer metaDataBuffer()
    {
        return logMetaDataBuffer;
    }

    public FileChannel fileChannel()
    {
        return fileChannel;
    }

    public void close()
    {
        CloseHelper.close(fileChannel);

        for (final MappedByteBuffer buffer : mappedByteBuffers)
        {
            IoUtil.unmap(buffer);
        }
    }

    public int termLength()
    {
        return termLength;
    }
}
