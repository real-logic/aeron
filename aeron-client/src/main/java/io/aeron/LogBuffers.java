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
package io.aeron;

import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.ManagedResource;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Takes a log file name and maps the file into memory and wraps it with {@link UnsafeBuffer}s as appropriate.
 *
 * @see io.aeron.logbuffer.LogBufferDescriptor
 */
public class LogBuffers implements AutoCloseable, ManagedResource
{
    private final int termLength;
    private int refCount;
    private long timeOfLastStateChangeNs;
    private final FileChannel fileChannel;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final MappedByteBuffer[] mappedByteBuffers;

    public LogBuffers(final String logFileName)
    {
        try
        {
            fileChannel = FileChannel.open(Paths.get(logFileName), READ, WRITE);

            final long logLength = fileChannel.size();

            if (logLength < Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, 0, logLength);
                mappedByteBuffers = new MappedByteBuffer[]{ mappedBuffer };

                logMetaDataBuffer = new UnsafeBuffer(
                    mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);

                final int termLength = LogBufferDescriptor.termLength(logMetaDataBuffer);
                final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);

                checkTermLength(termLength);
                checkPageSize(pageSize);
                this.termLength = termLength;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final int offset = i * termLength;
                    mappedBuffer.limit(offset + termLength).position(offset);

                    termBuffers[i] = new UnsafeBuffer(mappedBuffer.slice());
                }
            }
            else
            {
                mappedByteBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                final int assumedTermLength = TERM_MAX_LENGTH;

                final long metaDataSectionOffset = assumedTermLength * (long)PARTITION_COUNT;
                final long metaDataMappingLength = logLength - metaDataSectionOffset;

                final MappedByteBuffer metaDataMappedBuffer = fileChannel.map(
                    READ_WRITE, metaDataSectionOffset, metaDataMappingLength);

                mappedByteBuffers[mappedByteBuffers.length - 1] = metaDataMappedBuffer;

                logMetaDataBuffer =
                    new UnsafeBuffer(
                        metaDataMappedBuffer,
                        (int)metaDataMappingLength - LOG_META_DATA_LENGTH,
                        LOG_META_DATA_LENGTH);

                final int metaDataTermLength = LogBufferDescriptor.termLength(logMetaDataBuffer);
                final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);

                checkPageSize(pageSize);
                if (metaDataTermLength != assumedTermLength)
                {
                    throw new IllegalStateException(
                        "Assumed term length " + assumedTermLength +
                        " does not match metadata: termLength=" + metaDataTermLength);
                }

                this.termLength = assumedTermLength;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    mappedByteBuffers[i] =
                        fileChannel.map(READ_WRITE, assumedTermLength * (long)i, assumedTermLength);
                    termBuffers[i] = new UnsafeBuffer(mappedByteBuffers[i], 0, assumedTermLength);
                }
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
        catch (final IllegalStateException ex)
        {
            close();
            throw ex;
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
        for (final MappedByteBuffer buffer : mappedByteBuffers)
        {
            IoUtil.unmap(buffer);
        }

        CloseHelper.close(fileChannel);
    }

    public int termLength()
    {
        return termLength;
    }

    public int incRef()
    {
        return ++refCount;
    }

    public int decRef()
    {
        return --refCount;
    }

    public void timeOfLastStateChange(final long timeNs)
    {
        timeOfLastStateChangeNs = timeNs;
    }

    public long timeOfLastStateChange()
    {
        return timeOfLastStateChangeNs;
    }

    public void delete()
    {
        close();
    }
}
