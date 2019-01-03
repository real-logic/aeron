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
package io.aeron;

import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.ManagedResource;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Takes a log file name and maps the file into memory and wraps it with {@link UnsafeBuffer}s as appropriate.
 *
 * @see io.aeron.logbuffer.LogBufferDescriptor
 */
public class LogBuffers implements AutoCloseable, ManagedResource
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE, SPARSE);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private long timeOfLastStateChangeNs;
    private int refCount;
    private final int termLength;
    private final FileChannel fileChannel;
    private final ByteBuffer[] termBuffers = new ByteBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final MappedByteBuffer[] mappedByteBuffers;

    /**
     * Construct the log buffers for a given log file.
     *
     * @param logFileName to be mapped.
     */
    public LogBuffers(final String logFileName)
    {
        try
        {
            fileChannel = FileChannel.open(Paths.get(logFileName), FILE_OPTIONS, NO_ATTRIBUTES);
            final long logLength = fileChannel.size();

            if (logLength < Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, 0, logLength);
                mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
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

                    termBuffers[i] = mappedBuffer.slice();
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
                metaDataMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                mappedByteBuffers[LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;

                logMetaDataBuffer = new UnsafeBuffer(
                    metaDataMappedBuffer,
                    (int)metaDataMappingLength - LOG_META_DATA_LENGTH,
                    LOG_META_DATA_LENGTH);

                final int metaDataTermLength = LogBufferDescriptor.termLength(logMetaDataBuffer);
                final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);

                checkPageSize(pageSize);
                if (metaDataTermLength != assumedTermLength)
                {
                    throw new IllegalStateException(
                        "assumed term length " + assumedTermLength +
                        " does not match metadata: termLength=" + metaDataTermLength);
                }

                this.termLength = assumedTermLength;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final long position = assumedTermLength * (long)i;
                    final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, position, assumedTermLength);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    mappedByteBuffers[i] = mappedBuffer;
                    termBuffers[i] = mappedBuffer;
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
    }

    /**
     * Duplicate the underlying {@link ByteBuffer}s and wrap them for thread local access.
     *
     * @return duplicates of the wrapped underlying {@link ByteBuffer}s.
     */
    public UnsafeBuffer[] duplicateTermBuffers()
    {
        final UnsafeBuffer[] buffers = new UnsafeBuffer[PARTITION_COUNT];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            buffers[i] = new UnsafeBuffer(termBuffers[i].duplicate().order(ByteOrder.LITTLE_ENDIAN));
        }

        return buffers;
    }

    /**
     * Get the buffer which holds the log metadata.
     *
     * @return the buffer which holds the log metadata.
     */
    public UnsafeBuffer metaDataBuffer()
    {
        return logMetaDataBuffer;
    }

    /**
     * The {@link FileChannel} for the mapped log.
     *
     * @return the {@link FileChannel} for the mapped log.
     */
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

    /**
     * The length of the term buffer in each log partition.
     *
     * @return length of the term buffer in each log partition.
     */
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
