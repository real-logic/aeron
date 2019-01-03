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
package io.aeron.driver.buffer;

import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;

/**
 * Encapsulates responsibility for mapping the files into memory used by the log partitions.
 */
class MappedRawLog implements RawLog
{
    private static final int ONE_GIG = 1 << 30;
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE);
    private static final EnumSet<StandardOpenOption> SPARSE_FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE, SPARSE);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private final int termLength;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final ErrorHandler errorHandler;
    private File logFile;
    private MappedByteBuffer[] mappedBuffers;

    MappedRawLog(
        final File location,
        final boolean useSparseFiles,
        final int termLength,
        final int filePageSize,
        final ErrorHandler errorHandler)
    {
        this.termLength = termLength;
        this.errorHandler = errorHandler;
        this.logFile = location;

        final EnumSet<StandardOpenOption> options = useSparseFiles ? SPARSE_FILE_OPTIONS : FILE_OPTIONS;

        try (FileChannel logChannel = FileChannel.open(logFile.toPath(), options, NO_ATTRIBUTES))
        {
            final long logLength = computeLogLength(termLength, filePageSize);

            if (logLength <= Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = logChannel.map(READ_WRITE, 0, logLength);
                mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                if (!useSparseFiles)
                {
                    allocatePages(mappedBuffer, (int)logLength, filePageSize);
                }

                mappedBuffers = new MappedByteBuffer[]{ mappedBuffer };

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    termBuffers[i] = new UnsafeBuffer(mappedBuffer, i * termLength, termLength);
                }

                logMetaDataBuffer = new UnsafeBuffer(
                    mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
            }
            else
            {
                mappedBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final MappedByteBuffer buffer = logChannel.map(READ_WRITE, termLength * (long)i, termLength);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    mappedBuffers[i] = buffer;
                    if (!useSparseFiles)
                    {
                        allocatePages(buffer, termLength, filePageSize);
                    }

                    termBuffers[i] = new UnsafeBuffer(buffer, 0, termLength);
                }

                final int metaDataMappingLength = align(LOG_META_DATA_LENGTH, filePageSize);
                final long metaDataSectionOffset = termLength * (long)PARTITION_COUNT;

                final MappedByteBuffer metaDataMappedBuffer = logChannel.map(
                    READ_WRITE, metaDataSectionOffset, metaDataMappingLength);
                metaDataMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                mappedBuffers[LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;
                logMetaDataBuffer = new UnsafeBuffer(
                    metaDataMappedBuffer,
                    metaDataMappingLength - LOG_META_DATA_LENGTH,
                    LOG_META_DATA_LENGTH);
            }
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    public int termLength()
    {
        return termLength;
    }

    public boolean isInactive()
    {
        return null == mappedBuffers;
    }

    public boolean free()
    {
        if (null != mappedBuffers)
        {
            for (final MappedByteBuffer buffer : mappedBuffers)
            {
                IoUtil.unmap(buffer);
            }

            mappedBuffers = null;
        }

        if (null != logFile)
        {
            if (!logFile.delete())
            {
                return false;
            }

            logFile = null;
        }

        return true;
    }

    public void close()
    {
        if (!free())
        {
            errorHandler.onError(new AeronException("unable to delete " + logFile));
        }
    }

    public UnsafeBuffer[] termBuffers()
    {
        return termBuffers;
    }

    public UnsafeBuffer metaData()
    {
        return logMetaDataBuffer;
    }

    public ByteBuffer[] sliceTerms()
    {
        final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];

        if (termLength < ONE_GIG)
        {
            final MappedByteBuffer buffer = mappedBuffers[0];
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                buffer.limit((termLength * i) + termLength).position(termLength * i);
                terms[i] = buffer.slice();
            }
        }
        else
        {
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                terms[i] = mappedBuffers[i].duplicate();
            }
        }

        return terms;
    }

    public String fileName()
    {
        return logFile.getAbsolutePath();
    }

    private static void allocatePages(final MappedByteBuffer buffer, final int length, final int pageSize)
    {
        for (int i = 0; i < length; i += pageSize)
        {
            buffer.put(i, (byte)0);
        }
    }
}
