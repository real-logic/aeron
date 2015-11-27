/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Takes a log file name and maps the file into memory and wraps it with {@link UnsafeBuffer}s as appropriate.
 *
 * @see uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor
 */
public class LogBuffers implements AutoCloseable
{
    private final int termLength;
    private final FileChannel fileChannel;
    private final UnsafeBuffer[] atomicBuffers = new UnsafeBuffer[(PARTITION_COUNT * 2) + 1];
    private final MappedByteBuffer[] mappedByteBuffers;

    public LogBuffers(final String logFileName)
    {
        try
        {
            fileChannel = FileChannel.open(Paths.get(logFileName), StandardOpenOption.READ, StandardOpenOption.WRITE);

            final long logLength = fileChannel.size();
            final int termLength = computeTermLength(logLength);

            checkTermLength(termLength);
            this.termLength = termLength;

            // if log length exceeds MAX_INT we need multiple mapped buffers, (see FileChannel.map doc).
            if (logLength < Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, 0, logLength);
                mappedByteBuffers = new MappedByteBuffer[]{mappedBuffer};

                final int metaDataSectionOffset = termLength * PARTITION_COUNT;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final int metaDataOffset = metaDataSectionOffset + (i * TERM_META_DATA_LENGTH);

                    atomicBuffers[i] = new UnsafeBuffer(mappedBuffer, i * termLength, termLength);
                    atomicBuffers[i + PARTITION_COUNT] = new UnsafeBuffer(mappedBuffer, metaDataOffset, TERM_META_DATA_LENGTH);
                }

                atomicBuffers[atomicBuffers.length - 1] = new UnsafeBuffer(
                    mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
            }
            else
            {
                mappedByteBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];
                final long metaDataSectionOffset = termLength * (long)PARTITION_COUNT;
                final int metaDataSectionLength = (int)(logLength - metaDataSectionOffset);

                final MappedByteBuffer metaDataMappedBuffer = fileChannel.map(
                    READ_WRITE, metaDataSectionOffset, metaDataSectionLength);
                mappedByteBuffers[mappedByteBuffers.length - 1] = metaDataMappedBuffer;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    mappedByteBuffers[i] = fileChannel.map(READ_WRITE, termLength * (long)i, termLength);

                    atomicBuffers[i] = new UnsafeBuffer(mappedByteBuffers[i]);
                    atomicBuffers[i + PARTITION_COUNT] = new UnsafeBuffer(
                        metaDataMappedBuffer, i * TERM_META_DATA_LENGTH, TERM_META_DATA_LENGTH);
                }

                atomicBuffers[atomicBuffers.length - 1] = new UnsafeBuffer(
                    metaDataMappedBuffer, metaDataSectionLength - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        for (final UnsafeBuffer buffer : atomicBuffers)
        {
            buffer.verifyAlignment();
        }
    }

    public UnsafeBuffer[] atomicBuffers()
    {
        return atomicBuffers;
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
