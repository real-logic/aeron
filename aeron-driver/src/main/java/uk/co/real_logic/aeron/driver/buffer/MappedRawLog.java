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
package uk.co.real_logic.aeron.driver.buffer;

import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Encapsulates responsibility for mapping the files into memory used by the log partitions.
 */
class MappedRawLog implements RawLog
{
    private static final int ONE_GIG = 1 << 30;
    private static final int MAX_TREE_DEPTH = 3;
    private static final String LOG_FILE_NAME = "stream.log";

    private final RawLogPartition[] partitions;
    private final EventLogger logger;
    private final File logFile;
    private final MappedByteBuffer[] mappedBuffers;
    private final UnsafeBuffer logMetaDataBuffer;

    MappedRawLog(final File directory, final FileChannel blankTemplate, final int termLength, final EventLogger logger)
    {
        IoUtil.ensureDirectoryExists(directory, "log buffer directory");

        this.logger = logger;
        logFile = new File(directory, LOG_FILE_NAME);
        partitions = new RawLogPartition[PARTITION_COUNT];

        try (final FileChannel logChannel = new RandomAccessFile(logFile, "rw").getChannel())
        {
            final long logLength = computeLogLength(termLength);
            blankTemplate.transferTo(0, logLength, logChannel);

            if (logLength <= Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = logChannel.map(READ_WRITE, 0, logLength);
                mappedBuffers = new MappedByteBuffer[]{ mappedBuffer };
                final int metaDataSectionOffset = termLength * PARTITION_COUNT;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final int metaDataOffset = metaDataSectionOffset + (i * TERM_META_DATA_LENGTH);

                    partitions[i] = new RawLogPartition(
                        new UnsafeBuffer(mappedBuffer, i * termLength, termLength),
                        new UnsafeBuffer(mappedBuffer, metaDataOffset, TERM_META_DATA_LENGTH));
                }

                logMetaDataBuffer = new UnsafeBuffer(mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
            }
            else
            {
                mappedBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];
                final long metaDataSectionOffset = termLength * (long)PARTITION_COUNT;
                final int metaDataSectionLength = (int)(logLength - metaDataSectionOffset);

                final MappedByteBuffer metaDataMappedBuffer = logChannel.map(
                    READ_WRITE, metaDataSectionOffset, metaDataSectionLength);
                mappedBuffers[mappedBuffers.length - 1] = metaDataMappedBuffer;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    mappedBuffers[i] = logChannel.map(READ_WRITE, termLength * (long)i, termLength);

                    partitions[i] = new RawLogPartition(
                        new UnsafeBuffer(mappedBuffers[i]),
                        new UnsafeBuffer(metaDataMappedBuffer, i * TERM_META_DATA_LENGTH, TERM_META_DATA_LENGTH));
                }

                logMetaDataBuffer = new UnsafeBuffer(
                    metaDataMappedBuffer, metaDataSectionLength - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
            }
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    public void close()
    {
        for (final MappedByteBuffer buffer : mappedBuffers)
        {
            IoUtil.unmap(buffer);
        }

        if (logFile.delete())
        {
            final File directory = logFile.getParentFile();
            recursivelyDeleteUpTree(directory, MAX_TREE_DEPTH);
        }
        else
        {
            logger.log(EventCode.ERROR_DELETING_FILE, logFile);
        }
    }

    public Stream<RawLogPartition> stream()
    {
        return Stream.of(partitions);
    }

    public RawLogPartition[] partitions()
    {
        return partitions;
    }

    public UnsafeBuffer logMetaData()
    {
        return logMetaDataBuffer;
    }

    public ByteBuffer[] sliceTerms()
    {
        final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];
        final int termLength = partitions[0].termBuffer().capacity();

        if (termLength < ONE_GIG)
        {
            final MappedByteBuffer buffer = mappedBuffers[0];
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                buffer.limit((termLength * i) + termLength)
                      .position(termLength * i);

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

    public String logFileName()
    {
        return logFile.getAbsolutePath();
    }

    private void recursivelyDeleteUpTree(final File directory, final int remainingTreeDepth)
    {
        if (remainingTreeDepth > 0 && directory.list().length == 0)
        {
            if (!directory.delete())
            {
                logger.log(EventCode.ERROR_DELETING_FILE, directory);
            }
            else
            {
                recursivelyDeleteUpTree(directory.getParentFile(), remainingTreeDepth - 1);
            }
        }
    }
}
