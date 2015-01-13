/*
 * Copyright 2014 Real Logic Ltd.
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
import uk.co.real_logic.aeron.common.command.BuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
    private static final int MAX_TREE_DEPTH = 3;
    private static final String LOG_FILE_NAME = "stream.log";

    private final MappedRawLogPartition[] partitions;
    private final EventLogger logger;
    private final File logFile;
    private final MappedByteBuffer mappedLogMetaDataBuffer;
    private final UnsafeBuffer logMetaDataBuffer;

    MappedRawLog(
        final File directory, final FileChannel blankTemplate, final int termBufferLength, final EventLogger logger)
    {
        this.logger = logger;

        IoUtil.ensureDirectoryExists(directory, "log buffer directory");
        logFile = new File(directory, LOG_FILE_NAME);

        try
        {
            final long metaDataSectionOffset = termBufferLength * PARTITION_COUNT;
            final long logLength = metaDataSectionOffset + (TERM_META_DATA_LENGTH * 3) + LOG_META_DATA_LENGTH;

            final FileChannel logChannel = new RandomAccessFile(logFile, "rw").getChannel();
            blankTemplate.transferTo(0, logLength, logChannel);

            partitions = new MappedRawLogPartition[PARTITION_COUNT];
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                final long termBufferOffset = i * termBufferLength;
                final MappedByteBuffer mappedTermBuffer = logChannel.map(READ_WRITE, termBufferOffset, termBufferLength);

                final long metaDataOffset = metaDataSectionOffset + (i * TERM_META_DATA_LENGTH);
                final MappedByteBuffer mappedMetaDataBuffer = logChannel.map(READ_WRITE, metaDataOffset, TERM_META_DATA_LENGTH);

                partitions[i] = new MappedRawLogPartition(mappedTermBuffer, mappedMetaDataBuffer);
            }

            mappedLogMetaDataBuffer = logChannel.map(READ_WRITE, logLength - LOG_META_DATA_LENGTH, LOG_META_DATA_LENGTH);
            logMetaDataBuffer = new UnsafeBuffer(mappedLogMetaDataBuffer);

            logChannel.close();
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    public void close()
    {
        stream().forEach(MappedRawLogPartition::close);
        IoUtil.unmap(mappedLogMetaDataBuffer);

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

    public Stream<MappedRawLogPartition> stream()
    {
        return Stream.of(partitions);
    }

    public MappedRawLogPartition[] partitions()
    {
        return partitions;
    }

    public UnsafeBuffer logMetaData()
    {
        return logMetaDataBuffer;
    }

    public void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        final String absoluteFilePath = logFile.getAbsolutePath();
        final int termLength = partitions[0].termBuffer().capacity();
        final long metaDataSectionOffset = termLength * PARTITION_COUNT;

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            buffersReadyFlyweight.bufferOffset(i, i * (long)termLength);
            buffersReadyFlyweight.bufferLength(i, termLength);
            buffersReadyFlyweight.bufferLocation(i, absoluteFilePath);
        }

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            final int index = i + PARTITION_COUNT;
            buffersReadyFlyweight.bufferOffset(index, metaDataSectionOffset + (i * TERM_META_DATA_LENGTH));
            buffersReadyFlyweight.bufferLength(index, termLength);
            buffersReadyFlyweight.bufferLocation(index, absoluteFilePath);
        }

        final long logLength = computeLogLength(termLength);
        final int index = PARTITION_COUNT * 2;
        buffersReadyFlyweight.bufferOffset(index, logLength - LOG_META_DATA_LENGTH);
        buffersReadyFlyweight.bufferLength(index, LOG_META_DATA_LENGTH);
        buffersReadyFlyweight.bufferLocation(index, absoluteFilePath);
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
