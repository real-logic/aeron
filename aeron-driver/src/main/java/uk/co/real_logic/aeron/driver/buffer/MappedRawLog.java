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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_META_DATA_LENGTH;

/**
 * Encapsulates responsibility for mapping the files into memory used by the log buffers.
 */
class MappedRawLog implements RawLog
{
    private static final int MAX_TREE_DEPTH = 3;
    private static final String LOG_NAME = "stream.log";

    private final int termBufferLength;
    private final EventLogger logger;
    private final File logFile;
    private final MappedRawLogFragment[] buffers;

    MappedRawLog(
        final File directory, final FileChannel blankTemplate, final int termBufferLength, final EventLogger logger)
    {
        this.termBufferLength = termBufferLength;
        this.logger = logger;

        IoUtil.ensureDirectoryExists(directory, "log buffer directory");
        logFile = new File(directory, LOG_NAME);

        try
        {
            final long metaDataStartingOffset = termBufferLength * BUFFER_COUNT;
            final long totalLogLength = metaDataStartingOffset + (TERM_META_DATA_LENGTH * 3);

            final FileChannel logChannel = new RandomAccessFile(logFile, "rw").getChannel();
            blankTemplate.transferTo(0, totalLogLength, logChannel);

            buffers = new MappedRawLogFragment[BUFFER_COUNT];
            for (int i = 0; i < BUFFER_COUNT; i++)
            {
                final long termBufferOffset = i * termBufferLength;
                final MappedByteBuffer mappedTermBuffer = logChannel.map(READ_WRITE, termBufferOffset, termBufferLength);

                final long metaDataOffset = metaDataStartingOffset + (i * TERM_META_DATA_LENGTH);
                final MappedByteBuffer mappedMetaDataBuffer = logChannel.map(READ_WRITE, metaDataOffset, TERM_META_DATA_LENGTH);

                buffers[i] = new MappedRawLogFragment(mappedTermBuffer, mappedMetaDataBuffer);
            }

            logChannel.close();
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    public void close()
    {
        stream().forEach(MappedRawLogFragment::close);

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

    public Stream<MappedRawLogFragment> stream()
    {
        return Stream.of(buffers);
    }

    public MappedRawLogFragment[] fragments()
    {
        return buffers;
    }

    public void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        final String absoluteFilePath = logFile.getAbsolutePath();
        final int termBufferLength = this.termBufferLength;

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffersReadyFlyweight.bufferOffset(i, i * (long)termBufferLength);
            buffersReadyFlyweight.bufferLength(i, termBufferLength);
            buffersReadyFlyweight.bufferLocation(i, absoluteFilePath);
        }

        final long metaDataStartingOffset = termBufferLength * BUFFER_COUNT;

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final int index = i + BUFFER_COUNT;
            buffersReadyFlyweight.bufferOffset(index, metaDataStartingOffset + (i * TERM_META_DATA_LENGTH));
            buffersReadyFlyweight.bufferLength(index, termBufferLength);
            buffersReadyFlyweight.bufferLocation(index, absoluteFilePath);
        }
    }

    private void recursivelyDeleteUpTree(final File directory, int remainingTreeDepth)
    {
        if (remainingTreeDepth == 0)
        {
            return;
        }

        if (directory.list().length == 0)
        {
            if (directory.delete())
            {
                recursivelyDeleteUpTree(directory.getParentFile(), remainingTreeDepth - 1);
            }
            else
            {
                logger.log(EventCode.ERROR_DELETING_FILE, directory);
            }
        }
    }
}
