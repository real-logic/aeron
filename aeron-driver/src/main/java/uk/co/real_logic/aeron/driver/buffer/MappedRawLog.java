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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.aeron.common.command.BuffersReadyFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

/**
 * Memory mapped raw logs to make up a term log buffer.
 */
class MappedRawLog implements RawLog
{
    public static final int MAX_TREE_DEPTH = 3;

    private final File termFile;
    private final File stateFile;

    private final FileChannel termFileChannel;
    private final FileChannel stateFileChannel;

    private final MappedByteBuffer mappedTermBuffer;
    private final MappedByteBuffer mappedStateBuffer;

    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer stateBuffer;

    private final EventLogger logger;

    MappedRawLog(
        final File termFile,
        final File stateFile,
        final FileChannel termFileChannel,
        final FileChannel stateFileChannel,
        final MappedByteBuffer termBuffer,
        final MappedByteBuffer stateBuffer,
        final EventLogger logger)
    {
        this.termFile = termFile;
        this.stateFile = stateFile;
        this.termFileChannel = termFileChannel;
        this.stateFileChannel = stateFileChannel;

        this.mappedTermBuffer = termBuffer;
        this.mappedStateBuffer = stateBuffer;
        this.logger = logger;

        this.stateBuffer = new UnsafeBuffer(stateBuffer);
        this.termBuffer = new UnsafeBuffer(termBuffer);
    }

    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
    }

    public UnsafeBuffer termStateBuffer()
    {
        return stateBuffer;
    }

    public void close()
    {
        try
        {
            termFileChannel.close();
            stateFileChannel.close();

            IoUtil.unmap(mappedTermBuffer);
            IoUtil.unmap(mappedStateBuffer);

            if (termFile.delete() && stateFile.delete())
            {
                final File directory = stateFile.getParentFile();
                recursivelyDeleteUpTree(directory, MAX_TREE_DEPTH);
            }
            else
            {
                logger.log(EventCode.ERROR_DELETING_FILE, termFile);
                logger.log(EventCode.ERROR_DELETING_FILE, stateFile);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
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

    public void writeLogBufferLocation(final int index, final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        bufferLocation(index, buffersReadyFlyweight, mappedTermBuffer, termFile);
    }

    public void writeStateBufferLocation(final int index, final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        bufferLocation(index + BUFFER_COUNT, buffersReadyFlyweight, mappedStateBuffer, stateFile);
    }

    private void bufferLocation(
        final int index, final BuffersReadyFlyweight buffersReadyFlyweight, final MappedByteBuffer buffer, final File file)
    {
        final int offset = buffer.position();
        buffersReadyFlyweight.bufferOffset(index, offset);
        buffersReadyFlyweight.bufferLength(index, buffer.capacity() - offset);
        buffersReadyFlyweight.bufferLocation(index, file.getAbsolutePath());
    }
}
