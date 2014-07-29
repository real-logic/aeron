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

import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

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
    private final File logFile;
    private final File stateFile;

    private final FileChannel logFileChannel;
    private final FileChannel stateFileChannel;

    private final MappedByteBuffer mappedLogBuffer;
    private final MappedByteBuffer mappedStateBuffer;

    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;

    MappedRawLog(final File logFile,
                 final File stateFile,
                 final FileChannel logFileChannel,
                 final FileChannel stateFileChannel,
                 final MappedByteBuffer logBuffer,
                 final MappedByteBuffer stateBuffer)
    {
        this.logFile = logFile;
        this.stateFile = stateFile;
        this.logFileChannel = logFileChannel;
        this.stateFileChannel = stateFileChannel;

        this.mappedLogBuffer = logBuffer;
        this.mappedStateBuffer = stateBuffer;

        this.stateBuffer = new AtomicBuffer(stateBuffer);
        this.logBuffer = new AtomicBuffer(logBuffer);
    }

    public AtomicBuffer logBuffer()
    {
        return logBuffer;
    }

    public AtomicBuffer stateBuffer()
    {
        return stateBuffer;
    }

    public void close()
    {
        try
        {
            logFileChannel.close();
            stateFileChannel.close();

            IoUtil.unmap(mappedLogBuffer);
            IoUtil.unmap(mappedStateBuffer);

            IoUtil.deleteIfExists(logFile);
            IoUtil.deleteIfExists(stateFile);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public void logBufferInformation(final int index, final LogBuffersMessageFlyweight newBufferMessage)
    {
        bufferInformation(index, newBufferMessage, mappedLogBuffer, logFile);
    }

    public void stateBufferInformation(final int index, final LogBuffersMessageFlyweight newBufferMessage)
    {
        bufferInformation(index + BUFFER_COUNT, newBufferMessage, mappedStateBuffer, stateFile);
    }

    private void bufferInformation(final int index,
                                   final LogBuffersMessageFlyweight newBufferMessage,
                                   final MappedByteBuffer buffer,
                                   final File file)
    {
        final int offset = buffer.position();
        newBufferMessage.bufferOffset(index, offset);
        newBufferMessage.bufferLength(index, buffer.capacity() - offset);
        newBufferMessage.location(index, file.getAbsolutePath());
    }
}
