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
package uk.co.real_logic.aeron.mediadriver.buffer;

import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;

/**
 * .
 */
class MappedLogBuffers implements LogBuffers
{
    private final String logPath;
    private final String statePath;

    private final FileChannel logFile;
    private final FileChannel stateFile;

    private final MappedByteBuffer mappedLogBuffer;
    private final MappedByteBuffer mappedStateBuffer;

    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;

    MappedLogBuffers(final String logPath,
                     final String statePath,
                     final FileChannel logFile,
                     final FileChannel stateFile,
                     final MappedByteBuffer logBuffer,
                     final MappedByteBuffer stateBuffer)
    {
        this.logPath = logPath;
        this.statePath = statePath;
        this.logFile = logFile;
        this.stateFile = stateFile;

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

    public void reset(final FileChannel logTemplate, final FileChannel stateTemplate) throws IOException
    {
        MappedBufferRotator.reset(logFile, logTemplate, logBuffer.capacity());
        MappedBufferRotator.reset(stateFile, stateTemplate, stateBuffer.capacity());
    }

    public void close()
    {
        try
        {
            logFile.close();
            stateFile.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        IoUtil.unmap(mappedLogBuffer);
        IoUtil.unmap(mappedStateBuffer);
    }

    public void logBufferInformation(final int index, final NewBufferMessageFlyweight newBufferMessage)
    {
        bufferInformation(index, newBufferMessage, mappedLogBuffer, logPath);
    }

    public void stateBufferInformation(final int index, final NewBufferMessageFlyweight newBufferMessage)
    {
        bufferInformation(index + BUFFER_COUNT, newBufferMessage, mappedStateBuffer, statePath);
    }

    private void bufferInformation(final int index,
                                   final NewBufferMessageFlyweight newBufferMessage,
                                   final MappedByteBuffer buffer,
                                   final String path)
    {
        final int offset = buffer.position();
        newBufferMessage.bufferOffset(index, offset);
        newBufferMessage.bufferLength(index, buffer.position() - offset);
        newBufferMessage.location(index, path);
    }

}
