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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * .
 */
class MappedLogBuffers implements LogBuffers
{
    private final FileChannel logFile;
    private final FileChannel stateFile;

    private final MappedByteBuffer mappedLogBuffer;
    private final MappedByteBuffer mappedStateBuffer;

    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;

    MappedLogBuffers(final FileChannel logFile,
                     final FileChannel stateFile,
                     final MappedByteBuffer logBuffer,
                     final MappedByteBuffer stateBuffer)
    {
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

}
