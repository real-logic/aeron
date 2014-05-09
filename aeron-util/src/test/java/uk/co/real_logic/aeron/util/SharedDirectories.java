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
package uk.co.real_logic.aeron.util;

import org.junit.rules.ExternalResource;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.IoUtil.createEmptyFile;
import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class SharedDirectories extends ExternalResource
{
    private File dataDir;
    private FileMappingConvention mapping;
    private List<MappedByteBuffer> buffers;

    public static List<LogAppender> mapLoggers(final List<Buffers> termBuffers,
                                               final byte[] defaultHeader,
                                               final int maxFrameLength)
    {
        return termBuffers.stream()
                          .map(buffer -> new LogAppender(buffer.logBuffer(),
                                  buffer.stateBuffer(),
                                  defaultHeader,
                                  maxFrameLength))
                          .collect(toList());
    }

    protected void before() throws Throwable
    {
        dataDir = ensureDirectory(CommonConfiguration.DATA_DIR);
        mapping = new FileMappingConvention(dataDir.getAbsolutePath());
        buffers = new ArrayList<>();
    }

    protected void after()
    {
        try
        {
            // delete the dirs here so that if they error, we know the test that failed to unmap/close
            unmapBuffers();
            IoUtil.delete(dataDir, false);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void unmapBuffers()
    {
        buffers.forEach((b) -> IoUtil.unmap(b));
        buffers.clear();
    }

    private File ensureDirectory(final String path) throws IOException
    {
        final File dir = new File(path);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
        IoUtil.ensureDirectoryExists(dir, "data dir");

        return dir;
    }

    public static class Buffers
    {
        private final AtomicBuffer stateBuffer;
        private final AtomicBuffer logBuffer;

        public Buffers(final AtomicBuffer stateBuffer, final AtomicBuffer logBuffer)
        {
            this.stateBuffer = stateBuffer;
            this.logBuffer = logBuffer;
        }

        public AtomicBuffer logBuffer()
        {
            return logBuffer;
        }

        public AtomicBuffer stateBuffer()
        {
            return stateBuffer;
        }
    }

    public File senderDir()
    {
        return mapping.senderDir();
    }

    public File receiverDir()
    {
        return mapping.receiverDir();
    }

    public List<Buffers> createTermFile(final File rootDir,
                                        final String destination,
                                        final long sessionId,
                                        final long channelId) throws IOException
    {
        final List<Buffers> buffers = new ArrayList<>();
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final AtomicBuffer logBuffer = createTermFile(rootDir, destination, sessionId, channelId, i, LOG);
            final AtomicBuffer stateBuffer = createTermFile(rootDir, destination, sessionId, channelId, i, STATE);
            buffers.add(new Buffers(stateBuffer, logBuffer));
        }

        return buffers;
    }

    private AtomicBuffer createTermFile(final File rootDir,
                                        final String destination,
                                        final long sessionId,
                                        final long channelId,
                                        final long termId,
                                        final Type type) throws IOException
    {
        final File termLocation = termLocation(rootDir, sessionId, channelId, termId, true, destination, type);
        IoUtil.delete(termLocation, true);

        final MappedByteBuffer buffer = mapNewFile(termLocation, "Term Buffer", BufferDescriptor.LOG_MIN_SIZE);

        buffers.add(buffer);
        return new AtomicBuffer(buffer);
    }

    public String dataDir()
    {
        return dataDir.getAbsolutePath();
    }

    public List<Buffers> mapTermFile(final File rootDir,
                                     final String destination,
                                     final long sessionId,
                                     final long channelId) throws IOException
    {
        final List<Buffers> buffers = new ArrayList<>();
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final AtomicBuffer logBuffer = mapTermFile(rootDir, destination, sessionId, channelId, i, LOG);
            final AtomicBuffer stateBuffer = mapTermFile(rootDir, destination, sessionId, channelId, i, STATE);
            buffers.add(new Buffers(stateBuffer, logBuffer));
        }

        return buffers;
    }

    private AtomicBuffer mapTermFile(final File rootDir,
                                     final String destination,
                                     final long sessionId,
                                     final long channelId,
                                     final long termId,
                                     final Type type) throws IOException
    {
        final File termLocation = termLocation(rootDir, sessionId, channelId, termId, false, destination, type);

        final MappedByteBuffer buffer = IoUtil.mapExistingFile(termLocation, "Term Buffer");

        buffers.add(buffer);
        return new AtomicBuffer(buffer);
    }
}
