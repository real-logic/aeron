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
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class SharedDirectoriesExternalResource extends ExternalResource
{
    private File dataDir;
    private FileMappingConvention mapping;
    private List<MappedByteBuffer> mappedByteBuffers;

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
        dataDir = ensureDirectory(CommonContext.DATA_DIR_PROP_DEFAULT);
        mapping = new FileMappingConvention(dataDir.getAbsolutePath());
        mappedByteBuffers = new ArrayList<>(3);
    }

    protected void after()
    {
        try
        {
            unMapBuffers();
            IoUtil.delete(dataDir, false);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void unMapBuffers()
    {
        mappedByteBuffers.forEach(IoUtil::unmap);
        mappedByteBuffers.clear();
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

    public File publicationsDir()
    {
        return mapping.publicationsDir();
    }

    public File subscriptionsDir()
    {
        return mapping.subscriptionsDir();
    }

    public List<Buffers> createTermFile(final File rootDir,
                                        final String destination,
                                        final long sessionId,
                                        final long channelId) throws IOException
    {
        final List<Buffers> buffers = new ArrayList<>(BUFFER_COUNT);
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

        final MappedByteBuffer buffer = mapNewFile(termLocation, "Term Buffer", LogBufferDescriptor.LOG_MIN_SIZE);
        mappedByteBuffers.add(buffer);

        return new AtomicBuffer(buffer);
    }
}
