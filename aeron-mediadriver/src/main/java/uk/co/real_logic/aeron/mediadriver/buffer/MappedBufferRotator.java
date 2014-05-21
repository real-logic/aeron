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

import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.collections.CollectionUtil;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;

/**
 * Encapsulates responsibility for rotating and reusing memory mapped files used by
 * the buffers.
 *
 * Keeps 3 buffers on hold at any one time.
 */
class MappedBufferRotator implements BufferRotator, AutoCloseable
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel logTemplate;
    private final long logBufferSize;

    private final FileChannel stateTemplate;
    private final long stateBufferSize;
    private final MappedLogBuffers[] buffers;
    private final File directory;

    private MappedLogBuffers current;
    private MappedLogBuffers clean;
    private MappedLogBuffers dirty;

    MappedBufferRotator(final File directory,
                        final FileChannel logTemplate,
                        final long logBufferSize,
                        final FileChannel stateTemplate,
                        final long stateBufferSize)
    {
        this.directory = directory;
        IoUtil.ensureDirectoryExists(directory, "buffer directory");

        this.logTemplate = logTemplate;
        this.logBufferSize = logBufferSize;
        this.stateTemplate = stateTemplate;
        this.stateBufferSize = stateBufferSize;

        try
        {
            requireEqual(logTemplate.size(), logBufferSize);
            requireEqual(stateTemplate.size(), stateBufferSize);
            current = newTerm("0", directory);
            clean = newTerm("1", directory);
            dirty = newTerm("2", directory);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }

        buffers = new MappedLogBuffers[]{ current, clean, dirty };
    }

    private void requireEqual(final long a, final long b)
    {
        if (a != b)
        {
            throw new IllegalArgumentException("Values aren't equal: " + a + " and " + b);
        }
    }

    private MappedLogBuffers newTerm(final String prefix, final File directory) throws IOException
    {
        final String logPath = prefix + LOG_SUFFIX;
        final String statePath = prefix + STATE_SUFFIX;
        final FileChannel logFile = openFile(directory, logPath);
        final FileChannel stateFile = openFile(directory, statePath);

        return new MappedLogBuffers(logPath,
                                    statePath,
                                    logFile,
                                    stateFile,
                                    map(logBufferSize, logFile),
                                    map(stateBufferSize, stateFile));
    }

    public Stream<MappedLogBuffers> buffers()
    {
        return Stream.of(buffers);
    }

    public void rotate() throws IOException
    {
        final MappedLogBuffers newBuffer = clean;

        dirty.reset(logTemplate, stateTemplate);
        clean = dirty;
        dirty = current;
        current = newBuffer;
    }

    public void bufferInformation(final NewBufferMessageFlyweight newBufferMessage)
    {
        for(int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].logBufferInformation(i, newBufferMessage);
        }

        for(int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].stateBufferInformation(i, newBufferMessage);
        }
    }

    public String location()
    {
        return directory.getAbsolutePath();
    }

    private FileChannel openFile(final File directory, final String child) throws FileNotFoundException
    {
        return new RandomAccessFile(new File(directory, child), "rw").getChannel();
    }

    private MappedByteBuffer map(final long bufferSize, final FileChannel channel) throws IOException
    {
        reset(channel, logTemplate, logBufferSize);

        return channel.map(READ_WRITE, 0, bufferSize);
    }

    public static void reset(final FileChannel channel, final FileChannel template, final long bufferSize)
        throws IOException
    {
        channel.position(0);
        template.transferTo(0, bufferSize, channel);
    }

    public void close()
    {
        buffers().forEach(MappedLogBuffers::close);
    }
}
