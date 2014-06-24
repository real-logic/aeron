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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;

/**
 * Encapsulates responsibility for rotating and reusing memory mapped files used by
 * the buffers.
 * <p>
 * Keeps 3 buffers on hold at any one time.
 */
class MappedBufferRotator implements BufferRotator, AutoCloseable
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;
    private final long logBufferSize;
    private final long stateBufferSize;
    private final MappedLogBuffers[] buffers;

    private MappedLogBuffers current;
    private MappedLogBuffers clean;
    private MappedLogBuffers dirty;

    MappedBufferRotator(final File directory,
                        final FileChannel logTemplate,
                        final long logBufferSize,
                        final FileChannel stateTemplate,
                        final long stateBufferSize)
    {
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

        buffers = new MappedLogBuffers[]{current, clean, dirty};
    }

    public void close()
    {
        buffers().forEach(MappedLogBuffers::close);
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

    public void appendBufferLocationsTo(final NewBufferMessageFlyweight newBufferMessage)
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].logBufferInformation(i, newBufferMessage);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].stateBufferInformation(i, newBufferMessage);
        }
    }

    public static void reset(final FileChannel channel, final FileChannel template, final long bufferSize)
        throws IOException
    {
        channel.position(0);
        template.transferTo(0, bufferSize, channel);
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
        final File logFile = new File(directory, prefix + LOG_SUFFIX);
        final File stateFile = new File(directory, prefix + STATE_SUFFIX);
        final FileChannel logFileChannel = openBufferFile(logFile);
        final FileChannel stateFileChannel = openBufferFile(stateFile);

        return new MappedLogBuffers(logFile,
                                    stateFile,
                                    logFileChannel,
                                    stateFileChannel,
                                    mapBufferFile(logFileChannel, logBufferSize),
                                    mapBufferFile(stateFileChannel, stateBufferSize));
    }

    private FileChannel openBufferFile(final File file) throws FileNotFoundException
    {
        return new RandomAccessFile(file, "rw").getChannel();
    }

    private MappedByteBuffer mapBufferFile(final FileChannel channel, final long bufferSize) throws IOException
    {
        reset(channel, logTemplate, logBufferSize);

        return channel.map(READ_WRITE, 0, bufferSize);
    }
}
