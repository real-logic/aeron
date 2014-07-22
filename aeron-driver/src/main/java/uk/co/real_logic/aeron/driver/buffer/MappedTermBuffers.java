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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

/**
 * Encapsulates responsibility for rotating and reusing memory mapped files used by the log buffers.
 * <p>
 * Keeps 3 buffers on hold at any one time.
 */
class MappedTermBuffers implements TermBuffers
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel logTemplate;
    private final int logBufferLength;
    private final int stateBufferLength;
    private final MappedRawLog[] buffers;

    MappedTermBuffers(final File directory,
                      final FileChannel logTemplate,
                      final int logBufferLength,
                      final FileChannel stateTemplate,
                      final int stateBufferSize)
    {
        IoUtil.ensureDirectoryExists(directory, "buffer directory");

        this.logTemplate = logTemplate;
        this.logBufferLength = logBufferLength;
        this.stateBufferLength = stateBufferSize;

        try
        {
            requireEqual(logTemplate.size(), logBufferLength);
            requireEqual(stateTemplate.size(), stateBufferSize);
            final MappedRawLog active = newTerm("0", directory);
            final MappedRawLog clean = newTerm("1", directory);
            final MappedRawLog dirty = newTerm("2", directory);


            buffers = new MappedRawLog[]{active, clean, dirty};
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public void close()
    {
        stream().forEach(MappedRawLog::close);
    }

    public Stream<MappedRawLog> stream()
    {
        return Stream.of(buffers);
    }

    public MappedRawLog[] buffers()
    {
        return buffers;
    }

    public void appendBufferLocationsTo(final LogBuffersMessageFlyweight logBuffersMessage)
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].logBufferInformation(i, logBuffersMessage);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].stateBufferInformation(i, logBuffersMessage);
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

    private MappedRawLog newTerm(final String prefix, final File directory) throws IOException
    {
        final File logFile = new File(directory, prefix + LOG_SUFFIX);
        final File stateFile = new File(directory, prefix + STATE_SUFFIX);
        final FileChannel logFileChannel = openBufferFile(logFile);
        final FileChannel stateFileChannel = openBufferFile(stateFile);

        return new MappedRawLog(logFile,
                                stateFile,
                                logFileChannel,
                                stateFileChannel,
                                mapBufferFile(logFileChannel, logBufferLength),
                                mapBufferFile(stateFileChannel, stateBufferLength));
    }

    private FileChannel openBufferFile(final File file) throws FileNotFoundException
    {
        return new RandomAccessFile(file, "rw").getChannel();
    }

    private MappedByteBuffer mapBufferFile(final FileChannel channel, final long bufferSize)
        throws IOException
    {
        reset(channel, logTemplate, logBufferLength);

        return channel.map(READ_WRITE, 0, bufferSize);
    }
}
