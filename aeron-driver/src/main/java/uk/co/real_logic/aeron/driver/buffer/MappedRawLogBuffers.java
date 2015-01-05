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
import uk.co.real_logic.aeron.common.event.EventLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

/**
 * Encapsulates responsibility for mapping the files into memory used by the log buffers.
 */
class MappedRawLogBuffers implements RawLogBuffers
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel termTemplate;
    private final FileChannel stateTemplate;
    private final int termBufferLength;
    private final int termStateBufferLength;
    private final MappedRawLog[] buffers;
    private final EventLogger logger;

    MappedRawLogBuffers(
        final File directory,
        final FileChannel termTemplate,
        final int termBufferLength,
        final FileChannel termStateTemplate,
        final int termStateBufferSize,
        final EventLogger logger)
    {
        this.logger = logger;
        IoUtil.ensureDirectoryExists(directory, "buffer directory");

        this.termTemplate = termTemplate;
        this.termBufferLength = termBufferLength;
        this.termStateBufferLength = termStateBufferSize;
        this.stateTemplate = termStateTemplate;

        try
        {
            checkSizeLogBuffer(termTemplate.size(), termBufferLength);
            checkSizeStateBuffer(termStateTemplate.size(), termStateBufferSize);

            buffers = new MappedRawLog[]{mapRawLog("0", directory), mapRawLog("1", directory), mapRawLog("2", directory)};
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

    public void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].writeLogBufferLocation(i, buffersReadyFlyweight);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].writeStateBufferLocation(i, buffersReadyFlyweight);
        }
    }

    public static void reset(final FileChannel channel, final FileChannel template, final long bufferSize)
    {
        try
        {
            channel.position(0);
            template.transferTo(0, bufferSize, channel);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private static void checkSizeLogBuffer(final long templateSize, final long desiredSize)
    {
        if (desiredSize > templateSize)
        {
            throw new IllegalArgumentException("Desired size (" + desiredSize + ") > template size: " + templateSize);
        }
    }

    private static void checkSizeStateBuffer(final long templateSize, final long desiredSize)
    {
        if (desiredSize != templateSize)
        {
            throw new IllegalArgumentException("Values aren't equal: " + desiredSize + " and " + templateSize);
        }
    }

    private MappedRawLog mapRawLog(final String prefix, final File directory)
    {
        try
        {
            final File termFile = new File(directory, prefix + LOG_SUFFIX);
            final File stateFile = new File(directory, prefix + STATE_SUFFIX);
            final FileChannel termFileChannel = openBufferFile(termFile);
            final FileChannel stateFileChannel = openBufferFile(stateFile);

            return new MappedRawLog(
                termFile,
                stateFile,
                termFileChannel,
                stateFileChannel,
                mapBufferFile(termFileChannel, termTemplate, termBufferLength),
                mapBufferFile(stateFileChannel, stateTemplate, termStateBufferLength),
                logger);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private FileChannel openBufferFile(final File file) throws FileNotFoundException
    {
        return new RandomAccessFile(file, "rw").getChannel();
    }

    private MappedByteBuffer mapBufferFile(final FileChannel channel, final FileChannel template, final long bufferSize)
    {
        reset(channel, template, bufferSize);

        try
        {
            return channel.map(READ_WRITE, 0, bufferSize);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
