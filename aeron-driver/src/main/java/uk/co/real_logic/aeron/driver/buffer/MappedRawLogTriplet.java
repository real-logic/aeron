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
class MappedRawLogTriplet implements RawLogTriplet
{
    private static final String TERM_SUFFIX = "-term";
    private static final String STATE_SUFFIX = "-state";

    private final MappedRawLog[] buffers;

    MappedRawLogTriplet(
        final File directory,
        final FileChannel termTemplate,
        final int termBufferLength,
        final FileChannel termStateTemplate,
        final int termStateBufferLength,
        final EventLogger logger)
    {
        IoUtil.ensureDirectoryExists(directory, "log buffer directory");

        try
        {
            checkSizeTermBuffer(termTemplate.size(), termBufferLength);
            checkSizeStateBuffer(termStateTemplate.size(), termStateBufferLength);

            buffers = new MappedRawLog[]
            {
                mapRawLog("0", directory, termTemplate, termBufferLength, termStateTemplate, termStateBufferLength, logger),
                mapRawLog("1", directory, termTemplate, termBufferLength, termStateTemplate, termStateBufferLength, logger),
                mapRawLog("2", directory, termTemplate, termBufferLength, termStateTemplate, termStateBufferLength, logger),
            };
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
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
            buffers[i].writeTermBufferLocation(i, buffersReadyFlyweight);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].writeStateBufferLocation(i, buffersReadyFlyweight);
        }
    }

    public static void reset(final FileChannel channel, final FileChannel template, final long bufferLength)
    {
        try
        {
            channel.position(0);
            template.transferTo(0, bufferLength, channel);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private static void checkSizeTermBuffer(final long templateLength, final long desiredLength)
    {
        if (desiredLength > templateLength)
        {
            throw new IllegalArgumentException("Desired size (" + desiredLength + ") > template length: " + templateLength);
        }
    }

    private static void checkSizeStateBuffer(final long templateLength, final long desiredLength)
    {
        if (desiredLength != templateLength)
        {
            throw new IllegalArgumentException("Values aren't equal: " + desiredLength + " and " + templateLength);
        }
    }

    private MappedRawLog mapRawLog(
        final String prefix,
        final File directory,
        final FileChannel termTemplate,
        final int termBufferLength,
        final FileChannel stateTemplate,
        final int termStateBufferLength,
        final EventLogger logger)
    {
        try
        {
            final File termFile = new File(directory, prefix + TERM_SUFFIX);
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

    private MappedByteBuffer mapBufferFile(final FileChannel channel, final FileChannel template, final long bufferLength)
    {
        reset(channel, template, bufferLength);

        try
        {
            return channel.map(READ_WRITE, 0, bufferLength);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
