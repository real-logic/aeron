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
 * Encapsulates responsibility for rotating and reusing memory mapped files used by the log buffers.
 * <p>
 * Keeps 3 buffers on hold at any one time.
 */
class MappedTermBuffers implements TermBuffers
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel logTemplate;
    private final FileChannel stateTemplate;
    private final int logBufferLength;
    private final int stateBufferLength;
    private final MappedRawLog[] buffers;
    private final EventLogger logger;

    MappedTermBuffers(
        final File directory,
        final FileChannel logTemplate,
        final int logBufferLength,
        final FileChannel stateTemplate,
        final int stateBufferSize,
        final EventLogger logger)
    {
        this.logger = logger;
        IoUtil.ensureDirectoryExists(directory, "buffer directory");

        this.logTemplate = logTemplate;
        this.logBufferLength = logBufferLength;
        this.stateBufferLength = stateBufferSize;
        this.stateTemplate = stateTemplate;

        try
        {
            checkSizeLogBuffer(logTemplate.size(), logBufferLength);
            checkSizeStateBuffer(stateTemplate.size(), stateBufferSize);

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

    public void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight)
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].logBufferInformation(i, buffersReadyFlyweight);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            buffers[i].stateBufferInformation(i, buffersReadyFlyweight);
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

    private MappedRawLog newTerm(final String prefix, final File directory)
    {
        try
        {
            final File logFile = new File(directory, prefix + LOG_SUFFIX);
            final File stateFile = new File(directory, prefix + STATE_SUFFIX);
            final FileChannel logFileChannel = openBufferFile(logFile);
            final FileChannel stateFileChannel = openBufferFile(stateFile);

            return new MappedRawLog(
                logFile,
                stateFile,
                logFileChannel,
                stateFileChannel,
                mapBufferFile(logFileChannel, logTemplate, logBufferLength),
                mapBufferFile(stateFileChannel, stateTemplate, stateBufferLength),
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

    private MappedByteBuffer mapBufferFile(
        final FileChannel channel,
        final FileChannel template,
        final long bufferSize)
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
