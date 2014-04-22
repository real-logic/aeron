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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Encapsulates responsibility for rotating and reusing memory mapped files used by
 * the buffers.
 *
 * Keeps 3 buffers on hold at any one time.
 */
public class MappedBufferRotator implements BufferRotator
{
    private static final String LOG_SUFFIX = "-log";
    private static final String STATE_SUFFIX = "-state";

    private final FileChannel logTemplate;
    private final long logBufferSize;

    private final FileChannel stateTemplate;
    private final long stateBufferSize;
    private final BasicLogBuffers[] buffers;

    private BasicLogBuffers current;
    private BasicLogBuffers clean;
    private BasicLogBuffers dirty;

    public MappedBufferRotator(final File directory,
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
            current = newTerm("1", directory);
            clean = newTerm("2", directory);
            dirty = newTerm("3", directory);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }

        buffers = new BasicLogBuffers[]{ current, clean, dirty };
    }

    private BasicLogBuffers newTerm(final String prefix, final File directory) throws IOException
    {
        final FileChannel logFile = openFile(directory, prefix + LOG_SUFFIX);
        final FileChannel stateFile = openFile(directory, prefix + STATE_SUFFIX);

        return new BasicLogBuffers(logFile, stateFile, map(logBufferSize, logFile), map(stateBufferSize, stateFile));
    }

    public Stream<LogBuffers> buffers()
    {
        return Stream.of(buffers);
    }

    public void rotate() throws IOException
    {
        final BasicLogBuffers newBuffer = clean;

        clean = dirty;
        dirty = current;
        dirty.reset(logTemplate, stateTemplate);
        current = newBuffer;
    }

    private FileChannel openFile(final File directory, final String child) throws FileNotFoundException
    {
        final File fileToMap = new File(directory, child);
        final RandomAccessFile file = new RandomAccessFile(fileToMap, "rw");

        return file.getChannel();
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
}
