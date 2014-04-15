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

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Encapsulates responsibility for rotating and reusing memory mapped files used by
 * the buffers.
 *
 * Keeps 3 buffers on hold at any one time.
 */
public class MappedBufferRotator
{

    private final FileChannel templateFile;
    private final long bufferSize;

    private FileChannel currentFile;
    private FileChannel cleanFile;
    private FileChannel dirtyFile;

    // TODO: add state buffers as well
    private MappedByteBuffer currentBuffer;
    private MappedByteBuffer cleanBuffer;
    private MappedByteBuffer dirtyBuffer;

    public MappedBufferRotator(final FileChannel templateFile, final File directory, final long bufferSize)
    {
        this.templateFile = templateFile;
        this.bufferSize = bufferSize;
        IoUtil.ensureDirectoryExists(directory, "buffer directory");
        try
        {
            currentFile = openFile(directory, "1");
            currentBuffer = map(bufferSize, currentFile);

            cleanFile = openFile(directory, "2");
            cleanBuffer = map(bufferSize, cleanFile);

            dirtyFile = openFile(directory, "3");
            dirtyBuffer = map(bufferSize, dirtyFile);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public MappedByteBuffer rotate() throws IOException
    {
        final MappedByteBuffer newBuffer = cleanBuffer;
        final FileChannel newFile = cleanFile;

        cleanBuffer = dirtyBuffer;
        cleanFile = dirtyFile;

        dirtyBuffer = currentBuffer;
        dirtyFile = currentFile;
        reset(dirtyFile);

        currentBuffer = newBuffer;
        currentFile = newFile;

        return newBuffer;
    }

    public MappedByteBuffer dirtyBuffer()
    {
        return dirtyBuffer;
    }

    private FileChannel openFile(final File directory, final String child) throws FileNotFoundException
    {
        final File fileToMap = new File(directory, child);
        final RandomAccessFile file = new RandomAccessFile(fileToMap, "rw");
        return file.getChannel();
    }

    private void reset(final FileChannel channel) throws IOException
    {
        channel.position(0);
        templateFile.transferTo(0, bufferSize, channel);
    }

    private MappedByteBuffer map(final long bufferSize, final FileChannel channel) throws IOException
    {
        reset(channel);
        return channel.map(READ_WRITE, 0, bufferSize);
    }

}
