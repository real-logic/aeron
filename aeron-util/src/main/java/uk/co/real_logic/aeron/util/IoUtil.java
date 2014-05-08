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

import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Collection of IO utilities.
 */
public class IoUtil
{
    /** Size in bytes of a file page. */
    public static final int BLOCK_SIZE = 4 * 1024;

    /**
     * Fill a region of a file with a given byte value.
     *
     * @param fileChannel to fill
     * @param position at which to start writing.
     * @param length of the region to write.
     * @param value to fill the region with.
     */
    public static void fill(final FileChannel fileChannel, final long position, final long length, final byte value)
        throws IOException
    {
        final byte[] filler = new byte[BLOCK_SIZE];
        Arrays.fill(filler, value);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(filler);
        fileChannel.position(position);

        final int blocks = (int)(length / BLOCK_SIZE);
        final int blockRemainder = (int)(length % BLOCK_SIZE);

        for (int i = 0; i < blocks; i++)
        {
            byteBuffer.position(0);
            fileChannel.write(byteBuffer);
        }

        if (blockRemainder > 0)
        {
            byteBuffer.position(0);
            byteBuffer.limit(blockRemainder);
            fileChannel.write(byteBuffer);
        }
    }

    /**
     * Recursively delete a file or directory tree.
     *
     * @param file to be deleted.
     * @param ignoreFailures don't throw an exception if a delete fails.
     * @throws IOException if an error occurs while trying to delete the files.
     */
    public static void delete(final File file, final boolean ignoreFailures)
        throws IOException
    {
        if (file.isDirectory())
        {
            final File[] files = file.listFiles();
            if (null != files)
            {
                for (final File f : files)
                {
                    delete(f, ignoreFailures);
                }
            }
        }

        if (!file.delete() && !ignoreFailures)
        {
            Files.delete(file.toPath());
            // throw new FileNotFoundException("Failed to delete file: " + file);
        }
    }

    /**
     * Create a directory if it doesn't already exist.
     *
     * @param directory the directory which definitely exists after this method call.
     * @throws IllegalArgumentException thrown if the directory cannot be created
     */
    public static void ensureDirectoryExists(File directory, String name) throws IllegalArgumentException
    {
        if (!directory.exists())
        {
            if (!directory.mkdirs())
            {
                throw new IllegalArgumentException("could not create " + name + " directory: " + directory);
            }
        }
    }

    /**
     * Check that a direct exists, throwing an exception if it doesn't.
     */
    public static void checkDirectoryExists(File directory, String name)
    {
        if (!directory.exists() || !directory.isDirectory())
        {
            throw new IllegalArgumentException(name + " does not exist or is not a directory: " + directory);
        }
    }

    public static FileChannel createEmptyFile(final File file, final long size) throws IOException
    {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        final FileChannel templateFile = randomAccessFile.getChannel();
        fill(templateFile, 0, size, (byte) 0);

        return templateFile;
    }

    public static MappedByteBuffer mapExistingFile(final File location, final String name) throws IOException
    {
        checkFileExists(location, name);
        try (final RandomAccessFile file = new RandomAccessFile(location, "rw"))
        {
            final FileChannel channel = file.getChannel();
            return map(channel);
        }
    }

    public static MappedByteBuffer map(final FileChannel channel) throws IOException
    {
        long size = channel.size();
        return channel.map(READ_WRITE, 0, size);
    }

    /**
     * Create an initialised
     */
    public static MappedByteBuffer mapNewFile(final File location, final String name, final long size)
            throws IOException
    {
        try (final FileChannel channel = createEmptyFile(location, size))
        {
            return channel.map(READ_WRITE, 0, size);
        }
    }

    public static void checkFileExists(final File file, final String name)
    {
        if (!file.exists())
        {
            throw new IllegalStateException(String.format("Missing file for %1$s: %2$s", name, file.getAbsolutePath()));
        }
    }

    /**
     * Unmap a {@link MappedByteBuffer} without waiting for the next GC cycle.
     *
     * @param buffer to be unmapped.
     */
    public static void unmap(final MappedByteBuffer buffer)
    {
        if (buffer == null)
        {
            return;
        }
        
        try
        {
            final Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);

            method.setAccessible(true);
            method.invoke(null, buffer);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static String tmpDir()
    {
        return System.getProperty("java.io.tmpdir");
    }

}
