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
package uk.co.real_logic.agrona;

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
import java.util.function.BiConsumer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Collection of IO utilities.
 */
public class IoUtil
{
    /**
     * Size in bytes of a file page.
     */
    public static final int BLOCK_SIZE = 4 * 1024;

    /**
     * Fill a region of a file with a given byte value.
     *
     * @param fileChannel to fill
     * @param position    at which to start writing.
     * @param length      of the region to write.
     * @param value       to fill the region with.
     */
    public static void fill(final FileChannel fileChannel, final long position, final long length, final byte value)
    {
        try
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
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Recursively delete a file or directory tree.
     *
     * @param file           to be deleted.
     * @param ignoreFailures don't throw an exception if a delete fails.
     */
    public static void delete(final File file, final boolean ignoreFailures)
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
            try
            {
                Files.delete(file.toPath());
            }
            catch (final IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Create a directory if it doesn't already exist.
     *
     * @param directory        the directory which definitely exists after this method call.
     * @param descriptionLabel to associate with the directory for any exceptions.
     */
    public static void ensureDirectoryExists(final File directory, final String descriptionLabel)
    {
        if (!directory.exists())
        {
            if (!directory.mkdirs())
            {
                throw new IllegalArgumentException("could not create " + descriptionLabel + " directory: " + directory);
            }
        }
    }

    /**
     * Create a directory, removing previous directory if it already exists.
     * <p>
     * Call callback if it does exist.
     *
     * @param directory        the directory which definitely exists after this method call.
     * @param descriptionLabel to associate with the directory for any exceptions and callback.
     * @param callback         to call if directory exists passing back absolute path and descriptionLabel.
     */
    public static void ensureDirectoryIsRecreated(
        final File directory, final String descriptionLabel, final BiConsumer<String, String> callback)
    {
        if (directory.exists())
        {
            delete(directory, false);
            callback.accept(directory.getAbsolutePath(), descriptionLabel);
        }

        if (!directory.mkdirs())
        {
            throw new IllegalArgumentException("could not create " + descriptionLabel + " directory: " + directory);
        }
    }

    /**
     * Delete file only if it already exists.
     *
     * @param file to delete
     */
    public static void deleteIfExists(final File file)
    {
        if (file.exists())
        {
            try
            {
                Files.delete(file.toPath());
            }
            catch (final IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Create an empty file, fill with 0s, and return the {@link FileChannel}
     *
     * @param file to create
     * @param size of the file to create
     * @return {@link java.nio.channels.FileChannel} for the file
     */
    public static FileChannel createEmptyFile(final File file, final long size)
    {
        ensureDirectoryExists(file.getParentFile(), file.getParent());

        try
        {
            final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            final FileChannel templateFile = randomAccessFile.getChannel();
            fill(templateFile, 0, size, (byte)0);

            return templateFile;
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Check that file exists, open file, and return MappedByteBuffer for entire file
     * <p>
     * The file itself will be closed, but the mapping will persist.
     *
     * @param location         of the file to map
     * @param descriptionLabel to be associated for any exceptions
     * @return {@link java.nio.MappedByteBuffer} for the file
     */
    public static MappedByteBuffer mapExistingFile(final File location, final String descriptionLabel)
    {
        checkFileExists(location, descriptionLabel);
        try (final RandomAccessFile file = new RandomAccessFile(location, "rw"))
        {
            final FileChannel channel = file.getChannel();
            return channel.map(READ_WRITE, 0, channel.size());
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Check that file exists, open file, and return MappedByteBuffer for only region specified
     * <p>
     * The file itself will be closed, but the mapping will persist.
     *
     * @param location         of the file to map
     * @param descriptionLabel to be associated for an exceptions
     * @param offset           offset to start mapping at
     * @param size             length to map region
     * @return {@link java.nio.MappedByteBuffer} for the file
     */
    public static MappedByteBuffer mapExistingFile(
        final File location, final String descriptionLabel, final int offset, final int size)
    {
        checkFileExists(location, descriptionLabel);

        try (final RandomAccessFile file = new RandomAccessFile(location, "rw"))
        {
            final FileChannel channel = file.getChannel();
            return channel.map(READ_WRITE, offset, size);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a new file, fill with 0s, and return a {@link java.nio.MappedByteBuffer} for the file
     * <p>
     * The file itself will be closed, but the mapping will persist.
     *
     * @param location of the file to create and map
     * @param size     of the file to create and map
     * @return {@link java.nio.MappedByteBuffer} for the file
     */
    public static MappedByteBuffer mapNewFile(final File location, final long size)
    {
        try (final FileChannel channel = createEmptyFile(location, size))
        {
            return channel.map(READ_WRITE, 0, size);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Check that a file exists and throw an exception if not.
     *
     * @param file to check existence of.
     * @param name to associate for the exception
     */
    public static void checkFileExists(final File file, final String name)
    {
        if (!file.exists())
        {
            final String msg = String.format("Missing file for %1$s: %2$s", name, file.getAbsolutePath());
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Unmap a {@link MappedByteBuffer} without waiting for the next GC cycle.
     *
     * @param buffer to be unmapped.
     */
    public static void unmap(final MappedByteBuffer buffer)
    {
        if (null == buffer)
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

    /**
     * Return the system property for java.io.tmpdir ensuring a {@link File#separator} is at the end.
     *
     * @return tmp directory for the runtime
     */
    public static String tmpDirName()
    {
        String tmpDirName = System.getProperty("java.io.tmpdir");
        if (!tmpDirName.endsWith(File.separator))
        {
            tmpDirName += File.separator;
        }

        return tmpDirName;
    }
}
