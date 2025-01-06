/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.build;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * Removes duplicate entries from Jar files. Will process the file in place.
 */
public class DeduplicateTask extends DefaultTask
{
    private File source;

    /**
     * Get the input file to deduplicate.
     *
     * @return jar file that will be deduplicated.
     */
    @InputFile
    public File getSource()
    {
        return source;
    }

    /**
     * Set the input file to deduplicate.
     *
     * @param source jar file that will be deduplicated.
     */
    public void setSource(final File source)
    {
        this.source = source;
    }

    /**
     * Run the deduplication process.
     *
     * @throws IOException if an error occurs during the process.
     */
    @TaskAction
    @SuppressWarnings("NestedTryDepth")
    public void process() throws IOException
    {
        final File newFile = generateUnusedFileName(source, "new");
        final File oldFile = generateUnusedFileName(source, "old");

        try
        {
            try (JarFile inputFile = new JarFile(source))
            {
                final LinkedHashMap<String, JarEntry> entries = new LinkedHashMap<>();
                inputFile.stream().forEach((jarEntry) -> entries.putIfAbsent(jarEntry.getName(), jarEntry));

                try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(newFile.toPath())))
                {
                    entries.forEach(
                        (k, v) ->
                        {
                            final byte[] buf = new byte[4096];
                            try
                            {
                                final InputStream inputStream = inputFile.getInputStream(v);
                                jarOutputStream.putNextEntry(new ZipEntry(k));
                                int read;
                                while (-1 != (read = inputStream.read(buf)))
                                {
                                    jarOutputStream.write(buf, 0, read);
                                }
                                jarOutputStream.flush();
                            }
                            catch (final IOException ex)
                            {
                                throw new RuntimeException(ex);
                            }
                        });
                }
            }

            if (!source.renameTo(oldFile))
            {
                throw new IOException("Failed to rename: " + source + " to " + oldFile);
            }
            if (!newFile.renameTo(source))
            {
                throw new IOException("Failed to rename: " + newFile + " to " + source);
            }

            cleanup(oldFile, null);
        }
        catch (final IOException ex)
        {
            cleanup(newFile, ex);
            cleanup(oldFile, ex);
        }
    }

    private void cleanup(final File file, final IOException currentException) throws IOException
    {
        if (file.exists() && !file.delete())
        {
            final IOException ex = new IOException("Failed to delete: " + file.getPath());
            if (null != currentException)
            {
                currentException.addSuppressed(ex);
            }
            else
            {
                throw ex;
            }
        }
    }

    private File generateUnusedFileName(final File source, final String label)
    {
        final int maxIterations = 10_000;
        for (int i = 0; i < maxIterations; i++)
        {
            final String filepath = source.getPath() + ("._" + label + i);
            final File file = new File(filepath);
            if (!file.exists())
            {
                return file;
            }
        }

        throw new RuntimeException("Tried " + maxIterations + " filenames and couldn't find an unused name");
    }
}
