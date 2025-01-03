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
package io.aeron.samples.archive;

import io.aeron.logbuffer.FragmentHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Common constants and functions used in the Archive samples.
 */
class Samples
{
    /**
     * One MB constant for using in calculations.
     */
    public static final double MEGABYTE = 1024.0d * 1024.0d;

    /**
     * A {@link FragmentHandler} that consumes fragments with no side effects.
     */
    public static final FragmentHandler NOOP_FRAGMENT_HANDLER = (buffer, offset, length, header) -> {};

    /**
     * Create a temporary directory for storing a sample archive.
     *
     * @return a temporary directory for storing a sample archive.
     */
    public static File createTempDir()
    {
        final File tempDirForTest;
        try
        {
            tempDirForTest = Files.createTempFile("archive", "tmp").toFile();
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        if (!tempDirForTest.delete())
        {
            throw new IllegalStateException("failed to delete: " + tempDirForTest);
        }

        if (!tempDirForTest.mkdir())
        {
            throw new IllegalStateException("failed to create: " + tempDirForTest);
        }

        return tempDirForTest;
    }
}
