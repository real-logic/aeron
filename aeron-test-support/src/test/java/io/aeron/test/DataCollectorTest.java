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
package io.aeron.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.test.DataCollector.THREAD_DUMP_FILE_NAME;
import static io.aeron.test.DataCollector.UNIQUE_ID;
import static java.nio.file.Files.*;
import static org.agrona.collections.ArrayUtil.EMPTY_BYTE_ARRAY;
import static org.junit.jupiter.api.Assertions.*;

class DataCollectorTest
{
    @Test
    void throwsNullPointerExceptionIfTargetDirectoryIsNull()
    {
        assertThrows(NullPointerException.class, () -> new DataCollector(null));
    }

    @Test
    void throwsIllegalArgumentExceptionIfTargetDirectoryIsAFile(final @TempDir Path tempDir) throws IOException
    {
        final Path file = createFile(tempDir.resolve("my.txt"));
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, () -> new DataCollector(file));

        assertEquals(file + " is not a directory", exception.getMessage());
    }

    @Test
    void addFileThrowsNullPointerExceptionIfFileIsNull()
    {
        final DataCollector dataCollector = new DataCollector();
        assertThrows(NullPointerException.class, () -> dataCollector.add((Path)null));
    }

    @Test
    void dumpDataUsingDirectoryNameThrowsIllegalArgumentExceptionIfNull()
    {
        final DataCollector dataCollector = new DataCollector();
        assertThrows(IllegalArgumentException.class, () -> dataCollector.dumpData(null, EMPTY_BYTE_ARRAY));
    }

    @Test
    void dumpDataUsingDirectoryNameThrowsIllegalArgumentExceptionIfEmpty()
    {
        final DataCollector testWatcher = new DataCollector();
        assertThrows(IllegalArgumentException.class, () -> testWatcher.dumpData("", EMPTY_BYTE_ARRAY));
    }

    @Test
    void dumpDataUsingDirectoryPathThrowsNullPointerExceptionIfThreadDumpIsNull(final @TempDir Path tempDir)
    {
        final DataCollector dataCollector = new DataCollector(tempDir);
        assertThrowsExactly(NullPointerException.class, () -> dataCollector.dumpData("test", null));
    }

    @Test
    void dumpDataUsingDirectoryName(final @TempDir Path tempDir) throws Exception
    {
        testDumpDataUsingDirectoryName(tempDir);
    }

    @Test
    void dumpDataUsingDirectoryNameShouldHandleThreadInterrupt(final @TempDir Path tempDir) throws Exception
    {
        Thread.currentThread().interrupt();

        testDumpDataUsingDirectoryName(tempDir);

        assertTrue(Thread.interrupted());
    }

    @Test
    void dumpDataUsingDirectoryNameIsANoOpIfNoFilesRegistered(final @TempDir Path tempDir)
    {
        final Path rootDirectory = tempDir.resolve("no-copy");
        final DataCollector dataCollector = new DataCollector(rootDirectory);

        assertNull(dataCollector.dumpData("some-dir", EMPTY_BYTE_ARRAY));

        assertFalse(exists(rootDirectory));
    }

    @Test
    void dumpDataUsingDirectoryNameShouldProduceAThreadDump(final @TempDir Path tempDir) throws IOException
    {
        final Path rootDirectory = tempDir.resolve("thread-dump");
        final DataCollector dataCollector = new DataCollector(rootDirectory);
        dataCollector.add(createFile(tempDir.resolve("my.txt")));
        final byte[] threads = new byte[32];
        ThreadLocalRandom.current().nextBytes(threads);

        final Path destination = dataCollector.dumpData("my-out-dir", threads);

        assertNotNull(destination);
        assertTrue(exists(destination));
        assertTrue(exists(destination.resolve("my.txt")));
        final Path threadDump = destination.resolve(THREAD_DUMP_FILE_NAME);
        assertArrayEquals(threads, Files.readAllBytes(threadDump));
    }

    private void testDumpDataUsingDirectoryName(final Path tempDir) throws IOException
    {
        final Path rootDir = tempDir.resolve("copy-root");
        createDirectories(rootDir.resolve("destination"));

        final Path file0 = createFile(tempDir.resolve("my.txt"));
        final Path nonExistingDir = createDirectories(tempDir.resolve("non-existing/folder"));
        final Path file1 = createFile(nonExistingDir.resolve("some.png"));
        final Path dir1 = createDirectories(tempDir.resolve("my-dir/nested/again"));
        final Path dir2 = createDirectories(tempDir.resolve("again"));
        final Path dir3 = createDirectories(tempDir.resolve("nested/again"));
        final Path dir4 = createDirectories(tempDir.resolve("level1/level2/level3/again"));
        createFile(dir1.resolve("file1.txt"));
        createFile(dir2.resolve("file2.txt"));
        createFile(dir3.resolve("file3.txt"));
        createFile(dir4.resolve("file4.txt"));

        final DataCollector dataCollector = new DataCollector(rootDir);
        dataCollector.add(file0);
        dataCollector.add(tempDir.resolve("my-dir"));
        dataCollector.add(tempDir.resolve("again"));
        dataCollector.add(tempDir.resolve("nested"));
        dataCollector.add(dir4);
        dataCollector.add(file1);

        final Path destination = dataCollector.dumpData("destination", EMPTY_BYTE_ARRAY);

        assertNotNull(destination);
        assertTrue(exists(destination));
        assertEquals(rootDir.resolve("destination-" + UNIQUE_ID.get()), destination);
        assertTrue(exists(destination.resolve("my.txt")));
        assertTrue(exists(destination.resolve("my-dir/nested/again/file1.txt")));
        assertTrue(exists(destination.resolve("again/file2.txt")));
        assertTrue(exists(destination.resolve("nested/again/file3.txt")));
        assertTrue(exists(destination.resolve("level1/level2/level3/again/file4.txt")));
        assertTrue(exists(destination.resolve("non-existing/folder/some.png")));
    }
}
