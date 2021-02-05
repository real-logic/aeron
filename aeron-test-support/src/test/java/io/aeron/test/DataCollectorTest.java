/*
 * Copyright 2014-2021 Real Logic Limited.
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

import org.agrona.IoUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.aeron.test.DataCollector.THREAD_DUMP_FILE_NAME;
import static io.aeron.test.DataCollector.UNIQUE_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.*;
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
        assertThrows(NullPointerException.class, () -> dataCollector.add(null));
    }

    @Test
    void dumpDataUsingTestInfoThrowsNullPointerExceptionIfNull()
    {
        final DataCollector dataCollector = new DataCollector();
        assertThrows(NullPointerException.class, () -> dataCollector.dumpData((TestInfo)null));
    }

    @Test
    void dumpDataUsingDirectoryNameThrowsIllegalArgumentExceptionIfNull()
    {
        final DataCollector dataCollector = new DataCollector();
        assertThrows(IllegalArgumentException.class, () -> dataCollector.dumpData((String)null));
    }

    @Test
    void dumpDataUsingDirectoryNameThrowsIllegalArgumentExceptionIfEmpty()
    {
        final DataCollector testWatcher = new DataCollector();
        assertThrows(IllegalArgumentException.class, () -> testWatcher.dumpData(""));
    }

    @Test
    void dumpDataUsingTestInfo(final @TempDir Path tempDir, final TestInfo testInfo) throws Exception
    {
        final Path buildDir = Paths.get("build/test/source").toAbsolutePath();
        createDirectories(buildDir);

        try
        {
            final Path rootDir = tempDir.resolve("copy-root");
            final Path my1 = createFile(tempDir.resolve("my.txt"));
            final Path myDir1 = createDirectories(tempDir.resolve("my-dir"));
            createFile(myDir1.resolve("some1.txt"));

            final Path dir11 = createDirectories(tempDir.resolve("path1/dir1"));
            final Path dir12 = createDirectories(tempDir.resolve("path1/dir2"));
            final Path dir13 = createDirectories(tempDir.resolve("path1/dir2/dir3"));
            createFile(tempDir.resolve("path1/do_not_copy.txt"));
            createFile(dir11.resolve("file11.txt"));
            createFile(dir12.resolve("file12.txt"));
            createFile(dir13.resolve("file13.txt"));

            final Path my2 = createFile(dir13.resolve("my.txt"));
            final Path dir21 = createDirectories(tempDir.resolve("path2/dir1"));
            final Path dir22 = createDirectories(tempDir.resolve("path2/dir2"));
            final Path dir23 = createDirectories(tempDir.resolve("path2/dir3"));
            createFile(dir21.resolve("file21.txt"));
            createFile(dir22.resolve("file22.txt"));
            createFile(dir23.resolve("file23.txt"));

            final Path dir31 = createDirectories(buildDir.resolve("dir1"));
            final Path dir32 = createDirectories(buildDir.resolve("dir2"));
            final Path dir33 = createDirectories(buildDir.resolve("dir2/dir3"));
            createFile(dir31.resolve("file31.txt"));
            createFile(dir32.resolve("file32.txt"));
            createFile(dir33.resolve("file33.txt"));

            final Path my3 = createFile(buildDir.resolve("my.txt"));
            final Path myDir2 = createDirectories(buildDir.resolve("my-dir"));
            createFile(myDir2.resolve("some2.txt"));

            final DataCollector dataCollector = new DataCollector(rootDir);
            dataCollector.add(my1);
            dataCollector.add(my1);
            dataCollector.add(my2);
            dataCollector.add(dir11);
            dataCollector.add(dir12);
            dataCollector.add(dir13);
            dataCollector.add(dir21);
            dataCollector.add(dir22);
            dataCollector.add(dir23);
            dataCollector.add(dir31);
            dataCollector.add(dir32);
            dataCollector.add(dir33);
            dataCollector.add(my3);
            dataCollector.add(myDir1);
            dataCollector.add(myDir2);

            final Path destination = dataCollector.dumpData(testInfo);

            assertNotNull(destination);
            assertTrue(exists(destination));
            final String testClass = testInfo.getTestClass().orElseThrow(IllegalStateException::new).getName();
            final String testMethod = testInfo.getTestMethod().orElseThrow(IllegalStateException::new).getName();
            assertEquals(rootDir.resolve(testClass + "-" + testMethod), destination);
            assertTrue(exists(destination.resolve("my.txt")));
            assertTrue(exists(destination.resolve("my-dir/some1.txt")));
            assertTrue(exists(destination.resolve("path1/dir1/file11.txt")));
            assertTrue(exists(destination.resolve("path1/dir2/file12.txt")));
            assertTrue(exists(destination.resolve("path1/dir2/dir3/file13.txt")));
            assertTrue(exists(destination.resolve("path1/dir2/dir3/my.txt")));
            assertTrue(exists(destination.resolve("path2/dir1/file21.txt")));
            assertTrue(exists(destination.resolve("path2/dir2/file22.txt")));
            assertTrue(exists(destination.resolve("path2/dir3/file23.txt")));
            assertTrue(exists(destination.resolve("source/dir1/file31.txt")));
            assertTrue(exists(destination.resolve("source/dir2/file32.txt")));
            assertTrue(exists(destination.resolve("source/dir2/dir3/file33.txt")));
            assertTrue(exists(destination.resolve("source/my.txt")));
            assertTrue(exists(destination.resolve("source/my-dir/some2.txt")));
            assertFalse(exists(destination.resolve("path1/do_not_copy.txt")));
        }
        finally
        {
            IoUtil.delete(buildDir.toFile(), false);
        }
    }

    @Test
    void dumpDataUsingDirectoryName(final @TempDir Path tempDir) throws Exception
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

        final Path destination = dataCollector.dumpData("destination");

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

    @Test
    void dumpDataUsingTestInfoIsANoOpIfNoFilesRegistered(final @TempDir Path tempDir, final TestInfo testInfo)
    {
        final Path rootDirectory = tempDir.resolve("no-copy");
        final DataCollector dataCollector = new DataCollector(rootDirectory);

        assertNull(dataCollector.dumpData(testInfo));

        assertFalse(exists(rootDirectory));
    }

    @Test
    void dumpDataUsingDirectoryNameIsANoOpIfNoFilesRegistered(final @TempDir Path tempDir)
    {
        final Path rootDirectory = tempDir.resolve("no-copy");
        final DataCollector dataCollector = new DataCollector(rootDirectory);

        assertNull(dataCollector.dumpData("some-dir"));

        assertFalse(exists(rootDirectory));
    }

    @Test
    void dumpDataUsingTestInfoShouldProduceAThreadDump(
        final @TempDir Path tempDir, final TestInfo testInfo) throws IOException
    {
        final Path rootDirectory = tempDir.resolve("thread-dump");
        final DataCollector dataCollector = new DataCollector(rootDirectory);
        dataCollector.add(createFile(tempDir.resolve("my.txt")));

        final Path destination = dataCollector.dumpData(testInfo);

        assertNotNull(destination);
        assertTrue(exists(destination));
        assertTrue(exists(destination.resolve("my.txt")));
        final Path threadDump = destination.resolve(THREAD_DUMP_FILE_NAME);
        assertTrue(exists(threadDump));
        assertTrue(new String(readAllBytes(threadDump), UTF_8).contains(Thread.currentThread().getName()));
    }

    @Test
    void dumpDataUsingDirectoryNameShouldProduceAThreadDump(final @TempDir Path tempDir) throws IOException
    {
        final Path rootDirectory = tempDir.resolve("thread-dump");
        final DataCollector dataCollector = new DataCollector(rootDirectory);
        dataCollector.add(createFile(tempDir.resolve("my.txt")));

        final Path destination = dataCollector.dumpData("my-out-dir");

        assertNotNull(destination);
        assertTrue(exists(destination));
        assertTrue(exists(destination.resolve("my.txt")));
        final Path threadDump = destination.resolve(THREAD_DUMP_FILE_NAME);
        assertTrue(exists(threadDump));
        assertTrue(new String(readAllBytes(threadDump), UTF_8).contains(Thread.currentThread().getName()));
    }
}
