/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class LogBuffersTest
{
    @ParameterizedTest
    @ValueSource(ints = { -100, 0, TERM_MIN_LENGTH >> 1, TERM_MAX_LENGTH + 1, TERM_MAX_LENGTH - 1 })
    void throwsIllegalStateExceptionIfTermLengthIsInvalid(final int termLength, @TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final byte[] contents = new byte[LOG_META_DATA_LENGTH];
        final UnsafeBuffer buffer = new UnsafeBuffer(contents);
        termLength(buffer, termLength);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), allOf(startsWith("Term length"), endsWith("length=" + termLength)));
    }

    @ParameterizedTest
    @ValueSource(ints = { -100, 0, PAGE_MIN_SIZE >> 1, PAGE_MAX_SIZE + 1, PAGE_MAX_SIZE - 1 })
    void throwsIllegalStateExceptionIfPageSizeIsInvalid(final int pageSize, @TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final byte[] contents = new byte[LOG_META_DATA_LENGTH];
        final UnsafeBuffer buffer = new UnsafeBuffer(contents);
        termLength(buffer, TERM_MIN_LENGTH);
        pageSize(buffer, pageSize);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), allOf(startsWith("Page size"), endsWith("page size=" + pageSize)));
    }

    @Test
    void throwsIllegalStateExceptionIfLogFileSizeIsLessThanLogMetaDataLength(@TempDir final Path dir)
        throws IOException
    {
        final Path logFile = dir.resolve("test.log");
        assertNotNull(Files.createFile(logFile));
        final int fileLength = LOG_META_DATA_LENGTH - 5;
        final byte[] contents = new byte[fileLength];
        final UnsafeBuffer buffer = new UnsafeBuffer(contents);
        termLength(buffer, TERM_MIN_LENGTH);
        pageSize(buffer, PAGE_MIN_SIZE);
        assertNotNull(Files.write(logFile, contents));
        assertEquals(contents.length, Files.size(logFile));

        final IllegalStateException exception = assertThrowsExactly(
            IllegalStateException.class, () -> new LogBuffers(logFile.toAbsolutePath().toString()));
        assertEquals("Log file length less than min length of " + LOG_META_DATA_LENGTH + ": length=" + fileLength,
            exception.getMessage());
    }

    @Test
    void mapExistingFile(@TempDir final Path dir) throws IOException
    {
        final int termLength = TERM_MIN_LENGTH;
        final int pageSize = PAGE_MIN_SIZE;
        final Path logFile = createLogFile(dir, termLength, pageSize);

        try (LogBuffers logBuffers = new LogBuffers(logFile.toString()))
        {
            final UnsafeBuffer metaDataBuffer = logBuffers.metaDataBuffer();
            assertNotNull(metaDataBuffer);
            assertEquals(termLength, termLength(metaDataBuffer));
            assertEquals(termLength, logBuffers.termLength());
            assertEquals(pageSize, pageSize(metaDataBuffer));
        }
    }

    @Test
    void mapFileAndCaptureMappedSize() throws IOException
    {
        final Path logFile = createLogFile(Files.createTempDirectory("test"), TERM_MAX_LENGTH, PAGE_MIN_SIZE * 4);
        final long logFileSize = Files.size(logFile);

        final AtomicCounter mappedBytesCounter = mock(AtomicCounter.class);
        final LogBuffers logBuffers = new LogBuffers(logFile.toString(), mappedBytesCounter);

        for (int i = 0; i < 5; i++)
        {
            logBuffers.duplicateTermBuffers();
        }

        logBuffers.close();

        final InOrder inOrder = inOrder(mappedBytesCounter);
        inOrder.verify(mappedBytesCounter).getAndAdd(logFileSize);
        inOrder.verify(mappedBytesCounter).getAndAdd(-logFileSize);
        inOrder.verifyNoMoreInteractions();
    }

    private static Path createLogFile(final Path dir, final int termLength, final int pageSize) throws IOException
    {
        final long logFileSize = (long)termLength * PARTITION_COUNT + LOG_META_DATA_LENGTH;
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH));
        termLength(buffer, termLength);
        pageSize(buffer, pageSize);
        final Path logFile = dir.resolve("some.log");
        try (FileChannel fileChannel =
            FileChannel.open(logFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW))
        {
            final ByteBuffer byteBuffer = buffer.byteBuffer();
            byteBuffer.position(0).limit(LOG_META_DATA_LENGTH);

            long position = logFileSize - LOG_META_DATA_LENGTH;
            do
            {
                position += fileChannel.write(byteBuffer, position);
            }
            while (byteBuffer.remaining() > 0);
        }

        assertEquals(logFileSize, Files.size(logFile));
        return logFile;
    }
}
