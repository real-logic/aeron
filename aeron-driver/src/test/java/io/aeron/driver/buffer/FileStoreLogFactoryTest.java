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
package io.aeron.driver.buffer;

import io.aeron.driver.Configuration;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.StorageSpaceException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.List;

import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.computeLogLength;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FileStoreLogFactoryTest
{
    private static final int CREATION_ID = 102;
    private static final File DATA_DIR = new File(SystemUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final long LOW_STORAGE_THRESHOLD = Configuration.LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT;
    private static final int PAGE_SIZE = 4 * 1024;
    private static final boolean PRE_ZERO_LOG = true;
    private static final boolean PERFORM_STORAGE_CHECKS = true;
    private final AtomicCounter mockBytesMappedCounter = mock(AtomicCounter.class);
    private FileStoreLogFactory fileStoreLogFactory;
    private RawLog rawLog;

    @BeforeEach
    void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        final String absolutePath = DATA_DIR.getAbsolutePath();
        fileStoreLogFactory = new FileStoreLogFactory(
            absolutePath,
            PAGE_SIZE,
            PERFORM_STORAGE_CHECKS,
            LOW_STORAGE_THRESHOLD,
            mock(ErrorHandler.class),
            mockBytesMappedCounter);
    }

    @AfterEach
    void cleanupFiles()
    {
        CloseHelper.close(rawLog);
        CloseHelper.close(fileStoreLogFactory);
        IoUtil.delete(DATA_DIR, false);
    }

    @Test
    void shouldCreateCorrectLengthAndZeroedFilesForPublication()
    {
        rawLog = fileStoreLogFactory.newPublication(CREATION_ID, TERM_BUFFER_LENGTH, PRE_ZERO_LOG);

        assertEquals(TERM_BUFFER_LENGTH, rawLog.termLength());

        final UnsafeBuffer[] termBuffers = rawLog.termBuffers();
        assertEquals(PARTITION_COUNT, termBuffers.length);

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            assertEquals(TERM_BUFFER_LENGTH, termBuffer.capacity());
            assertEquals(0, termBuffer.getByte(0));
            assertEquals(0, termBuffer.getByte(TERM_BUFFER_LENGTH - 1));
        }

        final UnsafeBuffer metaData = rawLog.metaData();

        assertEquals(LogBufferDescriptor.LOG_META_DATA_LENGTH, metaData.capacity());
        assertEquals(0, metaData.getByte(0));
        assertEquals(0, metaData.getByte(LogBufferDescriptor.LOG_META_DATA_LENGTH - 1));
    }

    @Test
    void shouldCreateCorrectLengthAndZeroedFilesForImage()
    {
        final int imageTermBufferLength = TERM_BUFFER_LENGTH / 2;
        rawLog = fileStoreLogFactory.newImage(CREATION_ID, imageTermBufferLength, PRE_ZERO_LOG);

        assertEquals(imageTermBufferLength, rawLog.termLength());

        final UnsafeBuffer[] termBuffers = rawLog.termBuffers();
        assertEquals(PARTITION_COUNT, termBuffers.length);

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            assertEquals(imageTermBufferLength, termBuffer.capacity());
            assertEquals(0, termBuffer.getByte(0));
            assertEquals(0, termBuffer.getByte(imageTermBufferLength - 1));
        }

        final UnsafeBuffer metaData = rawLog.metaData();

        assertEquals(LogBufferDescriptor.LOG_META_DATA_LENGTH, metaData.capacity());
        assertEquals(0, metaData.getByte(0));
        assertEquals(0, metaData.getByte(LogBufferDescriptor.LOG_META_DATA_LENGTH - 1));
    }

    @Test
    void shouldThrowInsufficientUsableStorageExceptionIfNotEnoughSpaceOnDisc() throws IOException
    {
        final FileStore fileStore = mock(FileStore.class);
        final long usableSpace = 117L;
        when(fileStore.getUsableSpace()).thenReturn(usableSpace);
        when(fileStore.toString()).thenReturn("test-fs");
        final ErrorHandler errorHandler = mock(ErrorHandler.class);

        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class))
        {
            files.when(() -> Files.getFileStore(any())).thenReturn(fileStore);

            try (FileStoreLogFactory logFactory = new FileStoreLogFactory(
                DATA_DIR.getAbsolutePath(),
                PAGE_SIZE, true,
                LOW_STORAGE_THRESHOLD,
                errorHandler,
                mockBytesMappedCounter))
            {
                final int imageTermBufferLength = 64 * 1024;
                assertThrowsStorageSpaceException(
                    fileStore,
                    imageTermBufferLength,
                    usableSpace,
                    () -> logFactory.newImage(1, imageTermBufferLength, true));

                final int publicationTermBufferLength = 1024 * 1024;
                assertThrowsStorageSpaceException(
                    fileStore,
                    publicationTermBufferLength,
                    usableSpace,
                    () -> logFactory.newPublication(2, publicationTermBufferLength, false));

            }
        }

        verifyNoInteractions(errorHandler);
    }

    @Test
    void shouldWarnAboutLowStorageSpace() throws IOException
    {
        final int termLength = 64 * 1024;
        final long logLength = computeLogLength(termLength, PAGE_SIZE);
        final long lowStorageWarningThreshold = logLength * 3;
        final FileStore fileStore = mock(FileStore.class);
        when(fileStore.getUsableSpace()).thenReturn(logLength * 3, logLength);
        when(fileStore.toString()).thenReturn("test-fs");
        final ErrorHandler errorHandler = mock(ErrorHandler.class);

        try (MockedStatic<Files> files = Mockito.mockStatic(Files.class))
        {
            files.when(() -> Files.getFileStore(any())).thenReturn(fileStore);

            try (FileStoreLogFactory logFactory = new FileStoreLogFactory(
                DATA_DIR.getAbsolutePath(),
                PAGE_SIZE,
                true,
                lowStorageWarningThreshold,
                errorHandler,
                mockBytesMappedCounter))
            {
                try (RawLog rawLog = logFactory.newPublication(11, termLength, true))
                {
                    assertNotNull(rawLog);
                }
                try (RawLog rawLog = logFactory.newImage(2222, termLength, false))
                {
                    assertNotNull(rawLog);
                }
            }
        }

        final ArgumentCaptor<AeronException> exceptionArgumentCaptor = ArgumentCaptor.forClass(AeronException.class);
        verify(errorHandler, times(2)).onError(exceptionArgumentCaptor.capture());
        final List<AeronException> exceptions = exceptionArgumentCaptor.getAllValues();
        for (int i = 0; i < 2; i++)
        {
            final long usableSpace = 0 == i ? logLength * 3 : logLength;
            final AeronException exception = exceptions.get(i);
            assertEquals(AeronException.Category.WARN, exception.category());
            assertEquals("WARN - space is running low: threshold=" + lowStorageWarningThreshold +
                " usable=" + usableSpace + " in " + fileStore, exception.getMessage());
        }
        verifyNoMoreInteractions(errorHandler);
    }

    private static void assertThrowsStorageSpaceException(
        final FileStore fileStore, final int termBufferLength, final long usableSpace, final Executable executable)
    {
        final StorageSpaceException exception = assertThrowsExactly(StorageSpaceException.class, executable);
        assertEquals(
            "ERROR - insufficient usable storage for new log of length=" +
            computeLogLength(termBufferLength, PAGE_SIZE) + " usable=" + usableSpace + " in " + fileStore,
            exception.getMessage());
    }
}
