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
package io.aeron.driver.buffer;

import io.aeron.driver.Configuration;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class FileStoreLogFactoryTest
{
    private static final int CREATION_ID = 102;
    private static final File DATA_DIR = new File(SystemUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final long LOW_STORAGE_THRESHOLD = Configuration.LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT;
    private static final int PAGE_SIZE = 4 * 1024;
    private static final boolean PRE_ZERO_LOG = true;
    private static final boolean PERFORM_STORAGE_CHECKS = true;
    private FileStoreLogFactory fileStoreLogFactory;
    private RawLog rawLog;

    @BeforeEach
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        final String absolutePath = DATA_DIR.getAbsolutePath();
        fileStoreLogFactory = new FileStoreLogFactory(
            absolutePath, PAGE_SIZE, PERFORM_STORAGE_CHECKS, LOW_STORAGE_THRESHOLD, mock(ErrorHandler.class));
    }

    @AfterEach
    public void cleanupFiles()
    {
        CloseHelper.close(rawLog);
        CloseHelper.close(fileStoreLogFactory);
        IoUtil.delete(DATA_DIR, false);
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForPublication()
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
    public void shouldCreateCorrectLengthAndZeroedFilesForImage()
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
}
