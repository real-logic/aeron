/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import org.junit.*;

import java.io.*;

import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
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

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        final String absolutePath = DATA_DIR.getAbsolutePath();
        fileStoreLogFactory = new FileStoreLogFactory(
            absolutePath, PAGE_SIZE, PERFORM_STORAGE_CHECKS, LOW_STORAGE_THRESHOLD, mock(ErrorHandler.class));
    }

    @After
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

        assertThat(rawLog.termLength(), is(TERM_BUFFER_LENGTH));

        final UnsafeBuffer[] termBuffers = rawLog.termBuffers();
        assertThat(termBuffers.length, is(PARTITION_COUNT));

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            assertThat(termBuffer.capacity(), is(TERM_BUFFER_LENGTH));
            assertThat(termBuffer.getByte(0), is((byte)0));
            assertThat(termBuffer.getByte(TERM_BUFFER_LENGTH - 1), is((byte)0));
        }

        final UnsafeBuffer metaData = rawLog.metaData();

        assertThat(metaData.capacity(), is(LogBufferDescriptor.LOG_META_DATA_LENGTH));
        assertThat(metaData.getByte(0), is((byte)0));
        assertThat(metaData.getByte(LogBufferDescriptor.LOG_META_DATA_LENGTH - 1), is((byte)0));
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForImage()
    {
        final int imageTermBufferLength = TERM_BUFFER_LENGTH / 2;
        rawLog = fileStoreLogFactory.newImage(CREATION_ID, imageTermBufferLength, PRE_ZERO_LOG);

        assertThat(rawLog.termLength(), is(imageTermBufferLength));

        final UnsafeBuffer[] termBuffers = rawLog.termBuffers();
        assertThat(termBuffers.length, is(PARTITION_COUNT));

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            assertThat(termBuffer.capacity(), is(imageTermBufferLength));
            assertThat(termBuffer.getByte(0), is((byte)0));
            assertThat(termBuffer.getByte(imageTermBufferLength - 1), is((byte)0));
        }

        final UnsafeBuffer metaData = rawLog.metaData();

        assertThat(metaData.capacity(), is(LogBufferDescriptor.LOG_META_DATA_LENGTH));
        assertThat(metaData.getByte(0), is((byte)0));
        assertThat(metaData.getByte(LogBufferDescriptor.LOG_META_DATA_LENGTH - 1), is((byte)0));
    }
}
