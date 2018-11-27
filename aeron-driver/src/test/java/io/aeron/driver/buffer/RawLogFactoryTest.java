/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.buffer;

import io.aeron.driver.Configuration;
import io.aeron.driver.media.UdpChannel;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;

import java.io.*;

import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RawLogFactoryTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:4321";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID = 101;
    private static final int CREATION_ID = 102;
    private static final File DATA_DIR = new File(IoUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final int PAGE_SIZE = 4 * 1024;
    private static final boolean PRE_ZERO_LOG = false;
    private static final boolean PERFORM_STORAGE_CHECKS = false;
    private RawLogFactory rawLogFactory;
    private final UdpChannel udpChannel = UdpChannel.parse(CHANNEL);

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        rawLogFactory = new RawLogFactory(
            DATA_DIR.getAbsolutePath(), PAGE_SIZE, PERFORM_STORAGE_CHECKS, mock(ErrorHandler.class));
    }

    @After
    public void cleanupFiles()
    {
        IoUtil.delete(DATA_DIR, false);
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForPublication()
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final RawLog rawLog = rawLogFactory.newNetworkPublication(
            canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, TERM_BUFFER_LENGTH, PRE_ZERO_LOG);

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

        rawLog.close();
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForImage()
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int imageTermBufferMaxLength = TERM_BUFFER_LENGTH / 2;
        final RawLog rawLog = rawLogFactory.newNetworkedImage(
            canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, imageTermBufferMaxLength, PRE_ZERO_LOG);

        assertThat(rawLog.termLength(), is(imageTermBufferMaxLength));

        final UnsafeBuffer[] termBuffers = rawLog.termBuffers();
        assertThat(termBuffers.length, is(PARTITION_COUNT));

        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            assertThat(termBuffer.capacity(), is(imageTermBufferMaxLength));
            assertThat(termBuffer.getByte(0), is((byte)0));
            assertThat(termBuffer.getByte(imageTermBufferMaxLength - 1), is((byte)0));
        }

        final UnsafeBuffer metaData = rawLog.metaData();

        assertThat(metaData.capacity(), is(LogBufferDescriptor.LOG_META_DATA_LENGTH));
        assertThat(metaData.getByte(0), is((byte)0));
        assertThat(metaData.getByte(LogBufferDescriptor.LOG_META_DATA_LENGTH - 1), is((byte)0));

        rawLog.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionIfRequestedTermBufferLengthGreaterThanMax()
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int imageTermBufferMaxLength = TERM_MAX_LENGTH + 1;
        rawLogFactory.newNetworkedImage(
            canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, imageTermBufferMaxLength, PRE_ZERO_LOG);
    }
}
