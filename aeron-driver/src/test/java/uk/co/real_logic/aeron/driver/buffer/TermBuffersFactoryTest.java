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
package uk.co.real_logic.aeron.driver.buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.driver.UdpChannel;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class TermBuffersFactoryTest
{
    private static final String CHANNEL = "udp://localhost:4321";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID = 101;
    private static final int CREATION_ID = 102;
    private static final File DATA_DIR = new File(IoUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_SZ = Configuration.TERM_BUFFER_SZ_DEFAULT;
    private static final int TERM_BUFFER_SZ_MAX = Configuration.TERM_BUFFER_SZ_MAX_DEFAULT;
    private TermBuffersFactory termBuffersFactory;
    private UdpChannel udpChannel = UdpChannel.parse(CHANNEL);
    private EventLogger logger = mock(EventLogger.class);

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        termBuffersFactory =
            new TermBuffersFactory(DATA_DIR.getAbsolutePath(), TERM_BUFFER_SZ, TERM_BUFFER_SZ_MAX, logger);
    }

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(DATA_DIR, true);
    }

    @Test
    public void shouldCreateCorrectSizeAndZeroedFilesForPublication() throws Exception
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final TermBuffers termBuffers =
            termBuffersFactory.newPublication(canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID);

        termBuffers.stream().forEach(
            (rawLog) ->
            {
                final UnsafeBuffer log = rawLog.logBuffer();

                assertThat(log.capacity(), is(TERM_BUFFER_SZ));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte(TERM_BUFFER_SZ - 1), is((byte)0));

                final UnsafeBuffer state = rawLog.stateBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.STATE_BUFFER_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.STATE_BUFFER_LENGTH - 1), is((byte)0));
            });
    }

    @Test
    public void shouldCreateCorrectSizeAndZeroedFilesForConnection() throws Exception
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int maxConnectionTermBufferSize = TERM_BUFFER_SZ / 2;
        final TermBuffers termBuffers =
            termBuffersFactory.newConnection(
                canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, maxConnectionTermBufferSize);

        termBuffers.stream().forEach(
            (rawLog) ->
            {
                final UnsafeBuffer log = rawLog.logBuffer();

                assertThat(log.capacity(), is(maxConnectionTermBufferSize));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte(maxConnectionTermBufferSize - 1), is((byte)0));

                final UnsafeBuffer state = rawLog.stateBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.STATE_BUFFER_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.STATE_BUFFER_LENGTH - 1), is((byte)0));
            });
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionIfRequestedTermBufferSizeGreaterThanMax()
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int maxConnectionTermBufferSize = TERM_BUFFER_SZ_MAX * 2;
        termBuffersFactory.newConnection(
            canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, maxConnectionTermBufferSize);
    }
}
