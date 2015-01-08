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
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.driver.UdpChannel;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RawLogFragmentFactoryTest
{
    private static final String CHANNEL = "udp://localhost:4321";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID = 101;
    private static final int CREATION_ID = 102;
    private static final File DATA_DIR = new File(IoUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final int TERM_BUFFER_MAX_LENGTH = Configuration.TERM_BUFFER_LENGTH_MAX_DEFAULT;
    private RawLogFactory rawLogFactory;
    private UdpChannel udpChannel = UdpChannel.parse(CHANNEL);
    private EventLogger logger = mock(EventLogger.class);

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        rawLogFactory = new RawLogFactory(DATA_DIR.getAbsolutePath(), TERM_BUFFER_LENGTH, TERM_BUFFER_MAX_LENGTH, logger);
    }

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(DATA_DIR, true);
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForPublication() throws Exception
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final RawLog rawLogTriplet = rawLogFactory.newPublication(canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID);

        rawLogTriplet.stream().forEach(
            (rawLogFragment) ->
            {
                final UnsafeBuffer log = rawLogFragment.termBuffer();

                assertThat(log.capacity(), is(TERM_BUFFER_LENGTH));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte(TERM_BUFFER_LENGTH - 1), is((byte)0));

                final UnsafeBuffer state = rawLogFragment.metaDataBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.TERM_META_DATA_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.TERM_META_DATA_LENGTH - 1), is((byte)0));
            });
    }

    @Test
    public void shouldCreateCorrectLengthAndZeroedFilesForConnection() throws Exception
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int maxConnectionTermBufferSize = TERM_BUFFER_LENGTH / 2;
        final RawLog rawLogTriplet = rawLogFactory.newConnection(
            canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, maxConnectionTermBufferSize);

        rawLogTriplet.stream().forEach(
            (rawLogFragment) ->
            {
                final UnsafeBuffer log = rawLogFragment.termBuffer();

                assertThat(log.capacity(), is(maxConnectionTermBufferSize));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte(maxConnectionTermBufferSize - 1), is((byte)0));

                final UnsafeBuffer state = rawLogFragment.metaDataBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.TERM_META_DATA_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.TERM_META_DATA_LENGTH - 1), is((byte)0));
            });
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionIfRequestedTermBufferLengthGreaterThanMax()
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int connectionTermBufferMaxLength = TERM_BUFFER_MAX_LENGTH * 2;
        rawLogFactory.newConnection(canonicalForm, SESSION_ID, STREAM_ID, CREATION_ID, connectionTermBufferMaxLength);
    }
}
