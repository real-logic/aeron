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

import org.junit.*;
import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.UdpDestination;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TermBuffersFactoryTest
{
    private static final String DESTINATION_URI = "udp://localhost:4321";
    private static final long SESSION_ID = 100;
    private static final long CHANNEL_ID = 100;
    private static final File DATA_DIR = new File(IoUtil.tmpDirName(), "dataDirName");
    private static final int TERM_BUFFER_SZ = MediaDriver.TERM_BUFFER_SZ_DEFAULT;
    private TermBuffersFactory termBuffersFactory;
    private UdpDestination destination = UdpDestination.parse(DESTINATION_URI);

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        termBuffersFactory = new TermBuffersFactory(DATA_DIR.getAbsolutePath(), TERM_BUFFER_SZ);
    }

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(DATA_DIR, true);
    }

    @Test
    public void mappedFilesAreCorrectSizeAndZeroed() throws Exception
    {
        final TermBuffers termBuffers = termBuffersFactory.newPublication(destination, SESSION_ID, CHANNEL_ID);

        termBuffers.stream().forEach(
            (rawLog) ->
            {
                final AtomicBuffer log = rawLog.logBuffer();

                assertThat(log.capacity(), is(TERM_BUFFER_SZ));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte(TERM_BUFFER_SZ - 1), is((byte)0));

                final AtomicBuffer state = rawLog.stateBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.STATE_BUFFER_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.STATE_BUFFER_LENGTH - 1), is((byte)0));
            });
    }
}
