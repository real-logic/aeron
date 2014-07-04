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
package uk.co.real_logic.aeron.mediadriver.buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.UdpDestination;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MappedBufferManagementTest
{
    private static final String DESTINATION_URI = "udp://localhost:4321";
    private static final long SESSION_ID = 100;
    private static final long CHANNEL_ID = 100;
    private static final File DATA_DIR = new File(IoUtil.tmpDirName(), "dataDirName");
    private MappedBufferManagement bufferManagement;
    private UdpDestination destination = UdpDestination.parse(DESTINATION_URI);

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
        bufferManagement = new MappedBufferManagement(DATA_DIR.getAbsolutePath());
    }

    @After
    public void cleanupFiles() throws IOException
    {
        bufferManagement.close();
        IoUtil.delete(DATA_DIR, true);
    }

    @Test
    public void mappedFilesAreCorrectSizeAndZeroed() throws Exception
    {
        final BufferRotator rotator = bufferManagement.addPublication(destination, SESSION_ID, CHANNEL_ID);

        rotator.buffers().forEach(
            (logBuffer) ->
            {
                final AtomicBuffer log = logBuffer.logBuffer();

                assertThat((long)log.capacity(), is(MappedBufferManagement.LOG_BUFFER_SIZE));
                assertThat(log.getByte(0), is((byte)0));
                assertThat(log.getByte((int)MappedBufferManagement.LOG_BUFFER_SIZE - 1), is((byte)0));

                final AtomicBuffer state = logBuffer.stateBuffer();

                assertThat(state.capacity(), is(LogBufferDescriptor.STATE_BUFFER_LENGTH));
                assertThat(state.getByte(0), is((byte)0));
                assertThat(state.getByte(LogBufferDescriptor.STATE_BUFFER_LENGTH - 1), is((byte)0));
            }
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionWhenRemovingUnknownPublisherChannel() throws Exception
    {
        bufferManagement.removePublication(destination, SESSION_ID, CHANNEL_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionWhenRemovingUnknownSubscriberChannel() throws Exception
    {
        bufferManagement.removeConnectedSubscription(destination, SESSION_ID, CHANNEL_ID);
    }

    @Test
    public void shouldBeAbleToAddAndRemovePublisherChannel() throws Exception
    {
        bufferManagement.addPublication(destination, SESSION_ID, CHANNEL_ID);
        bufferManagement.removePublication(destination, SESSION_ID, CHANNEL_ID);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSubscriberChannel() throws Exception
    {
        bufferManagement.addConnectedSubscription(destination, SESSION_ID, CHANNEL_ID);
        bufferManagement.removeConnectedSubscription(destination, SESSION_ID, CHANNEL_ID);
    }
}
