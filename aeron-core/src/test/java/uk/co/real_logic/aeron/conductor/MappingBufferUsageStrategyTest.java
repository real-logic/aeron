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
package uk.co.real_logic.aeron.conductor;

import org.junit.ClassRule;
import org.junit.Test;
import uk.co.real_logic.aeron.util.SharedDirectories;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MappingBufferUsageStrategyTest
{
    private static final String DESTINATION = "udp://localhost:1234";
    private static final long SESSION_ID = 1L;
    private static final long CHANNEL_ID = 2L;
    private static final long OTHER_CHANNEL_ID = 3L;

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final BufferUsageStrategy usageStrategy = new MappingBufferUsageStrategy(directory.dataDir());

    @Test
    public void testInitiallyNoBuffersToRelease()
    {
        assertBuffersReleased(0);
    }

    @Test
    public void mappedBuffersShouldBeReleased() throws IOException
    {
        createTermBuffers(CHANNEL_ID);

        mapTermBuffers(CHANNEL_ID);

        assertBuffersReleased(3);
    }

    @Test
    public void mappedBuffersShouldOnlyBeReleasedOnce() throws IOException
    {
        createTermBuffers(CHANNEL_ID);

        mapTermBuffers(CHANNEL_ID);

        assertBuffersReleased(3);
        assertBuffersReleased(0);
    }

    @Test
    public void onlyRelevantMappedBuffersShouldBeReleased() throws IOException
    {
        createTermBuffers(CHANNEL_ID);
        createTermBuffers(OTHER_CHANNEL_ID);

        mapTermBuffers(CHANNEL_ID);
        mapTermBuffers(OTHER_CHANNEL_ID);

        assertBuffersReleased(3);
    }

    private void assertBuffersReleased(final int count)
    {
        int buffersReleased = usageStrategy.releaseSenderBuffers(DESTINATION, SESSION_ID, CHANNEL_ID);
        assertThat(buffersReleased, is(count));
    }

    private void mapTermBuffers(final long channelId) throws IOException
    {
        usageStrategy.newSenderLogBuffer(DESTINATION, SESSION_ID, channelId, 0);
        usageStrategy.newSenderLogBuffer(DESTINATION, SESSION_ID, channelId, 1);
        usageStrategy.newSenderLogBuffer(DESTINATION, SESSION_ID, channelId, 2);
    }

    private void createTermBuffers(final long channelId) throws IOException
    {
        directory.createTermFile(directory.senderDir(), DESTINATION, SESSION_ID, channelId);
    }
}