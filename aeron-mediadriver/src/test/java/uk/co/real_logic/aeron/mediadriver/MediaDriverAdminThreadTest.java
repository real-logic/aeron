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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.buffer.BasicBufferManagementStrategy;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_CHANNEL;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.REMOVE_CHANNEL;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;

public class MediaDriverAdminThreadTest
{

    private static final String ADMIN_DIR = "adminDir";
    private static final String DESTINATION = "udp://localhost:40124@localhost:40123";

    private static String adminPath;

    @BeforeClass
    public static void setupDirectories() throws IOException
    {
        final File adminDir = new File(System.getProperty("java.io.tmpdir"), ADMIN_DIR);
        if (adminDir.exists())
        {
            IoUtil.delete(adminDir, false);
        }
        IoUtil.ensureDirectoryExists(adminDir, ADMIN_DIR);
        adminPath = adminDir.getAbsolutePath();
    }

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private MediaDriverAdminThread mediaDriverAdminThread;
    private List<SenderChannel> addedChannels = new ArrayList<>();
    private List<SenderChannel> removedChannels = new ArrayList<>();

    @Before
    public void setUp()
    {
        final MediaDriver.TopologyBuilder builder = new MediaDriver.TopologyBuilder()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .adminBufferStrategy(new CreatingAdminBufferStrategy(adminPath, COMMAND_BUFFER_SZ + TRAILER_SIZE))
                .bufferManagementStrategy(new BasicBufferManagementStrategy(adminPath));

        SenderThread senderThread = new SenderThread(builder) {
            @Override
            public void addChannel(final SenderChannel channel)
            {
                addedChannels.add(channel);
            }

            @Override
            public void removeChannel(final SenderChannel channel)
            {
                removedChannels.add(channel);
            }
        };
        ReceiverThread receiverThread = mock(ReceiverThread.class);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
    }

    @Test
    public void addingChannelShouldNotifySenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L);

        mediaDriverAdminThread.process();

        assertThat(addedChannels, hasSize(1));
        assertChannelHas(addedChannels.get(0), 1L, 2L);
    }

    private void assertChannelHas(final SenderChannel channel, final long channelId, final long sessionId)
    {
        assertThat(channel, notNullValue());
        assertThat(channel.channelId(), is(channelId));
        assertThat(channel.sessionId(), is(sessionId));
    }

    @Test
    public void addingMultipleChannelsNotifiesSenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L);
        writeChannelMessage(ADD_CHANNEL, 1L, 3L);
        writeChannelMessage(ADD_CHANNEL, 3L, 2L);
        writeChannelMessage(ADD_CHANNEL, 3L, 4L);

        mediaDriverAdminThread.process();

        assertThat(addedChannels, hasSize(4));
        assertChannelHas(addedChannels.get(0), 1L, 2L);
        assertChannelHas(addedChannels.get(1), 1L, 3L);
        assertChannelHas(addedChannels.get(2), 3L, 2L);
        assertChannelHas(addedChannels.get(3), 3L, 4L);
    }

    @Test
    public void removingChannelShouldNotifySenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L);
        writeChannelMessage(REMOVE_CHANNEL, 1L, 2L);

        mediaDriverAdminThread.process();

        assertThat(removedChannels, is(addedChannels));
    }

    @Test
    public void removingMultipleChannelsNotifiesSenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L);
        writeChannelMessage(ADD_CHANNEL, 1L, 3L);
        writeChannelMessage(ADD_CHANNEL, 3L, 2L);
        writeChannelMessage(ADD_CHANNEL, 3L, 4L);

        writeChannelMessage(REMOVE_CHANNEL, 1L, 2L);
        writeChannelMessage(REMOVE_CHANNEL, 1L, 3L);
        writeChannelMessage(REMOVE_CHANNEL, 3L, 2L);
        writeChannelMessage(REMOVE_CHANNEL, 3L, 4L);

        mediaDriverAdminThread.process();

        assertThat(removedChannels, is(addedChannels));
    }

    private void writeChannelMessage(final int eventTypeId, final long channelId, final long sessionId)
            throws IOException
    {
        final ByteBuffer buffer = new MappingAdminBufferStrategy(adminPath).toMediaDriver();
        final RingBuffer adminCommands = new ManyToOneRingBuffer(new AtomicBuffer(buffer));

        final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
        channelMessage.reset(writeBuffer, 0);
        channelMessage.channelId(channelId);
        channelMessage.sessionId(sessionId);
        channelMessage.destination(DESTINATION);

        adminCommands.write(eventTypeId, writeBuffer, 0, channelMessage.length());
    }

}
