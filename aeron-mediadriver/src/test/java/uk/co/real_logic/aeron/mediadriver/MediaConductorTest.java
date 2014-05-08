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

import org.junit.*;
import uk.co.real_logic.aeron.util.CreatingConductorBufferStrategy;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.MappingConductorBufferStrategy;
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
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy.newMappedBufferManager;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_CHANNEL;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.REMOVE_CHANNEL;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

public class MediaConductorTest
{
    private static final String ADMIN_DIR = "adminDir";
    private static final String DESTINATION = "udp://localhost:";

    private static String adminPath;

    @BeforeClass
    public static void setupDirectories() throws IOException
    {
        final File adminDir = new File(IoUtil.tmpDir(), ADMIN_DIR);
        if (adminDir.exists())
        {
            IoUtil.delete(adminDir, false);
        }

        IoUtil.ensureDirectoryExists(adminDir, ADMIN_DIR);
        adminPath = adminDir.getAbsolutePath();
    }

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private MediaConductor mediaConductor;
    private List<SenderChannel> addedChannels = new ArrayList<>();
    private List<SenderChannel> removedChannels = new ArrayList<>();

    @Before
    public void setUp()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
            .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
            .rcvNioSelector(new NioSelector())
            .adminNioSelector(new NioSelector())
            .senderFlowControl(DefaultSenderControlStrategy::new)
            .adminBufferStrategy(new CreatingConductorBufferStrategy(adminPath, COMMAND_BUFFER_SZ + TRAILER_LENGTH))
            .bufferManagementStrategy(newMappedBufferManager(adminPath));

        final Sender sender = new Sender(ctx)
        {
            public void addChannel(final SenderChannel channel)
            {
                addedChannels.add(channel);
            }

            public void removeChannel(final SenderChannel channel)
            {
                removedChannels.add(channel);
            }
        };

        final Receiver receiver = mock(Receiver.class);
        mediaConductor = new MediaConductor(ctx, receiver, sender);
    }

    @After
    public void teardown()
    {
        mediaConductor.close();
    }

    @Test
    public void addingChannelShouldNotifySenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L, 4000);

        mediaConductor.process();

        assertThat(addedChannels, hasSize(1));
        assertChannelHas(addedChannels.get(0), 1L, 2L);
        assertAddedChannelsWereConnected();
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
        writeChannelMessage(ADD_CHANNEL, 1L, 2L, 4001);
        writeChannelMessage(ADD_CHANNEL, 1L, 3L, 4002);
        writeChannelMessage(ADD_CHANNEL, 3L, 2L, 4003);
        writeChannelMessage(ADD_CHANNEL, 3L, 4L, 4004);

        mediaConductor.process();

        assertThat(addedChannels, hasSize(4));
        assertChannelHas(addedChannels.get(0), 1L, 2L);
        assertChannelHas(addedChannels.get(1), 1L, 3L);
        assertChannelHas(addedChannels.get(2), 3L, 2L);
        assertChannelHas(addedChannels.get(3), 3L, 4L);

        assertAddedChannelsWereConnected();
    }

    @Test
    public void removingChannelShouldNotifySenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L, 4005);
        writeChannelMessage(REMOVE_CHANNEL, 1L, 2L, 4005);

        mediaConductor.process();

        assertThat(removedChannels, is(addedChannels));
        assertRemovedChannelsWereClosed();
    }

    @Test
    public void removingMultipleChannelsNotifiesSenderThread() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(ADD_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(ADD_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(ADD_CHANNEL, 3L, 4L, 4008);

        writeChannelMessage(REMOVE_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(REMOVE_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(REMOVE_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(REMOVE_CHANNEL, 3L, 4L, 4008);

        mediaConductor.process();

        assertThat(removedChannels, is(addedChannels));
        assertRemovedChannelsWereClosed();
    }

    private void assertRemovedChannelsWereClosed()
    {
        removedChannels.forEach(channel -> assertFalse("Channel wasn't closed", channel.isOpen()));
    }

    private void assertAddedChannelsWereConnected()
    {
        addedChannels.forEach(channel -> assertTrue("Channel isn't open", channel.isOpen()));
    }

    private void writeChannelMessage(final int eventTypeId, final long channelId, final long sessionId, final int port)
        throws IOException
    {
        final ByteBuffer buffer = new MappingConductorBufferStrategy(adminPath).toMediaDriver();
        final RingBuffer adminCommands = new ManyToOneRingBuffer(new AtomicBuffer(buffer));

        final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
        channelMessage.wrap(writeBuffer, 0);
        channelMessage.channelId(channelId);
        channelMessage.sessionId(sessionId);
        channelMessage.destination(DESTINATION + port);

        adminCommands.write(eventTypeId, writeBuffer, 0, channelMessage.length());
    }
}
