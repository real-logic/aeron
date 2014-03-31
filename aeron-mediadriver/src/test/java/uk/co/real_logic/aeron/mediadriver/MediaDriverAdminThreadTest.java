package uk.co.real_logic.aeron.mediadriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_CHANNEL;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;

public class MediaDriverAdminThreadTest
{

    private static final String adminPath;
    private static final String ADMIN_DIR = "adminDir";
    private static final String DESTINATION = "udp://localhost:40124@localhost:40123";

    static
    {
        final File adminDir = new File(System.getProperty("java.io.tmpdir"), ADMIN_DIR);
        IoUtil.ensureDirectoryExists(adminDir, ADMIN_DIR);
        adminPath = adminDir.getAbsolutePath();
    }

    final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private MediaDriverAdminThread mediaDriverAdminThread;
    private SenderChannel channel;

    @Before
    public void setup()
    {
        final MediaDriver.TopologyBuilder builder = new MediaDriver.TopologyBuilder()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .senderThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .adminBufferStrategy(new CreatingAdminBufferStrategy(adminPath, COMMAND_BUFFER_SZ + TRAILER_SIZE))
                .bufferManagementStrategy(new BasicBufferManagementStrategy(adminPath));

        SenderThread senderThread = new SenderThread(builder) {
            @Override
            public void addBuffer(final SenderChannel channel)
            {
                MediaDriverAdminThreadTest.this.channel = channel;
            }
        };
        ReceiverThread receiverThread = mock(ReceiverThread.class);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
    }

    @After
    public void cleanup() throws IOException
    {
        IoUtil.delete(new File(adminPath), true);
    }

    @Test
    public void addingChannelShouldNotifySenderThread() throws IOException
    {
        final ByteBuffer buffer = new MappingAdminBufferStrategy(adminPath).toMediaDriver();
        final RingBuffer adminCommands = new ManyToOneRingBuffer(new AtomicBuffer(buffer));

        final ChannelMessageFlyweight  channelMessage = new ChannelMessageFlyweight();
        channelMessage.reset(writeBuffer, 0);
        channelMessage.channelId(1L);
        channelMessage.sessionId(2L);
        channelMessage.destination(DESTINATION);

        adminCommands.write(ADD_CHANNEL, writeBuffer, 0, channelMessage.length());

        mediaDriverAdminThread.process();

        assertThat(channel, notNullValue());
        assertThat(channel.channelId(), is(1L));
        assertThat(channel.sessionId(), is(2L));
    }

}
