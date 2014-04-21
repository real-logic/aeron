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
import uk.co.real_logic.aeron.mediadriver.buffer.BasicBufferManagementStrategy;
import uk.co.real_logic.aeron.util.AdminBuffers;
import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;

public class UnicastSenderTest
{
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];
    private static final int MAX_FRAME_LENGTH = 1024;

    private static final String URI = "udp://localhost:45678";
    private static final long CHANNEL_ID = 0xA;
    private static final long SESSION_ID = 0xdeadbeefL;

    @ClassRule
    public static AdminBuffers buffers = new AdminBuffers(COMMAND_BUFFER_SZ + TRAILER_SIZE);

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final CompletelyIdentifiedMessageFlyweight bufferMessage = new CompletelyIdentifiedMessageFlyweight();

    private BasicBufferManagementStrategy bufferManagementStrategy;
    private SenderThread senderThread;
    private ReceiverThread receiverThread;
    private MediaDriverAdminThread mediaDriverAdminThread;
    private DatagramChannel receiverChannel;

    @Before
    public void setUp() throws Exception
    {
        bufferManagementStrategy = new BasicBufferManagementStrategy(directory.dataDir());

        final MediaDriver.TopologyBuilder builder = new MediaDriver.TopologyBuilder()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(buffers.strategy())
                .bufferManagementStrategy(bufferManagementStrategy);

        senderThread = new SenderThread(builder);
        receiverThread = mock(ReceiverThread.class);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
        receiverChannel = DatagramChannel.open();
        sendBuffer.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        receiverChannel.close();
        senderThread.close();
        mediaDriverAdminThread.close();
        mediaDriverAdminThread.nioSelector().selectNowWithNoProcessing();
        bufferManagementStrategy.close();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddChannel() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNotNull(mediaDriverAdminThread.frameHandler(UdpDestination.parse(URI)));

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToRemoveChannel() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNotNull(mediaDriverAdminThread.frameHandler(UdpDestination.parse(URI)));

        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNull(mediaDriverAdminThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToSendOnChannel() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNotNull(mediaDriverAdminThread.frameHandler(UdpDestination.parse(URI)));

        final AtomicLong termId = new AtomicLong();

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
            termId.set(bufferMessage.termId());
        });

        final LogAppender[] appenders = mapLogAppenders(directory.senderDir(), URI, SESSION_ID, CHANNEL_ID);

        // TODO: append onto appenders[0]
    }


    private void writeChannelMessage(final int eventTypeId, final String destination,
                                     final long sessionId, final long channelId)
            throws IOException
    {
        final RingBuffer adminCommands = buffers.mappedToMediaDriver();

        channelMessage.wrap(writeBuffer, 0);

        channelMessage.channelId(channelId)
                      .sessionId(sessionId)
                      .destination(destination);

        adminCommands.write(eventTypeId, writeBuffer, 0, channelMessage.length());
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach((i) ->
        {
            mediaDriverAdminThread.process();
            senderThread.process();
        });
    }

    private static LogAppender[] mapLogAppenders(final File rootDir, final String destination,
                                                 final long sessionId, final long channelId)
        throws IOException
    {
        final LogAppender[] appenders = new LogAppender[FileMappingConvention.BUFFER_COUNT];

        final List<SharedDirectories.Buffers> buffers = directory.mapTermFile(directory.senderDir(), destination,
                                                                              sessionId, sessionId);

        for (int i = 0; i < FileMappingConvention.BUFFER_COUNT; i++)
        {
            appenders[i] = new LogAppender(buffers.get(i).logBuffer(), buffers.get(i).stateBuffer(),
                                           DEFAULT_HEADER, MAX_FRAME_LENGTH);
        }

        return appenders;
    }
}
