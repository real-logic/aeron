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
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy;
import uk.co.real_logic.aeron.util.AdminBuffers;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.*;
import static uk.co.real_logic.aeron.mediadriver.MediaDriverAdminThread.HEADER_LENGTH;
import static uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy.newMappedBufferManager;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.ErrorCode.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;

public class UnicastSenderTest
{
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];
    private static final int MAX_FRAME_LENGTH = 1024;

    private static final String HOST = "localhost";
    private static final int PORT = 45678;
    private static final String URI = "udp://" + HOST + ":" + PORT;
    private static final long CHANNEL_ID = 0xA;
    private static final long SESSION_ID = 0xdeadbeefL;
    public static final int BUFFER_SIZE = 256;
    public static final int VALUE = 37;

    @ClassRule
    public static AdminBuffers buffers = new AdminBuffers(COMMAND_BUFFER_SZ + TRAILER_LENGTH);

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    private final ByteBuffer recvBuffer = ByteBuffer.allocate(256);
    private final AtomicBuffer readBuffer = new AtomicBuffer(recvBuffer);

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final CompletelyIdentifiedMessageFlyweight bufferMessage = new CompletelyIdentifiedMessageFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private TimerWheel timerWheel;

    private BufferManagementStrategy bufferManagementStrategy;
    private SenderThread senderThread;
    private ReceiverThread receiverThread;
    private MediaDriverAdminThread mediaDriverAdminThread;
    private DatagramChannel receiverChannel;

    private long controlledTimestamp;

    @Before
    public void setUp() throws Exception
    {
        bufferManagementStrategy = newMappedBufferManager(directory.dataDir());

        controlledTimestamp = 0;
        timerWheel = new TimerWheel(() -> controlledTimestamp,
                                    ADMIN_THREAD_TICK_DURATION_MICROSECONDS,
                                    TimeUnit.MICROSECONDS,
                                    ADMIN_THREAD_TICKS_PER_WHEEL);

        final MediaDriver.MediaDriverContext builder = new MediaDriver.MediaDriverContext()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(buffers.strategy())
                .bufferManagementStrategy(bufferManagementStrategy)
                .adminTimerWheel(timerWheel);

        senderThread = new SenderThread(builder);
        receiverThread = mock(ReceiverThread.class);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
        receiverChannel = DatagramChannel.open();

        receiverChannel.configureBlocking(false);
        receiverChannel.bind(new InetSocketAddress(HOST, PORT));

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

        assertRegisteredFrameHandler();

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

        assertRegisteredFrameHandler();

        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNull(mediaDriverAdminThread.frameHandler(udpDestination()));
    }

    @Test
    public void shouldErrorOnAddChannelOnExistingSession() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertRegisteredFrameHandler();

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
        });

        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CHANNEL_ALREADY_EXISTS));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(channelMessage.sessionId(), is(SESSION_ID));
            assertThat(channelMessage.channelId(), is(CHANNEL_ID));
            assertThat(channelMessage.destination(), is(URI));
        });
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(INVALID_DESTINATION));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(channelMessage.sessionId(), is(SESSION_ID));
            assertThat(channelMessage.channelId(), is(CHANNEL_ID));
            assertThat(channelMessage.destination(), is(URI));
        });
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSession() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertRegisteredFrameHandler();

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
        });

        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID + 1, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CHANNEL_UNKNOWN));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(channelMessage.sessionId(), is(SESSION_ID + 1));
            assertThat(channelMessage.channelId(), is(CHANNEL_ID));
            assertThat(channelMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToSendOnChannel() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertRegisteredFrameHandler();

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
        });

        // TODO: should only be able to send once an SM is sent

        appendValue();

        processThreads(1);

        final SocketAddress address = receiverChannel.receive(recvBuffer);
        assertNotNull(address);
        assertThat(recvBuffer.getInt(HEADER_LENGTH), is(VALUE));
    }

    @Test(timeout = 1000)
    public void shouldSend0LengthDataOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertRegisteredFrameHandler();

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));
        });

        advanceTimeMilliseconds(90);   // should not send yet....

        SocketAddress address = receiverChannel.receive(recvBuffer);
        assertNull(address);

        advanceTimeMilliseconds(110);  // should send 0 length data after 100 msec, so give a bit more time

        address = receiverChannel.receive(recvBuffer);
        assertNotNull(address);

        dataHeader.wrap(readBuffer, 0);
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH)); // 0 length data
    }

    @Test(timeout = 1000)
    public void shouldNotSend0LengthDataFrameAfterReceivingStatusMessage() throws Exception
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertRegisteredFrameHandler();

        final AtomicLong termId = new AtomicLong(0);
        final InetSocketAddress controlAddr = determineControlAddressToSendTo();

        assertEventRead(buffers.toApi(), assertNotifiesNewBuffer(termId));

        sendStatusMessage(controlAddr, termId.get(), 0, 0);

        advanceTimeMilliseconds(300);  // should send 0 length data after 100 msec, so give a bit more time

        final SocketAddress address = receiverChannel.receive(recvBuffer);
        assertNull(address);           // nothing there
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldNotSendUntilStatusMessageReceived() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);
        processThreads(5);
        assertRegisteredFrameHandler();
        appendValue();

        processThreads(1);
        assertNotReceivedValue();

        // TODO: finish. check make sure it doesn't send. send SM. check.
    }

    private void assertNotReceivedValue() throws IOException
    {
        final SocketAddress address = receiverChannel.receive(recvBuffer);
        assertNull(address);
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldSendHeartbeatWhenIdle()
    {
        // TODO: finish. add_channel, send SM, append data, send data, then idle for time. Make sure sends heartbeat.
    }

    private EventHandler assertNotifiesNewBuffer(final AtomicLong termId)
    {
        return (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));
            bufferMessage.wrap(buffer, index);
            termId.set(bufferMessage.termId());
        };
    }

    private void appendValue() throws IOException
    {
        final LogAppender logAppender = mapLogAppenders(URI, SESSION_ID, CHANNEL_ID).get(0);
        writeBuffer.putInt(0, VALUE, ByteOrder.BIG_ENDIAN);
        assertTrue(logAppender.append(writeBuffer, 0, 64));
    }

    private void assertRegisteredFrameHandler()
    {
        assertNotNull(mediaDriverAdminThread.frameHandler(udpDestination()));
    }

    private UdpDestination udpDestination()
    {
        return UdpDestination.parse(URI);
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

    private void sendStatusMessage(final InetSocketAddress destAddr,
                                   final long termId,
                                   final int seqNum,
                                   final long window)
            throws Exception
    {
        statusMessage.wrap(writeBuffer, 0);

        statusMessage.highestContiguousSequenceNumber(seqNum)
                     .receiverWindow(window)
                     .termId(termId)
                     .channelId(CHANNEL_ID)
                     .sessionId(SESSION_ID)
                     .version(HeaderFlyweight.CURRENT_VERSION)
                     .flags((byte) 0)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH);

        sendBuffer.position(0);
        sendBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);
        receiverChannel.send(sendBuffer, destAddr);
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach((i) ->
        {
            mediaDriverAdminThread.process();
            senderThread.process();
        });
    }

    private void advanceTimeMilliseconds(final int msec)
    {
        final long tickNanos = TimeUnit.MICROSECONDS.toNanos(ADMIN_THREAD_TICK_DURATION_MICROSECONDS);
        final long spanNanos = TimeUnit.MILLISECONDS.toNanos(msec);
        final long startTimestamp = controlledTimestamp;

        while ((controlledTimestamp - startTimestamp) < spanNanos)
        {
            controlledTimestamp += tickNanos;
            processThreads(1);
        }
    }

    private static List<LogAppender> mapLogAppenders(final String destination,
                                                     final long sessionId,
                                                     final long channelId)
        throws IOException
    {
        final List<SharedDirectories.Buffers> buffers = directory.mapTermFile(directory.senderDir(),
                destination,
                sessionId,
                channelId);

        return SharedDirectories.mapLoggers(buffers, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    private InetSocketAddress determineControlAddressToSendTo() throws Exception
    {
        final ControlFrameHandler frameHandler = mediaDriverAdminThread.frameHandler(udpDestination());
        final InetSocketAddress srcAddr = (InetSocketAddress)frameHandler.transport().channel().getLocalAddress();

        return new InetSocketAddress(HOST, srcAddr.getPort());
    }
}
