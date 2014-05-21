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
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.util.ConductorBuffersExternalResource;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.io.File;
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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaConductor.HEADER_LENGTH;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.*;
import static uk.co.real_logic.aeron.mediadriver.SenderChannel.INITIAL_HEARTBEAT_TIMEOUT_MS;
import static uk.co.real_logic.aeron.mediadriver.SenderChannel.HEARTBEAT_TIMEOUT_MS;
import static uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement.newMappedBufferManager;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.ErrorCode.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;
import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.HDR_TYPE_DATA;

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

    @Rule
    public ConductorBuffersExternalResource buffers
        = new ConductorBuffersExternalResource(COMMAND_BUFFER_SZ + TRAILER_LENGTH);

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final ByteBuffer wrappedWriteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(wrappedWriteBuffer);

    private final ByteBuffer wrappedReadBuffer = ByteBuffer.allocate(256);
    private final AtomicBuffer readBuffer = new AtomicBuffer(wrappedReadBuffer);

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final NewBufferMessageFlyweight bufferMessage = new NewBufferMessageFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    //private TimerWheel timerWheel;

    private BufferManagement bufferManagement;
    private Sender sender;
    private MediaConductor mediaConductor;
    private DatagramChannel receiverChannel;

    private long controlledTimestamp;

    @Before
    public void setUp() throws Exception
    {
        bufferManagement = newMappedBufferManager(directory.dataDir());

        controlledTimestamp = 0;
        final TimerWheel timerWheel = new TimerWheel(
            () -> controlledTimestamp,
            MEDIA_CONDUCTOR_TICK_DURATION_MICROS,
            TimeUnit.MICROSECONDS,
                MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

        final Context ctx = new Context()
            .conductorCommandBuffer(COMMAND_BUFFER_SZ)
            .receiverCommandBuffer(COMMAND_BUFFER_SZ)
            .rcvNioSelector(new NioSelector())
            .adminNioSelector(new NioSelector())
            .senderFlowControl(DefaultSenderControlStrategy::new)
            .conductorByteBuffers(buffers.mediaDriverBuffers())
            .bufferManagement(bufferManagement)
            .conductorTimerWheel(timerWheel);

        sender = new Sender(ctx);
        final Receiver receiver = mock(Receiver.class);
        mediaConductor = new MediaConductor(ctx, receiver, sender);
        receiverChannel = DatagramChannel.open();

        receiverChannel.configureBlocking(false);
        receiverChannel.bind(new InetSocketAddress(HOST, PORT));

        wrappedWriteBuffer.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        receiverChannel.close();
        sender.close();
        mediaConductor.close();
        mediaConductor.nioSelector().selectNowWithNoProcessing();
        bufferManagement.close();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddChannel() throws Exception
    {
        successfullyAddChannel();

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
            // TODO
            //assertIsDirectory(bufferMessage.location());
        });
    }

    private void assertIsDirectory(final String location)
    {
        final File dir = new File(location);
        assertTrue("Isn't a directory: " + location, dir.isDirectory());
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToRemoveChannel() throws Exception
    {
        successfullyAddChannel();

        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertNull(mediaConductor.frameHandler(udpDestination()));
    }

    @Test
    public void shouldErrorOnAddChannelOnExistingSession() throws Exception
    {
        successfullyAddChannel();

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
            // TODO
            //assertIsDirectory(bufferMessage.location());
        });

        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CHANNEL_ALREADY_EXISTS));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertMessageRefersToSession(channelMessage, SESSION_ID);
        });
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(INVALID_DESTINATION));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertMessageRefersToSession(channelMessage, SESSION_ID);
        });
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSession() throws Exception
    {
        successfullyAddChannel();

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));

            bufferMessage.wrap(buffer, index);
            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
            // TODO
            //assertIsDirectory(bufferMessage.location());
        });

        writeChannelMessage(REMOVE_CHANNEL, URI, SESSION_ID + 1, CHANNEL_ID);

        processThreads(5);

        assertEventRead(buffers.toClient(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CHANNEL_UNKNOWN));
            assertThat(error.errorStringLength(), greaterThan(0));

            channelMessage.wrap(buffer, error.offendingHeaderOffset());
            assertMessageRefersToSession(channelMessage, SESSION_ID + 1);
        });
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToSendOnChannel() throws Exception
    {
        successfullyAddChannel();

        final InetSocketAddress controlAddr = determineControlAddressToSendTo();
        final long termId = assertNotifiesTermBuffer();

        // give it enough window to send
        sendStatusMessage(controlAddr, termId, 0, 1000);
        appendValue();
        processThreads(5);

        assertReceivedPacket();
        assertPacketContainsValue();
    }

    @Test(timeout = 1000)
    public void shouldNotSend0LengthDataFrameAfterReceivingStatusMessage() throws Exception
    {
        successfullyAddChannel();

        final InetSocketAddress controlAddr = determineControlAddressToSendTo();
        final long termId = assertNotifiesTermBuffer();

        sendStatusMessage(controlAddr, termId, 0, 0);

        // should send 0 length data after 100 msec, so give a bit more time
        advanceTimeMilliseconds(3 * INITIAL_HEARTBEAT_TIMEOUT_MS);

        assertNotReceivedPacket();
    }

    @Test(timeout = 1000)
    @Ignore
    public void shouldNotSendUntilStatusMessageReceived() throws Exception
    {
        successfullyAddChannel();

        final InetSocketAddress controlAddr = determineControlAddressToSendTo();
        final long termId = assertNotifiesTermBuffer();

        appendValue();

        processThreads(5);
        assertNotReceivedPacket();

        sendStatusMessage(controlAddr, termId, 0, 1000);
        processThreads(5);

        assertReceivedPacket();
        assertPacketContainsValue();
    }

    @Test(timeout = 1000)
    public void shouldNotBeAbleToSendAfterUsingUpYourWindow() throws Exception
    {
        successfullyAddChannel();

        final InetSocketAddress controlAddr = determineControlAddressToSendTo();
        final long termId = assertNotifiesTermBuffer();

        // give it enough window to send
        sendStatusMessage(controlAddr, termId, 0, 1000);

        final LogAppender logAppender = mapLogAppenders(URI, SESSION_ID, CHANNEL_ID).get(0);
        writeBuffer.putInt(0, VALUE, ByteOrder.BIG_ENDIAN);
        assertTrue(logAppender.append(writeBuffer, 0, 64));
        processThreads(5);

        assertReceivedPacket();

        assertTrue(logAppender.append(writeBuffer, 0, 64));
        processThreads(5);
        assertNotReceivedPacket();
    }

    @Test(timeout = 1000)
    public void shouldSend0LengthDataOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        successfullyAddChannel();

        assertEventRead(buffers.toClient(),
                        (eventTypeId, buffer, index, length) ->
                            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION)));

        advanceTimeMilliseconds(INITIAL_HEARTBEAT_TIMEOUT_MS - 10);   // should not send yet....

        assertNotReceivedPacket();

        // should send 0 length data after 100 msec, so give a bit more time
        advanceTimeMilliseconds(INITIAL_HEARTBEAT_TIMEOUT_MS + 10);

        assertReceivedZeroLengthPacket();
    }

    @Test(timeout = 100000)
    public void shouldSendHeartbeatWhenIdle() throws Exception
    {
        successfullyAddChannel();

        final InetSocketAddress controlAddr = determineControlAddressToSendTo();
        final long termId = assertNotifiesTermBuffer();

        // give it enough window to send
        sendStatusMessage(controlAddr, termId, 0, 1000);

        // append & send data
        final LogAppender logAppender = mapLogAppenders(URI, SESSION_ID, CHANNEL_ID).get(0);
        writeBuffer.putInt(0, VALUE, ByteOrder.BIG_ENDIAN);
        assertTrue(logAppender.append(writeBuffer, 0, 64));
        processThreads(5);

        // data packet
        assertReceivedPacket();
        assertPacketContainsValue();

        // idle for time
        advanceTimeMilliseconds(2 * HEARTBEAT_TIMEOUT_MS);

        // heartbeat
        assertReceivedZeroLengthPacket();
    }

    @Test(timeout = 1000)
    @Ignore
    public void shouldSendMultipleHeartbeatsCorrectly() throws Exception
    {
    }

    private void assertReceivedZeroLengthPacket() throws IOException
    {
        assertReceivedPacket();

        dataHeader.wrap(readBuffer, 0);
        assertThat(dataHeader.headerType(), is(HDR_TYPE_DATA));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH)); // 0 length data
    }

    private void assertReceivedPacket() throws IOException
    {
        wrappedReadBuffer.clear();
        SocketAddress address = receiverChannel.receive(wrappedReadBuffer);
        assertNotNull(address);
    }

    private void assertMessageRefersToSession(final ChannelMessageFlyweight message, final long sessionId)
    {
        assertThat(message.sessionId(), is(sessionId));
        assertThat(channelMessage.channelId(), is(CHANNEL_ID));
        assertThat(channelMessage.destination(), is(URI));
    }

    private long assertNotifiesTermBuffer()
    {
        final AtomicLong termId = new AtomicLong(0);
        assertEventRead(buffers.toClient(), assertNotifiesNewBuffer(termId));
        return termId.get();
    }

    private void assertPacketContainsValue()
    {
        assertThat(wrappedReadBuffer.getInt(HEADER_LENGTH), is(VALUE));
    }

    private void assertNotReceivedPacket() throws IOException
    {
        final SocketAddress address = receiverChannel.receive(wrappedReadBuffer);
        assertNull(address);
    }

    private EventHandler assertNotifiesNewBuffer(final AtomicLong termId)
    {
        return (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(NEW_SEND_BUFFER_NOTIFICATION));
            bufferMessage.wrap(buffer, index);

            assertThat(bufferMessage.sessionId(), is(SESSION_ID));
            assertThat(bufferMessage.channelId(), is(CHANNEL_ID));
            assertThat(bufferMessage.destination(), is(URI));
            // TODO
            //assertIsDirectory(bufferMessage.location());

            termId.set(bufferMessage.termId());
        };
    }

    private void successfullyAddChannel() throws IOException
    {
        writeChannelMessage(ADD_CHANNEL, URI, SESSION_ID, CHANNEL_ID);
        processThreads(1);
        assertRegisteredFrameHandler();
    }

    private void appendValue() throws IOException
    {
        final LogAppender logAppender = mapLogAppenders(URI, SESSION_ID, CHANNEL_ID).get(0);
        writeBuffer.putInt(0, VALUE, ByteOrder.BIG_ENDIAN);
        assertTrue(logAppender.append(writeBuffer, 0, 64));
    }

    private void assertRegisteredFrameHandler()
    {
        assertNotNull(mediaConductor.frameHandler(udpDestination()));
    }

    private UdpDestination udpDestination()
    {
        return UdpDestination.parse(URI);
    }

    private void writeChannelMessage(final int eventTypeId,
                                     final String destination,
                                     final long sessionId,
                                     final long channelId)
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

        statusMessage.highestContiguousTermOffset(seqNum)
                     .receiverWindow(window)
                     .termId(termId)
                     .channelId(CHANNEL_ID)
                     .sessionId(SESSION_ID)
                     .version(HeaderFlyweight.CURRENT_VERSION)
                     .flags((byte)0)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH);

        wrappedWriteBuffer.position(0);
        wrappedWriteBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);
        receiverChannel.send(wrappedWriteBuffer, destAddr);
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach(
            (i) ->
            {
                mediaConductor.process();
                sender.process();
            });
    }

    private void advanceTimeMilliseconds(final int msec)
    {
        final long tickNanos = TimeUnit.MICROSECONDS.toNanos(MEDIA_CONDUCTOR_TICK_DURATION_MICROS);
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
        final ControlFrameHandler frameHandler = mediaConductor.frameHandler(udpDestination());
        final InetSocketAddress srcAddr = (InetSocketAddress)frameHandler.transport().channel().getLocalAddress();

        return new InetSocketAddress(HOST, srcAddr.getPort());
    }
}
