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

import org.hamcrest.Matchers;
import org.junit.*;
import uk.co.real_logic.aeron.mediadriver.buffer.BasicBufferManagementStrategy;
import uk.co.real_logic.aeron.util.AdminBuffers;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.StateViewer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.ErrorCode.CONSUMER_NOT_REGISTERED;
import static uk.co.real_logic.aeron.util.SharedDirectories.Buffers;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;
import static uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class UnicastReceiverTest
{
    private static final String URI = "udp://localhost:45678";
    private static final String INVALID_URI = "udp://";
    private static final long CHANNEL_ID = 10;
    private static final long[] ONE_CHANNEL = { CHANNEL_ID };
    private static final long[] ANOTHER_CHANNEL = { 20 };
    private static final long[] TWO_CHANNELS = { 20, 30 };
    private static final long[] THREE_CHANNELS = { 10, 20, 30 };
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long TERM_ID = 0xec1L;
    private static final int VALUE = 37;

    @ClassRule
    public static AdminBuffers buffers = new AdminBuffers(COMMAND_BUFFER_SZ + TRAILER_LENGTH);

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final ConsumerMessageFlyweight receiverMessage = new ConsumerMessageFlyweight();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private BasicBufferManagementStrategy bufferManagementStrategy;
    private SenderThread senderThread;
    private ReceiverThread receiverThread;
    private MediaDriverAdminThread mediaDriverAdminThread;
    private DatagramChannel senderChannel;

    @Before
    public void setUp() throws Exception
    {
        bufferManagementStrategy = new BasicBufferManagementStrategy(directory.dataDir());

        NioSelector nioSelector = new NioSelector();
        final MediaDriver.MediaDriverContext builder = new MediaDriver.MediaDriverContext()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(nioSelector)
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(buffers.strategy())
                .bufferManagementStrategy(bufferManagementStrategy);

        builder.rcvFrameHandlerFactory(new RcvFrameHandlerFactory(nioSelector,
                new MediaDriverAdminThreadCursor(builder.adminThreadCommandBuffer(), nioSelector)));

        senderThread = mock(SenderThread.class);
        receiverThread = new ReceiverThread(builder);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
        senderChannel = DatagramChannel.open();
        sendBuffer.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        senderChannel.close();
        receiverThread.close();
        receiverThread.nioSelector().selectNowWithNoProcessing();
        mediaDriverAdminThread.close();
        mediaDriverAdminThread.nioSelector().selectNowWithNoProcessing();
        bufferManagementStrategy.close();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddReceiver() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForInvalidUri() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, INVALID_URI, ONE_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(INVALID_DESTINATION));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(INVALID_URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForRemovingNonExistentReceiver() throws Exception
    {
        writeReceiverMessage(REMOVE_CONSUMER, URI, ONE_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CONSUMER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForRemovingReceiverFromWrongChannels() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, URI, ONE_CHANNEL);
        writeReceiverMessage(REMOVE_CONSUMER, URI, ANOTHER_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(CONSUMER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ANOTHER_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldKeepFrameHandlerUponRemoveOfAllButOneChannel() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse(URI);

        writeReceiverMessage(ADD_CONSUMER, URI, THREE_CHANNELS);

        processThreads(5);

        final RcvFrameHandler frameHandler = receiverThread.frameHandler(dest);

        assertNotNull(frameHandler);
        assertThat(frameHandler.channelInterestMap().size(), is(3));

        writeReceiverMessage(REMOVE_CONSUMER, URI, TWO_CHANNELS);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(dest));
        assertThat(frameHandler.channelInterestMap().size(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldOnlyRemoveFrameHandlerUponRemovalOfAllChannels() throws Exception
    {
        final UdpDestination dest = UdpDestination.parse(URI);

        writeReceiverMessage(ADD_CONSUMER, URI, THREE_CHANNELS);

        processThreads(5);

        final RcvFrameHandler frameHandler = receiverThread.frameHandler(dest);

        assertNotNull(frameHandler);
        assertThat(frameHandler.channelInterestMap().size(), is(3));

        writeReceiverMessage(REMOVE_CONSUMER, URI, TWO_CHANNELS);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(dest));
        assertThat(frameHandler.channelInterestMap().size(), is(1));

        writeReceiverMessage(REMOVE_CONSUMER, URI, ONE_CHANNEL);
        processThreads(5);

        assertNull(receiverThread.frameHandler(dest));
    }

    @Test(timeout = 200000)
    public void shouldBeAbleToCreateRcvTermOnZeroLengthData() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, URI, ONE_CHANNEL);

        processThreads(10);

        final UdpDestination dest = UdpDestination.parse(URI);

        assertNotNull(receiverThread.frameHandler(dest));

        sendDataFrame(dest, ONE_CHANNEL[0], 0x0);

        processThreads(5);

        final RcvFrameHandler frameHandler = receiverThread.frameHandler(dest);
        assertNotNull(frameHandler);
        final RcvChannelState channelState = frameHandler.channelInterestMap().get(ONE_CHANNEL[0]);
        assertNotNull(channelState);
        final RcvSessionState sessionState = channelState.getSessionState(SESSION_ID);
        assertNotNull(sessionState);
        final InetSocketAddress srcAddr = (InetSocketAddress)senderChannel.getLocalAddress();
        assertThat(sessionState.sourceAddress().getPort(), is(srcAddr.getPort()));

        processThreads(5);

        // once the buffer is attached, the receiver should send back an SM
        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddr = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusMessage.wrap(rcvBuffer);
        assertNotNull(rcvAddr);
        assertThat(rcvAddr.getPort(), is(dest.remoteData().getPort()));
        assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusMessage.channelId(), is(ONE_CHANNEL[0]));
        assertThat(statusMessage.sessionId(), is(SESSION_ID));
        assertThat(statusMessage.termId(), is(TERM_ID));
        assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.LENGTH));
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddThenRemoveReceiverWithoutBuffers() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(UdpDestination.parse(URI)));

        writeReceiverMessage(ControlProtocolEvents.REMOVE_CONSUMER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNull(receiverThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToReceiveDataFromNetwork() throws Exception
    {
        writeReceiverMessage(ADD_CONSUMER, URI, ONE_CHANNEL);
        processThreads(5);

        UdpDestination destination = UdpDestination.parse(URI);
        sendDataFrame(destination, CHANNEL_ID, 0);
        processThreads(5);

        sendDataFrame(destination, CHANNEL_ID, 1);
        processThreads(5);

        List<Buffers> buffers = directory.mapTermFile(directory.receiverDir(), URI, SESSION_ID, CHANNEL_ID);
        StateViewer stateViewer = new StateViewer(buffers.get(0).stateBuffer());
        assertThat(stateViewer.tailVolatile(), is(greaterThan(0)));
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToAddThenRemoveReceiverWithBuffers() throws Exception
    {
        // TODO
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToHandleTermBufferRolloverCorrectly()
    {
        // TODO: finish
    }

    private void writeReceiverMessage(final int eventTypeId, final String destination, final long[] channelIds)
            throws IOException
    {
        final RingBuffer adminCommands = buffers.mappedToMediaDriver();

        receiverMessage.wrap(writeBuffer, 0);

        receiverMessage.channelIds(channelIds)
                                .destination(destination);

        adminCommands.write(eventTypeId, writeBuffer, 0, receiverMessage.length());
    }

    private void sendDataFrame(final UdpDestination destination, final long channelId, final int seqNum)
            throws Exception
    {
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        dataHeaderFlyweight.wrap(writeBuffer, 0);

        dataHeaderFlyweight.sequenceNumber(seqNum)
                           .sessionId(SESSION_ID)
                           .channelId(channelId)
                           .termId(TERM_ID)
                           .version(HeaderFlyweight.CURRENT_VERSION)
                           .flags((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                           .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                           .frameLength(HEADER_LENGTH);

        sendBuffer.position(0);
        sendBuffer.putInt(dataHeaderFlyweight.dataOffset(), VALUE);
        sendBuffer.limit(BitUtil.align(HEADER_LENGTH + SIZE_OF_INT, FRAME_ALIGNMENT));
        senderChannel.send(sendBuffer, destination.remoteData());
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach((i) ->
        {
            mediaDriverAdminThread.process();
            receiverThread.process();
        });
    }
}
