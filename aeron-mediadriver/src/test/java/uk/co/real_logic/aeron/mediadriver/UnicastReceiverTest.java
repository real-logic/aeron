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
import org.junit.rules.ExpectedException;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.StateViewer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US;
import static uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement.newMappedBufferManager;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.ErrorCode.SUBSCRIBER_NOT_REGISTERED;
import static uk.co.real_logic.aeron.util.SharedDirectoriesExternalResource.Buffers;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertMsgRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;
import static uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class UnicastReceiverTest
{
    private static final String URI = "udp://localhost:45678";
    private static final String INVALID_URI = "udp://";
    private static final long CHANNEL_ID = 10;
    private static final long[] ONE_CHANNEL = {CHANNEL_ID};
    private static final long[] ANOTHER_CHANNEL = {20};
    private static final long[] TWO_CHANNELS = {20, 30};
    private static final long[] THREE_CHANNELS = {10, 20, 30};
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long TERM_ID = 0xec1L;
    private static final int VALUE = 37;
    private static final int FAKE_PACKET_SIZE = 128;

    @ClassRule
    public static ConductorBuffersExternalResource buffers =
        new ConductorBuffersExternalResource(COMMAND_BUFFER_SZ + TRAILER_LENGTH);

    @ClassRule
    public static SharedDirectoriesExternalResource directory = new SharedDirectoriesExternalResource();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final SubscriberMessageFlyweight receiverMessage = new SubscriberMessageFlyweight();

    private BufferManagement bufferManagement;
    private Receiver receiver;
    private MediaConductor mediaConductor;
    private DatagramChannel senderChannel;

    @Before
    public void setUp() throws Exception
    {
        bufferManagement = newMappedBufferManager(directory.dataDir());

        final NioSelector nioSelector = new NioSelector();

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .conductorCommandBuffer(COMMAND_BUFFER_SZ)
            .receiverCommandBuffer(COMMAND_BUFFER_SZ)
            .receiverNioSelector(nioSelector)
            .conductorNioSelector(new NioSelector())
            .senderFlowControl(UnicastSenderControlStrategy::new)
            .conductorShmBuffers(new ConductorShmBuffers(buffers.adminDirName()))
            .conductorTimerWheel(new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                TimeUnit.MICROSECONDS,
                                                MEDIA_CONDUCTOR_TICKS_PER_WHEEL))
            .newReceiveBufferEventQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .bufferManagement(bufferManagement);

        ctx.rcvFrameHandlerFactory(
            new DataFrameHandlerFactory(nioSelector, new MediaConductorProxy(ctx.mediaCommandBuffer(), nioSelector))
        );

        ctx.receiverProxy(new ReceiverProxy(ctx.receiverCommandBuffer(),
                                            ctx.conductorNioSelector(),
                                            ctx.newReceiveBufferEventQueue()));

        ctx.mediaConductorProxy(new MediaConductorProxy(ctx.mediaCommandBuffer(), ctx.conductorNioSelector()));

        final Sender sender = mock(Sender.class);
        receiver = new Receiver(ctx);
        mediaConductor = new MediaConductor(ctx, receiver, sender);
        senderChannel = DatagramChannel.open();
        sendBuffer.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        skip(buffers.toClient(), 10);
        senderChannel.close();
        receiver.close();
        receiver.nioSelector().selectNowWithNoProcessing();
        mediaConductor.close();
        mediaConductor.nioSelector().selectNowWithNoProcessing();
        bufferManagement.close();
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldBeAbleToAddReceiver() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);

        processThreads(5);

        assertReceiverRegistered();
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldSendErrorForInvalidUri() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, INVALID_URI, ONE_CHANNEL);

        processThreads(5);

        assertMsgRead(buffers.toClient(), (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(INVALID_DESTINATION));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(INVALID_URI));
        });
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldSendErrorForRemovingNonExistentReceiver() throws Exception
    {
        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, ONE_CHANNEL);

        processThreads(5);

        assertMsgRead(buffers.toClient(), (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(SUBSCRIBER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldSendErrorForRemovingReceiverFromWrongChannels() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);
        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, ANOTHER_CHANNEL);

        processThreads(5);

        assertMsgRead(buffers.toClient(), (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(SUBSCRIBER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ANOTHER_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldKeepFrameHandlerUponRemoveOfAllButOneChannel() throws Exception
    {
        final UdpDestination destination = udpDestination();

        writeSubscriberMessage(ADD_SUBSCRIBER, URI, THREE_CHANNELS);

        processThreads(5);

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, TWO_CHANNELS);

        processThreads(5);

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldOnlyRemoveFrameHandlerUponRemovalOfAllChannels() throws Exception
    {
        final UdpDestination destination = udpDestination();

        writeSubscriberMessage(ADD_SUBSCRIBER, URI, THREE_CHANNELS);

        processThreads(5);

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, TWO_CHANNELS);

        processThreads(5);

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));

        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads(5);

        assertNull(receiver.frameHandler(destination));
    }

    @Test(timeout = 200000)
    public void shouldBeAbleToCreateRcvTermOnZeroLengthData() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);

        processThreads(10);

        final UdpDestination destination = udpDestination();

        assertNotNull(receiver.frameHandler(destination));

        sendDataFrame(destination, ONE_CHANNEL[0], 0x0);

        processThreads(5);

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);
        assertNotNull(frameHandler);
        final Subscription subscription = frameHandler.subscriptionMap().get(ONE_CHANNEL[0]);
        assertNotNull(subscription);
        final SubscribedSession subscribedSession = subscription.getSubscribedSession(SESSION_ID);
        assertNotNull(subscribedSession);
        final InetSocketAddress srcAddr = (InetSocketAddress)senderChannel.getLocalAddress();
        assertThat(subscribedSession.sourceAddress().getPort(), is(srcAddr.getPort()));

        processThreads(5);

        // once the buffer is attached, the receiver should send back an SM
        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddr = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusMessage.wrap(rcvBuffer);

        assertNotNull(rcvAddr);
        assertThat(rcvAddr.getPort(), is(destination.remoteData().getPort()));
        assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusMessage.channelId(), is(ONE_CHANNEL[0]));
        assertThat(statusMessage.sessionId(), is(SESSION_ID));
        assertThat(statusMessage.termId(), is(TERM_ID));
        assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    @Test(timeout = 1000)
    @Ignore("moved to MediaConductorTest")
    public void shouldBeAbleToAddThenRemoveReceiverWithoutBuffers() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads();

        assertReceiverRegistered();

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads(2);

        assertReceiverNotRegistered();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToReceiveDataFromNetwork() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads();

        sendDataFrame(udpDestination(), CHANNEL_ID, 0);
        processThreads(2);

        sendDataFrame(udpDestination(), CHANNEL_ID, 1);
        processThreads(2);

        assertReceivedDataFromNetwork();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddThenRemoveReceiverWithBuffers() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads();

        final UdpDestination destination = udpDestination();
        sendDataFrame(destination, CHANNEL_ID, 0);
        processThreads();

        sendDataFrame(destination, CHANNEL_ID, 1);
        processThreads();

        writeSubscriberMessage(REMOVE_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads(5);
        assertReceiverNotRegistered();

        assertBuffersNotRegistered();
    }

    @Test(timeout = 1500)
    public void shouldBeAbleToHandleTermBufferRolloverCorrectly() throws Exception
    {
        writeSubscriberMessage(ADD_SUBSCRIBER, URI, ONE_CHANNEL);
        processThreads();

        final UdpDestination destination = udpDestination();
        final int packetsToFillBuffer = COMMAND_BUFFER_SZ / FAKE_PACKET_SIZE;
        final int iterations = 4 * packetsToFillBuffer;
        long termId = 0;

        for (int i = 0; i < iterations; i++)
        {
            if ((i % packetsToFillBuffer) == 0)
            {
                termId++;
            }

            sendDataFrame(destination, CHANNEL_ID, termId, i, FAKE_PACKET_SIZE);
            processThreads(2);
        }
    }

    private void assertReceivedDataFromNetwork() throws IOException
    {
        final List<Buffers> buffers1 = directory.mapTermFile(directory.receiverDir(), URI, SESSION_ID, CHANNEL_ID);
        final StateViewer stateViewer = new StateViewer(buffers1.get(0).stateBuffer());

        assertThat(stateViewer.tailVolatile(), is(greaterThan(0)));
    }

    private void assertBuffersNotRegistered()
    {
        exception.expect(IllegalArgumentException.class);
        bufferManagement.removeSubscriberChannel(udpDestination(), SESSION_ID, CHANNEL_ID);
    }

    private void assertReceiverNotRegistered()
    {
        assertNull(receiver.frameHandler(udpDestination()));
    }

    private void assertReceiverRegistered()
    {
        assertNotNull(receiver.frameHandler(udpDestination()));
    }

    private UdpDestination udpDestination()
    {
        return UdpDestination.parse(URI);
    }

    private void writeSubscriberMessage(final int msgTypeId, final String destination, final long[] channelIds)
        throws IOException
    {
        final RingBuffer adminCommands = buffers.mappedToMediaDriver();

        receiverMessage.wrap(writeBuffer, 0);

        receiverMessage.channelIds(channelIds)
                       .destination(destination);

        adminCommands.write(msgTypeId, writeBuffer, 0, receiverMessage.length());
    }

    private void sendDataFrame(final UdpDestination destination,
                               final long channelId,
                               final long termId,
                               final int seqNum,
                               final int amount)
        throws Exception
    {
        final int alignedAmount = BitUtil.align(amount, FRAME_ALIGNMENT);

        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        dataHeaderFlyweight.wrap(writeBuffer, 0);

        dataHeaderFlyweight.termOffset(seqNum)
                           .sessionId(SESSION_ID)
                           .channelId(channelId)
                           .termId(termId)
                           .version(HeaderFlyweight.CURRENT_VERSION)
                           .flags((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                           .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                           .frameLength(alignedAmount);

        sendBuffer.position(0);
        sendBuffer.putInt(dataHeaderFlyweight.dataOffset(), VALUE);
        sendBuffer.limit(alignedAmount);
        final int sent = senderChannel.send(sendBuffer, destination.remoteData());

        assertThat(sent, is(alignedAmount));
    }

    private void sendDataFrame(final UdpDestination destination, final long channelId, final int seqNum)
        throws Exception
    {
        sendDataFrame(destination, channelId, TERM_ID, seqNum, HEADER_LENGTH + SIZE_OF_INT);
    }

    private void processThreads()
    {
        processThreads(1);
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach(
            (i) ->
            {
                mediaConductor.process();
                receiver.process();
            }
        );
    }
}
