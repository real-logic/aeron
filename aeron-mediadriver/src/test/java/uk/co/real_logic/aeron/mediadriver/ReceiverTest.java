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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.buffer.TermBufferManager;
import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;
import uk.co.real_logic.aeron.mediadriver.cmd.NewConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.util.concurrent.AtomicArray;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class ReceiverTest
{
    public static final EventLogger LOGGER = new EventLogger(ReceiverTest.class);

    public static final long LOG_BUFFER_SIZE = (64 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    private static final String URI = "udp://localhost:45678";
    private static final UdpDestination destination = UdpDestination.parse(URI);
    private static final long CHANNEL_ID = 10;
    private static final long[] ONE_CHANNEL = {CHANNEL_ID};
    private static final long TERM_ID = 3;
    private static final long SESSION_ID = 1;
    private static final byte[] FAKE_PAYLOAD = "Hello thare, message!".getBytes();
    private static final byte[] NO_PAYLOAD = new byte[0];

    private final NioSelector mockNioSelector = mock(NioSelector.class);
    private final TermBufferManager mockTermBufferManager = mock(TermBufferManager.class);
    private final ByteBuffer dataFrameBuffer = ByteBuffer.allocate(2 * 1024);
    private final AtomicBuffer dataBuffer = new AtomicBuffer(dataFrameBuffer);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final QualifiedMessageFlyweight messageHeader = new QualifiedMessageFlyweight();
    private final StatusMessageFlyweight statusHeader = new StatusMessageFlyweight();

    private final TermBuffers termBuffers =
        BufferAndFrameUtils.createTestTermBuffers(LOG_BUFFER_SIZE, LogBufferDescriptor.STATE_BUFFER_LENGTH);

    private LogReader[] logReaders;

    private DatagramChannel senderChannel;
    private InetSocketAddress senderAddress = new InetSocketAddress("localhost", 40123);

    private Receiver receiver;
    private ReceiverProxy receiverProxy;
    private RingBuffer toConductorBuffer;

    @Before
    public void setUp() throws Exception
    {
        final MediaDriver.MediaDriverContext ctx = new MediaDriver.MediaDriverContext()
            .driverCommandBuffer(MediaDriver.COMMAND_BUFFER_SZ)
            .receiverNioSelector(mockNioSelector)
            .conductorNioSelector(mockNioSelector)
            .bufferManagement(mockTermBufferManager)
            .conductorTimerWheel(new TimerWheel(MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US,
                                 TimeUnit.MICROSECONDS,
                                 MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL))
            .connectedSubscriptions(new AtomicArray<>())
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024));

        toConductorBuffer = ctx.driverCommandBuffer();
        ctx.mediaConductorProxy(new MediaConductorProxy(toConductorBuffer));

        receiverProxy = new ReceiverProxy(ctx.receiverCommandQueue());

        receiver = new Receiver(ctx);

        senderChannel = DatagramChannel.open();
        senderChannel.bind(senderAddress);
        senderChannel.configureBlocking(false);

        logReaders = termBuffers.stream()
                                .map((rawLog) -> new LogReader(rawLog.logBuffer(), rawLog.stateBuffer()))
                                .toArray(LogReader[]::new);
    }

    @After
    public void tearDown() throws Exception
    {
        senderChannel.close();
        receiver.close();
        receiver.nioSelector().selectNowWithoutProcessing();
    }

    @Test
    public void shouldCreateRcvTermAndSendSmOnZeroLengthData() throws Exception
    {
        LOGGER.logInvocation();

        receiverProxy.addSubscription(URI, ONE_CHANNEL);  // ADD_SUBSCRIPTION from client

        receiver.doWork();

        DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final int messagesRead = toConductorBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.CREATE_CONNECTED_SUBSCRIPTION));
                messageHeader.wrap(buffer, index);
                assertThat(messageHeader.termId(), is(TERM_ID));
                assertThat(messageHeader.channelId(), is(CHANNEL_ID));
                assertThat(messageHeader.sessionId(), is(SESSION_ID));
                assertThat(messageHeader.destination(), is(URI));

                // pass in new term buffer from media conductor, which should trigger SM
                receiverProxy.newConnectedSubscription(
                    new NewConnectedSubscriptionCmd(destination, SESSION_ID, CHANNEL_ID, TERM_ID, termBuffers));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddress = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusHeader.wrap(rcvBuffer);

        assertNotNull(rcvAddress);
        assertThat(rcvAddress.getPort(), is(destination.remoteData().getPort()));
        assertThat(statusHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusHeader.channelId(), is(ONE_CHANNEL[0]));
        assertThat(statusHeader.sessionId(), is(SESSION_ID));
        assertThat(statusHeader.termId(), is(TERM_ID));
        assertThat(statusHeader.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldInsertDataIntoLogAfterInitialExchange() throws Exception
    {
        LOGGER.logInvocation();

        receiverProxy.addSubscription(URI, ONE_CHANNEL);  // ADD_SUBSCRIPTION from client

        receiver.doWork();

        DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
              assertThat(msgTypeId, is(ControlProtocolEvents.CREATE_CONNECTED_SUBSCRIPTION));
              // pass in new term buffer from media conductor, which should trigger SM
              receiverProxy.newConnectedSubscription(
                  new NewConnectedSubscriptionCmd(destination, SESSION_ID,CHANNEL_ID, TERM_ID, termBuffers));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);
        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0L));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldNotOverwriteDataFrameWithHeartbeat() throws Exception
    {
        LOGGER.logInvocation();

        receiverProxy.addSubscription(URI, ONE_CHANNEL);  // ADD_SUBSCRIPTION from client

        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
              assertThat(msgTypeId, is(ControlProtocolEvents.CREATE_CONNECTED_SUBSCRIPTION));
              // pass in new term buffer from media conductor, which should trigger SM
              receiverProxy.newConnectedSubscription(
                  new NewConnectedSubscriptionCmd(destination, SESSION_ID, CHANNEL_ID, TERM_ID, termBuffers));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);  // heartbeat with same term offset
        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0L));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldOverwriteHeartbeatWithDataFrame() throws Exception
    {
        LOGGER.logInvocation();

        receiverProxy.addSubscription(URI, ONE_CHANNEL);  // ADD_SUBSCRIPTION from client

        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
              assertThat(msgTypeId, is(ControlProtocolEvents.CREATE_CONNECTED_SUBSCRIPTION));
              // pass in new term buffer from media conductor, which should trigger SM
              receiverProxy.newConnectedSubscription(
                  new NewConnectedSubscriptionCmd(destination, SESSION_ID, CHANNEL_ID, TERM_ID, termBuffers));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);  // heartbeat with same term offset
        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0L));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    @Test
    @Ignore("does not work correctly yet")
    public void shouldBeAbleToHandleTermBufferRolloverCorrectly() throws Exception
    {
        LOGGER.logInvocation();

        receiverProxy.addSubscription(URI, ONE_CHANNEL);  // ADD_SUBSCRIPTION from client

        receiver.doWork();

        DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final int messagesRead = toConductorBuffer.read(
            (msgTypeId, buffer, index, length) ->
                receiverProxy.newConnectedSubscription(
                    new NewConnectedSubscriptionCmd(destination, SESSION_ID, CHANNEL_ID, TERM_ID, termBuffers)));

        assertThat(messagesRead, is(1));

        final int packetsToFillBuffer = MediaDriver.COMMAND_BUFFER_SZ / FAKE_PAYLOAD.length;
        final int iterations = 4 * packetsToFillBuffer;
        final int offset = 0;

        for (int i = 0; i < iterations; i++)
        {
            fillDataFrame(dataHeader, offset, FAKE_PAYLOAD);
            frameHandler.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);
            receiver.doWork();
        }
    }

    private void fillDataFrame(final DataHeaderFlyweight header, final int termOffset, final byte[] payload)
    {
        header.wrap(dataBuffer, 0);
        header.termOffset(termOffset)
              .termId(TERM_ID)
              .channelId(CHANNEL_ID)
              .sessionId(SESSION_ID)
              .frameLength(DataHeaderFlyweight.HEADER_LENGTH + payload.length)
              .headerType(HeaderFlyweight.HDR_TYPE_DATA)
              .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
              .version(HeaderFlyweight.CURRENT_VERSION);

        if (0 < payload.length)
        {
            dataBuffer.putBytes(header.dataOffset(), payload);
        }
    }
}
