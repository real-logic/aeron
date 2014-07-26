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
package uk.co.real_logic.aeron.driver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.NewConnectionCmd;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.driver.BufferAndFrameHelper.newTestTermBuffers;

public class ReceiverTest
{
    public static final long LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final String URI = "udp://localhost:45678";
    private static final UdpChannel UDP_CHANNEL = UdpChannel.parse(URI);
    private static final int STREAM_ID = 10;
    private static final int TERM_ID = 3;
    private static final int SESSION_ID = 1;
    private static final byte[] FAKE_PAYLOAD = "Hello there, message!".getBytes();
    private static final byte[] NO_PAYLOAD = new byte[0];
    private static final int INITIAL_WINDOW_SIZE = MediaDriver.INITIAL_WINDOW_SIZE_DEFAULT;
    private static final PositionIndicator POSITION_INDICATOR = mock(PositionIndicator.class);

    public final LossHandler mockLossHandler = mock(LossHandler.class);
    private final NioSelector mockNioSelector = mock(NioSelector.class);
    private final TermBuffersFactory mockTermBuffersFactory = mock(TermBuffersFactory.class);
    private final ByteBuffer dataFrameBuffer = ByteBuffer.allocate(2 * 1024);
    private final AtomicBuffer dataBuffer = new AtomicBuffer(dataFrameBuffer);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusHeader = new StatusMessageFlyweight();

    private final TermBuffers termBuffers = newTestTermBuffers(LOG_BUFFER_SIZE, LogBufferDescriptor.STATE_BUFFER_LENGTH);
    private final EventLogger mockLogger = mock(EventLogger.class);

    private LogReader[] logReaders;
    private DatagramChannel senderChannel;

    private InetSocketAddress senderAddress = new InetSocketAddress("localhost", 40123);
    private Receiver receiver;
    private ReceiverProxy receiverProxy;
    private OneToOneConcurrentArrayQueue<? super Object> toConductorQueue;

    private ChannelReceiveEndpoint channelReceiveEndpoint;

    @Before
    public void setUp() throws Exception
    {
        when(POSITION_INDICATOR.position()).thenReturn(Long.MAX_VALUE - LOG_BUFFER_SIZE);
        when(mockLossHandler.activeTermId()).thenReturn(TERM_ID);

        final MediaDriver.DriverContext ctx = new MediaDriver.DriverContext()
            .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverNioSelector(mockNioSelector)
            .conductorNioSelector(mockNioSelector)
            .termBuffersFactory(mockTermBuffersFactory)
            .conductorTimerWheel(new TimerWheel(MediaDriver.CONDUCTOR_TICK_DURATION_US,
                                                TimeUnit.MICROSECONDS,
                                                MediaDriver.CONDUCTOR_TICKS_PER_WHEEL))
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverLogger(mockLogger);

        toConductorQueue = ctx.conductorCommandQueue();
        final DriverConductorProxy driverConductorProxy = new DriverConductorProxy(toConductorQueue);
        ctx.driverConductorProxy(driverConductorProxy);

        receiverProxy = new ReceiverProxy(ctx.receiverCommandQueue());

        receiver = new Receiver(ctx);

        senderChannel = DatagramChannel.open();
        senderChannel.bind(senderAddress);
        senderChannel.configureBlocking(false);

        logReaders = termBuffers.stream()
                                .map((rawLog) -> new LogReader(rawLog.logBuffer(), rawLog.stateBuffer()))
                                .toArray(LogReader[]::new);

        channelReceiveEndpoint = new ChannelReceiveEndpoint(UdpChannel.parse(URI), driverConductorProxy, mockLogger);
    }

    @After
    public void tearDown() throws Exception
    {
        channelReceiveEndpoint.close();
        senderChannel.close();
        receiver.close();
        receiver.nioSelector().selectNowWithoutProcessing();
    }

    @Test
    public void shouldCreateRcvTermAndSendSmOnZeroLengthData() throws Exception
    {
        EventLogger.logInvocation();

        receiverProxy.registerMediaEndpoint(channelReceiveEndpoint);
        receiverProxy.addSubscription(channelReceiveEndpoint, STREAM_ID);

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final DriverConnection connection =
            new DriverConnection(
                UDP_CHANNEL,
                SESSION_ID,
                STREAM_ID,
                TERM_ID,
                INITIAL_WINDOW_SIZE,
                termBuffers,
                mockLossHandler,
                channelReceiveEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID), POSITION_INDICATOR);

        final int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                final CreateConnectionCmd cmd = (CreateConnectionCmd)e;

                assertThat(cmd.channelEndpoint().udpTransport().udpChannel(), is(UDP_CHANNEL));
                assertThat(cmd.streamId(), is(STREAM_ID));
                assertThat(cmd.sessionId(), is(SESSION_ID));
                assertThat(cmd.termId(), is(TERM_ID));

                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(new NewConnectionCmd(channelReceiveEndpoint, connection));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        connection.sendPendingStatusMessages(1000);

        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddress = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusHeader.wrap(rcvBuffer);

        assertNotNull(rcvAddress);
        assertThat(rcvAddress.getPort(), is(UDP_CHANNEL.remoteData().getPort()));
        assertThat(statusHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusHeader.streamId(), is(STREAM_ID));
        assertThat(statusHeader.sessionId(), is(SESSION_ID));
        assertThat(statusHeader.termId(), is(TERM_ID));
        assertThat(statusHeader.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldInsertDataIntoLogAfterInitialExchange() throws Exception
    {
        EventLogger.logInvocation();

        receiverProxy.registerMediaEndpoint(channelReceiveEndpoint);
        receiverProxy.addSubscription(channelReceiveEndpoint, STREAM_ID);

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd(
                        channelReceiveEndpoint,
                        new DriverConnection(
                            UDP_CHANNEL,
                            SESSION_ID,
                            STREAM_ID,
                            TERM_ID,
                            INITIAL_WINDOW_SIZE,
                            termBuffers,
                            mockLossHandler,
                            channelReceiveEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                            POSITION_INDICATOR)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);
        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.streamId(), is(STREAM_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldNotOverwriteDataFrameWithHeartbeat() throws Exception
    {
        EventLogger.logInvocation();

        receiverProxy.registerMediaEndpoint(channelReceiveEndpoint);
        receiverProxy.addSubscription(channelReceiveEndpoint, STREAM_ID);

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd
                        (channelReceiveEndpoint,
                         new DriverConnection(
                             UDP_CHANNEL,
                             SESSION_ID,
                             STREAM_ID,
                             TERM_ID,
                             INITIAL_WINDOW_SIZE,
                             termBuffers,
                             mockLossHandler,
                             channelReceiveEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                             POSITION_INDICATOR)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);  // heartbeat with same term offset
        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.streamId(), is(STREAM_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldOverwriteHeartbeatWithDataFrame() throws Exception
    {
        EventLogger.logInvocation();

        receiverProxy.registerMediaEndpoint(channelReceiveEndpoint);
        receiverProxy.addSubscription(channelReceiveEndpoint, STREAM_ID);

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);

        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd(
                        channelReceiveEndpoint,
                        new DriverConnection(
                            UDP_CHANNEL,
                            SESSION_ID,
                            STREAM_ID,
                            TERM_ID,
                            INITIAL_WINDOW_SIZE,
                            termBuffers,
                            mockLossHandler,
                            channelReceiveEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                            POSITION_INDICATOR)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, NO_PAYLOAD);  // heartbeat with same term offset
        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        channelReceiveEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        messagesRead = logReaders[0].read(
            (buffer, offset, length) ->
            {
                dataHeader.wrap(buffer, offset);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.termId(), is(TERM_ID));
                assertThat(dataHeader.streamId(), is(STREAM_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termOffset(), is(0));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            }, Integer.MAX_VALUE
        );

        assertThat(messagesRead, is(1));
    }

    private void fillDataFrame(final DataHeaderFlyweight header, final int termOffset, final byte[] payload)
    {
        header.wrap(dataBuffer, 0);
        header.termOffset(termOffset)
              .termId(TERM_ID)
              .streamId(STREAM_ID)
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
