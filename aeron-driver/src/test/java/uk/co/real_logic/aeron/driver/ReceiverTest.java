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
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.NewConnectionCmd;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.driver.BufferAndFrameHelper.newTestTermBuffers;

public class ReceiverTest
{
    private static final int LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final String URI = "udp://localhost:45678";
    private static final UdpChannel UDP_CHANNEL = UdpChannel.parse(URI);
    private static final int STREAM_ID = 10;
    private static final int TERM_ID = 3;
    private static final int SESSION_ID = 1;
    private static final byte[] FAKE_PAYLOAD = "Hello there, message!".getBytes();
    private static final int INITIAL_WINDOW_SIZE = Configuration.INITIAL_WINDOW_SIZE_DEFAULT;
    private static final long STATUS_MESSAGE_TIMEOUT = Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS;
    private static final PositionIndicator POSITION_INDICATOR = mock(PositionIndicator.class);

    private final LossHandler mockLossHandler = mock(LossHandler.class);
    private final NioSelector mockNioSelector = mock(NioSelector.class);
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final TermBuffersFactory mockTermBuffersFactory = mock(TermBuffersFactory.class);
    private final PositionReporter mockCompletedReceivedPosition = mock(PositionReporter.class);
    private final PositionReporter mockHighestReceivedPosition = mock(PositionReporter.class);
    private final ByteBuffer dataFrameBuffer = ByteBuffer.allocate(2 * 1024);
    private final AtomicBuffer dataBuffer = new AtomicBuffer(dataFrameBuffer);
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocate(SetupFlyweight.HEADER_LENGTH);
    private final AtomicBuffer setupBuffer = new AtomicBuffer(setupFrameBuffer);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusHeader = new StatusMessageFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private long currentTime = 0;
    private final LongSupplier clock = () -> currentTime;

    private final TermBuffers termBuffers = newTestTermBuffers(LOG_BUFFER_SIZE, LogBufferDescriptor.STATE_BUFFER_LENGTH);
    private final EventLogger mockLogger = mock(EventLogger.class);
    private final TimerWheel timerWheel = new TimerWheel(
        clock, Configuration.CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, Configuration.CONDUCTOR_TICKS_PER_WHEEL);

    private LogReader[] logReaders;
    private DatagramChannel senderChannel;

    private InetSocketAddress senderAddress = new InetSocketAddress("localhost", 40123);
    private Receiver receiver;
    private ReceiverProxy receiverProxy;
    private OneToOneConcurrentArrayQueue<Object> toConductorQueue;

    private ReceiveChannelEndpoint receiveChannelEndpoint;

    @Before
    public void setUp() throws Exception
    {
        when(POSITION_INDICATOR.position())
            .thenReturn(TermHelper.calculatePosition(TERM_ID, 0, Integer.numberOfTrailingZeros(LOG_BUFFER_SIZE), TERM_ID));
        when(mockLossHandler.activeTermId()).thenReturn(TERM_ID);
        when(mockSystemCounters.statusMessagesSent()).thenReturn(mock(AtomicCounter.class));
        when(mockSystemCounters.flowControlUnderRuns()).thenReturn(mock(AtomicCounter.class));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverNioSelector(mockNioSelector)
            .conductorNioSelector(mockNioSelector)
            .termBuffersFactory(mockTermBuffersFactory)
            .conductorTimerWheel(timerWheel)
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .eventLogger(mockLogger);

        toConductorQueue = ctx.conductorCommandQueue();
        final DriverConductorProxy driverConductorProxy = new DriverConductorProxy(toConductorQueue);
        ctx.driverConductorProxy(driverConductorProxy);

        receiverProxy = new ReceiverProxy(ctx.receiverCommandQueue());

        receiver = new Receiver(ctx);

        senderChannel = DatagramChannel.open();
        senderChannel.bind(senderAddress);
        senderChannel.configureBlocking(false);

        logReaders =
            termBuffers.stream()
                       .map((rawLog) -> new LogReader(rawLog.logBuffer(), rawLog.stateBuffer()))
                       .toArray(LogReader[]::new);

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            UdpChannel.parse(URI), driverConductorProxy, mockLogger, (address, length) -> false);
    }

    @After
    public void tearDown() throws Exception
    {
        receiveChannelEndpoint.close();
        senderChannel.close();
        receiver.close();
        receiver.nioSelector().selectNowWithoutProcessing();
    }

    @Test
    public void shouldCreateRcvTermAndSendSmOnSetup() throws Exception
    {
        receiverProxy.registerMediaEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupFrame(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final DriverConnection connection = new DriverConnection(
            receiveChannelEndpoint,
            SESSION_ID,
            STREAM_ID,
            TERM_ID,
            INITIAL_WINDOW_SIZE,
            STATUS_MESSAGE_TIMEOUT,
            termBuffers,
            mockLossHandler,
            receiveChannelEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
            POSITION_INDICATOR,
            mockCompletedReceivedPosition,
            mockHighestReceivedPosition,
            clock,
            mockSystemCounters,
            mockLogger);

        final int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                final CreateConnectionCmd cmd = (CreateConnectionCmd)e;

                assertThat(cmd.channelEndpoint().udpTransport().udpChannel(), is(UDP_CHANNEL));
                assertThat(cmd.streamId(), is(STREAM_ID));
                assertThat(cmd.sessionId(), is(SESSION_ID));
                assertThat(cmd.termId(), is(TERM_ID));

                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(new NewConnectionCmd(receiveChannelEndpoint, connection));
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
        receiverProxy.registerMediaEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupFrame(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd(
                        receiveChannelEndpoint,
                        new DriverConnection(
                            receiveChannelEndpoint,
                            SESSION_ID,
                            STREAM_ID,
                            TERM_ID,
                            INITIAL_WINDOW_SIZE,
                            STATUS_MESSAGE_TIMEOUT,
                            termBuffers,
                            mockLossHandler,
                            receiveChannelEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                            POSITION_INDICATOR,
                            mockCompletedReceivedPosition,
                            mockHighestReceivedPosition,
                            clock,
                            mockSystemCounters,
                            mockLogger)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);
        receiveChannelEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

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
            },
            Integer.MAX_VALUE);

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldNotOverwriteDataFrameWithHeartbeat() throws Exception
    {
        receiverProxy.registerMediaEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupFrame(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd
                        (receiveChannelEndpoint,
                            new DriverConnection(
                                receiveChannelEndpoint,
                                SESSION_ID,
                                STREAM_ID,
                                TERM_ID,
                                INITIAL_WINDOW_SIZE,
                                STATUS_MESSAGE_TIMEOUT,
                                termBuffers,
                                mockLossHandler,
                                receiveChannelEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                                POSITION_INDICATOR,
                                mockCompletedReceivedPosition,
                                mockHighestReceivedPosition,
                                clock,
                                mockSystemCounters,
                                mockLogger)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

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
            },
            Integer.MAX_VALUE);

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldOverwriteHeartbeatWithDataFrame() throws Exception
    {
        receiverProxy.registerMediaEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupFrame(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    new NewConnectionCmd(
                        receiveChannelEndpoint,
                        new DriverConnection(
                            receiveChannelEndpoint,
                            SESSION_ID,
                            STREAM_ID,
                            TERM_ID,
                            INITIAL_WINDOW_SIZE,
                            STATUS_MESSAGE_TIMEOUT,
                            termBuffers,
                            mockLossHandler,
                            receiveChannelEndpoint.composeStatusMessageSender(senderAddress, SESSION_ID, STREAM_ID),
                            POSITION_INDICATOR,
                            mockCompletedReceivedPosition,
                            mockHighestReceivedPosition,
                            clock,
                            mockSystemCounters,
                            mockLogger)));
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataFrame(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

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
            },
            Integer.MAX_VALUE);

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

    private void fillSetupFrame(final SetupFlyweight header)
    {
        header.wrap(setupBuffer, 0);
        header.streamId(STREAM_ID)
              .sessionId(SESSION_ID)
              .termId(TERM_ID)
              .frameLength(SetupFlyweight.HEADER_LENGTH)
              .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
              .flags((byte)0)
              .version(HeaderFlyweight.CURRENT_VERSION);
    }
}
