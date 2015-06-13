/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import uk.co.real_logic.aeron.driver.buffer.RawLogPartition;
import uk.co.real_logic.aeron.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.logbuffer.TermReader;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.driver.buffer.RawLog;
import uk.co.real_logic.aeron.driver.buffer.RawLogFactory;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.DriverConductorCmd;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.TransportPoller;
import uk.co.real_logic.aeron.driver.media.UdpChannel;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.AtomicLongPosition;
import uk.co.real_logic.agrona.concurrent.status.Position;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.driver.LogBufferHelper.newTestLogBuffers;
import static uk.co.real_logic.agrona.BitUtil.align;

public class ReceiverTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final String URI = "udp://localhost:45678";
    private static final UdpChannel UDP_CHANNEL = UdpChannel.parse(URI);
    private static final long CORRELATION_ID = 20;
    private static final int STREAM_ID = 10;
    private static final int INITIAL_TERM_ID = 3;
    private static final int ACTIVE_TERM_ID = 3;
    private static final int SESSION_ID = 1;
    private static final int INITIAL_TERM_OFFSET = 0;
    private static final int ACTIVE_INDEX = indexByTerm(ACTIVE_TERM_ID, ACTIVE_TERM_ID);
    private static final byte[] FAKE_PAYLOAD = "Hello there, message!".getBytes();
    private static final int INITIAL_WINDOW_LENGTH = Configuration.INITIAL_WINDOW_LENGTH_DEFAULT;
    private static final long STATUS_MESSAGE_TIMEOUT = Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS;
    private static final InetSocketAddress SOURCE_ADDRESS = new InetSocketAddress("localhost", 45679);

    private static final ReadablePosition POSITION = mock(ReadablePosition.class);
    private static final List<ReadablePosition> POSITIONS = Collections.singletonList(POSITION);

    private final FeedbackDelayGenerator mockFeedbackDelayGenerator = mock(FeedbackDelayGenerator.class);
    private final TransportPoller mockTransportPoller = mock(TransportPoller.class);
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final RawLogFactory mockRawLogFactory = mock(RawLogFactory.class);
    private final Position mockHighestReceivedPosition = spy(new AtomicLongPosition());
    private final ByteBuffer dataFrameBuffer = ByteBuffer.allocateDirect(2 * 1024);
    private final UnsafeBuffer dataBuffer = new UnsafeBuffer(dataFrameBuffer);
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);
    private final UnsafeBuffer setupBuffer = new UnsafeBuffer(setupFrameBuffer);
    private final Consumer<Throwable> mockErrorHandler = mock(Consumer.class);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusHeader = new StatusMessageFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private long currentTime = 0;
    private final NanoClock clock = () -> currentTime;

    private final RawLog rawLog = newTestLogBuffers(TERM_BUFFER_LENGTH, TERM_META_DATA_LENGTH);
    private final EventLogger mockLogger = mock(EventLogger.class);
    private final TimerWheel timerWheel = new TimerWheel(
        clock, Configuration.CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, Configuration.CONDUCTOR_TICKS_PER_WHEEL);

    private final Header header = new Header(INITIAL_TERM_ID, TERM_BUFFER_LENGTH);
    private UnsafeBuffer[] termBuffers;
    private DatagramChannel senderChannel;

    private InetSocketAddress senderAddress = new InetSocketAddress("localhost", 40123);
    private Receiver receiver;
    private ReceiverProxy receiverProxy;
    private OneToOneConcurrentArrayQueue<DriverConductorCmd> toConductorQueue;

    private ReceiveChannelEndpoint receiveChannelEndpoint;

    // TODO rework test to use proxies rather than the command queues.

    @Before
    public void setUp() throws Exception
    {
        when(POSITION.getVolatile())
            .thenReturn(computePosition(ACTIVE_TERM_ID, 0, Integer.numberOfTrailingZeros(TERM_BUFFER_LENGTH), ACTIVE_TERM_ID));
        when(mockSystemCounters.statusMessagesSent()).thenReturn(mock(AtomicCounter.class));
        when(mockSystemCounters.flowControlUnderRuns()).thenReturn(mock(AtomicCounter.class));
        when(mockSystemCounters.bytesReceived()).thenReturn(mock(AtomicCounter.class));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .toConductorFromReceiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverNioSelector(mockTransportPoller)
            .senderNioSelector(mockTransportPoller)
            .rawLogBuffersFactory(mockRawLogFactory)
            .conductorTimerWheel(timerWheel)
            .systemCounters(mockSystemCounters)
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .eventLogger(mockLogger);

        toConductorQueue = ctx.toConductorFromReceiverCommandQueue();
        final DriverConductorProxy driverConductorProxy =
            new DriverConductorProxy(ThreadingMode.DEDICATED, toConductorQueue, mock(AtomicCounter.class));
        ctx.fromReceiverDriverConductorProxy(driverConductorProxy);

        receiverProxy = new ReceiverProxy(ThreadingMode.DEDICATED, ctx.receiverCommandQueue(), mock(AtomicCounter.class));

        receiver = new Receiver(ctx);

        senderChannel = DatagramChannel.open();
        senderChannel.bind(senderAddress);
        senderChannel.configureBlocking(false);

        termBuffers = rawLog
            .stream()
            .map(RawLogPartition::termBuffer)
            .toArray(UnsafeBuffer[]::new);

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            UdpChannel.parse(URI),
            driverConductorProxy,
            receiver,
            mockLogger,
            mockSystemCounters,
            (address, buffer, length) -> false);
    }

    @After
    public void tearDown() throws Exception
    {
        receiveChannelEndpoint.close();
        senderChannel.close();
        receiver.onClose();
    }

    @Test
    public void shouldCreateRcvTermAndSendSmOnSetup() throws Exception
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final NetworkConnection connection = new NetworkConnection(
            CORRELATION_ID, receiveChannelEndpoint,
            senderAddress,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            ACTIVE_TERM_ID,
            INITIAL_TERM_OFFSET,
            INITIAL_WINDOW_LENGTH,
            rawLog,
            timerWheel,
            mockFeedbackDelayGenerator,
            POSITIONS,
            mockHighestReceivedPosition,
            clock,
            mockSystemCounters,
            SOURCE_ADDRESS);

        final int messagesRead = toConductorQueue.drain(
            (e) ->
            {
                final CreateConnectionCmd cmd = (CreateConnectionCmd)e;

                assertThat(cmd.channelEndpoint().udpChannel(), is(UDP_CHANNEL));
                assertThat(cmd.streamId(), is(STREAM_ID));
                assertThat(cmd.sessionId(), is(SESSION_ID));
                assertThat(cmd.termId(), is(ACTIVE_TERM_ID));

                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(receiveChannelEndpoint, connection);
            });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        connection.trackRebuild();
        connection.sendPendingStatusMessage(1000, STATUS_MESSAGE_TIMEOUT);

        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddress = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusHeader.wrap(rcvBuffer);

        assertNotNull(rcvAddress);
        assertThat(rcvAddress.getPort(), is(UDP_CHANNEL.remoteData().getPort()));
        assertThat(statusHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusHeader.streamId(), is(STREAM_ID));
        assertThat(statusHeader.sessionId(), is(SESSION_ID));
        assertThat(statusHeader.consumptionTermId(), is(ACTIVE_TERM_ID));
        assertThat(statusHeader.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldInsertDataIntoLogAfterInitialExchange() throws Exception
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final int commandsRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    receiveChannelEndpoint,
                    new NetworkConnection(
                        CORRELATION_ID, receiveChannelEndpoint,
                        senderAddress,
                        SESSION_ID,
                        STREAM_ID,
                        INITIAL_TERM_ID,
                        ACTIVE_TERM_ID,
                        INITIAL_TERM_OFFSET,
                        INITIAL_WINDOW_LENGTH,
                        rawLog,
                        timerWheel,
                        mockFeedbackDelayGenerator,
                        POSITIONS,
                        mockHighestReceivedPosition,
                        clock,
                        mockSystemCounters,
                        SOURCE_ADDRESS));
            });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final long readOutcome = TermReader.read(
            termBuffers[ACTIVE_INDEX],
            INITIAL_TERM_OFFSET,
            (buffer, offset, length, header) ->
            {
                assertThat(header.type(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.termId(), is(ACTIVE_TERM_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.termOffset(), is(0));
                assertThat(header.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            },
            Integer.MAX_VALUE,
            header,
            mockErrorHandler);

        assertThat(TermReader.fragmentsRead(readOutcome), is(1));
    }

    @Test
    public void shouldNotOverwriteDataFrameWithHeartbeat() throws Exception
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final int commandsRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    receiveChannelEndpoint,
                    new NetworkConnection(
                        CORRELATION_ID, receiveChannelEndpoint,
                        senderAddress,
                        SESSION_ID,
                        STREAM_ID,
                        INITIAL_TERM_ID,
                        ACTIVE_TERM_ID,
                        INITIAL_TERM_OFFSET,
                        INITIAL_WINDOW_LENGTH,
                        rawLog,
                        timerWheel,
                        mockFeedbackDelayGenerator,
                        POSITIONS,
                        mockHighestReceivedPosition,
                        clock,
                        mockSystemCounters,
                        SOURCE_ADDRESS));
            });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final long readOutcome = TermReader.read(
            termBuffers[ACTIVE_INDEX],
            INITIAL_TERM_OFFSET,
            (buffer, offset, length, header) ->
            {
                assertThat(header.type(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.termId(), is(ACTIVE_TERM_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.termOffset(), is(0));
                assertThat(header.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            },
            Integer.MAX_VALUE,
            header,
            mockErrorHandler);

        assertThat(TermReader.fragmentsRead(readOutcome), is(1));
    }

    @Test
    public void shouldOverwriteHeartbeatWithDataFrame() throws Exception
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final int commandsRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    receiveChannelEndpoint,
                    new NetworkConnection(
                        CORRELATION_ID, receiveChannelEndpoint,
                        senderAddress,
                        SESSION_ID,
                        STREAM_ID,
                        INITIAL_TERM_ID,
                        ACTIVE_TERM_ID,
                        INITIAL_TERM_OFFSET,
                        INITIAL_WINDOW_LENGTH,
                        rawLog,
                        timerWheel,
                        mockFeedbackDelayGenerator,
                        POSITIONS,
                        mockHighestReceivedPosition,
                        clock,
                        mockSystemCounters,
                        SOURCE_ADDRESS));
            });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress);

        final long readOutcome = TermReader.read(
            termBuffers[ACTIVE_INDEX],
            INITIAL_TERM_OFFSET,
            (buffer, offset, length, header) ->
            {
                assertThat(header.type(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.termId(), is(ACTIVE_TERM_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.termOffset(), is(0));
                assertThat(header.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            },
            Integer.MAX_VALUE,
            header,
            mockErrorHandler);

        assertThat(TermReader.fragmentsRead(readOutcome), is(1));
    }

    @Test
    public void shouldHandleNonZeroTermOffsetCorrectly() throws Exception
    {
        final int initialTermOffset = align(TERM_BUFFER_LENGTH / 16, FrameDescriptor.FRAME_ALIGNMENT);
        final int alignedDataFrameLength =
            align(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);

        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader, initialTermOffset);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, setupHeader.frameLength(), senderAddress);

        final int commandsRead = toConductorQueue.drain(
            (e) ->
            {
                assertTrue(e instanceof CreateConnectionCmd);
                // pass in new term buffer from conductor, which should trigger SM
                receiverProxy.newConnection(
                    receiveChannelEndpoint,
                    new NetworkConnection(
                        CORRELATION_ID, receiveChannelEndpoint,
                        senderAddress,
                        SESSION_ID,
                        STREAM_ID,
                        INITIAL_TERM_ID,
                        ACTIVE_TERM_ID,
                        initialTermOffset,
                        INITIAL_WINDOW_LENGTH,
                        rawLog,
                        timerWheel,
                        mockFeedbackDelayGenerator,
                        POSITIONS,
                        mockHighestReceivedPosition,
                        clock,
                        mockSystemCounters,
                        SOURCE_ADDRESS));
            });

        assertThat(commandsRead, is(1));

        verify(mockHighestReceivedPosition).setOrdered(initialTermOffset);

        receiver.doWork();

        fillDataFrame(dataHeader, initialTermOffset, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, alignedDataFrameLength, senderAddress);

        verify(mockHighestReceivedPosition).setOrdered(initialTermOffset + alignedDataFrameLength);

        final long readOutcome = TermReader.read(
            termBuffers[ACTIVE_INDEX],
            initialTermOffset,
            (buffer, offset, length, header) ->
            {
                assertThat(header.type(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(header.termId(), is(ACTIVE_TERM_ID));
                assertThat(header.streamId(), is(STREAM_ID));
                assertThat(header.sessionId(), is(SESSION_ID));
                assertThat(header.termOffset(), is(initialTermOffset));
                assertThat(header.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length));
            },
            Integer.MAX_VALUE,
            header,
            mockErrorHandler);

        assertThat(TermReader.fragmentsRead(readOutcome), is(1));
    }

    private void fillDataFrame(final DataHeaderFlyweight header, final int termOffset, final byte[] payload)
    {
        header.wrap(dataBuffer, 0);
        header.termOffset(termOffset)
              .termId(ACTIVE_TERM_ID)
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
        fillSetupFrame(header, 0);
    }

    private void fillSetupFrame(final SetupFlyweight header, final int termOffset)
    {
        header.wrap(setupBuffer, 0);
        header.streamId(STREAM_ID)
              .sessionId(SESSION_ID)
              .initialTermId(INITIAL_TERM_ID)
              .activeTermId(ACTIVE_TERM_ID)
              .termOffset(termOffset)
              .frameLength(SetupFlyweight.HEADER_LENGTH)
              .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
              .flags((byte)0)
              .version(HeaderFlyweight.CURRENT_VERSION);
    }
}
