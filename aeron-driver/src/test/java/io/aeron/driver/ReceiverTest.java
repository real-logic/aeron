/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.buffer.*;
import io.aeron.driver.media.*;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.*;
import io.aeron.protocol.*;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.*;
import org.junit.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.agrona.BitUtil.align;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class ReceiverTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_BUFFER_LENGTH);
    private static final String URI = "aeron:udp?endpoint=localhost:45678";
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
    private static final ReadablePosition[] POSITIONS = new ReadablePosition[]{ POSITION };

    private final FeedbackDelayGenerator mockFeedbackDelayGenerator = mock(FeedbackDelayGenerator.class);
    private final DataTransportPoller mockDataTransportPoller = mock(DataTransportPoller.class);
    private final ControlTransportPoller mockControlTransportPoller = mock(ControlTransportPoller.class);

    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final RawLogFactory mockRawLogFactory = mock(RawLogFactory.class);
    private final Position mockHighestReceivedPosition = spy(new AtomicLongPosition());
    private final Position mockRebuildPosition = spy(new AtomicLongPosition());
    private final Position mockSubscriberPosition = mock(Position.class);
    private final ByteBuffer dataFrameBuffer = ByteBuffer.allocateDirect(2 * 1024);
    private final UnsafeBuffer dataBuffer = new UnsafeBuffer(dataFrameBuffer);
    private final ByteBuffer setupFrameBuffer = ByteBuffer.allocateDirect(SetupFlyweight.HEADER_LENGTH);
    private final UnsafeBuffer setupBuffer = new UnsafeBuffer(setupFrameBuffer);

    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusHeader = new StatusMessageFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private long currentTime = 0;
    private final NanoClock nanoClock = () -> currentTime;
    private final EpochClock epochClock = mock(EpochClock.class);
    private final LossReport lossReport = mock(LossReport.class);

    private final RawLog rawLog = LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH);

    private final Header header = new Header(INITIAL_TERM_ID, TERM_BUFFER_LENGTH);
    private UnsafeBuffer[] termBuffers;
    private DatagramChannel senderChannel;

    private InetSocketAddress senderAddress = new InetSocketAddress("localhost", 40123);
    private Receiver receiver;
    private ReceiverProxy receiverProxy;
    private final ManyToOneConcurrentArrayQueue<Runnable> toConductorQueue =
        new ManyToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY);

    private ReceiveChannelEndpoint receiveChannelEndpoint;
    private CongestionControl congestionControl = mock(CongestionControl.class);

    // TODO rework test to use proxies rather than the command queues.

    @Before
    public void setUp() throws Exception
    {
        when(POSITION.getVolatile())
            .thenReturn(computePosition(ACTIVE_TERM_ID, 0, POSITION_BITS_TO_SHIFT, ACTIVE_TERM_ID));
        when(mockSystemCounters.get(any())).thenReturn(mock(AtomicCounter.class));
        when(congestionControl.onTrackRebuild(
            anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyBoolean()))
            .thenReturn(CongestionControlUtil.packOutcome(INITIAL_WINDOW_LENGTH, false));
        when(congestionControl.initialWindowLength()).thenReturn(INITIAL_WINDOW_LENGTH);

        final CachedNanoClock mockCachedNanoClock = mock(CachedNanoClock.class);
        when(mockCachedNanoClock.nanoTime()).thenAnswer((invocation) -> currentTime);

        final DriverConductorProxy driverConductorProxy =
            new DriverConductorProxy(ThreadingMode.DEDICATED, toConductorQueue, mock(AtomicCounter.class));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .driverCommandQueue(toConductorQueue)
            .dataTransportPoller(mockDataTransportPoller)
            .controlTransportPoller(mockControlTransportPoller)
            .rawLogBuffersFactory(mockRawLogFactory)
            .systemCounters(mockSystemCounters)
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
            .nanoClock(() -> currentTime)
            .cachedNanoClock(mockCachedNanoClock)
            .driverConductorProxy(driverConductorProxy);

        receiverProxy = new ReceiverProxy(
            ThreadingMode.DEDICATED, ctx.receiverCommandQueue(), mock(AtomicCounter.class));

        ctx.receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals(ctx));

        receiver = new Receiver(ctx);
        receiverProxy.receiver(receiver);

        senderChannel = DatagramChannel.open();
        senderChannel.bind(senderAddress);
        senderChannel.configureBlocking(false);

        termBuffers = rawLog.termBuffers();

        final MediaDriver.Context context = new MediaDriver.Context()
            .systemCounters(mockSystemCounters);

        context.receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals(context));

        receiveChannelEndpoint = new ReceiveChannelEndpoint(
            UdpChannel.parse(URI),
            new DataPacketDispatcher(driverConductorProxy, receiver),
            mock(AtomicCounter.class),
            context);
    }

    @After
    public void tearDown() throws Exception
    {
        receiveChannelEndpoint.close();
        senderChannel.close();
        receiver.onClose();
    }

    @Test(timeout = 10000)
    public void shouldCreateRcvTermAndSendSmOnSetup() throws Exception
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final PublicationImage image = new PublicationImage(
            CORRELATION_ID,
            Configuration.IMAGE_LIVENESS_TIMEOUT_NS,
            receiveChannelEndpoint,
            0,
            senderAddress,
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            ACTIVE_TERM_ID,
            INITIAL_TERM_OFFSET,
            rawLog,
            mockFeedbackDelayGenerator,
            POSITIONS,
            mockHighestReceivedPosition,
            mockRebuildPosition,
            nanoClock,
            nanoClock,
            epochClock,
            mockSystemCounters,
            SOURCE_ADDRESS,
            congestionControl,
            lossReport,
            true);

        final int messagesRead = toConductorQueue.drain((e) ->
        {
            // pass in new term buffer from conductor, which should trigger SM
            receiverProxy.newPublicationImage(receiveChannelEndpoint, image);
        });

        assertThat(messagesRead, is(1));

        receiver.doWork();

        image.trackRebuild(currentTime + (2 * STATUS_MESSAGE_TIMEOUT), STATUS_MESSAGE_TIMEOUT);
        image.sendPendingStatusMessage();

        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        InetSocketAddress rcvAddress;

        do
        {
            rcvAddress = (InetSocketAddress)senderChannel.receive(rcvBuffer);
        }
        while (null == rcvAddress);

        statusHeader.wrap(new UnsafeBuffer(rcvBuffer));

        assertNotNull(rcvAddress);
        assertThat(rcvAddress.getPort(), is(UDP_CHANNEL.remoteData().getPort()));
        assertThat(statusHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusHeader.streamId(), is(STREAM_ID));
        assertThat(statusHeader.sessionId(), is(SESSION_ID));
        assertThat(statusHeader.consumptionTermId(), is(ACTIVE_TERM_ID));
        assertThat(statusHeader.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    @Test
    public void shouldInsertDataIntoLogAfterInitialExchange()
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final int commandsRead = toConductorQueue.drain((e) ->
        {
            // pass in new term buffer from conductor, which should trigger SM
            final PublicationImage image = new PublicationImage(
                CORRELATION_ID,
                Configuration.IMAGE_LIVENESS_TIMEOUT_NS,
                receiveChannelEndpoint,
                0,
                senderAddress,
                SESSION_ID,
                STREAM_ID,
                INITIAL_TERM_ID,
                ACTIVE_TERM_ID,
                INITIAL_TERM_OFFSET,
                rawLog,
                mockFeedbackDelayGenerator,
                POSITIONS,
                mockHighestReceivedPosition,
                mockRebuildPosition,
                nanoClock,
                nanoClock,
                epochClock,
                mockSystemCounters,
                SOURCE_ADDRESS,
                congestionControl,
                lossReport,
                true);

            receiverProxy.newPublicationImage(receiveChannelEndpoint, image);
        });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress, 0);

        final int readOutcome = TermReader.read(
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
            mockErrorHandler,
            0,
            mockSubscriberPosition);

        assertThat(readOutcome, is(1));
    }

    @Test
    public void shouldNotOverwriteDataFrameWithHeartbeat()
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final int commandsRead = toConductorQueue.drain((e) ->
        {
            // pass in new term buffer from conductor, which should trigger SM
            final PublicationImage image = new PublicationImage(
                CORRELATION_ID,
                Configuration.IMAGE_LIVENESS_TIMEOUT_NS,
                receiveChannelEndpoint,
                0,
                senderAddress,
                SESSION_ID,
                STREAM_ID,
                INITIAL_TERM_ID,
                ACTIVE_TERM_ID,
                INITIAL_TERM_OFFSET,
                rawLog,
                mockFeedbackDelayGenerator,
                POSITIONS,
                mockHighestReceivedPosition,
                mockRebuildPosition,
                nanoClock,
                nanoClock,
                epochClock,
                mockSystemCounters,
                SOURCE_ADDRESS,
                congestionControl,
                lossReport,
                true);

            receiverProxy.newPublicationImage(receiveChannelEndpoint, image);
        });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress, 0);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress, 0);

        final int readOutcome = TermReader.read(
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
            mockErrorHandler,
            0,
            mockSubscriberPosition);

        assertThat(readOutcome, is(1));
    }

    @Test
    public void shouldOverwriteHeartbeatWithDataFrame()
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final int commandsRead = toConductorQueue.drain((e) ->
        {
            // pass in new term buffer from conductor, which should trigger SM
            final PublicationImage image = new PublicationImage(
                CORRELATION_ID,
                Configuration.IMAGE_LIVENESS_TIMEOUT_NS,
                receiveChannelEndpoint,
                0,
                senderAddress,
                SESSION_ID,
                STREAM_ID,
                INITIAL_TERM_ID,
                ACTIVE_TERM_ID,
                INITIAL_TERM_OFFSET,
                rawLog,
                mockFeedbackDelayGenerator,
                POSITIONS,
                mockHighestReceivedPosition,
                mockRebuildPosition,
                nanoClock,
                nanoClock,
                epochClock,
                mockSystemCounters,
                SOURCE_ADDRESS,
                congestionControl,
                lossReport,
                true);

            receiverProxy.newPublicationImage(receiveChannelEndpoint, image);
        });

        assertThat(commandsRead, is(1));

        receiver.doWork();

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // heartbeat with same term offset
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress, 0);

        fillDataFrame(dataHeader, 0, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, dataHeader.frameLength(), senderAddress, 0);

        final int readOutcome = TermReader.read(
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
            mockErrorHandler,
            0,
            mockSubscriberPosition);

        assertThat(readOutcome, is(1));
    }

    @Test
    public void shouldHandleNonZeroTermOffsetCorrectly()
    {
        final int initialTermOffset = align(TERM_BUFFER_LENGTH / 16, FrameDescriptor.FRAME_ALIGNMENT);
        final int alignedDataFrameLength =
            align(DataHeaderFlyweight.HEADER_LENGTH + FAKE_PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);

        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader, initialTermOffset);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final int commandsRead = toConductorQueue.drain((e) ->
        {
            // pass in new term buffer from conductor, which should trigger SM
            final PublicationImage image = new PublicationImage(
                CORRELATION_ID,
                Configuration.IMAGE_LIVENESS_TIMEOUT_NS,
                receiveChannelEndpoint,
                0,
                senderAddress,
                SESSION_ID,
                STREAM_ID,
                INITIAL_TERM_ID,
                ACTIVE_TERM_ID,
                initialTermOffset,
                rawLog,
                mockFeedbackDelayGenerator,
                POSITIONS,
                mockHighestReceivedPosition,
                mockRebuildPosition,
                nanoClock,
                nanoClock,
                epochClock,
                mockSystemCounters,
                SOURCE_ADDRESS,
                congestionControl,
                lossReport,
                true);

            receiverProxy.newPublicationImage(receiveChannelEndpoint, image);
        });

        assertThat(commandsRead, is(1));

        verify(mockHighestReceivedPosition).setOrdered(initialTermOffset);

        receiver.doWork();

        fillDataFrame(dataHeader, initialTermOffset, FAKE_PAYLOAD);  // initial data frame
        receiveChannelEndpoint.onDataPacket(dataHeader, dataBuffer, alignedDataFrameLength, senderAddress, 0);

        verify(mockHighestReceivedPosition).setOrdered(initialTermOffset + alignedDataFrameLength);

        final int readOutcome = TermReader.read(
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
            mockErrorHandler,
            0,
            mockSubscriberPosition);

        assertThat(readOutcome, is(1));
    }

    @Test
    public void shouldRemoveImageFromDispatcherWithNoActivity()
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final PublicationImage mockImage = mock(PublicationImage.class);
        when(mockImage.sessionId()).thenReturn(SESSION_ID);
        when(mockImage.streamId()).thenReturn(STREAM_ID);
        when(mockImage.hasActivityAndNotEndOfStream(anyLong())).thenReturn(false);

        receiver.onNewPublicationImage(receiveChannelEndpoint, mockImage);
        receiver.doWork();

        verify(mockImage).removeFromDispatcher();
    }

    @Test
    public void shouldNotRemoveImageFromDispatcherOnRemoveSubscription()
    {
        receiverProxy.registerReceiveChannelEndpoint(receiveChannelEndpoint);
        receiverProxy.addSubscription(receiveChannelEndpoint, STREAM_ID);

        receiver.doWork();

        fillSetupFrame(setupHeader);
        receiveChannelEndpoint.onSetupMessage(setupHeader, setupBuffer, SetupFlyweight.HEADER_LENGTH, senderAddress, 0);

        final PublicationImage mockImage = mock(PublicationImage.class);
        when(mockImage.sessionId()).thenReturn(SESSION_ID);
        when(mockImage.streamId()).thenReturn(STREAM_ID);
        when(mockImage.hasActivityAndNotEndOfStream(anyLong())).thenReturn(true);

        receiver.onNewPublicationImage(receiveChannelEndpoint, mockImage);
        receiver.onRemoveSubscription(receiveChannelEndpoint, STREAM_ID);
        receiver.doWork();

        verify(mockImage).ifActiveGoInactive();
        verify(mockImage, never()).removeFromDispatcher();
    }

    private void fillDataFrame(final DataHeaderFlyweight header, final int termOffset, final byte[] payload)
    {
        header.wrap(dataBuffer);
        header
            .termOffset(termOffset)
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
        header.wrap(setupBuffer);
        header
            .streamId(STREAM_ID)
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
