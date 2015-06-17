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
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import uk.co.real_logic.aeron.command.*;
import uk.co.real_logic.aeron.driver.buffer.RawLogFactory;
import uk.co.real_logic.aeron.driver.event.EventConfiguration;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.TransportPoller;
import uk.co.real_logic.aeron.driver.media.UdpChannel;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.ErrorCode.INVALID_CHANNEL;
import static uk.co.real_logic.aeron.ErrorCode.UNKNOWN_PUBLICATION;
import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.Configuration.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_META_DATA_LENGTH;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computePosition;

public class DriverConductorTest
{
    private static final String CHANNEL_URI = "udp://localhost:";
    private static final String INVALID_URI = "udp://";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID_1 = 10;
    private static final int STREAM_ID_2 = 20;
    private static final int STREAM_ID_3 = 30;
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final long CORRELATION_ID_1 = 1429;
    private static final long CORRELATION_ID_2 = 1430;
    private static final long CORRELATION_ID_3 = 1431;
    private static final long CORRELATION_ID_4 = 1432;
    private static final long CLIENT_ID = 1433;
    private static final int BUFFER_LENGTH = 1024 * 1024;

    private final ByteBuffer toDriverBuffer = ByteBuffer.allocateDirect(Configuration.CONDUCTOR_BUFFER_LENGTH);
    private final ByteBuffer toEventBuffer = ByteBuffer.allocateDirect(
        EventConfiguration.BUFFER_LENGTH_DEFAULT + RingBufferDescriptor.TRAILER_LENGTH);

    private final TransportPoller transportPoller = mock(TransportPoller.class);
    private final RawLogFactory mockRawLogFactory = mock(RawLogFactory.class);

    private final RingBuffer fromClientCommands = new ManyToOneRingBuffer(new UnsafeBuffer(toDriverBuffer));
    private final RingBuffer toEventReader = new ManyToOneRingBuffer(new UnsafeBuffer(toEventBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final UnsafeBuffer writeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

    private final EventLogger mockConductorLogger = mock(EventLogger.class);

    private final SenderProxy senderProxy = mock(SenderProxy.class);
    private final ReceiverProxy receiverProxy = mock(ReceiverProxy.class);
    private final DriverConductorProxy fromSenderConductorProxy = mock(DriverConductorProxy.class);
    private final DriverConductorProxy fromReceiverConductorProxy = mock(DriverConductorProxy.class);

    private long currentTime;
    private final TimerWheel wheel = new TimerWheel(
        () -> currentTime, CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, CONDUCTOR_TICKS_PER_WHEEL);

    private DriverConductor driverConductor;

    private final Answer<Void> closeChannelEndpointAnswer =
        (invocation) ->
        {
            final Object args[] = invocation.getArguments();
            final ReceiveChannelEndpoint channelEndpoint = (ReceiveChannelEndpoint)args[0];
            channelEndpoint.close();

            return null;
        };

    @Before
    public void setUp() throws Exception
    {
        when(mockRawLogFactory.newPublication(anyObject(), anyInt(), anyInt(), anyInt()))
            .thenReturn(LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH, TERM_META_DATA_LENGTH));
        when(mockRawLogFactory.newConnection(anyObject(), anyInt(), anyInt(), anyInt(), eq(TERM_BUFFER_LENGTH)))
            .thenReturn(LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH, TERM_META_DATA_LENGTH));

        currentTime = 0;

        final UnsafeBuffer counterBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_LENGTH));
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_LENGTH)), counterBuffer);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .receiverNioSelector(transportPoller)
            .senderNioSelector(transportPoller)
            .unicastSenderFlowControl(UnicastFlowControl::new)
            .multicastSenderFlowControl(MaxMulticastFlowControl::new)
            .conductorTimerWheel(wheel)
            // TODO: remove
            .toConductorFromReceiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .toConductorFromSenderCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .eventLogger(mockConductorLogger)
            .rawLogBuffersFactory(mockRawLogFactory)
            .countersManager(countersManager);

        ctx.toEventReader(toEventReader);
        ctx.toDriverCommands(fromClientCommands);
        ctx.clientProxy(mockClientProxy);
        ctx.countersBuffer(counterBuffer);

        final SystemCounters mockSystemCounters = mock(SystemCounters.class);
        ctx.systemCounters(mockSystemCounters);
        when(mockSystemCounters.bytesReceived()).thenReturn(mock(AtomicCounter.class));
        when(mockSystemCounters.clientKeepAlives()).thenReturn(mock(AtomicCounter.class));

        ctx.epochClock(new SystemEpochClock());
        ctx.receiverProxy(receiverProxy);
        ctx.senderProxy(senderProxy);
        ctx.fromReceiverDriverConductorProxy(fromReceiverConductorProxy);
        ctx.fromSenderDriverConductorProxy(fromSenderConductorProxy);

        driverConductor = new DriverConductor(ctx);

        doAnswer(closeChannelEndpointAnswer).when(receiverProxy).closeReceiveChannelEndpoint(any());
    }

    @After
    public void tearDown() throws Exception
    {
        driverConductor.onClose();
    }

    @Test
    public void shouldBeAbleToAddSinglePublication() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        verify(senderProxy).registerSendChannelEndpoint(any());
        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newPublication(captor.capture(), any(), any());

        final NetworkPublication publication = captor.getValue();
        assertThat(publication.sessionId(), is(1));
        assertThat(publication.streamId(), is(2));

        verify(mockClientProxy).onPublicationReady(eq(2), anyInt(), any(), anyLong(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddSingleSubscription() throws Exception
    {
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        verify(receiverProxy).registerReceiveChannelEndpoint(any());
        verify(receiverProxy).addSubscription(any(), eq(STREAM_ID_1));
        verify(mockClientProxy).operationSucceeded(CORRELATION_ID_1);

        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscription() throws Exception
    {
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddMultipleStreams() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4001, CORRELATION_ID_1);
        writePublicationMessage(ADD_PUBLICATION, 1, 3, 4002, CORRELATION_ID_2);
        writePublicationMessage(ADD_PUBLICATION, 3, 2, 4003, CORRELATION_ID_3);
        writePublicationMessage(ADD_PUBLICATION, 3, 4, 4004, CORRELATION_ID_4);

        driverConductor.doWork();

        verify(senderProxy, times(4)).newPublication(any(), any(), any());
    }

    @Test
    public void shouldBeAbleToRemoveSingleStream() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4005, CORRELATION_ID_1);
        writePublicationMessage(REMOVE_PUBLICATION, 1, 2, 4005, CORRELATION_ID_1);

        driverConductor.doWork();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + PUBLICATION_LINGER_NS * 2);

        verify(senderProxy).removePublication(any());
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4005)));
    }

    @Test
    public void shouldBeAbleToRemoveMultipleStreams() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4006, CORRELATION_ID_1);
        writePublicationMessage(ADD_PUBLICATION, 1, 3, 4007, CORRELATION_ID_2);
        writePublicationMessage(ADD_PUBLICATION, 3, 2, 4008, CORRELATION_ID_3);
        writePublicationMessage(ADD_PUBLICATION, 3, 4, 4008, CORRELATION_ID_4);

        removePublicationMessage(CORRELATION_ID_1);
        removePublicationMessage(CORRELATION_ID_2);
        removePublicationMessage(CORRELATION_ID_3);
        removePublicationMessage(CORRELATION_ID_4);

        driverConductor.doWork();

        doWorkUntil(() -> wheel.clock().nanoTime() >= PUBLICATION_LINGER_NS * 2 + CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy, times(4)).removePublication(any());
    }

    // TODO: check publication refs from 0 to 1

    private void removePublicationMessage(final long registrationId)
    {
        removeMessage.wrap(writeBuffer, 0);
        removeMessage.registrationId(registrationId);
        assertTrue(fromClientCommands.write(REMOVE_PUBLICATION, writeBuffer, 0, RemoveMessageFlyweight.length()));
    }

    @Test
    public void shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_URI + 4000);

        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();

        final ReceiveChannelEndpoint channelEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(channelEndpoint);
        assertThat(channelEndpoint.streamCount(), is(3));

        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);

        driverConductor.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(channelEndpoint.streamCount(), is(1));
    }

    @Test
    public void shouldOnlyRemoveSubscriptionMediaEndpointUponRemovalOfAllSubscribers() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_URI + 4000);

        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();

        final ReceiveChannelEndpoint channelEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(channelEndpoint);
        assertThat(channelEndpoint.streamCount(), is(3));

        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(channelEndpoint.streamCount(), is(1));

        writeSubscriptionMessage(REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(udpChannel));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);
        writePublicationMessage(REMOVE_PUBLICATION, 2, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        verify(senderProxy).newPublication(any(), any(), any());
        verify(mockClientProxy).onError(eq(UNKNOWN_PUBLICATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verify(mockClientProxy, never()).operationSucceeded(anyLong());
        verify(mockConductorLogger).logException(any());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownStreamId() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);
        writePublicationMessage(REMOVE_PUBLICATION, 1, 3, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        verify(senderProxy).newPublication(any(), any(), any());
        verify(senderProxy, never()).removePublication(any());
        verify(mockClientProxy).onError(eq(UNKNOWN_PUBLICATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verify(mockClientProxy, never()).operationSucceeded(anyLong());
        verify(mockConductorLogger).logException(any());
    }

    @Test
    public void shouldErrorOnAddSubscriptionWithInvalidUri() throws Exception
    {
        writeSubscriptionMessage(ADD_SUBSCRIPTION, INVALID_URI, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        driverConductor.doWork();

        verify(senderProxy, never()).newPublication(any(), any(), any());

        verify(mockClientProxy).onError(eq(INVALID_CHANNEL), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verify(mockClientProxy, never()).operationSucceeded(anyLong());
        verify(mockConductorLogger).logException(any());
    }

    @Test
    public void shouldTimeoutPublication() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newPublication(captor.capture(), any(), any());

        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> wheel.clock().nanoTime() >= PUBLICATION_LINGER_NS + CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy).removePublication(eq(publication));
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldNotTimeoutPublicationOnKeepAlive() throws Exception
    {
        writePublicationMessage(ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newPublication(captor.capture(), any(), any());

        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS / 2);

        writeKeepaliveClientMessage();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        writeKeepaliveClientMessage();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy, never()).removePublication(eq(publication));
    }

    @Test
    public void shouldTimeoutSubscription() throws Exception
    {
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, times(1)).removeSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldNotTimeoutSubscriptionOnKeepAlive() throws Exception
    {
        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        writeKeepaliveClientMessage();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        writeKeepaliveClientMessage();

        doWorkUntil(() -> wheel.clock().nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, never()).removeSubscription(any(), anyInt());
        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldCreateConnectionOnSubscription() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);
        final int initialTermId = 1;
        final int activeTermId = 2;
        final int termOffset = 100;

        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreateConnection(
            SESSION_ID, STREAM_ID_1, initialTermId, activeTermId, termOffset, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<NetworkConnection> captor = ArgumentCaptor.forClass(NetworkConnection.class);
        verify(receiverProxy).newConnection(eq(receiveChannelEndpoint), captor.capture());

        final NetworkConnection networkConnection = captor.getValue();
        assertThat(networkConnection.sessionId(), is(SESSION_ID));
        assertThat(networkConnection.streamId(), is(STREAM_ID_1));

        final long position =
            computePosition(activeTermId, termOffset, Integer.numberOfTrailingZeros(TERM_BUFFER_LENGTH), initialTermId);
        verify(mockClientProxy).onConnectionReady(
            eq(STREAM_ID_1), eq(SESSION_ID), eq(position), anyObject(), anyLong(), anyObject(), anyString());
    }

    @Test
    public void shouldNotCreateConnectionOnUnknownSubscription() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreateConnection(
            SESSION_ID, STREAM_ID_2, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        verify(receiverProxy, never()).newConnection(any(), any());
        verify(mockClientProxy, never()).onConnectionReady(
            anyInt(), anyInt(), anyLong(), anyObject(), anyLong(), anyObject(), anyString());
    }

    @Test
    public void shouldSignalInactiveConnectionWhenConnectionTimesout() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        writeSubscriptionMessage(ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreateConnection(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<NetworkConnection> captor = ArgumentCaptor.forClass(NetworkConnection.class);
        verify(receiverProxy).newConnection(eq(receiveChannelEndpoint), captor.capture());

        final NetworkConnection networkConnection = captor.getValue();

        networkConnection.status(NetworkConnection.Status.INACTIVE);

        doWorkUntil(() -> wheel.clock().nanoTime() >= CONNECTION_LIVENESS_TIMEOUT_NS + 1000);

        verify(mockClientProxy).onInactiveConnection(
            eq(networkConnection.correlationId()), eq(SESSION_ID), eq(STREAM_ID_1), eq(0L), anyString());
    }

    @Test
    public void shouldAlwaysGiveNetworkConnectionCorrelationIdToClientCallbacks() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        writeSubscriptionMessage(
            ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, fromClientCommands.nextCorrelationId());

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreateConnection(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<NetworkConnection> captor = ArgumentCaptor.forClass(NetworkConnection.class);
        verify(receiverProxy).newConnection(eq(receiveChannelEndpoint), captor.capture());

        final NetworkConnection networkConnection = captor.getValue();

        networkConnection.status(NetworkConnection.Status.ACTIVE);

        writeSubscriptionMessage(
            ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, fromClientCommands.nextCorrelationId());

        driverConductor.doWork();

        networkConnection.status(NetworkConnection.Status.INACTIVE);

        doWorkUntil(() -> wheel.clock().nanoTime() >= CONNECTION_LIVENESS_TIMEOUT_NS + 1000);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy, times(2)).onConnectionReady(
            eq(STREAM_ID_1), eq(SESSION_ID), eq(0L), anyObject(),
            eq(networkConnection.correlationId()), anyObject(), anyString());
        inOrder.verify(mockClientProxy, times(1)).onInactiveConnection(
            eq(networkConnection.correlationId()), eq(SESSION_ID), eq(STREAM_ID_1), eq(0L), anyString());
    }

    private void writePublicationMessage(
        final int msgTypeId, final int sessionId, final int streamId, final int port, final long correlationId)
    {
        publicationMessage.wrap(writeBuffer, 0);
        publicationMessage.streamId(streamId);
        publicationMessage.sessionId(sessionId);
        publicationMessage.channel(CHANNEL_URI + port);
        publicationMessage.clientId(CLIENT_ID);
        publicationMessage.correlationId(correlationId);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, publicationMessage.length());
    }

    private void writeSubscriptionMessage(
        final int msgTypeId, final String channel, final int streamId, final long registrationCorrelationId)
    {
        subscriptionMessage.wrap(writeBuffer, 0);
        subscriptionMessage.streamId(streamId)
                           .channel(channel)
                           .registrationCorrelationId(registrationCorrelationId)
                           .correlationId(registrationCorrelationId)
                           .clientId(CLIENT_ID);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length());
    }

    private void writeKeepaliveClientMessage()
    {
        correlatedMessage.wrap(writeBuffer, 0);
        correlatedMessage.clientId(CLIENT_ID);
        correlatedMessage.correlationId(0);

        fromClientCommands.write(ControlProtocolEvents.CLIENT_KEEPALIVE, writeBuffer, 0, CorrelatedMessageFlyweight.LENGTH);
    }

    private long doWorkUntil(final BooleanSupplier condition) throws Exception
    {
        final long startTime = wheel.clock().nanoTime();

        while (!condition.getAsBoolean())
        {
            if (wheel.computeDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(Configuration.CONDUCTOR_TICK_DURATION_US);
            }

            driverConductor.doWork();
        }

        return wheel.clock().nanoTime() - startTime;
    }
}
