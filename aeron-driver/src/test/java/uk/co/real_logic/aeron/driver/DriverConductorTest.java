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
import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.CountersManager;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.ErrorCode.INVALID_CHANNEL;
import static uk.co.real_logic.aeron.common.ErrorCode.PUBLICATION_STREAM_UNKNOWN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.driver.MediaDriver.CONDUCTOR_TICKS_PER_WHEEL;
import static uk.co.real_logic.aeron.driver.MediaDriver.CONDUCTOR_TICK_DURATION_US;

/**
 * Test the Media Driver Conductor in isolation
 *
 * Tests for interaction of media conductor with client protocol
 */
public class DriverConductorTest
{
    private static final String CHANNEL_URI = "udp://localhost:";
    private static final String INVALID_URI = "udp://";
    private static final int STREAM_ID_1 = 10;
    private static final int STREAM_ID_2 = 20;
    private static final int STREAM_ID_3 = 30;
    private static final int TERM_BUFFER_SZ = MediaDriver.TERM_BUFFER_SZ_DEFAULT;
    private static final long CORRELATION_ID_1 = 1429;
    private static final long CORRELATION_ID_2 = 1430;
    private static final long CORRELATION_ID_3 = 1431;
    private static final long CORRELATION_ID_4 = 1432;

    private final ByteBuffer toDriverBuffer =
        ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_LENGTH);

    private final NioSelector nioSelector = mock(NioSelector.class);
    private final TermBuffersFactory mockTermBuffersFactory = mock(TermBuffersFactory.class);

    private final RingBuffer fromClientCommands = new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();

    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private final AtomicArray<DriverPublication> publications = new AtomicArray<>();
    private final AtomicArray<DriverSubscription> subscriptions = new AtomicArray<>();
    private final EventLogger mockConductorLogger = mock(EventLogger.class);

    private long currentTime;
    private final TimerWheel wheel = new TimerWheel(() -> currentTime,
        CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, CONDUCTOR_TICKS_PER_WHEEL);

    private DriverConductor driverConductor;
    private Receiver receiver;

    @Before
    public void setUp() throws Exception
    {
        when(mockTermBuffersFactory.newPublication(anyObject(), anyInt(), anyInt()))
            .thenReturn(BufferAndFrameHelper.newTestTermBuffers(TERM_BUFFER_SZ, STATE_BUFFER_LENGTH));

        currentTime = 0;

        final AtomicBuffer counterBuffer = new AtomicBuffer(new byte[4096]);
        final CountersManager countersManager =
            new CountersManager(new AtomicBuffer(new byte[8192]), counterBuffer);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .receiverNioSelector(nioSelector)
            .conductorNioSelector(nioSelector)
            .unicastSenderFlowControl(UnicastSenderControlStrategy::new)
            .multicastSenderFlowControl(DefaultMulticastSenderControlStrategy::new)
            .conductorTimerWheel(wheel)
            .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .publications(publications)
            .subscriptions(subscriptions)
            .eventLogger(mockConductorLogger)
            .termBuffersFactory(mockTermBuffersFactory)
            .countersManager(countersManager);

        ctx.fromClientCommands(fromClientCommands);
        ctx.clientProxy(mockClientProxy);
        ctx.countersBuffer(counterBuffer);

        ctx.receiverProxy(new ReceiverProxy(ctx.receiverCommandQueue()));
        ctx.driverConductorProxy(new DriverConductorProxy(ctx.conductorCommandQueue()));

        receiver = new Receiver(ctx);
        driverConductor = new DriverConductor(ctx);
    }

    @After
    public void tearDown() throws Exception
    {
        receiver.close();
        receiver.nioSelector().selectNowWithoutProcessing();
        driverConductor.close();
        driverConductor.nioSelector().selectNowWithoutProcessing();
    }

    @Test
    public void shouldBeAbleToAddSinglePublication() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        verify(mockClientProxy).onNewTermBuffers(eq(ControlProtocolEvents.ON_NEW_PUBLICATION),
            eq(1), eq(2), anyInt(), eq(CHANNEL_URI + 4000),
            any(), anyLong(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddSingleSubscription() throws Exception
    {
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();

        verify(mockClientProxy).operationSucceeded(CORRELATION_ID_1);

        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscription() throws Exception
    {
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddMultipleStreams() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4001, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 3, 4002, CORRELATION_ID_2);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 3, 2, 4003, CORRELATION_ID_3);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 3, 4, 4004, CORRELATION_ID_4);

        driverConductor.doWork();

        assertThat(publications.size(), is(4));
    }

    @Test
    public void shouldBeAbleToRemoveSingleStream() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4005, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1, 2, 4005, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(0));
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4005)));
    }

    @Test
    public void shouldBeAbleToRemoveMultipleStreams() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4006, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 3, 4007, CORRELATION_ID_2);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 3, 2, 4008, CORRELATION_ID_3);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 3, 4, 4008, CORRELATION_ID_4);

        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1, 2, 4006, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1, 3, 4007, CORRELATION_ID_2);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 3, 2, 4008, CORRELATION_ID_3);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 3, 4, 4008, CORRELATION_ID_4);

        driverConductor.doWork();

        assertThat(publications.size(), is(0));
    }

    @Test
    public void shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_URI + 4000);

        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();
        receiver.doWork();

        final ReceiveChannelEndpoint mediaEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(mediaEndpoint);
        assertThat(mediaEndpoint.streamCount(), is(3));

        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);

        driverConductor.doWork();
        receiver.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(mediaEndpoint.streamCount(), is(1));
    }

    @Test
    public void shouldOnlyRemoveSubscriptionMediaEndpointUponRemovalOfAllSubscribers() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_URI + 4000);

        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();
        receiver.doWork();

        final ReceiveChannelEndpoint mediaEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(mediaEndpoint);
        assertThat(mediaEndpoint.streamCount(), is(3));

        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_2, CORRELATION_ID_2);
        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_3, CORRELATION_ID_3);

        driverConductor.doWork();
        receiver.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(mediaEndpoint.streamCount(), is(1));

        writeSubscriptionMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(udpChannel));
    }

    @Test
    public void shouldErrorOnAddDuplicateChannelOnExistingSession() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        verify(mockClientProxy)
            .onError(eq(ErrorCode.PUBLICATION_STREAM_ALREADY_EXISTS), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verifyNeverSucceeds();
        verifyExceptionLogged();
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownChannel() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(0));

        verify(mockClientProxy).onError(eq(INVALID_CHANNEL), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verifyNeverSucceeds();
        verifyExceptionLogged();
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 2, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        verify(mockClientProxy).onError(eq(PUBLICATION_STREAM_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verifyNeverSucceeds();
        verifyExceptionLogged();
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownStreamId() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);
        writePublicationMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1, 3, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        verify(mockClientProxy).onError(eq(PUBLICATION_STREAM_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verifyNeverSucceeds();
        verifyExceptionLogged();
    }

    @Test
    public void shouldErrorOnAddSubscriptionWithInvalidUri() throws Exception
    {
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, INVALID_URI, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(INVALID_CHANNEL), argThat(not(isEmptyOrNullString())), any(), anyInt());
        verifyNeverSucceeds();
        verifyExceptionLogged();
    }

    @Test
    public void shouldTimeoutPublication() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS * 2));

        assertThat(publications.size(), is(0));
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldntTimeoutPublicationOnKeepAlive() throws Exception
    {
        writePublicationMessage(ControlProtocolEvents.ADD_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        driverConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1));
        assertThat(publications.get(0).streamId(), is(2));

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS / 2));

        writePublicationMessage(ControlProtocolEvents.KEEPALIVE_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS + 1000));

        writePublicationMessage(ControlProtocolEvents.KEEPALIVE_PUBLICATION, 1, 2, 4000, CORRELATION_ID_1);

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS * 2));

        assertThat(publications.size(), is(1));
    }

    @Test
    public void shouldTimeoutSubscription() throws Exception
    {
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();

        assertThat(subscriptions.size(), is(1));
        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS * 2));

        assertThat(subscriptions.size(), is(0));
        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }

    @Test
    public void shouldntTimeoutSubscriptionOnKeepAlive() throws Exception
    {
        writeSubscriptionMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        driverConductor.doWork();
        receiver.doWork();

        assertThat(subscriptions.size(), is(1));
        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS / 1));

        writeSubscriptionMessage(ControlProtocolEvents.KEEPALIVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS + 1000));

        writeSubscriptionMessage(ControlProtocolEvents.KEEPALIVE_SUBSCRIPTION, CHANNEL_URI + 4000, STREAM_ID_1, CORRELATION_ID_1);

        processTimersUntil(() -> wheel.now() >= TimeUnit.NANOSECONDS.toNanos(DriverConductor.LIVENESS_CLIENT_TIMEOUT_NS * 2));

        assertThat(subscriptions.size(), is(1));
        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_URI + 4000)));
    }


    private void verifyExceptionLogged()
    {
        verify(mockConductorLogger).logException(any());
    }

    private void verifyNeverSucceeds()
    {
        verify(mockClientProxy, never()).operationSucceeded(anyLong());
    }

    private void writePublicationMessage(final int msgTypeId,
                                         final int sessionId,
                                         final int streamId,
                                         final int port,
                                         final long correlationId)
        throws IOException
    {
        publicationMessage.wrap(writeBuffer, 0);
        publicationMessage.streamId(streamId);
        publicationMessage.sessionId(sessionId);
        publicationMessage.channel(CHANNEL_URI + port);
        publicationMessage.correlationId(correlationId);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, publicationMessage.length());
    }

    private void writeSubscriptionMessage(final int msgTypeId,
                                          final String channel,
                                          final int streamId,
                                          final long registrationCorrelationId)
        throws IOException
    {
        subscriptionMessage.wrap(writeBuffer, 0);
        subscriptionMessage.streamId(streamId)
                           .channel(channel)
                           .registrationCorrelationId(registrationCorrelationId)
                           .correlationId(registrationCorrelationId);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length());
    }

    private long processTimersUntil(final BooleanSupplier condition) throws Exception
    {
        final long startTime = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.CONDUCTOR_TICK_DURATION_US);
            }

            driverConductor.doWork();
        }

        return (wheel.now() - startTime);
    }
}
