/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import io.aeron.CommonContext;
import io.aeron.DriverProxy;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.RawLogFactory;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveChannelEndpointThreadLocals;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermAppender;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static io.aeron.ErrorCode.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class DriverConductorTest
{
    private static final String CHANNEL_4000 = "aeron:udp?endpoint=localhost:4000";
    private static final String CHANNEL_4001 = "aeron:udp?endpoint=localhost:4001";
    private static final String CHANNEL_4002 = "aeron:udp?endpoint=localhost:4002";
    private static final String CHANNEL_4003 = "aeron:udp?endpoint=localhost:4003";
    private static final String CHANNEL_4004 = "aeron:udp?endpoint=localhost:4004";
    private static final String CHANNEL_IPC = "aeron:ipc";
    private static final String INVALID_URI = "aeron:udp://";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID_1 = 10;
    private static final int STREAM_ID_2 = 20;
    private static final int STREAM_ID_3 = 30;
    private static final int STREAM_ID_4 = 40;
    private static final int TERM_BUFFER_LENGTH = Configuration.TERM_BUFFER_LENGTH_DEFAULT;
    private static final int BUFFER_LENGTH = 16 * 1024;

    private final ByteBuffer toDriverBuffer = ByteBuffer.allocateDirect(Configuration.CONDUCTOR_BUFFER_LENGTH);

    private final RawLogFactory mockRawLogFactory = mock(RawLogFactory.class);

    private final RingBuffer fromClientCommands = new ManyToOneRingBuffer(new UnsafeBuffer(toDriverBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final DistinctErrorLog mockErrorLog = mock(DistinctErrorLog.class);
    private final AtomicCounter mockErrorCounter = mock(AtomicCounter.class);

    private final SenderProxy senderProxy = mock(SenderProxy.class);
    private final ReceiverProxy receiverProxy = mock(ReceiverProxy.class);
    private final DriverConductorProxy fromSenderConductorProxy = mock(DriverConductorProxy.class);
    private final DriverConductorProxy fromReceiverConductorProxy = mock(DriverConductorProxy.class);

    private long currentTime;
    private NanoClock nanoClock = () -> currentTime;

    private DriverProxy driverProxy;

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
        // System GC required in order to ensure that the direct byte buffers get cleaned and avoid OOM.
        System.gc();

        when(mockRawLogFactory.newNetworkPublication(anyString(), anyInt(), anyInt(), anyLong(), anyInt()))
            .thenReturn(LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH));
        when(mockRawLogFactory.newNetworkedImage(anyString(), anyInt(), anyInt(), anyLong(), eq(TERM_BUFFER_LENGTH)))
            .thenReturn(LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH));
        when(mockRawLogFactory.newDirectPublication(anyInt(), anyInt(), anyLong(), anyInt()))
            .thenReturn(LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH));

        currentTime = 0;

        final UnsafeBuffer counterBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_LENGTH));
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_LENGTH * 2)), counterBuffer);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .unicastFlowControlSupplier(Configuration.unicastFlowControlSupplier())
            .multicastFlowControlSupplier(Configuration.multicastFlowControlSupplier())
                // TODO: remove
            .toConductorFromReceiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .toConductorFromSenderCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .errorLog(mockErrorLog)
            .rawLogBuffersFactory(mockRawLogFactory)
            .countersManager(countersManager)
            .nanoClock(nanoClock)
            .sendChannelEndpointSupplier(Configuration.sendChannelEndpointSupplier())
            .receiveChannelEndpointSupplier(Configuration.receiveChannelEndpointSupplier())
            .congestControlSupplier(Configuration.congestionControlSupplier());

        ctx.toDriverCommands(fromClientCommands);
        ctx.clientProxy(mockClientProxy);
        ctx.countersValuesBuffer(counterBuffer);

        final SystemCounters mockSystemCounters = mock(SystemCounters.class);
        ctx.systemCounters(mockSystemCounters);
        when(mockSystemCounters.get(any())).thenReturn(mockErrorCounter);

        ctx.epochClock(new SystemEpochClock());
        ctx.receiverProxy(receiverProxy);
        ctx.senderProxy(senderProxy);
        ctx.fromReceiverDriverConductorProxy(fromReceiverConductorProxy);
        ctx.fromSenderDriverConductorProxy(fromSenderConductorProxy);
        ctx.clientLivenessTimeoutNs(CLIENT_LIVENESS_TIMEOUT_NS);
        ctx.receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals(ctx));

        driverProxy = new DriverProxy(fromClientCommands);
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
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        verify(senderProxy).registerSendChannelEndpoint(any());
        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();
        assertThat(publication.streamId(), is(STREAM_ID_1));

        verify(mockClientProxy).onPublicationReady(anyLong(), eq(STREAM_ID_1), anyInt(), any(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddSingleSubscription() throws Exception
    {
        final long id = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        verify(receiverProxy).registerReceiveChannelEndpoint(any());
        verify(receiverProxy).addSubscription(any(), eq(STREAM_ID_1));
        verify(mockClientProxy).operationSucceeded(id);

        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscription() throws Exception
    {
        final long id = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removeSubscription(id);

        driverConductor.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldBeAbleToAddMultipleStreams() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4001, STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4002, STREAM_ID_2);
        driverProxy.addPublication(CHANNEL_4003, STREAM_ID_3);
        driverProxy.addPublication(CHANNEL_4004, STREAM_ID_4);

        driverConductor.doWork();

        verify(senderProxy, times(4)).newNetworkPublication(any());
    }

    @Test
    public void shouldBeAbleToRemoveSingleStream() throws Exception
    {
        final long id = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removePublication(id);

        driverConductor.doWork();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + PUBLICATION_LINGER_NS * 2);

        verify(senderProxy).removeNetworkPublication(any());
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldBeAbleToRemoveMultipleStreams() throws Exception
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4001, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_4002, STREAM_ID_2);
        final long id3 = driverProxy.addPublication(CHANNEL_4003, STREAM_ID_3);
        final long id4 = driverProxy.addPublication(CHANNEL_4004, STREAM_ID_4);

        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);
        driverProxy.removePublication(id3);
        driverProxy.removePublication(id4);

        driverConductor.doWork();

        doWorkUntil(() -> nanoClock.nanoTime() >= PUBLICATION_LINGER_NS * 2 + CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy, times(4)).removeNetworkPublication(any());
    }

    @Test
    public void shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_4000);

        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_3);

        driverConductor.doWork();

        final ReceiveChannelEndpoint channelEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(channelEndpoint);
        assertThat(channelEndpoint.streamCount(), is(3));

        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        driverConductor.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(channelEndpoint.streamCount(), is(1));
    }

    @Test
    public void shouldOnlyRemoveSubscriptionMediaEndpointUponRemovalOfAllSubscribers() throws Exception
    {
        final UdpChannel udpChannel = UdpChannel.parse(CHANNEL_4000);

        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        final long id3 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_3);

        driverConductor.doWork();

        final ReceiveChannelEndpoint channelEndpoint = driverConductor.receiverChannelEndpoint(udpChannel);

        assertNotNull(channelEndpoint);
        assertThat(channelEndpoint.streamCount(), is(3));

        driverProxy.removeSubscription(id2);
        driverProxy.removeSubscription(id3);

        driverConductor.doWork();

        assertNotNull(driverConductor.receiverChannelEndpoint(udpChannel));
        assertThat(channelEndpoint.streamCount(), is(1));

        driverProxy.removeSubscription(id1);

        driverConductor.doWork();

        assertNull(driverConductor.receiverChannelEndpoint(udpChannel));
    }

    @Test
    public void shouldErrorOnRemovePublicationOnUnknownRegistrationId() throws Exception
    {
        final long id = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removePublication(id + 1);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(senderProxy, mockClientProxy);

        inOrder.verify(senderProxy).newNetworkPublication(any());
        inOrder.verify(mockClientProxy).onPublicationReady(eq(id), eq(STREAM_ID_1), anyInt(), any(), anyInt());
        inOrder.verify(mockClientProxy).onError(eq(UNKNOWN_PUBLICATION), anyString(), anyLong());
        inOrder.verifyNoMoreInteractions();

        verify(mockErrorCounter).increment();
        verify(mockErrorLog).record(any(Throwable.class));
    }

    @Test
    public void shouldErrorOnRemoveSubscriptionOnUnknownRegistrationId() throws Exception
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removeSubscription(id1 + 100);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(receiverProxy, mockClientProxy);

        inOrder.verify(receiverProxy).addSubscription(any(), anyInt());
        inOrder.verify(mockClientProxy).operationSucceeded(id1);
        inOrder.verify(mockClientProxy).onError(eq(UNKNOWN_SUBSCRIPTION), anyString(), anyLong());
        inOrder.verifyNoMoreInteractions();

        verify(mockErrorLog).record(any(Throwable.class));
    }

    @Test
    public void shouldErrorOnAddSubscriptionWithInvalidUri() throws Exception
    {
        driverProxy.addSubscription(INVALID_URI, STREAM_ID_1);

        driverConductor.doWork();
        driverConductor.doWork();

        verify(senderProxy, never()).newNetworkPublication(any());

        verify(mockClientProxy).onError(eq(INVALID_CHANNEL), anyString(), anyLong());
        verify(mockClientProxy, never()).operationSucceeded(anyLong());

        verify(mockErrorCounter).increment();
        verify(mockErrorLog).record(any(Throwable.class));
    }

    @Test
    public void shouldTimeoutPublication() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> nanoClock.nanoTime() >= PUBLICATION_LINGER_NS + CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy).removeNetworkPublication(eq(publication));
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldNotTimeoutPublicationOnKeepAlive() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS / 2);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy, never()).removeNetworkPublication(eq(publication));
    }

    @Test
    public void shouldTimeoutPublicationWithNoKeepaliveButNotFlushed() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();

        final int termId = 101;
        final int index = LogBufferDescriptor.indexByTerm(termId, termId);
        final RawLog rawLog = publication.rawLog();
        final TermAppender appender = new TermAppender(rawLog.termBuffers()[index], rawLog.logMetaData(), index);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[256]);
        final HeaderWriter headerWriter = new HeaderWriter(createDefaultHeader(publication.sessionId(), STREAM_ID_1, termId));

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(termId);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(10);

        publication.onStatusMessage(msg, new InetSocketAddress("localhost", 4059));

        appender.appendUnfragmentedMessage(headerWriter, srcBuffer, 0, 256, null);

        doWorkUntil(() -> nanoClock.nanoTime() >= PUBLICATION_LINGER_NS + CLIENT_LIVENESS_TIMEOUT_NS * 2);

        assertTrue(publication.hasReachedEndOfLife());

        verify(senderProxy).removeNetworkPublication(eq(publication));
        assertNull(driverConductor.senderChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldTimeoutSubscription() throws Exception
    {
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, times(1)).removeSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldNotTimeoutSubscriptionOnKeepAlive() throws Exception
    {
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, never()).removeSubscription(any(), anyInt());
        assertNotNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldCreateImageOnSubscription() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);
        final int initialTermId = 1;
        final int activeTermId = 2;
        final int termOffset = 100;

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, initialTermId, activeTermId, termOffset, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor.capture());

        final PublicationImage publicationImage = captor.getValue();
        assertThat(publicationImage.sessionId(), is(SESSION_ID));
        assertThat(publicationImage.streamId(), is(STREAM_ID_1));

        verify(mockClientProxy).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), eq(SESSION_ID), any(), any(), anyString());
    }

    @Test
    public void shouldNotCreateImageOnUnknownSubscription() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_2, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        verify(receiverProxy, never()).newPublicationImage(any(), any());
        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), anyInt(), anyInt(), any(), any(), anyString());
    }

    @Test
    public void shouldSignalInactiveImageWhenImageTimesout() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor.capture());

        final PublicationImage publicationImage = captor.getValue();

        publicationImage.status(PublicationImage.Status.INACTIVE);

        doWorkUntil(() -> nanoClock.nanoTime() >= IMAGE_LIVENESS_TIMEOUT_NS + 1000);

        verify(mockClientProxy).onUnavailableImage(eq(publicationImage.correlationId()), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldAlwaysGiveNetworkPublicationCorrelationIdToClientCallbacks() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor.capture());

        final PublicationImage publicationImage = captor.getValue();

        publicationImage.status(PublicationImage.Status.ACTIVE);

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        publicationImage.status(PublicationImage.Status.INACTIVE);

        doWorkUntil(() -> nanoClock.nanoTime() >= IMAGE_LIVENESS_TIMEOUT_NS + 1000);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy, times(2)).onAvailableImage(
            eq(publicationImage.correlationId()), eq(STREAM_ID_1), eq(SESSION_ID), any(), any(), anyString());
        inOrder.verify(mockClientProxy, times(1)).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldNotSendAvailableImageWhileImageNotActiveOnAddSubscription() throws Exception
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        final long subOneId = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ReceiveChannelEndpoint receiveChannelEndpoint =
            driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000));
        assertNotNull(receiveChannelEndpoint);

        receiveChannelEndpoint.openChannel();

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor.capture());

        final PublicationImage publicationImage = captor.getValue();

        publicationImage.status(PublicationImage.Status.INACTIVE);

        doWorkUntil(() -> nanoClock.nanoTime() >= IMAGE_LIVENESS_TIMEOUT_NS / 2);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= IMAGE_LIVENESS_TIMEOUT_NS + 1000);

        final long subTwoId = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy, times(1)).operationSucceeded(subOneId);
        inOrder.verify(mockClientProxy, times(1)).onAvailableImage(
            eq(publicationImage.correlationId()), eq(STREAM_ID_1), eq(SESSION_ID), any(), any(), anyString());
        inOrder.verify(mockClientProxy, times(1)).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(STREAM_ID_1), anyString());
        inOrder.verify(mockClientProxy, times(1)).operationSucceeded(subTwoId);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldBeAbleToAddSingleDirectPublicationPublication() throws Exception
    {
        final long id = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        assertNotNull(driverConductor.getDirectPublication(STREAM_ID_1));
        verify(mockClientProxy).onPublicationReady(eq(id), eq(STREAM_ID_1), anyInt(), any(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddSingleDirectPublicationSubscription() throws Exception
    {
        final long id = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        final DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).operationSucceeded(eq(id));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(directPublication.correlationId()), eq(STREAM_ID_1), eq(directPublication.sessionId()),
            eq(directPublication.rawLog().logFileName()), any(), anyString());
    }

    @Test
    public void shouldBeAbleToAddDirectPublicationPublicationThenSubscription() throws Exception
    {
        final long idPub = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        final long idSub = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        final DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onPublicationReady(eq(idPub), eq(STREAM_ID_1), anyInt(), any(), anyInt());
        inOrder.verify(mockClientProxy).operationSucceeded(eq(idSub));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(directPublication.correlationId()), eq(STREAM_ID_1), eq(directPublication.sessionId()),
            eq(directPublication.rawLog().logFileName()), any(), anyString());
    }

    @Test
    public void shouldBeAbleToAddDirectPublicationSubscriptionThenPublication() throws Exception
    {
        final long idSub = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idPub = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        final DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).operationSucceeded(eq(idSub));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(directPublication.correlationId()), eq(STREAM_ID_1), eq(directPublication.sessionId()),
            eq(directPublication.rawLog().logFileName()), any(), anyString());
        inOrder.verify(mockClientProxy).onPublicationReady(eq(idPub), eq(STREAM_ID_1), anyInt(), any(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddAndRemoveDirectPublicationPublication() throws Exception
    {
        final long idAdd = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removePublication(idAdd);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        final DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveDirectPublicationSubscription() throws Exception
    {
        final long idAdd = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removeSubscription(idAdd);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        final DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveDirectPublicationTwoPublications() throws Exception
    {
        final long idAdd1 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        final long idAdd2 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removePublication(idAdd1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        driverProxy.removePublication(idAdd2);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveDirectPublicationTwoSubscriptions() throws Exception
    {
        final long idAdd1 = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idAdd2 = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removeSubscription(idAdd1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        driverProxy.removeSubscription(idAdd2);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveDirectPublicationPublicationAndSubscription() throws Exception
    {
        final long idAdd1 = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idAdd2 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removeSubscription(idAdd1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        driverProxy.removePublication(idAdd2);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldTimeoutDirectPublicationPublication() throws Exception
    {
        driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldNotTimeoutDirectPublicationPublicationWithKeepalive() throws Exception
    {
        driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);
    }

    @Test
    public void shouldTimeoutDirectPublicationSubscription() throws Exception
    {
        driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNull(directPublication);
    }

    @Test
    public void shouldNotTimeoutDirectPublicationSubscriptionWithKeepalive() throws Exception
    {
        driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        DirectPublication directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        directPublication = driverConductor.getDirectPublication(STREAM_ID_1);
        assertNotNull(directPublication);
    }

    @Test
    public void shouldBeAbleToAddSingleSpy() throws Exception
    {
        final long id = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        verify(receiverProxy, never()).registerReceiveChannelEndpoint(any());
        verify(receiverProxy, never()).addSubscription(any(), eq(STREAM_ID_1));
        verify(mockClientProxy).operationSucceeded(id);

        assertNull(driverConductor.receiverChannelEndpoint(UdpChannel.parse(CHANNEL_4000)));
    }

    @Test
    public void shouldBeAbleToAddNetworkPublicationThenSingleSpy() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).operationSucceeded(eq(idSpy));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            eq(publication.rawLog().logFileName()), any(), anyString());
    }

    @Test
    public void shouldBeAbleToAddSingleSpyThenNetworkPublication() throws Exception
    {
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).operationSucceeded(eq(idSpy));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            eq(publication.rawLog().logFileName()), any(), anyString());
    }

    @Test
    public void shouldBeAbleToAddNetworkPublicationThenSingleSpyThenRemoveSpy() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);
        driverProxy.removeSubscription(idSpy);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertFalse(publication.hasSpies());
    }

    @Test
    public void shouldTimeoutSpy() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        assertFalse(publication.hasSpies());
    }

    @Test
    public void shouldNotTimeoutSpyWithKeepalive() throws Exception
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        driverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        assertTrue(publication.hasSpies());
    }

    @Test
    public void shouldTimeoutNetworkPublicationWithSpy() throws Exception
    {
        final DriverProxy spyDriverProxy = new DriverProxy(fromClientCommands);

        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        spyDriverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS / 2);

        spyDriverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        spyDriverProxy.sendClientKeepalive();

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy).removeNetworkPublication(eq(publication));
        verify(mockClientProxy).onUnavailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldOnlyCloseSendChannelEndpointOnceWithMultiplePublications() throws Exception
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_2);
        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);

        driverConductor.doWork();

        doWorkUntil(
            () ->
            {
                driverProxy.sendClientKeepalive();
                return nanoClock.nanoTime() >= PUBLICATION_LINGER_NS * 2;
            });

        verify(senderProxy, times(1)).closeSendChannelEndpoint(any());
    }

    @Test
    public void shouldOnlyCloseReceiveChannelEndpointOnceWithMultipleSubscriptions() throws Exception
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        driverConductor.doWork();

        doWorkUntil(
            () ->
            {
                driverProxy.sendClientKeepalive();
                return nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2;
            });

        verify(receiverProxy, times(1)).closeReceiveChannelEndpoint(any());
    }

    private long doWorkUntil(final BooleanSupplier condition) throws Exception
    {
        final long startTime = currentTime;

        while (!condition.getAsBoolean())
        {
            currentTime += TimeUnit.MILLISECONDS.toNanos(10);
            driverConductor.doWork();
        }

        return currentTime - startTime;
    }

    private static String spyForChannel(final String channel)
    {
        return CommonContext.SPY_PREFIX + channel;
    }

    private static long networkPublicationCorrelationId(final NetworkPublication publication)
    {
        return LogBufferDescriptor.correlationId(publication.rawLog().logMetaData());
    }
}
