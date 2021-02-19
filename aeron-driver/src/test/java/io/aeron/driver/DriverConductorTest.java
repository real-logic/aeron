/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.ErrorCode;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveChannelEndpointThreadLocals;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermAppender;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.*;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

import static io.aeron.ErrorCode.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.status.ClientHeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static io.aeron.driver.status.SystemCounterDescriptor.CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.driver.status.SystemCounterDescriptor.CONDUCTOR_MAX_CYCLE_TIME;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.concurrent.status.CountersReader.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DriverConductorTest
{
    private static final String CHANNEL_4000 = "aeron:udp?endpoint=localhost:4000";
    private static final String CHANNEL_4001 = "aeron:udp?endpoint=localhost:4001";
    private static final String CHANNEL_4002 = "aeron:udp?endpoint=localhost:4002";
    private static final String CHANNEL_4003 = "aeron:udp?endpoint=localhost:4003";
    private static final String CHANNEL_4004 = "aeron:udp?endpoint=localhost:4004";
    private static final String CHANNEL_4000_TAG_ID_1 = "aeron:udp?endpoint=localhost:4000|tags=1001";
    private static final String CHANNEL_TAG_ID_1 = "aeron:udp?tags=1001";
    private static final String CHANNEL_SUB_CONTROL_MODE_MANUAL = "aeron:udp?control-mode=manual";
    private static final String CHANNEL_IPC = "aeron:ipc";
    private static final String INVALID_URI = "aeron:udp://";
    private static final String COUNTER_LABEL = "counter label";
    private static final int SESSION_ID = 100;
    private static final int STREAM_ID_1 = 1010;
    private static final int STREAM_ID_2 = 1020;
    private static final int STREAM_ID_3 = 1030;
    private static final int STREAM_ID_4 = 1040;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int BUFFER_LENGTH = 16 * 1024;
    private static final int COUNTER_TYPE_ID = 101;
    private static final int COUNTER_KEY_OFFSET = 0;
    private static final int COUNTER_KEY_LENGTH = 12;
    private static final int COUNTER_LABEL_OFFSET = COUNTER_KEY_OFFSET + COUNTER_KEY_LENGTH;
    private static final int COUNTER_LABEL_LENGTH = COUNTER_LABEL.length();
    private static final long CLIENT_LIVENESS_TIMEOUT_NS = CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS;
    private static final long PUBLICATION_LINGER_TIMEOUT_NS = PUBLICATION_LINGER_DEFAULT_NS;
    private static final int MTU_LENGTH = MTU_LENGTH_DEFAULT;

    private final ByteBuffer conductorBuffer = ByteBuffer.allocate(CONDUCTOR_BUFFER_LENGTH_DEFAULT);
    private final UnsafeBuffer counterKeyAndLabel = new UnsafeBuffer(new byte[BUFFER_LENGTH]);

    private final RingBuffer toDriverCommands = new ManyToOneRingBuffer(new UnsafeBuffer(conductorBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
    private final AtomicCounter mockErrorCounter = mock(AtomicCounter.class);

    private final SenderProxy senderProxy = mock(SenderProxy.class);
    private final ReceiverProxy receiverProxy = mock(ReceiverProxy.class);
    private final DriverConductorProxy driverConductorProxy = mock(DriverConductorProxy.class);
    private ReceiveChannelEndpoint receiveChannelEndpoint = null;

    private final CachedNanoClock nanoClock = new CachedNanoClock();
    private final CachedEpochClock epochClock = new CachedEpochClock();

    private SystemCounters spySystemCounters;

    private CountersManager spyCountersManager;
    private DriverProxy driverProxy;
    private DriverConductor driverConductor;

    private final Answer<Void> closeChannelEndpointAnswer =
        (invocation) ->
        {
            final Object[] args = invocation.getArguments();
            final ReceiveChannelEndpoint channelEndpoint = (ReceiveChannelEndpoint)args[0];
            channelEndpoint.close();

            return null;
        };

    @BeforeEach
    public void before()
    {
        counterKeyAndLabel.putInt(COUNTER_KEY_OFFSET, 42);
        counterKeyAndLabel.putStringAscii(COUNTER_LABEL_OFFSET, COUNTER_LABEL);

        final UnsafeBuffer counterBuffer = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_LENGTH));
        final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(
            ByteBuffer.allocate(Configuration.countersMetadataBufferLength(BUFFER_LENGTH)));
        spyCountersManager = spy(new CountersManager(metaDataBuffer, counterBuffer, StandardCharsets.US_ASCII));

        spySystemCounters = spy(new SystemCounters(spyCountersManager));

        when(spySystemCounters.get(SystemCounterDescriptor.ERRORS)).thenReturn(mockErrorCounter);
        when(mockErrorCounter.appendToLabel(any())).thenReturn(mockErrorCounter);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .tempBuffer(new UnsafeBuffer(new byte[METADATA_LENGTH]))
            .timerIntervalNs(DEFAULT_TIMER_INTERVAL_NS)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .unicastFlowControlSupplier(Configuration.unicastFlowControlSupplier())
            .multicastFlowControlSupplier(Configuration.multicastFlowControlSupplier())
            .driverCommandQueue(new ManyToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
            .errorHandler(mockErrorHandler)
            .logFactory(new TestLogFactory())
            .countersManager(spyCountersManager)
            .epochClock(epochClock)
            .nanoClock(nanoClock)
            .senderCachedNanoClock(nanoClock)
            .receiverCachedNanoClock(nanoClock)
            .cachedEpochClock(new CachedEpochClock())
            .cachedNanoClock(new CachedNanoClock())
            .sendChannelEndpointSupplier(Configuration.sendChannelEndpointSupplier())
            .receiveChannelEndpointSupplier(Configuration.receiveChannelEndpointSupplier())
            .congestControlSupplier(Configuration.congestionControlSupplier())
            .toDriverCommands(toDriverCommands)
            .clientProxy(mockClientProxy)
            .countersValuesBuffer(counterBuffer)
            .systemCounters(spySystemCounters)
            .receiverProxy(receiverProxy)
            .senderProxy(senderProxy)
            .driverConductorProxy(driverConductorProxy)
            .receiveChannelEndpointThreadLocals(new ReceiveChannelEndpointThreadLocals())
            .conductorCycleThresholdNs(600_000_000)
            .nameResolver(DefaultNameResolver.INSTANCE);

        driverProxy = new DriverProxy(toDriverCommands, toDriverCommands.nextCorrelationId());
        driverConductor = new DriverConductor(ctx);

        doAnswer(closeChannelEndpointAnswer).when(receiverProxy).closeReceiveChannelEndpoint(any());
    }

    @AfterEach
    public void after()
    {
        CloseHelper.close(receiveChannelEndpoint);
        driverConductor.onClose();
    }

    @Test
    public void shouldErrorWhenOriginalPublicationHasNoDistinguishingCharacteristicBeyondTag()
    {
        final String expectedMessage =
            "URI must have explicit control, endpoint, or be manual control-mode when original:";

        driverProxy.addPublication("aeron:udp?tags=1001", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockErrorHandler).onError(argThat(
            (ex) ->
            {
                assertThat(ex, instanceOf(InvalidChannelException.class));
                assertThat(ex.getMessage(), containsString(expectedMessage));
                return true;
            }));
    }


    @Test
    public void shouldBeAbleToAddSinglePublication()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        verify(senderProxy).registerSendChannelEndpoint(any());
        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();
        assertEquals(STREAM_ID_1, publication.streamId());

        verify(mockClientProxy).onPublicationReady(
            anyLong(), anyLong(), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
    }

    @Test
    public void shouldBeAbleToAddPublicationForReplay()
    {
        final int mtu = 1024 * 8;
        final int termLength = 128 * 1024;
        final int initialTermId = 7;
        final int termId = 11;
        final int termOffset = 64;
        final String params =
            "|mtu=" + mtu +
            "|term-length=" + termLength +
            "|init-term-id=" + initialTermId +
            "|term-id=" + termId +
            "|term-offset=" + termOffset;

        driverProxy.addExclusivePublication(CHANNEL_4000 + params, STREAM_ID_1);

        driverConductor.doWork();

        verify(senderProxy).registerSendChannelEndpoint(any());
        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();
        assertEquals(STREAM_ID_1, publication.streamId());
        assertEquals(mtu, publication.mtuLength());

        final long expectedPosition = termLength * (termId - initialTermId) + termOffset;
        assertEquals(expectedPosition, publication.producerPosition());
        assertEquals(expectedPosition, publication.consumerPosition());

        verify(mockClientProxy).onPublicationReady(
            anyLong(), anyLong(), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(true));
    }

    @Test
    public void shouldBeAbleToAddIpcPublicationForReplay()
    {
        final int termLength = 128 * 1024;
        final int initialTermId = 7;
        final int termId = 11;
        final int termOffset = 64;
        final String params =
            "?term-length=" + termLength +
            "|init-term-id=" + initialTermId +
            "|term-id=" + termId +
            "|term-offset=" + termOffset;

        driverProxy.addExclusivePublication(CHANNEL_IPC + params, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
        verify(mockClientProxy).onPublicationReady(
            anyLong(), captor.capture(), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(true));

        final long registrationId = captor.getValue();
        final IpcPublication publication = driverConductor.getIpcPublication(registrationId);
        assertNotNull(publication);
        assertEquals(STREAM_ID_1, publication.streamId());

        final long expectedPosition = termLength * (termId - initialTermId) + termOffset;
        assertEquals(expectedPosition, publication.producerPosition());
        assertEquals(expectedPosition, publication.consumerPosition());
    }

    @Test
    public void shouldBeAbleToAddSingleSubscription()
    {
        final long id = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        verify(receiverProxy).addSubscription(any(), eq(STREAM_ID_1));
        verify(mockClientProxy).onSubscriptionReady(eq(id), anyInt());
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscription()
    {
        final long id = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removeSubscription(id);

        driverConductor.doWork();

        verify(receiverProxy).registerReceiveChannelEndpoint(any());
        verify(receiverProxy).closeReceiveChannelEndpoint(any());
    }

    @Test
    public void shouldBeAbleToAddMultipleStreams()
    {
        driverProxy.addPublication(CHANNEL_4001, STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4002, STREAM_ID_2);
        driverProxy.addPublication(CHANNEL_4003, STREAM_ID_3);
        driverProxy.addPublication(CHANNEL_4004, STREAM_ID_4);

        while (true)
        {
            if (0 == driverConductor.doWork())
            {
                break;
            }
        }

        verify(senderProxy, times(4)).newNetworkPublication(any());
    }

    @Test
    public void shouldBeAbleToRemoveSingleStream()
    {
        final long id = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removePublication(id);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS + PUBLICATION_LINGER_TIMEOUT_NS * 2) - nanoClock.nanoTime() < 0);

        verify(senderProxy).registerSendChannelEndpoint(any());
        verify(senderProxy).removeNetworkPublication(any());
    }

    @Test
    public void shouldBeAbleToRemoveMultipleStreams()
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4001, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_4002, STREAM_ID_2);
        final long id3 = driverProxy.addPublication(CHANNEL_4003, STREAM_ID_3);
        final long id4 = driverProxy.addPublication(CHANNEL_4004, STREAM_ID_4);

        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);
        driverProxy.removePublication(id3);
        driverProxy.removePublication(id4);

        doWorkUntil(
            () -> (CLIENT_LIVENESS_TIMEOUT_NS * 2 + PUBLICATION_LINGER_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(senderProxy, times(4)).removeNetworkPublication(any());
    }

    @Test
    public void shouldKeepSubscriptionMediaEndpointUponRemovalOfAllButOneSubscriber()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_3);

        while (true)
        {
            if (0 == driverConductor.doWork())
            {
                break;
            }
        }

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        assertNotNull(receiveChannelEndpoint);
        assertEquals(3, receiveChannelEndpoint.streamCount());

        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        driverConductor.doWork();

        assertEquals(1, receiveChannelEndpoint.streamCount());
    }

    @Test
    public void shouldOnlyRemoveSubscriptionMediaEndpointUponRemovalOfAllSubscribers()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        final long id3 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_3);

        while (true)
        {
            if (0 == driverConductor.doWork())
            {
                break;
            }
        }

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        assertNotNull(receiveChannelEndpoint);
        assertEquals(3, receiveChannelEndpoint.streamCount());

        driverProxy.removeSubscription(id2);
        driverProxy.removeSubscription(id3);

        driverConductor.doWork();

        assertEquals(1, receiveChannelEndpoint.streamCount());

        driverProxy.removeSubscription(id1);

        driverConductor.doWork();

        verify(receiverProxy).closeReceiveChannelEndpoint(receiveChannelEndpoint);
    }

    @Test
    public void shouldErrorOnRemovePublicationOnUnknownRegistrationId()
    {
        final long id = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removePublication(id + 1);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(senderProxy, mockClientProxy);

        inOrder.verify(senderProxy).newNetworkPublication(any());
        inOrder.verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(id), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
        inOrder.verify(mockClientProxy).onError(anyLong(), eq(UNKNOWN_PUBLICATION), anyString());
        inOrder.verifyNoMoreInteractions();

        verify(mockErrorCounter).increment();
        verify(mockErrorHandler).onError(any(Throwable.class));
    }

    @Test
    public void shouldAddPublicationWithMtu()
    {
        final int mtuLength = 4096;
        final String mtuParam = "|" + CommonContext.MTU_LENGTH_PARAM_NAME + "=" + mtuLength;
        driverProxy.addPublication(CHANNEL_4000 + mtuParam, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        assertEquals(mtuLength, argumentCaptor.getValue().mtuLength());
    }

    @Test
    public void shouldErrorOnRemoveSubscriptionOnUnknownRegistrationId()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverProxy.removeSubscription(id1 + 100);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(receiverProxy, mockClientProxy);

        inOrder.verify(receiverProxy).addSubscription(any(), anyInt());
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(id1), anyInt());
        inOrder.verify(mockClientProxy).onError(anyLong(), eq(UNKNOWN_SUBSCRIPTION), anyString());
        inOrder.verifyNoMoreInteractions();

        verify(mockErrorHandler).onError(any(Throwable.class));
    }

    @Test
    public void shouldErrorOnAddSubscriptionWithInvalidChannel()
    {
        driverProxy.addSubscription(INVALID_URI, STREAM_ID_1);

        driverConductor.doWork();
        driverConductor.doWork();

        verify(senderProxy, never()).newNetworkPublication(any());

        verify(mockClientProxy).onError(anyLong(), eq(INVALID_CHANNEL), anyString());
        verify(mockClientProxy, never()).operationSucceeded(anyLong());

        verify(mockErrorCounter).increment();
        verify(mockErrorHandler).onError(any(Throwable.class));
    }

    @Test
    public void shouldTimeoutPublication()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS + PUBLICATION_LINGER_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(senderProxy).removeNetworkPublication(eq(publication));
        verify(senderProxy).registerSendChannelEndpoint(any());
    }

    @Test
    public void shouldNotTimeoutPublicationOnKeepAlive()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();
        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS / 2) - nanoClock.nanoTime() <= 0);

        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS + 1000) - nanoClock.nanoTime() <= 0);

        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(senderProxy, never()).removeNetworkPublication(eq(publication));
    }

    @Test
    public void shouldTimeoutPublicationWithNoKeepaliveButNotDrained()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());

        final NetworkPublication publication = captor.getValue();

        final int termId = 101;
        final int index = LogBufferDescriptor.indexByTerm(termId, termId);
        final RawLog rawLog = publication.rawLog();
        LogBufferDescriptor.rawTail(rawLog.metaData(), index, LogBufferDescriptor.packTail(termId, 0));
        final TermAppender appender = new TermAppender(rawLog.termBuffers()[index], rawLog.metaData(), index);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[256]);
        final HeaderWriter headerWriter = HeaderWriter.newInstance(
            createDefaultHeader(SESSION_ID, STREAM_ID_1, termId));

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(termId);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(10);

        publication.onStatusMessage(msg, new InetSocketAddress("localhost", 4059));
        appender.appendUnfragmentedMessage(headerWriter, srcBuffer, 0, 256, null, termId);

        assertEquals(NetworkPublication.State.ACTIVE, publication.state());

        doWorkUntil(
            () -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 1.25,
            (timeNs) ->
            {
                publication.onStatusMessage(msg, new InetSocketAddress("localhost", 4059));
                publication.updateHasReceivers(timeNs);
            });

        assertThat(publication.state(),
            Matchers.anyOf(is(NetworkPublication.State.DRAINING), is(NetworkPublication.State.LINGER)));

        final long endTime = nanoClock.nanoTime() + publicationConnectionTimeoutNs() + DEFAULT_TIMER_INTERVAL_NS;
        doWorkUntil(() -> nanoClock.nanoTime() >= endTime, publication::updateHasReceivers);

        assertThat(publication.state(),
            Matchers.anyOf(is(NetworkPublication.State.LINGER), is(NetworkPublication.State.DONE)));

        nanoClock.advance(DEFAULT_TIMER_INTERVAL_NS + PUBLICATION_LINGER_TIMEOUT_NS);
        driverConductor.doWork();
        assertEquals(NetworkPublication.State.DONE, publication.state());

        verify(senderProxy).removeNetworkPublication(eq(publication));
        verify(senderProxy).registerSendChannelEndpoint(any());
    }

    @Test
    public void shouldTimeoutSubscription()
    {
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, times(1))
            .removeSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));

        verify(receiverProxy).closeReceiveChannelEndpoint(receiveChannelEndpoint);
    }

    @Test
    public void shouldNotTimeoutSubscriptionOnKeepAlive()
    {
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        verify(receiverProxy).addSubscription(eq(receiveChannelEndpoint), eq(STREAM_ID_1));
        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS + 1000);

        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS * 2);

        verify(receiverProxy, never()).removeSubscription(any(), anyInt());
    }

    @Test
    public void shouldCreateImageOnSubscription()
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);
        final int initialTermId = 1;
        final int activeTermId = 2;
        final int termOffset = 100;

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor1 = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor1.capture());
        receiveChannelEndpoint = captor1.getValue();

        receiveChannelEndpoint.openChannel(driverConductorProxy);

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, initialTermId, activeTermId, termOffset, TERM_BUFFER_LENGTH, MTU_LENGTH, 0,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor2 = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor2.capture());

        final PublicationImage publicationImage = captor2.getValue();
        assertEquals(SESSION_ID, publicationImage.sessionId());
        assertEquals(STREAM_ID_1, publicationImage.streamId());

        verify(mockClientProxy).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), eq(SESSION_ID), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldNotCreateImageOnUnknownSubscription()
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor.capture());
        receiveChannelEndpoint = captor.getValue();

        receiveChannelEndpoint.openChannel(driverConductorProxy);

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_2, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH, 0,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        verify(receiverProxy, never()).newPublicationImage(any(), any());
        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), anyInt(), anyInt(), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldSignalInactiveImageWhenImageTimesOut()
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        final long subId = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor1 = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor1.capture());
        receiveChannelEndpoint = captor1.getValue();

        receiveChannelEndpoint.openChannel(driverConductorProxy);

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH, 0,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor2 = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor2.capture());

        final PublicationImage publicationImage = captor2.getValue();

        publicationImage.activate();
        publicationImage.deactivate();

        doWorkUntil(() -> nanoClock.nanoTime() >= imageLivenessTimeoutNs() + 1000);

        verify(mockClientProxy).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(subId), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldAlwaysGiveNetworkPublicationCorrelationIdToClientCallbacks()
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        final long subId1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor1 = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor1.capture());
        receiveChannelEndpoint = captor1.getValue();

        receiveChannelEndpoint.openChannel(driverConductorProxy);

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH, 0,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor2 = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor2.capture());

        final PublicationImage publicationImage = captor2.getValue();

        publicationImage.activate();

        final long subId2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        publicationImage.deactivate();

        doWorkUntil(() -> nanoClock.nanoTime() >= imageLivenessTimeoutNs() + 1000);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy, times(2)).onAvailableImage(
            eq(publicationImage.correlationId()),
            eq(STREAM_ID_1),
            eq(SESSION_ID),
            anyLong(),
            anyInt(),
            anyString(),
            anyString());
        inOrder.verify(mockClientProxy, times(1)).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(subId1), eq(STREAM_ID_1), anyString());
        inOrder.verify(mockClientProxy, times(1)).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(subId2), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldNotSendAvailableImageWhileImageNotActiveOnAddSubscription()
    {
        final InetSocketAddress sourceAddress = new InetSocketAddress("localhost", 4400);

        final long subOneId = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<ReceiveChannelEndpoint> captor1 = ArgumentCaptor.forClass(ReceiveChannelEndpoint.class);
        verify(receiverProxy).registerReceiveChannelEndpoint(captor1.capture());
        receiveChannelEndpoint = captor1.getValue();

        receiveChannelEndpoint.openChannel(driverConductorProxy);

        driverConductor.onCreatePublicationImage(
            SESSION_ID, STREAM_ID_1, 1, 1, 0, TERM_BUFFER_LENGTH, MTU_LENGTH, 0,
            mock(InetSocketAddress.class), sourceAddress, receiveChannelEndpoint);

        final ArgumentCaptor<PublicationImage> captor2 = ArgumentCaptor.forClass(PublicationImage.class);
        verify(receiverProxy).newPublicationImage(eq(receiveChannelEndpoint), captor2.capture());

        final PublicationImage publicationImage = captor2.getValue();

        publicationImage.activate();
        publicationImage.deactivate();

        doWorkUntil(() -> nanoClock.nanoTime() >= imageLivenessTimeoutNs() / 2);

        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);
        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> nanoClock.nanoTime() >= imageLivenessTimeoutNs() + 1000);

        final long subTwoId = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy, times(1)).onSubscriptionReady(eq(subOneId), anyInt());
        inOrder.verify(mockClientProxy, times(1)).onAvailableImage(
            eq(publicationImage.correlationId()),
            eq(STREAM_ID_1),
            eq(SESSION_ID),
            anyLong(),
            anyInt(),
            anyString(),
            anyString());
        inOrder.verify(mockClientProxy, times(1)).onUnavailableImage(
            eq(publicationImage.correlationId()), eq(subOneId), eq(STREAM_ID_1), anyString());
        inOrder.verify(mockClientProxy, times(1)).onSubscriptionReady(eq(subTwoId), anyInt());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldBeAbleToAddSingleIpcPublication()
    {
        final long id = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        assertNotNull(driverConductor.getSharedIpcPublication(STREAM_ID_1));
        verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(id), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
    }

    @Test
    public void shouldBeAbleToAddIpcPublicationThenSubscription()
    {
        final long idPub = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        final long idSub = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(idPub), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSub), anyInt());
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(ipcPublication.registrationId()), eq(STREAM_ID_1), eq(ipcPublication.sessionId()),
            anyLong(), anyInt(), eq(ipcPublication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddThenRemoveTheAddIpcPublicationWithExistingSubscription()
    {
        final long idSub = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idPubOne = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverConductor.doWork();

        final IpcPublication ipcPublicationOne = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublicationOne);

        final long idPubOneRemove = driverProxy.removePublication(idPubOne);
        driverConductor.doWork();

        final long idPubTwo = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverConductor.doWork();

        final IpcPublication ipcPublicationTwo = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublicationTwo);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSub), anyInt());
        inOrder.verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(idPubOne), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(ipcPublicationOne.registrationId()), eq(STREAM_ID_1), eq(ipcPublicationOne.sessionId()),
            anyLong(), anyInt(), eq(ipcPublicationOne.rawLog().fileName()), anyString());
        inOrder.verify(mockClientProxy).operationSucceeded(eq(idPubOneRemove));
        inOrder.verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(idPubTwo), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(ipcPublicationTwo.registrationId()), eq(STREAM_ID_1), eq(ipcPublicationTwo.sessionId()),
            anyLong(), anyInt(), eq(ipcPublicationTwo.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddSubscriptionThenIpcPublication()
    {
        final long idSub = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idPub = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSub), anyInt());
        inOrder.verify(mockClientProxy).onPublicationReady(
            anyLong(), eq(idPub), eq(STREAM_ID_1), anyInt(), any(), anyInt(), anyInt(), eq(false));
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(ipcPublication.registrationId()), eq(STREAM_ID_1), eq(ipcPublication.sessionId()),
            anyLong(), anyInt(), eq(ipcPublication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddAndRemoveIpcPublication()
    {
        final long idAdd = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removePublication(idAdd);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNull(ipcPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSubscriptionToIpcPublication()
    {
        final long idAdd = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removeSubscription(idAdd);

        doWorkUntil(() -> nanoClock.nanoTime() >= CLIENT_LIVENESS_TIMEOUT_NS);

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNull(ipcPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveTwoIpcPublications()
    {
        final long idAdd1 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        final long idAdd2 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removePublication(idAdd1);

        driverConductor.doWork();

        IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        driverProxy.removePublication(idAdd2);

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNull(ipcPublication);
    }

    @Test
    public void shouldBeAbleToAddAndRemoveIpcPublicationAndSubscription()
    {
        final long idAdd1 = driverProxy.addSubscription(CHANNEL_IPC, STREAM_ID_1);
        final long idAdd2 = driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);
        driverProxy.removeSubscription(idAdd1);

        driverConductor.doWork();

        IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        driverProxy.removePublication(idAdd2);

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNull(ipcPublication);
    }

    @Test
    public void shouldTimeoutIpcPublication()
    {
        driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNull(ipcPublication);
    }

    @Test
    public void shouldNotTimeoutIpcPublicationWithKeepalive()
    {
        driverProxy.addPublication(CHANNEL_IPC, STREAM_ID_1);

        driverConductor.doWork();

        IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);
        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);
    }

    @Test
    public void shouldBeAbleToAddSingleSpy()
    {
        final long id = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        verify(receiverProxy, never()).registerReceiveChannelEndpoint(any());
        verify(receiverProxy, never()).addSubscription(any(), eq(STREAM_ID_1));
        verify(mockClientProxy).onSubscriptionReady(eq(id), anyInt());
    }

    @Test
    public void shouldBeAbleToAddNetworkPublicationThenSingleSpy()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSpy), anyInt());
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddSingleSpyThenNetworkPublication()
    {
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSpy), anyInt());
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddNetworkPublicationThenSingleSpyThenRemoveSpy()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);
        driverProxy.removeSubscription(idSpy);

        while (true)
        {
            if (0 == driverConductor.doWork())
            {
                break;
            }
        }

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertFalse(publication.hasSpies());
    }

    @Test
    public void shouldTimeoutSpy()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        assertFalse(publication.hasSpies());
    }

    @Test
    public void shouldNotTimeoutSpyWithKeepalive()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);
        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> CLIENT_LIVENESS_TIMEOUT_NS - nanoClock.nanoTime() <= 0);

        assertTrue(publication.hasSpies());
    }

    @Test
    public void shouldTimeoutNetworkPublicationWithSpy()
    {
        final long clientId = toDriverCommands.nextCorrelationId();
        final DriverProxy spyDriverProxy = new DriverProxy(toDriverCommands, clientId);

        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long subId = spyDriverProxy.addSubscription(spyForChannel(CHANNEL_4000), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();
        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS / 2) - nanoClock.nanoTime() <= 0);

        heartbeatCounter.setOrdered(epochClock.time());

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS + 1000) - nanoClock.nanoTime() <= 0);

        heartbeatCounter.setOrdered(0);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(mockClientProxy).onUnavailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(subId), eq(STREAM_ID_1), anyString());
    }

    @Test
    public void shouldOnlyCloseSendChannelEndpointOnceWithMultiplePublications()
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_4000, STREAM_ID_2);
        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);

        doWorkUntil(() -> (PUBLICATION_LINGER_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(senderProxy, times(1)).closeSendChannelEndpoint(any());
    }

    @Test
    public void shouldOnlyCloseReceiveChannelEndpointOnceWithMultipleSubscriptions()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_2);
        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(receiverProxy, times(1)).closeReceiveChannelEndpoint(any());
    }

    @Test
    public void shouldErrorWhenConflictingUnreliableSubscriptionAdded()
    {
        driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000 + "|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(id2), any(ErrorCode.class), anyString());
    }

    @Test
    public void shouldErrorWhenConflictingUnreliableSessionSpecificSubscriptionAdded()
    {
        driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1024", STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1024|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(id2), any(ErrorCode.class), anyString());
    }

    @Test
    public void shouldNotErrorWhenConflictingUnreliableSessionSpecificSubscriptionAddedToDifferentSessions()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1024|reliable=true", STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1025|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onSubscriptionReady(eq(id1), anyInt());
        verify(mockClientProxy).onSubscriptionReady(eq(id2), anyInt());
    }

    @Test
    public void shouldNotErrorWhenConflictingUnreliableSessionSpecificSubscriptionAddedToDifferentSessionsVsWildcard()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1024|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000 + "|reliable=true", STREAM_ID_1);
        driverConductor.doWork();

        final long id3 = driverProxy.addSubscription(CHANNEL_4000 + "|session-id=1025|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onSubscriptionReady(eq(id1), anyInt());
        verify(mockClientProxy).onSubscriptionReady(eq(id2), anyInt());
        verify(mockClientProxy).onSubscriptionReady(eq(id3), anyInt());
    }

    @Test
    public void shouldErrorWhenConflictingDefaultReliableSubscriptionAdded()
    {
        driverProxy.addSubscription(CHANNEL_4000 + "|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(id2), any(ErrorCode.class), anyString());
    }

    @Test
    public void shouldErrorWhenConflictingReliableSubscriptionAdded()
    {
        driverProxy.addSubscription(CHANNEL_4000 + "|reliable=false", STREAM_ID_1);
        driverConductor.doWork();

        final long id2 = driverProxy.addSubscription(CHANNEL_4000 + "|reliable=true", STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(id2), any(ErrorCode.class), anyString());
    }

    @Test
    public void shouldAddSingleCounter()
    {
        final long registrationId = driverProxy.addCounter(
            COUNTER_TYPE_ID,
            counterKeyAndLabel,
            COUNTER_KEY_OFFSET,
            COUNTER_KEY_LENGTH,
            counterKeyAndLabel,
            COUNTER_LABEL_OFFSET,
            COUNTER_LABEL_LENGTH);

        driverConductor.doWork();

        verify(mockClientProxy).onCounterReady(eq(registrationId), anyInt());
        verify(spyCountersManager).newCounter(
            eq(COUNTER_TYPE_ID),
            any(),
            anyInt(),
            eq(COUNTER_KEY_LENGTH),
            any(),
            anyInt(),
            eq(COUNTER_LABEL_LENGTH));
    }

    @Test
    public void shouldRemoveSingleCounter()
    {
        final long registrationId = driverProxy.addCounter(
            COUNTER_TYPE_ID,
            counterKeyAndLabel,
            COUNTER_KEY_OFFSET,
            COUNTER_KEY_LENGTH,
            counterKeyAndLabel,
            COUNTER_LABEL_OFFSET,
            COUNTER_LABEL_LENGTH);

        driverConductor.doWork();

        final long removeCorrelationId = driverProxy.removeCounter(registrationId);
        driverConductor.doWork();

        final ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onCounterReady(eq(registrationId), captor.capture());
        inOrder.verify(mockClientProxy).operationSucceeded(removeCorrelationId);

        verify(spyCountersManager).free(captor.getValue());
    }

    @Test
    public void shouldRemoveCounterOnClientTimeout()
    {
        final long registrationId = driverProxy.addCounter(
            COUNTER_TYPE_ID,
            counterKeyAndLabel,
            COUNTER_KEY_OFFSET,
            COUNTER_KEY_LENGTH,
            counterKeyAndLabel,
            COUNTER_LABEL_OFFSET,
            COUNTER_LABEL_LENGTH);

        driverConductor.doWork();

        final ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        verify(mockClientProxy).onCounterReady(eq(registrationId), captor.capture());

        doWorkUntil(() -> (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(spyCountersManager).free(captor.getValue());
    }

    @Test
    public void shouldNotRemoveCounterOnClientKeepalive()
    {
        final long registrationId = driverProxy.addCounter(
            COUNTER_TYPE_ID,
            counterKeyAndLabel,
            COUNTER_KEY_OFFSET,
            COUNTER_KEY_LENGTH,
            counterKeyAndLabel,
            COUNTER_LABEL_OFFSET,
            COUNTER_LABEL_LENGTH);

        driverConductor.doWork();

        final ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        verify(mockClientProxy).onCounterReady(eq(registrationId), captor.capture());
        final AtomicCounter heartbeatCounter = clientHeartbeatCounter(spyCountersManager);

        doWorkUntil(() ->
        {
            heartbeatCounter.setOrdered(epochClock.time());
            return (CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0;
        });

        verify(spyCountersManager, never()).free(captor.getValue());
    }

    @Test
    public void shouldInformClientsOfRemovedCounter()
    {
        final long registrationId = driverProxy.addCounter(
            COUNTER_TYPE_ID,
            counterKeyAndLabel,
            COUNTER_KEY_OFFSET,
            COUNTER_KEY_LENGTH,
            counterKeyAndLabel,
            COUNTER_LABEL_OFFSET,
            COUNTER_LABEL_LENGTH);

        driverConductor.doWork();

        final long removeCorrelationId = driverProxy.removeCounter(registrationId);
        driverConductor.doWork();

        final ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onCounterReady(eq(registrationId), captor.capture());
        inOrder.verify(mockClientProxy).operationSucceeded(removeCorrelationId);
        inOrder.verify(mockClientProxy).onUnavailableCounter(eq(registrationId), captor.capture());

        verify(spyCountersManager).free(captor.getValue());
    }

    @Test
    public void shouldAddPublicationWithSessionId()
    {
        final int sessionId = 4096;
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        driverProxy.addPublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        assertEquals(sessionId, argumentCaptor.getValue().sessionId());
    }

    @Test
    public void shouldAddExclusivePublicationWithSessionId()
    {
        final int sessionId = 4096;
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        driverProxy.addExclusivePublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        assertEquals(sessionId, argumentCaptor.getValue().sessionId());
    }

    @Test
    public void shouldAddPublicationWithSameSessionId()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        final int sessionId = argumentCaptor.getValue().sessionId();
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        driverProxy.addPublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy, times(2)).onPublicationReady(
            anyLong(), anyLong(), eq(STREAM_ID_1), eq(sessionId), anyString(), anyInt(), anyInt(), eq(false));
    }

    @Test
    public void shouldAddExclusivePublicationWithSameSessionId()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        final int sessionId = argumentCaptor.getValue().sessionId();
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + (sessionId + 1);
        driverProxy.addExclusivePublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onPublicationReady(
            anyLong(), anyLong(), eq(STREAM_ID_1), eq(sessionId), anyString(), anyInt(), anyInt(), eq(false));
        verify(mockClientProxy).onPublicationReady(
            anyLong(), anyLong(), eq(STREAM_ID_1), eq(sessionId + 1), anyString(), anyInt(), anyInt(), eq(true));
    }

    @Test
    public void shouldErrorOnAddPublicationWithNonEqualSessionId()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        final String sessionIdParam =
            "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + (argumentCaptor.getValue().sessionId() + 1);
        final long correlationId = driverProxy.addPublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(correlationId), eq(GENERIC_ERROR), anyString());
        verify(mockErrorCounter).increment();
        verify(mockErrorHandler).onError(any(Throwable.class));
    }

    @Test
    public void shouldErrorOnAddPublicationWithClashingSessionId()
    {
        driverProxy.addPublication(CHANNEL_4000, STREAM_ID_1);
        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> argumentCaptor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy).newNetworkPublication(argumentCaptor.capture());

        final String sessionIdParam =
            "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + argumentCaptor.getValue().sessionId();
        final long correlationId = driverProxy.addExclusivePublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);
        driverConductor.doWork();

        verify(mockClientProxy).onError(eq(correlationId), eq(GENERIC_ERROR), anyString());
        verify(mockErrorCounter).increment();
        verify(mockErrorHandler).onError(any(Throwable.class));
    }

    @Test
    public void shouldAddIpcPublicationThenSubscriptionWithSessionId()
    {
        final int sessionId = -4097;
        final String sessionIdParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        final String channelIpcAndSessionId = CHANNEL_IPC + sessionIdParam;

        driverProxy.addPublication(channelIpcAndSessionId, STREAM_ID_1);
        driverProxy.addSubscription(channelIpcAndSessionId, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        verify(mockClientProxy).onAvailableImage(
            eq(ipcPublication.registrationId()), eq(STREAM_ID_1), eq(ipcPublication.sessionId()),
            anyLong(), anyInt(), eq(ipcPublication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldAddIpcSubscriptionThenPublicationWithSessionId()
    {
        final int sessionId = -4097;
        final String sessionIdParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        final String channelIpcAndSessionId = CHANNEL_IPC + sessionIdParam;

        driverProxy.addSubscription(channelIpcAndSessionId, STREAM_ID_1);
        driverProxy.addPublication(channelIpcAndSessionId, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        verify(mockClientProxy).onAvailableImage(
            eq(ipcPublication.registrationId()), eq(STREAM_ID_1), eq(ipcPublication.sessionId()),
            anyLong(), anyInt(), eq(ipcPublication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldNotAddIpcPublicationThenSubscriptionWithDifferentSessionId()
    {
        final int sessionIdPub = -4097;
        final int sessionIdSub = -4098;
        final String sessionIdPubParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdPub;
        final String sessionIdSubParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdSub;

        driverProxy.addPublication(CHANNEL_IPC + sessionIdPubParam, STREAM_ID_1);
        driverProxy.addSubscription(CHANNEL_IPC + sessionIdSubParam, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), anyInt(), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldNotAddIpcSubscriptionThenPublicationWithDifferentSessionId()
    {
        final int sessionIdPub = -4097;
        final int sessionIdSub = -4098;
        final String sessionIdPubParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdPub;
        final String sessionIdSubParam = "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdSub;

        driverProxy.addSubscription(CHANNEL_IPC + sessionIdSubParam, STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_IPC + sessionIdPubParam, STREAM_ID_1);

        driverConductor.doWork();

        final IpcPublication ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID_1);
        assertNotNull(ipcPublication);

        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), anyInt(), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldAddNetworkPublicationThenSingleSpyWithSameSessionId()
    {
        final int sessionId = -4097;
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        driverProxy.addPublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000 + sessionIdParam), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldNotAddNetworkPublicationThenSingleSpyWithDifferentSessionId()
    {
        final int sessionIdPub = -4097;
        final int sessionIdSub = -4098;
        final String sessionIdPubParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdPub;
        final String sessionIdSubParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdSub;
        driverProxy.addPublication(CHANNEL_4000 + sessionIdPubParam, STREAM_ID_1);
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000 + sessionIdSubParam), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertFalse(publication.hasSpies());

        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), anyInt(), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldAddSingleSpyThenNetworkPublicationWithSameSessionId()
    {
        final int sessionId = -4097;
        final String sessionIdParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionId;
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000 + sessionIdParam), STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4000 + sessionIdParam, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldNotAddSingleSpyThenNetworkPublicationWithDifferentSessionId()
    {
        final int sessionIdPub = -4097;
        final int sessionIdSub = -4098;
        final String sessionIdPubParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdPub;
        final String sessionIdSubParam = "|" + CommonContext.SESSION_ID_PARAM_NAME + "=" + sessionIdSub;
        driverProxy.addSubscription(spyForChannel(CHANNEL_4000 + sessionIdSubParam), STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4000 + sessionIdPubParam, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertFalse(publication.hasSpies());

        verify(mockClientProxy, never()).onAvailableImage(
            anyLong(), eq(STREAM_ID_1), anyInt(), anyLong(), anyInt(), anyString(), anyString());
    }

    @Test
    public void shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdAndSameStreamId()
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4000_TAG_ID_1, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_TAG_ID_1, STREAM_ID_1);

        driverConductor.doWork();
        verify(mockErrorHandler, never()).onError(any());

        verify(senderProxy).registerSendChannelEndpoint(any());
        verify(senderProxy).newNetworkPublication(any());

        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);

        doWorkUntil(
            () -> (PUBLICATION_LINGER_TIMEOUT_NS * 2 + CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(senderProxy).closeSendChannelEndpoint(any());
    }

    @Test
    public void shouldUseExistingChannelEndpointOnAddPublicationWithSameTagIdDifferentStreamId()
    {
        final long id1 = driverProxy.addPublication(CHANNEL_4000_TAG_ID_1, STREAM_ID_1);
        final long id2 = driverProxy.addPublication(CHANNEL_TAG_ID_1, STREAM_ID_2);

        driverConductor.doWork();
        verify(mockErrorHandler, never()).onError(any());

        verify(senderProxy).registerSendChannelEndpoint(any());
        verify(senderProxy, times(2)).newNetworkPublication(any());

        driverProxy.removePublication(id1);
        driverProxy.removePublication(id2);

        doWorkUntil(
            () -> (PUBLICATION_LINGER_TIMEOUT_NS * 2 + CLIENT_LIVENESS_TIMEOUT_NS * 2) - nanoClock.nanoTime() <= 0);

        verify(senderProxy).closeSendChannelEndpoint(any());
    }

    @Test
    public void shouldUseExistingChannelEndpointOnAddSubscriptionWithSameTagId()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_4000_TAG_ID_1, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_TAG_ID_1, STREAM_ID_1);

        driverConductor.doWork();
        verify(mockErrorHandler, never()).onError(any());

        verify(receiverProxy).registerReceiveChannelEndpoint(any());

        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        driverConductor.doWork();

        verify(receiverProxy).closeReceiveChannelEndpoint(any());
    }

    @Test
    public void shouldUseUniqueChannelEndpointOnAddSubscriptionWithNoDistinguishingCharacteristics()
    {
        final long id1 = driverProxy.addSubscription(CHANNEL_SUB_CONTROL_MODE_MANUAL, STREAM_ID_1);
        final long id2 = driverProxy.addSubscription(CHANNEL_SUB_CONTROL_MODE_MANUAL, STREAM_ID_1);

        driverConductor.doWork();

        verify(receiverProxy, times(2)).registerReceiveChannelEndpoint(any());

        driverProxy.removeSubscription(id1);
        driverProxy.removeSubscription(id2);

        driverConductor.doWork();

        verify(receiverProxy, times(2)).closeReceiveChannelEndpoint(any());

        verify(mockErrorHandler, never()).onError(any());
    }

    @Test
    public void shouldBeAbleToAddNetworkPublicationThenSingleSpyWithTag()
    {
        driverProxy.addPublication(CHANNEL_4000_TAG_ID_1, STREAM_ID_1);
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_TAG_ID_1), STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSpy), anyInt());
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    public void shouldBeAbleToAddSingleSpyThenNetworkPublicationWithTag()
    {
        final long idSpy = driverProxy.addSubscription(spyForChannel(CHANNEL_TAG_ID_1), STREAM_ID_1);
        driverProxy.addPublication(CHANNEL_4000_TAG_ID_1, STREAM_ID_1);

        driverConductor.doWork();

        final ArgumentCaptor<NetworkPublication> captor = ArgumentCaptor.forClass(NetworkPublication.class);
        verify(senderProxy, times(1)).newNetworkPublication(captor.capture());
        final NetworkPublication publication = captor.getValue();

        assertTrue(publication.hasSpies());

        final InOrder inOrder = inOrder(mockClientProxy);
        inOrder.verify(mockClientProxy).onSubscriptionReady(eq(idSpy), anyInt());
        inOrder.verify(mockClientProxy).onAvailableImage(
            eq(networkPublicationCorrelationId(publication)), eq(STREAM_ID_1), eq(publication.sessionId()),
            anyLong(), anyInt(), eq(publication.rawLog().fileName()), anyString());
    }

    @Test
    void shouldIncrementCounterOnConductorThresholdExceeded()
    {
        final AtomicCounter maxCycleTime = spySystemCounters.get(CONDUCTOR_MAX_CYCLE_TIME);
        final AtomicCounter thresholdExceeded = spySystemCounters.get(CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED);

        driverConductor.doWork();
        nanoClock.advance(MILLISECONDS.toNanos(750));
        driverConductor.doWork();
        nanoClock.advance(MILLISECONDS.toNanos(1000));
        driverConductor.doWork();
        nanoClock.advance(MILLISECONDS.toNanos(500));
        driverConductor.doWork();
        nanoClock.advance(MILLISECONDS.toNanos(600));
        driverConductor.doWork();
        nanoClock.advance(MILLISECONDS.toNanos(601));
        driverConductor.doWork();

        assertEquals(SECONDS.toNanos(1), maxCycleTime.get());
        assertEquals(3, thresholdExceeded.get());
    }

    private void doWorkUntil(final BooleanSupplier condition, final LongConsumer timeConsumer)
    {
        while (!condition.getAsBoolean())
        {
            final long millisecondsToAdvance = 16;

            nanoClock.advance(TimeUnit.MILLISECONDS.toNanos(millisecondsToAdvance));
            epochClock.advance(millisecondsToAdvance);
            timeConsumer.accept(nanoClock.nanoTime());
            driverConductor.doWork();
        }
    }

    private void doWorkUntil(final BooleanSupplier condition)
    {
        doWorkUntil(condition, (j) -> {});
    }

    private static String spyForChannel(final String channel)
    {
        return CommonContext.SPY_PREFIX + channel;
    }

    private static long networkPublicationCorrelationId(final NetworkPublication publication)
    {
        return LogBufferDescriptor.correlationId(publication.rawLog().metaData());
    }

    private static AtomicCounter clientHeartbeatCounter(final CountersReader countersReader)
    {
        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            final int counterState = countersReader.getCounterState(i);
            if (counterState == RECORD_ALLOCATED && countersReader.getCounterTypeId(i) == HEARTBEAT_TYPE_ID)
            {
                return new AtomicCounter(countersReader.valuesBuffer(), i);
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        throw new IllegalStateException("could not find client heartbeat counter");
    }
}
