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
package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.aeron.common.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.Signal;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.ErrorCode.INVALID_CHANNEL;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.ON_CONNECTION_READY;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.ON_PUBLICATION_READY;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

public class ClientConductorTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final int NUM_BUFFERS = (PARTITION_COUNT * 2) + 1;

    protected static final int SESSION_ID_1 = 13;
    protected static final int SESSION_ID_2 = 15;

    private static final int COUNTER_BUFFER_LENGTH = 1024;
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int STREAM_ID_2 = 4;
    private static final int SEND_BUFFER_CAPACITY = 1024;

    private static final int BROADCAST_BUFFER_LENGTH = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    private static final long CORRELATION_ID = 2000;
    private static final long CORRELATION_ID_2 = 2002;

    private static final int AWAIT_TIMEOUT = 100;

    private static final String SOURCE_NAME = "127.0.0.1:40789";

    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ConnectionBuffersReadyFlyweight connectionReady = new ConnectionBuffersReadyFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(sendBuffer);

    private final UnsafeBuffer toClientBuffer = new UnsafeBuffer(new byte[BROADCAST_BUFFER_LENGTH]);
    private final CopyBroadcastReceiver toClientReceiver = new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final UnsafeBuffer counterValuesBuffer = new UnsafeBuffer(new byte[COUNTER_BUFFER_LENGTH]);

    private final TimerWheel timerWheel = mock(TimerWheel.class);
    private final Consumer<Throwable> mockClientErrorHandler = Throwable::printStackTrace;

    private Signal signal;
    private DriverProxy driverProxy;
    private ClientConductor conductor;
    private DataHandler dataHandler = mock(DataHandler.class);
    private InactiveConnectionHandler mockInactiveConnectionHandler = mock(InactiveConnectionHandler.class);
    private LogBuffersFactory logBuffersFactory = mock(LogBuffersFactory.class);

    @Before
    public void setUp() throws Exception
    {
        driverProxy = mock(DriverProxy.class);
        signal = mock(Signal.class);

        when(driverProxy.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1)).thenReturn(CORRELATION_ID);
        when(driverProxy.addPublication(CHANNEL, STREAM_ID_2, SESSION_ID_2)).thenReturn(CORRELATION_ID_2);
        when(driverProxy.addSubscription(any(), anyInt())).thenReturn(CORRELATION_ID);

        willNotifyNewBuffer(STREAM_ID_1, SESSION_ID_1, CORRELATION_ID);

        conductor = new ClientConductor(
            toClientReceiver,
            logBuffersFactory,
            counterValuesBuffer,
            driverProxy,
            signal,
            timerWheel,
            mockClientErrorHandler,
            null,
            mockInactiveConnectionHandler,
            AWAIT_TIMEOUT);

        publicationReady.wrap(atomicSendBuffer, 0);
        connectionReady.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);

        final UnsafeBuffer[] atomicBuffersSession1 = new UnsafeBuffer[NUM_BUFFERS];
        final UnsafeBuffer[] atomicBuffersSession2 = new UnsafeBuffer[NUM_BUFFERS];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            final UnsafeBuffer termBuffersSession1 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            final UnsafeBuffer metaDataBuffersSession1 = new UnsafeBuffer(new byte[TERM_META_DATA_LENGTH]);
            final UnsafeBuffer termBuffersSession2 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            final UnsafeBuffer metaDataBuffersSession2 = new UnsafeBuffer(new byte[TERM_META_DATA_LENGTH]);

            atomicBuffersSession1[i] = termBuffersSession1;
            atomicBuffersSession1[i + PARTITION_COUNT] = metaDataBuffersSession1;
            atomicBuffersSession2[i] = termBuffersSession2;
            atomicBuffersSession2[i + PARTITION_COUNT] = metaDataBuffersSession2;
        }

        atomicBuffersSession1[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
        atomicBuffersSession2[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);

        final MutableDirectBuffer header1 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_1, STREAM_ID_1, 0);
        final MutableDirectBuffer header2 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_2, STREAM_ID_2, 0);

        LogBufferDescriptor.storeDefaultFrameHeaders(atomicBuffersSession1[LOG_META_DATA_SECTION_INDEX], header1);
        LogBufferDescriptor.storeDefaultFrameHeaders(atomicBuffersSession2[LOG_META_DATA_SECTION_INDEX], header2);

        final LogBuffers logBuffersSession1 = mock(LogBuffers.class);
        final LogBuffers logBuffersSession2 = mock(LogBuffers.class);

        when(logBuffersFactory.map(eq(SESSION_ID_1 + "-log"))).thenReturn(logBuffersSession1);
        when(logBuffersFactory.map(eq(SESSION_ID_2 + "-log"))).thenReturn(logBuffersSession2);

        when(logBuffersSession1.atomicBuffers()).thenReturn(atomicBuffersSession1);
        when(logBuffersSession2.atomicBuffers()).thenReturn(atomicBuffersSession2);
    }

    // --------------------------------
    // Publication related interactions
    // --------------------------------

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        addPublication();

        verify(driverProxy).addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test(expected = DriverTimeoutException.class)
    public void cannotCreatePublisherUntilBuffersMapped()
    {
        willSignalTimeOut();

        addPublication();
    }

    @Test
    public void conductorCachesPublicationInstances()
    {
        final Publication firstPublication = addPublication();
        final Publication secondPublication = addPublication();

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        final Publication publication = addPublication();
        willNotifyOperationSucceeded();

        publication.close();

        driverProxy.removePublication(CORRELATION_ID);
    }

    @Test
    public void closingPublicationShouldPurgeCache() throws Exception
    {
        final Publication firstPublication = addPublication();

        willNotifyOperationSucceeded();
        firstPublication.close();

        willNotifyNewBuffer(STREAM_ID_1, SESSION_ID_1, CORRELATION_ID);
        final Publication secondPublication = addPublication();

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToRemoveOnMediaDriverError()
    {
        final Publication publication = addPublication();

        doAnswer(
            (invocation) ->
            {
                conductor.onError(INVALID_CHANNEL, "channel unknown");
                return null;
            }).when(signal).await(anyLong());

        publication.close();
    }

    @Test
    public void publicationsOnlyRemovedOnLastClose() throws Exception
    {
        final Publication publication = addPublication();
        addPublication();

        publication.close();
        verify(driverProxy, never()).removePublication(CORRELATION_ID);

        willNotifyOperationSucceeded();

        publication.close();
        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    public void closingAPublicationDoesNotRemoveOtherPublications() throws Exception
    {
        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        willNotifyNewBuffer(STREAM_ID_2, SESSION_ID_2, CORRELATION_ID_2);
        conductor.addPublication(CHANNEL, STREAM_ID_2, SESSION_ID_2);

        willNotifyOperationSucceeded();
        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
        verify(driverProxy, never()).removePublication(CORRELATION_ID_2);
    }

    // ---------------------------------
    // Subscription related interactions
    // ---------------------------------

    @Test
    public void registeringSubscriberNotifiesMediaDriver() throws Exception
    {
        willNotifyOperationSucceeded();

        addSubscription();

        verify(driverProxy).addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void removingSubscriberNotifiesMediaDriver()
    {
        willNotifyOperationSucceeded();

        final Subscription subscription = addSubscription();

        subscription.close();

        verify(driverProxy).removeSubscription(CORRELATION_ID);
    }

    @Test(expected = DriverTimeoutException.class)
    public void cannotCreateSubscriberIfMediaDriverDoesNotReply()
    {
        willSignalTimeOut();

        addSubscription();
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToAddSubscriptionOnMediaDriverError()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onError(INVALID_CHANNEL, "Multicast data address must be odd");
                return null;
            }).when(signal).await(anyLong());

        addSubscription();
    }

    @Test
    public void clientNotifiedOfInactiveConnections()
    {
        willNotifyOperationSucceeded();

        final Subscription subscription = addSubscription();

        sendConnectionReady(SESSION_ID_1, STREAM_ID_1, CORRELATION_ID);
        conductor.doWork();

        assertFalse(subscription.hasNoConnections());

        conductor.onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, CORRELATION_ID);

        verify(mockInactiveConnectionHandler).onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        assertTrue(subscription.hasNoConnections());
        assertFalse(subscription.isConnected(SESSION_ID_1));
    }

    private Subscription addSubscription()
    {
        return conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);
    }

    private void sendPublicationReady(final int sessionId, final int streamId, final long correlationId)
    {
        publicationReady.streamId(streamId)
                        .sessionId(sessionId)
                        .correlationId(correlationId);

        publicationReady.channel(CHANNEL);
        publicationReady.logFileName(sessionId + "-log");

        toClientTransmitter.transmit(ON_PUBLICATION_READY, atomicSendBuffer, 0, publicationReady.length());
    }

    private void sendConnectionReady(final int sessionId, final int streamId, final long correlationId)
    {
        connectionReady.streamId(streamId)
                       .sessionId(sessionId)
                       .correlationId(correlationId);

        connectionReady.channel(CHANNEL);
        connectionReady.logFileName(sessionId + "-log");
        connectionReady.sourceInfo(SOURCE_NAME);

        connectionReady.positionIndicatorCount(1);
        connectionReady.positionIndicatorCounterId(0, 0);
        connectionReady.positionIndicatorRegistrationId(0, correlationId);

        toClientTransmitter.transmit(ON_CONNECTION_READY, atomicSendBuffer, 0, connectionReady.length());
    }

    private void willSignalTimeOut()
    {
        doAnswer(
            (invocation) ->
            {
                Thread.sleep(AWAIT_TIMEOUT + 1);
                return null;
            }).when(signal).await(anyLong());
    }

    private void willNotifyOperationSucceeded()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded();
                return null;
            }).when(signal).await(anyLong());
    }

    private void willNotifyNewBuffer(final int streamId, final int sessionId, final long correlationId)
    {
        doAnswer(
            (invocation) ->
            {
                sendPublicationReady(sessionId, streamId, correlationId);
                conductor.doWork();
                return null;
            }).when(signal).await(anyLong());
    }

    private Publication addPublication()
    {
        return conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }
}
