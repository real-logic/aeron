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
import uk.co.real_logic.aeron.common.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.ErrorCode.INVALID_CHANNEL;
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

    private static final long CORRELATION_ID = 2000;
    private static final long CORRELATION_ID_2 = 2002;
    private static final long CLOSE_CORRELATION_ID = 2001;

    private static final int AWAIT_TIMEOUT = 100;

    private static final String SOURCE_INFO = "127.0.0.1:40789";

    private final ConnectionBuffersReadyFlyweight connectionReady = new ConnectionBuffersReadyFlyweight();

    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY));

    private final CopyBroadcastReceiver mockToClientReceiver = mock(CopyBroadcastReceiver.class);

    private final UnsafeBuffer counterValuesBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_BUFFER_LENGTH));

    private final TimerWheel timerWheel = mock(TimerWheel.class);
    private final Consumer<Throwable> mockClientErrorHandler = Throwable::printStackTrace;

    private DriverProxy driverProxy;
    private ClientConductor conductor;
    private DataHandler dataHandler = mock(DataHandler.class);
    private NewConnectionHandler mockNewConnectionHandler = mock(NewConnectionHandler.class);
    private InactiveConnectionHandler mockInactiveConnectionHandler = mock(InactiveConnectionHandler.class);
    private LogBuffersFactory logBuffersFactory = mock(LogBuffersFactory.class);

    @Before
    public void setUp() throws Exception
    {
        driverProxy = mock(DriverProxy.class);

        when(driverProxy.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1)).thenReturn(CORRELATION_ID);
        when(driverProxy.addPublication(CHANNEL, STREAM_ID_2, SESSION_ID_2)).thenReturn(CORRELATION_ID_2);
        when(driverProxy.removePublication(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);
        when(driverProxy.addSubscription(anyString(), anyInt())).thenReturn(CORRELATION_ID);
        when(driverProxy.removeSubscription(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);
        when(timerWheel.clock()).thenReturn(System::nanoTime);

        conductor = new ClientConductor(
            mockToClientReceiver,
            logBuffersFactory,
            counterValuesBuffer,
            driverProxy,
            timerWheel,
            mockClientErrorHandler,
            mockNewConnectionHandler,
            mockInactiveConnectionHandler,
            AWAIT_TIMEOUT);

        connectionReady.wrap(atomicSendBuffer, 0);

        connectionReady.sourceInfo(SOURCE_INFO);
        connectionReady.subscriberPositionCount(1);
        connectionReady.subscriberPositionId(0, 0);
        connectionReady.positionIndicatorRegistrationId(0, CORRELATION_ID);

        final UnsafeBuffer[] atomicBuffersSession1 = new UnsafeBuffer[NUM_BUFFERS];
        final UnsafeBuffer[] atomicBuffersSession2 = new UnsafeBuffer[NUM_BUFFERS];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            final UnsafeBuffer termBuffersSession1 = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));
            final UnsafeBuffer metaDataBuffersSession1 = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_META_DATA_LENGTH));
            final UnsafeBuffer termBuffersSession2 = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));
            final UnsafeBuffer metaDataBuffersSession2 = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_META_DATA_LENGTH));

            atomicBuffersSession1[i] = termBuffersSession1;
            atomicBuffersSession1[i + PARTITION_COUNT] = metaDataBuffersSession1;
            atomicBuffersSession2[i] = termBuffersSession2;
            atomicBuffersSession2[i + PARTITION_COUNT] = metaDataBuffersSession2;
        }

        atomicBuffersSession1[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));
        atomicBuffersSession2[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));

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
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        verify(driverProxy).addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test(expected = DriverTimeoutException.class)
    public void cannotCreatePublisherUntilBuffersMapped()
    {
        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test
    public void conductorCachesPublicationInstances()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    public void closingPublicationShouldPurgeCache() throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        firstPublication.close();

        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToRemoveOnMediaDriverError()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        doAnswer(
            (invocation) ->
            {
                conductor.onError(INVALID_CHANNEL, "channel unknown", CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        publication.close();
    }

    @Test
    public void publicationsOnlyRemovedOnLastClose() throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        publication.close();

        verify(driverProxy, never()).removePublication(CORRELATION_ID);

        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    public void closingAPublicationDoesNotRemoveOtherPublications() throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_1, SESSION_ID_1, 0, SESSION_ID_1 + "-log", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        doAnswer(
            (invocation) ->
            {
                conductor.onNewPublication(STREAM_ID_2, SESSION_ID_2, 0, SESSION_ID_2 + "-log", CORRELATION_ID_2);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        conductor.addPublication(CHANNEL, STREAM_ID_2, SESSION_ID_2);

        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

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
        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);

        verify(driverProxy).addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void removingSubscriberNotifiesMediaDriver()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Subscription subscription = conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);

        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CLOSE_CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        subscription.close();

        verify(driverProxy).removeSubscription(CORRELATION_ID);
    }

    @Test(expected = DriverTimeoutException.class)
    public void cannotCreateSubscriberIfMediaDriverDoesNotReply()
    {
        conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToAddSubscriptionOnMediaDriverError()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.onError(INVALID_CHANNEL, "Multicast data address must be odd", CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);
    }

    @Test
    public void clientNotifiedOfNewConnectionsAndInactiveConnections()
    {
        doAnswer(
            (invocation) ->
            {
                conductor.operationSucceeded(CORRELATION_ID);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());

        final Subscription subscription = conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);

        conductor.onNewConnection(STREAM_ID_1, SESSION_ID_1, 0L, SESSION_ID_1 + "-log", connectionReady, CORRELATION_ID);

        assertFalse(subscription.hasNoConnections());
        verify(mockNewConnectionHandler).onNewConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, 0L, SOURCE_INFO);

        final long position = 0L;
        conductor.onInactiveConnection(STREAM_ID_1, SESSION_ID_1, position, CORRELATION_ID);

        verify(mockInactiveConnectionHandler).onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, position);
        assertTrue(subscription.hasNoConnections());
        assertFalse(subscription.isConnected(SESSION_ID_1));
    }
}
