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
package uk.co.real_logic.aeron;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.IdleStrategy;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.ErrorCode.INVALID_CHANNEL;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.ON_NEW_CONNECTION;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.ON_NEW_PUBLICATION;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;

public class ClientConductorTest extends MockBufferUsage
{
    private static final int COUNTER_BUFFER_SZ = 1024;
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int STREAM_ID_2 = 4;
    private static final int TERM_ID_1 = 1;
    private static final int SEND_BUFFER_CAPACITY = 1024;

    private static final int BROADCAST_BUFFER_SZ = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    private static final long CORRELATION_ID = 2000;
    private static final long CORRELATION_ID_2 = 2002;

    private static final int AWAIT_TIMEOUT = 100;
    private static final int MTU_LENGTH = 1280; // from CommonContext

    private final LogBuffersMessageFlyweight newBufferMessage = new LogBuffersMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private final AtomicBuffer toClientBuffer = new AtomicBuffer(new byte[BROADCAST_BUFFER_SZ]);
    private final CopyBroadcastReceiver toClientReceiver = new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

    private final TimerWheel timerWheel = mock(TimerWheel.class);
    private final IdleStrategy idleStrategy = mock(IdleStrategy.class);

    private final Consumer<Exception> mockClientErrorHandler = Throwable::printStackTrace;

    private Signal signal;
    private DriverProxy driverProxy;
    private ClientConductor conductor;
    private DataHandler dataHandler = mock(DataHandler.class);
    private InactiveConnectionHandler mockInactiveConnectionHandler = mock(InactiveConnectionHandler.class);

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
            idleStrategy,
            toClientReceiver,
            mockBufferUsage,
            counterValuesBuffer,
            driverProxy,
            signal,
            timerWheel,
            mockClientErrorHandler,
            null,
            mockInactiveConnectionHandler,
            AWAIT_TIMEOUT,
            MTU_LENGTH);

        newBufferMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
    }

    @After
    public void tearDown()
    {
        conductor.close();
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
        Publication firstPublication = addPublication();
        Publication secondPublication = addPublication();

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        Publication publication = addPublication();
        willNotifyOperationSucceeded();

        publication.close();

        driverProxy.removePublication(CORRELATION_ID);
    }

    @Test
    public void closingPublicationShouldPurgeCache() throws Exception
    {
        Publication firstPublication = addPublication();

        willNotifyOperationSucceeded();
        firstPublication.close();

        willNotifyNewBuffer(STREAM_ID_1, SESSION_ID_1, CORRELATION_ID);
        Publication secondPublication = addPublication();

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToRemoveOnMediaDriverError()
    {

        Publication publication = addPublication();

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
        Publication publication = addPublication();
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
        Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

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

        Subscription subscription = addSubscription();

        sendNewBufferNotification(ON_NEW_CONNECTION, SESSION_ID_1, TERM_ID_1, STREAM_ID_1, CORRELATION_ID);
        conductor.doWork();

        assertFalse(subscription.hasNoConnections());

        conductor.onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, null);

        verify(mockInactiveConnectionHandler).onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        assertTrue(subscription.hasNoConnections());
        assertFalse(subscription.isConnected(SESSION_ID_1));
    }

    private Subscription addSubscription()
    {
        return conductor.addSubscription(CHANNEL, STREAM_ID_1, dataHandler);
    }

    private void sendNewBufferNotification(final int msgTypeId, final int sessionId, final int termId, final int streamId, final long correlationId)
    {
        newBufferMessage.streamId(streamId)
                        .sessionId(sessionId)
                        .correlationId(correlationId)
                        .termId(termId);

        IntStream.range(0, TermHelper.BUFFER_COUNT).forEach(
            (i) ->
            {
                newBufferMessage.location(i, sessionId + "-log-" + i);
                newBufferMessage.bufferOffset(i, 0);
                newBufferMessage.bufferLength(i, LOG_BUFFER_SZ);
            });

        IntStream.range(0, TermHelper.BUFFER_COUNT).forEach(
            (i) ->
            {
                newBufferMessage.location(i + TermHelper.BUFFER_COUNT, sessionId + "-state-" + i);
                newBufferMessage.bufferOffset(i + TermHelper.BUFFER_COUNT, 0);
                newBufferMessage.bufferLength(i + TermHelper.BUFFER_COUNT, STATE_BUFFER_LENGTH);
            });

        newBufferMessage.channel(CHANNEL);

        toClientTransmitter.transmit(msgTypeId, atomicSendBuffer, 0, newBufferMessage.length());
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
            invocation ->
            {
                sendNewBufferNotification(ON_NEW_PUBLICATION, sessionId, TERM_ID_1, streamId, correlationId);
                conductor.doWork();
                return null;
            }).when(signal).await(anyLong());
    }

    private Publication addPublication()
    {
        return conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }
}
