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
import uk.co.real_logic.aeron.command.*;
import uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.ErrorFlyweight;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.SystemEpochClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.ErrorCode.INVALID_CHANNEL;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

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
    private static final long UNKNOWN_CORRELATION_ID = 3000;

    private static final int AWAIT_TIMEOUT = 100;

    private static final String SOURCE_INFO = "127.0.0.1:40789";

    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ConnectionBuffersReadyFlyweight connectionReady = new ConnectionBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ErrorFlyweight errorMessage = new ErrorFlyweight();

    private final UnsafeBuffer publicationReadyBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY));
    private final UnsafeBuffer connectionReadyBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY));
    private final UnsafeBuffer correlatedMessageBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY));
    private final UnsafeBuffer errorMessageBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY));

    private final CopyBroadcastReceiver mockToClientReceiver = mock(CopyBroadcastReceiver.class);

    private final UnsafeBuffer counterValuesBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_BUFFER_LENGTH));

    private final EpochClock epochClock = new SystemEpochClock();
    private final TimerWheel timerWheel = mock(TimerWheel.class);
    private final ErrorHandler mockClientErrorHandler = Throwable::printStackTrace;

    private DriverProxy driverProxy;
    private ClientConductor conductor;
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
            epochClock,
            mockToClientReceiver,
            logBuffersFactory,
            counterValuesBuffer,
            driverProxy,
            timerWheel,
            mockClientErrorHandler,
            mockNewConnectionHandler,
            mockInactiveConnectionHandler,
            AWAIT_TIMEOUT);

        publicationReady.wrap(publicationReadyBuffer, 0);
        connectionReady.wrap(connectionReadyBuffer, 0);
        correlatedMessage.wrap(correlatedMessageBuffer, 0);
        errorMessage.wrap(errorMessageBuffer, 0);

        publicationReady.correlationId(CORRELATION_ID);
        publicationReady.sessionId(SESSION_ID_1);
        publicationReady.streamId(STREAM_ID_1);
        publicationReady.logFileName(SESSION_ID_1 + "-log");

        connectionReady.sourceIdentity(SOURCE_INFO);
        connectionReady.subscriberPositionCount(1);
        connectionReady.subscriberPositionId(0, 0);
        connectionReady.positionIndicatorRegistrationId(0, CORRELATION_ID);

        correlatedMessage.correlationId(CLOSE_CORRELATION_ID);

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
    public void addPublicationShouldNotifyMediaDriver() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        verify(driverProxy).addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test
    public void addPublicationShouldMapLogFile() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        verify(logBuffersFactory).map(SESSION_ID_1 + "-log");
    }

    @Test(expected = DriverTimeoutException.class)
    public void addPublicationShouldTimeoutWithoutReadyMessage()
    {
        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test
    public void conductorShouldCachePublicationInstances()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS, correlatedMessageBuffer, (buffer) -> CorrelatedMessageFlyweight.LENGTH);

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    public void closingPublicationShouldPurgeCache() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS, correlatedMessageBuffer, (buffer) -> CorrelatedMessageFlyweight.LENGTH);

        firstPublication.close();

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToClosePublicationOnMediaDriverError()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                final byte[] message = "channel unknown".getBytes();
                final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
                removeMessage.wrap(new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY)), 0);
                removeMessage.correlationId(CLOSE_CORRELATION_ID);

                errorMessage.errorCode(INVALID_CHANNEL);
                errorMessage.offendingFlyweight(removeMessage, RemoveMessageFlyweight.LENGTH);
                errorMessage.errorMessage(message);
                errorMessage.frameLength(ErrorFlyweight.HEADER_LENGTH + message.length + RemoveMessageFlyweight.LENGTH);
                return errorMessage.frameLength();
            });

        publication.close();
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToAddPublicationOnMediaDriverError()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                final byte[] message = "invalid channel".getBytes();
                final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
                publicationMessage.wrap(new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY)), 0);
                publicationMessage.correlationId(CORRELATION_ID);

                errorMessage.errorCode(INVALID_CHANNEL);
                errorMessage.offendingFlyweight(publicationMessage, publicationMessage.length());
                errorMessage.errorMessage(message);
                errorMessage.frameLength(ErrorFlyweight.HEADER_LENGTH + message.length + publicationMessage.length());
                return errorMessage.frameLength();
            });

        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
    }

    @Test
    public void publicationOnlyRemovedOnLastClose() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        publication.close();

        verify(driverProxy, never()).removePublication(CORRELATION_ID);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS, correlatedMessageBuffer, (buffer) -> CorrelatedMessageFlyweight.LENGTH);

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
    }

    @Test
    public void closingPublicationDoesNotRemoveOtherPublications() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.streamId(STREAM_ID_2);
                publicationReady.sessionId(SESSION_ID_2);
                publicationReady.logFileName(SESSION_ID_2 + "-log");
                publicationReady.correlationId(CORRELATION_ID_2);
                return publicationReady.length();
            });

        conductor.addPublication(CHANNEL, STREAM_ID_2, SESSION_ID_2);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS, correlatedMessageBuffer, (buffer) -> CorrelatedMessageFlyweight.LENGTH);

        publication.close();

        verify(driverProxy).removePublication(CORRELATION_ID);
        verify(driverProxy, never()).removePublication(CORRELATION_ID_2);
    }

    @Test
    public void shouldNotMapBuffersForUnknownCorrelationId() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.correlationId(UNKNOWN_CORRELATION_ID);
                return publicationReady.length();
            });

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) ->
            {
                publicationReady.correlationId(CORRELATION_ID);
                return publicationReady.length();
            });

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1, SESSION_ID_1);
        conductor.doWork();

        verify(logBuffersFactory, times(1)).map(anyString());
        assertThat(publication.registrationId(), is(CORRELATION_ID));
    }

    // ---------------------------------
    // Subscription related interactions
    // ---------------------------------

    @Test
    public void addSubscriptionShouldNotifyMediaDriver() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            correlatedMessageBuffer,
            (buffer) ->
            {
                correlatedMessage.correlationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        conductor.addSubscription(CHANNEL, STREAM_ID_1);

        verify(driverProxy).addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void closingSubscriptionShouldNotifyMediaDriver()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            correlatedMessageBuffer,
            (buffer) ->
            {
                correlatedMessage.correlationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        final Subscription subscription = conductor.addSubscription(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            correlatedMessageBuffer,
            (buffer) ->
            {
                correlatedMessage.correlationId(CLOSE_CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        subscription.close();

        verify(driverProxy).removeSubscription(CORRELATION_ID);
    }

    @Test(expected = DriverTimeoutException.class)
    public void addSubscriptionShouldTimeoutWithoutOperationSuccessful()
    {
        conductor.addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToAddSubscriptionOnMediaDriverError()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                final byte[] message = "invalid channel".getBytes();
                final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
                subscriptionMessage.wrap(new UnsafeBuffer(ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY)), 0);
                subscriptionMessage.correlationId(CORRELATION_ID);

                errorMessage.errorCode(INVALID_CHANNEL);
                errorMessage.offendingFlyweight(subscriptionMessage, subscriptionMessage.length());
                errorMessage.errorMessage(message);
                errorMessage.frameLength(ErrorFlyweight.HEADER_LENGTH + message.length + subscriptionMessage.length());
                return errorMessage.frameLength();
            });

        conductor.addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void clientNotifiedOfNewConnectionShouldMapLogFile()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            correlatedMessageBuffer,
            (buffer) ->
            {
                correlatedMessage.correlationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        conductor.addSubscription(CHANNEL, STREAM_ID_1);

        conductor.onNewConnection(STREAM_ID_1, SESSION_ID_1, 0L, SESSION_ID_1 + "-log", connectionReady, CORRELATION_ID);

        verify(logBuffersFactory).map(SESSION_ID_1 + "-log");
    }

    @Test
    public void clientNotifiedOfNewConnectionsAndInactiveConnections()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS,
            correlatedMessageBuffer,
            (buffer) ->
            {
                correlatedMessage.correlationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

        final Subscription subscription = conductor.addSubscription(CHANNEL, STREAM_ID_1);

        conductor.onNewConnection(STREAM_ID_1, SESSION_ID_1, 0L, SESSION_ID_1 + "-log", connectionReady, CORRELATION_ID);

        assertFalse(subscription.hasNoConnections());
        verify(mockNewConnectionHandler).onNewConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, 0L, SOURCE_INFO);

        final long position = 0L;
        conductor.onInactiveConnection(STREAM_ID_1, SESSION_ID_1, position, CORRELATION_ID);

        verify(mockInactiveConnectionHandler).onInactiveConnection(CHANNEL, STREAM_ID_1, SESSION_ID_1, position);
        assertTrue(subscription.hasNoConnections());
        assertFalse(subscription.isConnected(SESSION_ID_1));
    }

    @Test
    public void shouldIgnoreUnknownNewConnection()
    {
        conductor.onNewConnection(STREAM_ID_2, SESSION_ID_2, 0L, SESSION_ID_2 + "-log", connectionReady, CORRELATION_ID_2);

        verify(logBuffersFactory, never()).map(anyString());
        verify(mockNewConnectionHandler, never()).onNewConnection(anyString(), anyInt(), anyInt(), anyLong(), anyString());
    }

    @Test
    public void shouldIgnoreUnknownInactiveConnection()
    {
        conductor.onInactiveConnection(STREAM_ID_2, SESSION_ID_2, 0L, CORRELATION_ID_2);

        verify(logBuffersFactory, never()).map(anyString());
        verify(mockInactiveConnectionHandler, never()).onInactiveConnection(anyString(), anyInt(), anyInt(), anyLong());
    }

    private void whenReceiveBroadcastOnMessage(
        final int msgTypeId,
        final MutableDirectBuffer buffer,
        final Function<MutableDirectBuffer, Integer> filler)
    {
        doAnswer(
            (invocation) ->
            {
                final int length = filler.apply(buffer);
                conductor.driverListenerAdapter().onMessage(msgTypeId, buffer, 0, length);
                return 1;
            }).when(mockToClientReceiver).receive(anyObject());
    }
}
