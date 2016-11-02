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
package io.aeron;

import org.junit.Before;
import org.junit.Test;
import io.aeron.command.ControlProtocolEvents;
import io.aeron.command.CorrelatedMessageFlyweight;
import io.aeron.command.ErrorResponseFlyweight;
import io.aeron.command.PublicationBuffersReadyFlyweight;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static io.aeron.ErrorCode.INVALID_CHANNEL;
import static io.aeron.logbuffer.LogBufferDescriptor.*;

public class ClientConductorTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;

    protected static final int SESSION_ID_1 = 13;
    protected static final int SESSION_ID_2 = 15;

    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int STREAM_ID_2 = 4;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int COUNTER_BUFFER_LENGTH = 1024;

    private static final long CORRELATION_ID = 2000;
    private static final long CORRELATION_ID_2 = 2002;
    private static final long CLOSE_CORRELATION_ID = 2001;
    private static final long UNKNOWN_CORRELATION_ID = 3000;

    private static final long KEEP_ALIVE_INTERVAL = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long AWAIT_TIMEOUT = 100;
    private static final long INTER_SERVICE_TIMEOUT_MS = 1000;
    private static final long PUBLICATION_CONNECTION_TIMEOUT_MS = 5000;

    private static final String SOURCE_INFO = "127.0.0.1:40789";

    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();

    private final UnsafeBuffer publicationReadyBuffer = new UnsafeBuffer(allocateDirect(SEND_BUFFER_CAPACITY));
    private final UnsafeBuffer correlatedMessageBuffer = new UnsafeBuffer(allocateDirect(SEND_BUFFER_CAPACITY));
    private final UnsafeBuffer errorMessageBuffer = new UnsafeBuffer(allocateDirect(SEND_BUFFER_CAPACITY));

    private final CopyBroadcastReceiver mockToClientReceiver = mock(CopyBroadcastReceiver.class);

    private final UnsafeBuffer counterValuesBuffer = new UnsafeBuffer(allocateDirect(COUNTER_BUFFER_LENGTH));

    private final EpochClock epochClock = new SystemEpochClock();
    private final NanoClock nanoClock = new SystemNanoClock();
    private final ErrorHandler mockClientErrorHandler = spy(new PrintError());

    private DriverProxy driverProxy;
    private ClientConductor conductor;
    private AvailableImageHandler mockAvailableImageHandler = mock(AvailableImageHandler.class);
    private UnavailableImageHandler mockUnavailableImageHandler = mock(UnavailableImageHandler.class);
    private LogBuffersFactory logBuffersFactory = mock(LogBuffersFactory.class);
    private Long2LongHashMap subscriberPositionMap = new Long2LongHashMap(-1L);
    private boolean suppressPrintError = false;

    @Before
    public void setUp() throws Exception
    {
        driverProxy = mock(DriverProxy.class);

        when(driverProxy.addPublication(CHANNEL, STREAM_ID_1)).thenReturn(CORRELATION_ID);
        when(driverProxy.addPublication(CHANNEL, STREAM_ID_2)).thenReturn(CORRELATION_ID_2);
        when(driverProxy.removePublication(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);
        when(driverProxy.addSubscription(anyString(), anyInt())).thenReturn(CORRELATION_ID);
        when(driverProxy.removeSubscription(CORRELATION_ID)).thenReturn(CLOSE_CORRELATION_ID);

        conductor = new ClientConductor(
            epochClock,
            nanoClock,
            mockToClientReceiver,
            logBuffersFactory,
            counterValuesBuffer,
            driverProxy,
            mockClientErrorHandler,
            mockAvailableImageHandler,
            mockUnavailableImageHandler,
            READ_ONLY,
            KEEP_ALIVE_INTERVAL,
            AWAIT_TIMEOUT,
            TimeUnit.MILLISECONDS.toNanos(INTER_SERVICE_TIMEOUT_MS),
            PUBLICATION_CONNECTION_TIMEOUT_MS);

        publicationReady.wrap(publicationReadyBuffer, 0);
        correlatedMessage.wrap(correlatedMessageBuffer, 0);
        errorResponse.wrap(errorMessageBuffer, 0);

        publicationReady.correlationId(CORRELATION_ID);
        publicationReady.sessionId(SESSION_ID_1);
        publicationReady.streamId(STREAM_ID_1);
        publicationReady.logFileName(SESSION_ID_1 + "-log");

        subscriberPositionMap.put(CORRELATION_ID, 0);

        correlatedMessage.correlationId(CLOSE_CORRELATION_ID);

        final UnsafeBuffer[] termBuffersSession1 = new UnsafeBuffer[PARTITION_COUNT];
        final UnsafeBuffer[] termBuffersSession2 = new UnsafeBuffer[PARTITION_COUNT];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffersSession1[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
            termBuffersSession2[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
        }

        final UnsafeBuffer logMetaDataSession1 = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
        final UnsafeBuffer logMetaDataSession2 = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));

        final MutableDirectBuffer header1 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_1, STREAM_ID_1, 0);
        final MutableDirectBuffer header2 = DataHeaderFlyweight.createDefaultHeader(SESSION_ID_2, STREAM_ID_2, 0);

        LogBufferDescriptor.storeDefaultFrameHeader(logMetaDataSession1, header1);
        LogBufferDescriptor.storeDefaultFrameHeader(logMetaDataSession2, header2);

        final LogBuffers logBuffersSession1 = mock(LogBuffers.class);
        final LogBuffers logBuffersSession2 = mock(LogBuffers.class);

        when(logBuffersFactory.map(SESSION_ID_1 + "-log", READ_WRITE)).thenReturn(logBuffersSession1);
        when(logBuffersFactory.map(SESSION_ID_2 + "-log", READ_WRITE)).thenReturn(logBuffersSession2);
        when(logBuffersFactory.map(SESSION_ID_1 + "-log", READ_ONLY)).thenReturn(logBuffersSession1);
        when(logBuffersFactory.map(SESSION_ID_2 + "-log", READ_ONLY)).thenReturn(logBuffersSession2);

        when(logBuffersSession1.termBuffers()).thenReturn(termBuffersSession1);
        when(logBuffersSession2.termBuffers()).thenReturn(termBuffersSession2);

        when(logBuffersSession1.metaDataBuffer()).thenReturn(logMetaDataSession1);
        when(logBuffersSession2.metaDataBuffer()).thenReturn(logMetaDataSession2);
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

        conductor.addPublication(CHANNEL, STREAM_ID_1);

        verify(driverProxy).addPublication(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void addPublicationShouldMapLogFile() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        conductor.addPublication(CHANNEL, STREAM_ID_1);

        verify(logBuffersFactory).map(SESSION_ID_1 + "-log", READ_WRITE);
    }

    @Test(expected = DriverTimeoutException.class)
    public void addPublicationShouldTimeoutWithoutReadyMessage()
    {
        conductor.addPublication(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void conductorShouldCachePublicationInstances()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY,
            publicationReadyBuffer,
            (buffer) -> publicationReady.length());

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);
        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);

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

        final Publication firstPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_OPERATION_SUCCESS, correlatedMessageBuffer, (buffer) -> CorrelatedMessageFlyweight.LENGTH);

        firstPublication.close();

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication secondPublication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        assertThat(firstPublication, not(sameInstance(secondPublication)));
    }

    @Test(expected = RegistrationException.class)
    public void shouldFailToClosePublicationOnMediaDriverError()
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);

        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_ERROR,
            errorMessageBuffer,
            (buffer) ->
            {
                errorResponse.errorCode(INVALID_CHANNEL);
                errorResponse.errorMessage("channel unknown");
                errorResponse.offendingCommandCorrelationId(CLOSE_CORRELATION_ID);
                return errorResponse.length();
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
                errorResponse.errorCode(INVALID_CHANNEL);
                errorResponse.errorMessage("invalid channel");
                errorResponse.offendingCommandCorrelationId(CORRELATION_ID);
                return errorResponse.length();
            });

        conductor.addPublication(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void publicationOnlyRemovedOnLastClose() throws Exception
    {
        whenReceiveBroadcastOnMessage(
            ControlProtocolEvents.ON_PUBLICATION_READY, publicationReadyBuffer, (buffer) -> publicationReady.length());

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);
        conductor.addPublication(CHANNEL, STREAM_ID_1);

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

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);

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

        conductor.addPublication(CHANNEL, STREAM_ID_2);

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

        final Publication publication = conductor.addPublication(CHANNEL, STREAM_ID_1);
        conductor.doWork();

        verify(logBuffersFactory, times(1)).map(anyString(), any(FileChannel.MapMode.class));
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
                errorResponse.errorCode(INVALID_CHANNEL);
                errorResponse.errorMessage("invalid channel");
                errorResponse.offendingCommandCorrelationId(CORRELATION_ID);
                return errorResponse.length();
            });

        conductor.addSubscription(CHANNEL, STREAM_ID_1);
    }

    @Test
    public void clientNotifiedOfNewImageShouldMapLogFile()
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

        conductor.onAvailableImage(
            STREAM_ID_1, SESSION_ID_1, subscriberPositionMap, SESSION_ID_1 + "-log", SOURCE_INFO, CORRELATION_ID);

        verify(logBuffersFactory).map(eq(SESSION_ID_1 + "-log"), any(FileChannel.MapMode.class));
    }

    @Test
    public void clientNotifiedOfNewAndInactiveImages()
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

        conductor.onAvailableImage(
            STREAM_ID_1, SESSION_ID_1, subscriberPositionMap, SESSION_ID_1 + "-log", SOURCE_INFO, CORRELATION_ID);

        assertFalse(subscription.hasNoImages());
        verify(mockAvailableImageHandler).onAvailableImage(any(Image.class));

        conductor.onUnavailableImage(STREAM_ID_1, CORRELATION_ID);

        verify(mockUnavailableImageHandler).onUnavailableImage(any(Image.class));
        assertTrue(subscription.hasNoImages());
    }

    @Test
    public void shouldIgnoreUnknownNewImage()
    {
        conductor.onAvailableImage(
            STREAM_ID_2, SESSION_ID_2, subscriberPositionMap, SESSION_ID_2 + "-log", SOURCE_INFO, CORRELATION_ID_2);

        verify(logBuffersFactory, never()).map(anyString(), any(FileChannel.MapMode.class));
        verify(mockAvailableImageHandler, never()).onAvailableImage(any(Image.class));
    }

    @Test
    public void shouldIgnoreUnknownInactiveImage()
    {
        conductor.onUnavailableImage(STREAM_ID_2, CORRELATION_ID_2);

        verify(logBuffersFactory, never()).map(anyString(), any(FileChannel.MapMode.class));
        verify(mockUnavailableImageHandler, never()).onUnavailableImage(any(Image.class));
    }

    @Test
    public void shouldTimeoutInterServiceIfTooLongBetweenDoWorkCalls() throws Exception
    {
        suppressPrintError = true;

        conductor.doWork();
        Thread.sleep(INTER_SERVICE_TIMEOUT_MS + 100);
        conductor.doWork();

        verify(mockClientErrorHandler).onError(any(ConductorServiceTimeoutException.class));
    }

    private void whenReceiveBroadcastOnMessage(
        final int msgTypeId, final MutableDirectBuffer buffer, final Function<MutableDirectBuffer, Integer> filler)
    {
        doAnswer(
            (invocation) ->
            {
                final int length = filler.apply(buffer);
                conductor.driverListenerAdapter().onMessage(msgTypeId, buffer, 0, length);

                return 1;
            }).when(mockToClientReceiver).receive(any());
    }

    private class PrintError implements ErrorHandler
    {
        public void onError(Throwable throwable)
        {
            if (!suppressPrintError)
            {
                throwable.printStackTrace();
            }
        }
    }
}
