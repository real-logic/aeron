/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive.Context;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.AeronException;
import io.aeron.security.CredentialsSupplier;
import org.agrona.ErrorHandler;
import org.agrona.SemanticVersion;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AeronArchiveTest
{
    private final Aeron aeron = mock(Aeron.class);
    private final ControlResponsePoller controlResponsePoller = mock(ControlResponsePoller.class);
    private final ArchiveProxy archiveProxy = mock(ArchiveProxy.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    @Test
    void closeNotOwningAeronClient()
    {
        final long controlSessionId = 42;
        final long archiveId = -190;

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final IllegalMonitorStateException aeronException = new IllegalMonitorStateException("aeron closed");
        doThrow(aeronException).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        final IllegalStateException publicationException = new IllegalStateException("publication is closed");
        doThrow(publicationException).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        final IndexOutOfBoundsException subscriptionException = new IndexOutOfBoundsException("subscription");
        doThrow(subscriptionException).when(subscription).close();

        when(archiveProxy.publication()).thenReturn(publication);
        final IndexOutOfBoundsException closeSessionException = new IndexOutOfBoundsException();
        when(archiveProxy.closeSession(controlSessionId)).thenThrow(closeSessionException);

        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(false);
        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        aeronArchive.close();

        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(argThat(
            ex ->
            {
                final Throwable[] suppressed = ex.getSuppressed();
                return closeSessionException == ex &&
                    publicationException == suppressed[0] &&
                    subscriptionException == suppressed[1];
            }));
        inOrder.verifyNoMoreInteractions();
        verify(publication).close();
        verify(subscription).close();
    }

    @Test
    void closeOwningAeronClient()
    {
        final long controlSessionId = 42;
        final long archiveId = 555;

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final IllegalMonitorStateException aeronException = new IllegalMonitorStateException("aeron closed");
        doThrow(aeronException).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        doThrow(new IllegalStateException("publication is closed")).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        doThrow(new IndexOutOfBoundsException("subscription")).when(subscription).close();

        when(archiveProxy.publication()).thenReturn(publication);
        final IndexOutOfBoundsException closeSessionException = new IndexOutOfBoundsException();
        when(archiveProxy.closeSession(controlSessionId)).thenThrow(closeSessionException);

        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(true);
        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        final IndexOutOfBoundsException ex = assertThrows(IndexOutOfBoundsException.class, aeronArchive::close);

        assertSame(closeSessionException, ex);
        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(closeSessionException);
        inOrder.verifyNoMoreInteractions();

        assertEquals(aeronException, ex.getSuppressed()[0]);
    }

    @Test
    void shouldClose() throws Exception
    {
        final Exception previousException = new Exception();
        final Exception thrownException = new Exception();

        final AutoCloseable throwingCloseable = mock(AutoCloseable.class);
        final AutoCloseable nonThrowingCloseable = mock(AutoCloseable.class);
        doThrow(thrownException).when(throwingCloseable).close();

        assertNull(AeronArchive.quietClose(null, nonThrowingCloseable));
        assertEquals(previousException, AeronArchive.quietClose(previousException, nonThrowingCloseable));
        final Exception ex = AeronArchive.quietClose(previousException, throwingCloseable);
        assertEquals(previousException, ex);
        assertEquals(thrownException, ex.getSuppressed()[0]);
        assertEquals(thrownException, AeronArchive.quietClose(null, throwingCloseable));
    }

    @ParameterizedTest
    @ValueSource(longs = { Aeron.NULL_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, 0, 4468236482L })
    void shouldReturnAssignedArchiveId(final long archiveId)
    {
        final long controlSessionId = -3924293;
        when(aeron.context()).thenReturn(new Aeron.Context());
        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(true);

        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        assertEquals(archiveId, aeronArchive.archiveId());
    }

    @Test
    void shouldAsyncConnectWithoutAuthChallenge()
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(false, true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn(null, "sub-channel");
        when(subscription.isConnected()).thenReturn(false, true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final Context ctx = spy(new Context());
        ctx.aeron(aeron).ownsAeronClient(true).messageTimeoutNs(TimeUnit.HOURS.toNanos(1));

        final AeronArchive aeronArchive;
        try (AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy))
        {
            assertEquals(1, asyncConnect.step());

            assertNull(asyncConnect.poll());
            assertEquals(1, asyncConnect.step()); // publication not connected

            assertNull(asyncConnect.poll());
            assertEquals(2, asyncConnect.step()); // channel not resolved
            assertEquals(Aeron.NULL_VALUE, asyncConnect.correlationId());

            when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(false, true);
            assertNull(asyncConnect.poll());
            assertEquals(2, asyncConnect.step()); // tryConnect failed
            assertEquals(lastCorrelationId.get(), asyncConnect.correlationId());

            assertNull(asyncConnect.poll());
            assertEquals(3, asyncConnect.step()); // subscription not connected

            final long correlationIdConnect = asyncConnect.correlationId();
            assertEquals(lastCorrelationId.get(), correlationIdConnect);

            when(controlResponsePoller.isPollComplete()).thenReturn(false);
            assertNull(asyncConnect.poll());
            assertEquals(4, asyncConnect.step()); // poll not complete

            when(controlResponsePoller.isPollComplete()).thenReturn(true);
            when(controlResponsePoller.correlationId()).thenReturn(-correlationIdConnect);
            assertNull(asyncConnect.poll());
            assertEquals(4, asyncConnect.step()); // wrong correlationId

            when(controlResponsePoller.isPollComplete()).thenReturn(true);
            when(controlResponsePoller.correlationId()).thenReturn(correlationIdConnect);
            when(controlResponsePoller.wasChallenged()).thenReturn(false);
            when(controlResponsePoller.code()).thenReturn(ControlResponseCode.OK);
            when(controlResponsePoller.version()).thenReturn(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION);
            final long controlSessionIdConnect = 3759235739475L;
            when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionIdConnect);
            assertNull(asyncConnect.poll());
            assertEquals(5, asyncConnect.step()); // need to send `archive-id` request
            final long correlationIdArchiveId = lastCorrelationId.get();
            assertEquals(correlationIdArchiveId, asyncConnect.correlationId());
            assertNotEquals(correlationIdConnect, asyncConnect.correlationId());
            assertEquals(controlSessionIdConnect, asyncConnect.controlSessionId());

            when(archiveProxy.archiveId(asyncConnect.correlationId(), asyncConnect.controlSessionId()))
                .thenReturn(false, true);
            assertNull(asyncConnect.poll());
            assertEquals(5, asyncConnect.step()); // failed to send `archive-id` request

            assertNull(asyncConnect.poll());
            assertEquals(6, asyncConnect.step()); // wrong correlationId

            final long controlSessionIdArchiveId = Long.MIN_VALUE;
            final long archiveId = 8888;
            when(controlResponsePoller.correlationId()).thenReturn(correlationIdArchiveId);
            when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionIdArchiveId);
            when(controlResponsePoller.relevantId()).thenReturn(archiveId);
            when(archiveProxy.keepAlive(controlSessionIdArchiveId, Aeron.NULL_VALUE)).thenReturn(true);

            aeronArchive = asyncConnect.poll();
            assertNotNull(aeronArchive);
            assertEquals(7, asyncConnect.step());
            assertEquals(controlSessionIdArchiveId, asyncConnect.controlSessionId());
            assertEquals(controlSessionIdArchiveId, aeronArchive.controlSessionId());
            assertEquals(archiveId, aeronArchive.archiveId());
        }

        verify(publication, never()).close();
        verify(subscription, never()).close();
        verify(ctx, never()).close();
    }

    @Test
    @SuppressWarnings("MethodLength")
    void shouldAsyncConnectWithAuthChallenge()
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(false, true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn(null, "sub-channel");
        when(subscription.isConnected()).thenReturn(false, true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final CredentialsSupplier credentialsSupplier = mock(CredentialsSupplier.class);
        final byte[] challengeResponseBytes = { 0x2 };
        when(credentialsSupplier.onChallenge(any(byte[].class))).thenReturn(challengeResponseBytes);
        final Context ctx = spy(new Context())
            .aeron(aeron)
            .ownsAeronClient(true)
            .messageTimeoutNs(TimeUnit.HOURS.toNanos(1))
            .credentialsSupplier(credentialsSupplier);

        final AeronArchive aeronArchive;
        try (AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy))
        {
            assertEquals(1, asyncConnect.step());

            assertNull(asyncConnect.poll());
            assertEquals(1, asyncConnect.step()); // publication not connected

            assertNull(asyncConnect.poll());
            assertEquals(2, asyncConnect.step()); // channel not resolved
            assertEquals(Aeron.NULL_VALUE, asyncConnect.correlationId());

            when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(false, true);

            assertNull(asyncConnect.poll());
            assertEquals(2, asyncConnect.step()); // tryConnect failed
            assertEquals(lastCorrelationId.get(), asyncConnect.correlationId());

            assertNull(asyncConnect.poll());
            assertEquals(3, asyncConnect.step()); // subscription not connected

            final long correlationIdConnect = asyncConnect.correlationId();
            assertEquals(lastCorrelationId.get(), correlationIdConnect);

            when(controlResponsePoller.isPollComplete()).thenReturn(false);

            assertNull(asyncConnect.poll());
            assertEquals(4, asyncConnect.step()); // poll not complete

            when(controlResponsePoller.isPollComplete()).thenReturn(true);
            when(controlResponsePoller.correlationId()).thenReturn(-correlationIdConnect);

            assertNull(asyncConnect.poll());
            assertEquals(4, asyncConnect.step()); // wrong correlationId

            when(controlResponsePoller.isPollComplete()).thenReturn(true);
            when(controlResponsePoller.correlationId()).thenReturn(correlationIdConnect);
            when(controlResponsePoller.wasChallenged()).thenReturn(true);
            final long controlSessionIdChallenge = -232;
            when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionIdChallenge);
            final byte[] encodedChallenge = { 0x1 };
            when(controlResponsePoller.encodedChallenge()).thenReturn(encodedChallenge);

            assertNull(asyncConnect.poll());
            assertEquals(8, asyncConnect.step()); // need to send `challenge` response
            assertEquals(controlSessionIdChallenge, asyncConnect.controlSessionId());
            final long correlationIdChallenge = lastCorrelationId.get();
            assertEquals(correlationIdChallenge, asyncConnect.correlationId());
            assertNotEquals(correlationIdConnect, asyncConnect.correlationId());
            verify(credentialsSupplier, only()).onChallenge(encodedChallenge);

            when(archiveProxy.tryChallengeResponse(
                challengeResponseBytes, correlationIdChallenge, controlSessionIdChallenge)).thenReturn(false, true);

            assertNull(asyncConnect.poll());
            assertEquals(8, asyncConnect.step()); // failed to send `challenge` response

            when(controlResponsePoller.correlationId()).thenReturn(Long.MAX_VALUE);
            when(controlResponsePoller.wasChallenged()).thenReturn(false);
            when(controlResponsePoller.code()).thenReturn(ControlResponseCode.OK);
            when(controlResponsePoller.version())
                .thenReturn(AeronArchive.AsyncConnect.PROTOCOL_VERSION_WITH_ARCHIVE_ID);

            assertNull(asyncConnect.poll());
            assertEquals(9, asyncConnect.step()); // wrong correlationId

            assertEquals(correlationIdChallenge, asyncConnect.correlationId());
            when(controlResponsePoller.correlationId()).thenReturn(correlationIdChallenge);

            assertNull(asyncConnect.poll());
            assertEquals(5, asyncConnect.step()); // need to send `archive-id` request

            when(archiveProxy.archiveId(asyncConnect.correlationId(), asyncConnect.controlSessionId()))
                .thenReturn(false, true);

            assertNull(asyncConnect.poll());
            assertEquals(5, asyncConnect.step()); // failed to send `archive-id` request
            assertNotEquals(correlationIdChallenge, asyncConnect.correlationId());
            final long correlationIdArchiveId = asyncConnect.correlationId();

            assertNull(asyncConnect.poll());
            assertEquals(6, asyncConnect.step()); // wrong correlationId

            final long controlSessionIdArchiveId = -4345983675937534593L;
            final long archiveId = -42;
            when(controlResponsePoller.correlationId()).thenReturn(correlationIdArchiveId);
            when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionIdArchiveId);
            when(controlResponsePoller.relevantId()).thenReturn(archiveId);
            when(archiveProxy.keepAlive(controlSessionIdArchiveId, Aeron.NULL_VALUE)).thenReturn(true);

            aeronArchive = asyncConnect.poll();
            assertNotNull(aeronArchive);
            assertEquals(7, asyncConnect.step());
            assertEquals(controlSessionIdArchiveId, asyncConnect.controlSessionId());
            assertEquals(controlSessionIdArchiveId, aeronArchive.controlSessionId());
            assertEquals(archiveId, aeronArchive.archiveId());
        }

        verify(publication, never()).close();
        verify(subscription, never()).close();
        verify(ctx, never()).close();
    }

    @Test
    void shouldThrowArchiveExceptionUponErrorResponse()
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn("sub-channel");
        when(subscription.isConnected()).thenReturn(true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final Context ctx = spy(new Context());
        ctx.aeron(aeron).ownsAeronClient(true).messageTimeoutNs(TimeUnit.HOURS.toNanos(1));

        when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(true);
        when(controlResponsePoller.isPollComplete()).thenReturn(true);
        when(controlResponsePoller.correlationId())
            .thenAnswer((Answer<Long>)invocation -> lastCorrelationId.get());
        final long controlSessionId = -8901;
        when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionId);

        when(controlResponsePoller.code()).thenReturn(ControlResponseCode.ERROR);
        final int errorCode = ArchiveException.ACTIVE_SUBSCRIPTION;
        when(controlResponsePoller.relevantId()).thenReturn((long)errorCode);
        final String errorMessage = "test error";
        when(controlResponsePoller.errorMessage()).thenReturn(errorMessage);

        final AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy);

        final ArchiveException exception = assertThrowsExactly(ArchiveException.class, asyncConnect::poll);

        assertEquals("ERROR - " + errorMessage, exception.getMessage());
        assertEquals(AeronException.Category.ERROR, exception.category());
        assertEquals(errorCode, exception.errorCode());
        assertEquals(1, exception.correlationId());
        verify(archiveProxy, times(1)).closeSession(controlSessionId);
    }

    @ParameterizedTest
    @EnumSource(
        value = ControlResponseCode.class,
        mode = EnumSource.Mode.EXCLUDE,
        names = { "OK", "ERROR", "NULL_VAL" })
    void shouldThrowArchiveExceptionIfUnknownCode(final ControlResponseCode code)
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn("sub-channel");
        when(subscription.isConnected()).thenReturn(true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final Context ctx = spy(new Context());
        ctx.aeron(aeron).ownsAeronClient(true).messageTimeoutNs(TimeUnit.HOURS.toNanos(1));

        when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(true);
        when(controlResponsePoller.isPollComplete()).thenReturn(true);
        when(controlResponsePoller.correlationId())
            .thenAnswer((Answer<Long>)invocation -> lastCorrelationId.get());
        final long controlSessionId = -8901;
        when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionId);

        when(controlResponsePoller.code()).thenReturn(code);
        when(controlResponsePoller.relevantId()).thenReturn(Long.MAX_VALUE);
        when(controlResponsePoller.errorMessage()).thenReturn("garbage");

        final AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy);

        final ArchiveException exception = assertThrowsExactly(ArchiveException.class, asyncConnect::poll);

        assertEquals("ERROR - unexpected response: code=" + code, exception.getMessage());
        assertEquals(AeronException.Category.ERROR, exception.category());
        assertEquals(ArchiveException.GENERIC, exception.errorCode());
        assertEquals(1, exception.correlationId());
        verify(archiveProxy, times(1)).closeSession(controlSessionId);
    }

    @Test
    void shouldThrowArchiveExceptionIfArchiveIdNotSupported()
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn("sub-channel");
        when(subscription.isConnected()).thenReturn(true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final Context ctx = spy(new Context());
        ctx.aeron(aeron).ownsAeronClient(true).messageTimeoutNs(TimeUnit.HOURS.toNanos(1));

        when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(true);
        when(controlResponsePoller.isPollComplete()).thenReturn(true);
        when(controlResponsePoller.correlationId())
            .thenAnswer((Answer<Long>)invocation -> lastCorrelationId.get());
        final long controlSessionId = -8901;
        when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionId);

        when(controlResponsePoller.code()).thenReturn(ControlResponseCode.OK);
        final int invalidVersion = SemanticVersion.compose(1, 5, 9);
        when(controlResponsePoller.version()).thenReturn(invalidVersion);

        final AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy);

        final ArchiveException exception = assertThrowsExactly(ArchiveException.class, asyncConnect::poll);

        assertEquals("ERROR - archive's protocol (" +
            SemanticVersion.toString(invalidVersion) + ") does not support " +
            "`archive-id` command added in version (" +
            SemanticVersion.toString(AeronArchive.AsyncConnect.PROTOCOL_VERSION_WITH_ARCHIVE_ID) + ")",
            exception.getMessage());
        assertEquals(AeronException.Category.ERROR, exception.category());
        assertEquals(ArchiveException.GENERIC, exception.errorCode());
        assertEquals(Aeron.NULL_VALUE, exception.correlationId());
        verify(archiveProxy, times(1)).closeSession(controlSessionId);
    }

    @Test
    void shouldThrowArchiveExceptionIfSendingKeepAliveFails()
    {
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.nanoClock(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final MutableLong lastCorrelationId = new MutableLong();
        when(aeron.nextCorrelationId()).thenAnswer((args) -> lastCorrelationId.incrementAndGet());

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        when(archiveProxy.publication()).thenReturn(publication);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.tryResolveChannelEndpointPort()).thenReturn("sub-channel");
        when(subscription.isConnected()).thenReturn(true);
        when(controlResponsePoller.subscription()).thenReturn(subscription);

        final Context ctx = spy(new Context());
        ctx.aeron(aeron).ownsAeronClient(true).messageTimeoutNs(TimeUnit.HOURS.toNanos(1));

        when(archiveProxy.tryConnect(anyString(), anyInt(), anyLong())).thenReturn(true);
        when(controlResponsePoller.isPollComplete()).thenReturn(true);
        when(controlResponsePoller.correlationId())
            .thenAnswer((Answer<Long>)invocation -> lastCorrelationId.get());
        final long controlSessionId = 4753498593L;
        when(controlResponsePoller.controlSessionId()).thenReturn(controlSessionId);

        when(controlResponsePoller.code()).thenReturn(ControlResponseCode.OK);
        when(controlResponsePoller.version()).thenReturn(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION);

        when(archiveProxy.archiveId(anyLong(), anyLong())).thenReturn(true);

        final AeronArchive.AsyncConnect asyncConnect =
            new AeronArchive.AsyncConnect(ctx, controlResponsePoller, archiveProxy);

        asyncConnect.poll();
        assertEquals(5, asyncConnect.step());

        final ArchiveException exception = assertThrowsExactly(ArchiveException.class, asyncConnect::poll);

        assertEquals("ERROR - failed to send keep alive after archive connect", exception.getMessage());
        assertEquals(AeronException.Category.ERROR, exception.category());
        assertEquals(ArchiveException.GENERIC, exception.errorCode());
        assertEquals(Aeron.NULL_VALUE, exception.correlationId());
        verify(archiveProxy, times(1)).closeSession(controlSessionId);
    }
}
