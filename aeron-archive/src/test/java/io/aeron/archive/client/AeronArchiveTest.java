/*
 * Copyright 2014-2021 Real Logic Limited.
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
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        doThrow(new IllegalMonitorStateException("aeron closed")).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        final IllegalAccessError publicationException = new IllegalAccessError("publication is closed");
        doThrow(publicationException).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        final NoClassDefFoundError subscriptionException = new NoClassDefFoundError("subscription");
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
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId);

        aeronArchive.close();

        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(closeSessionException);
        inOrder.verify(errorHandler).onError(publicationException);
        inOrder.verify(errorHandler).onError(subscriptionException);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closeOwningAeronClient()
    {
        final long controlSessionId = 42;

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final IllegalMonitorStateException aeronException = new IllegalMonitorStateException("aeron closed");
        doThrow(aeronException).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        doThrow(new IllegalAccessError("publication is closed")).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        doThrow(new NoClassDefFoundError("subscription")).when(subscription).close();

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
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId);

        final IllegalMonitorStateException ex = assertThrows(IllegalMonitorStateException.class, aeronArchive::close);

        assertSame(aeronException, ex);
        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(closeSessionException);
        inOrder.verifyNoMoreInteractions();
    }
}