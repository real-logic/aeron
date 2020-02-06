/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster.Context;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static io.aeron.test.Tests.throwOnClose;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class AeronClusterCloseTests
{
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final Context context = new Context();
    private final Publication publication = mock(Publication.class);
    private final IllegalArgumentException publicationException = new IllegalArgumentException();
    private final Subscription subscription = mock(Subscription.class);
    private final OutOfMemoryError subscriptionException = new OutOfMemoryError();
    private final Aeron aeron = mock(Aeron.class);
    private final AssertionError aeronException = new AssertionError();
    private String tryClaimExceptionMessage;
    private AeronCluster aeronCluster;

    @BeforeEach
    void before() throws Exception
    {
        context
            .errorHandler(errorHandler)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .aeron(aeron);

        when(publication.isConnected()).thenReturn(true);

        throwOnClose(publication, publicationException);
        throwOnClose(subscription, subscriptionException);
        throwOnClose(aeron, aeronException);

        aeronCluster = new AeronCluster(
            context,
            new MessageHeaderEncoder(),
            publication,
            subscription,
            new Int2ObjectHashMap<>(),
            42L,
            21L,
            1);
    }

    @Test
    void ownsAeronClient()
    {
        mockTryClaim(Publication.MAX_POSITION_EXCEEDED);
        context.ownsAeronClient(true);

        final AssertionError ex = assertThrows(AssertionError.class, aeronCluster::close);

        assertSame(aeronException, ex);
        final InOrder inOrder = inOrder(aeron, errorHandler);
        inOrder.verify(errorHandler).onError(argThat(t -> tryClaimExceptionMessage.equals(t.getMessage())));
        inOrder.verify(aeron).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void doesNotOwnAeronClient()
    {
        mockTryClaim(Publication.NOT_CONNECTED);
        context.ownsAeronClient(false);

        aeronCluster.close();

        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(argThat(t -> tryClaimExceptionMessage.equals(t.getMessage())));
        inOrder.verify(errorHandler).onError(subscriptionException);
        inOrder.verify(errorHandler).onError(publicationException);
        inOrder.verifyNoMoreInteractions();
    }

    private void mockTryClaim(final long result)
    {
        when(publication.tryClaim(anyInt(), any(BufferClaim.class))).thenReturn(result);
        tryClaimExceptionMessage = "unexpected publication state: " + result;
    }
}