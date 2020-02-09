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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.cluster.ClusterBackup.Context;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.CountedErrorHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;

import static io.aeron.test.Tests.throwOnClose;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class ClusterBackupContextCloseTests
{
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
    private final Aeron aeron = mock(Aeron.class);
    private final Counter stateCounter = mock(Counter.class);
    private final Counter liveLogPositionCounter = mock(Counter.class);
    private final ClusterMarkFile markFile = mock(ClusterMarkFile.class);
    private final AeronException aeronException = new AeronException("client is dead");
    private final IOException markFileException = new IOException("file");
    private final IllegalStateException stateCounterException = new IllegalStateException("stateCounter");
    private final LinkageError liveLogPositionCounterException = new LinkageError("live log position");
    private Context context;

    @BeforeEach
    void before() throws Exception
    {
        throwOnClose(aeron, aeronException);
        throwOnClose(stateCounter, stateCounterException);
        throwOnClose(liveLogPositionCounter, liveLogPositionCounterException);
        throwOnClose(markFile, markFileException);

        context = new Context()
            .countedErrorHandler(countedErrorHandler)
            .aeron(aeron)
            .stateCounter(stateCounter)
            .liveLogPositionCounter(liveLogPositionCounter)
            .clusterMarkFile(markFile);
    }

    @Test
    void closeOwnsAeronClient()
    {
        context.ownsAeronClient(true);

        context.close();

        final InOrder inOrder = inOrder(countedErrorHandler);
        inOrder.verify(countedErrorHandler).onError(aeronException);
        inOrder.verify(countedErrorHandler).onError(markFileException);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closeDoesNotOwnAeronClient()
    {
        context.ownsAeronClient(false);

        context.close();

        final InOrder inOrder = inOrder(countedErrorHandler);
        inOrder.verify(countedErrorHandler).onError(stateCounterException);
        inOrder.verify(countedErrorHandler).onError(liveLogPositionCounterException);
        inOrder.verify(countedErrorHandler).onError(markFileException);
        inOrder.verifyNoMoreInteractions();
    }
}