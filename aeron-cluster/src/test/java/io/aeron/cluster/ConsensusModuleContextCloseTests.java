/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CountedErrorHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import javax.naming.InvalidNameException;
import java.io.IOException;

import static io.aeron.test.Tests.throwOnClose;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsensusModuleContextCloseTests
{
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
    private final ErrorHandler errorHandler = mock(
        ErrorHandler.class, withSettings().extraInterfaces(AutoCloseable.class));
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ReflectiveOperationException recodingLogException = new ReflectiveOperationException();
    private final ClusterMarkFile markFile = mock(ClusterMarkFile.class);
    private final IOException markFileException = new IOException();
    private final Aeron aeron = mock(Aeron.class);
    private final AeronException aeronException = new AeronException();
    private final Counter moduleState = mock(Counter.class);
    private final IllegalStateException moduleStateException = new IllegalStateException();
    private final Counter commitPosition = mock(Counter.class);
    private final InvalidNameException commitPositionException = new InvalidNameException();
    private final Counter clusterNodeRole = mock(Counter.class);
    private final UnsupportedOperationException clusterNodeRoleException = new UnsupportedOperationException();
    private final Counter controlToggle = mock(Counter.class);
    private final IllegalArgumentException controlToggleException = new IllegalArgumentException();
    private final Counter snapshotCounter = mock(Counter.class);
    private final IllegalMonitorStateException snapshotCounterException = new IllegalMonitorStateException();
    private final Counter timedOutClientCounter = mock(Counter.class);
    private final IndexOutOfBoundsException timedOutClientCounterException = new IndexOutOfBoundsException();
    private ConsensusModule.Context context;

    @BeforeEach
    void before() throws Exception
    {
        throwOnClose(recordingLog, recodingLogException);
        throwOnClose(markFile, markFileException);
        throwOnClose(aeron, aeronException);
        throwOnClose(timedOutClientCounter, timedOutClientCounterException);
        throwOnClose(snapshotCounter, snapshotCounterException);
        throwOnClose(controlToggle, controlToggleException);
        throwOnClose(moduleState, moduleStateException);
        throwOnClose(clusterNodeRole, clusterNodeRoleException);
        throwOnClose(commitPosition, commitPositionException);

        context = new ConsensusModule.Context()
            .countedErrorHandler(countedErrorHandler)
            .errorHandler(errorHandler)
            .recordingLog(recordingLog)
            .clusterMarkFile(markFile)
            .aeron(aeron)
            .moduleStateCounter(moduleState)
            .clusterNodeRoleCounter(clusterNodeRole)
            .commitPositionCounter(commitPosition)
            .controlToggleCounter(controlToggle)
            .snapshotCounter(snapshotCounter)
            .timedOutClientCounter(timedOutClientCounter);
    }

    @Test
    void ownsAeronClient() throws Exception
    {
        context.ownsAeronClient(true);

        final AeronException ex = assertThrows(AeronException.class, context::close);

        assertSame(aeronException, ex);

        final InOrder inOrder = inOrder(countedErrorHandler, errorHandler, aeron);
        inOrder.verify(countedErrorHandler).onError(recodingLogException);
        inOrder.verify(countedErrorHandler).onError(markFileException);
        inOrder.verify((AutoCloseable)errorHandler).close();
        inOrder.verify(aeron).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void doesNotOwnAeronClientAndClientIsNotClosed() throws Exception
    {
        context.ownsAeronClient(false);
        when(aeron.isClosed()).thenReturn(false);

        final IndexOutOfBoundsException ex = assertThrows(IndexOutOfBoundsException.class, context::close);

        assertSame(timedOutClientCounterException, ex);

        final Throwable[] expected =
        {
            controlToggleException,
            snapshotCounterException,
            moduleStateException,
            clusterNodeRoleException,
            commitPositionException,
        };

        assertArrayEquals(expected, ex.getSuppressed());

        final InOrder inOrder = inOrder(countedErrorHandler, errorHandler, aeron);
        inOrder.verify(countedErrorHandler).onError(recodingLogException);
        inOrder.verify(countedErrorHandler).onError(markFileException);
        inOrder.verify((AutoCloseable)errorHandler).close();
        inOrder.verify(aeron).isClosed();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void doesNotOwnAeronClientAndClientIsClosed() throws Exception
    {
        context.ownsAeronClient(false);
        when(aeron.isClosed()).thenReturn(true);

        context.close();

        final InOrder inOrder = inOrder(countedErrorHandler, errorHandler, aeron);
        inOrder.verify(countedErrorHandler).onError(recodingLogException);
        inOrder.verify(countedErrorHandler).onError(markFileException);
        inOrder.verify((AutoCloseable)errorHandler).close();
        inOrder.verify(aeron).isClosed();
        inOrder.verifyNoMoreInteractions();
    }
}
