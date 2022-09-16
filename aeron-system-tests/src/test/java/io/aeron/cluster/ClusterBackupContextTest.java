/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.RethrowingErrorHandler;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterBackupContextTest
{
    @TempDir
    private File clusterDir;

    @Test
    void throwsIllegalStateExceptionIfThereIsAnActiveMarkFile()
    {
        final RethrowingErrorHandler errorHandler = mock(RethrowingErrorHandler.class);
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.aeronDirectoryName()).thenReturn("funny");
        when(aeronContext.subscriberErrorHandler()).thenReturn(errorHandler);
        final AgentInvoker conductorAgentInvoker = mock(AgentInvoker.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.conductorAgentInvoker()).thenReturn(conductorAgentInvoker);
        final AtomicCounter errorCounter = mock(AtomicCounter.class);
        final ClusterBackup.Context context = new ClusterBackup.Context()
            .aeron(aeron)
            .errorCounter(errorCounter)
            .errorHandler(errorHandler)
            .clusterDir(clusterDir)
            .catchupEndpoint("something");
        final ClusterBackup.Context other = context.clone();

        try
        {
            context.conclude();

            final RuntimeException exception = assertThrowsExactly(RuntimeException.class, other::conclude);
            final Throwable cause = exception.getCause();
            assertInstanceOf(IllegalStateException.class, cause);
            assertEquals("active Mark file detected", cause.getMessage());
        }
        finally
        {
            context.close();
        }
    }
}
