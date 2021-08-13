/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.RethrowingErrorHandler;
import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsensusModuleContextTest
{
    @TempDir
    public File clusterDir;

    private ConsensusModule.Context context;

    @BeforeEach
    void beforeEach()
    {
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.subscriberErrorHandler()).thenReturn(new RethrowingErrorHandler());
        final AgentInvoker conductorInvoker = mock(AgentInvoker.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.conductorAgentInvoker()).thenReturn(conductorInvoker);

        context = new ConsensusModule.Context()
            .clusterDir(clusterDir)
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .ingressChannel("must be specified")
            .replicationChannel("must be specified")
            .moduleStateCounter(mock(Counter.class))
            .electionStateCounter(mock(Counter.class))
            .clusterNodeRoleCounter(mock(Counter.class))
            .commitPositionCounter(mock(Counter.class))
            .controlToggleCounter(mock(Counter.class))
            .snapshotCounter(mock(Counter.class))
            .timedOutClientCounter(mock(Counter.class));
    }

    @ParameterizedTest
    @ValueSource(strings = { TIMER_SERVICE_SUPPLIER_TIMER_WHEEL, TIMER_SERVICE_SUPPLIER_SEQUENTIAL })
    void validTimerServiceSupplier(final String supplierName)
    {
        System.setProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, supplierName);
        try
        {
            context.conclude();

            final TimerServiceSupplier supplier = context.timerServiceSupplier();
            assertNotNull(supplier);

            final TimerService.TimerHandler timerHandler = mock(TimerService.TimerHandler.class);
            final TimerService timerService = supplier.newInstance(timerHandler);

            assertNotNull(timerService);
            assertEquals(supplierName, timerService.getClass().getSimpleName());
        }
        finally
        {
            System.clearProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void unknownTimerServiceSupplier()
    {
        final String supplierName = "unknown timer service supplier";
        System.setProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, supplierName);
        try
        {
            final ClusterException exception = assertThrows(ClusterException.class, context::conclude);
            assertEquals("ERROR - invalid TimerServiceSupplier: " + supplierName, exception.getMessage());
        }
        finally
        {
            System.clearProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME);
        }
    }
}