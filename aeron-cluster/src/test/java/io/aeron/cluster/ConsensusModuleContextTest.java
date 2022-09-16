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
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.TestContexts;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
    File clusterDir;

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

        context = TestContexts.localhostConsensusModule()
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

    @AfterEach
    void afterEach()
    {
        context.close();
    }

    @ParameterizedTest
    @ValueSource(strings = { TIMER_SERVICE_SUPPLIER_WHEEL, TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP })
    void validTimerServiceSupplier(final String supplierName)
    {
        System.setProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, supplierName);
        try
        {
            context.conclude();

            final TimerServiceSupplier supplier = context.timerServiceSupplier();
            assertNotNull(supplier);

            final TimerService.TimerHandler timerHandler = mock(TimerService.TimerHandler.class);
            final TimerService timerService = supplier.newInstance(context.clusterClock().timeUnit(), timerHandler);

            assertNotNull(timerService);
            assertEquals(supplierName, supplier.getClass().getName());
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

    @Test
    void defaultTimerServiceSupplier()
    {
        context.conclude();

        final TimerServiceSupplier supplier = context.timerServiceSupplier();
        assertNotNull(supplier);

        final TimerService.TimerHandler timerHandler = mock(TimerService.TimerHandler.class);
        final TimerService timerService = supplier.newInstance(context.clusterClock().timeUnit(), timerHandler);

        assertNotNull(timerService);
        assertEquals(WheelTimerService.class, timerService.getClass());
    }

    @Test
    void explicitTimerServiceSupplier()
    {
        final TimerServiceSupplier supplier = (clusterClock, timerHandler) -> null;

        context.timerServiceSupplier(supplier);
        assertSame(supplier, context.timerServiceSupplier());

        context.conclude();

        assertSame(supplier, context.timerServiceSupplier());
    }

    @Test
    void rejectInvalidLogChannelParameters()
    {
        final String channelTermId = context.logChannel() + "|" + CommonContext.TERM_ID_PARAM_NAME + "=0";
        final String channelInitialTermId =
            context.logChannel() + "|" + CommonContext.INITIAL_TERM_ID_PARAM_NAME + "=0";
        final String channelTermOffset = context.logChannel() + "|" + CommonContext.TERM_OFFSET_PARAM_NAME + "=0";

        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelTermId).conclude());
        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelInitialTermId).conclude());
        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelTermOffset).conclude());
    }

    @Test
    void defaultAuthorisationServiceSupplierReturnsADenyAllAuthorisationService()
    {
        assertSame(AuthorisationService.DENY_ALL, DEFAULT_AUTHORISATION_SERVICE_SUPPLIER.get());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsNotSet()
    {
        assertNull(context.authorisationServiceSupplier());

        context.conclude();

        System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsSetToEmptyValue()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, "");
        try
        {
            assertNull(context.authorisationServiceSupplier());

            context.conclude();

            assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldInstantiateAuthorisationServiceSupplierBasedOnTheSystemProperty()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            final AuthorisationServiceSupplier supplier = context.authorisationServiceSupplier();
            assertNotSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, supplier);
            assertInstanceOf(TestAuthorisationSupplier.class, supplier);
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldUseProvidedAuthorisationServiceSupplierInstance()
    {
        final AuthorisationServiceSupplier providedSupplier = mock(AuthorisationServiceSupplier.class);
        context.authorisationServiceSupplier(providedSupplier);
        assertSame(providedSupplier, context.authorisationServiceSupplier());

        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            assertSame(providedSupplier, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldThrowIllegalStateExceptionIfAnActiveMarkFileExists()
    {
        final ConsensusModule.Context another = context.clone();
        context.conclude();

        final RuntimeException exception = assertThrowsExactly(RuntimeException.class, another::conclude);
        final Throwable cause = exception.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("active Mark file detected", cause.getMessage());
    }

    public static class TestAuthorisationSupplier implements AuthorisationServiceSupplier
    {
        public AuthorisationService get()
        {
            return new TestAuthorisationService();
        }
    }

    static class TestAuthorisationService implements AuthorisationService
    {
        public boolean isAuthorised(
            final int protocolId, final int actionId, final Object type, final byte[] encodedPrincipal)
        {
            return false;
        }
    }
}
