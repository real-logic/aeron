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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.test.TestContexts;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.aeron.archive.Archive.Configuration.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ArchiveContextTest
{
    private final Archive.Context context = TestContexts.localhostArchive();
    private static final int ARCHIVE_CONTROL_SESSIONS_COUNTER_ID = 928234;

    @BeforeEach
    void beforeEach(final @TempDir Path tempDir)
    {
        final Aeron aeron = mock(Aeron.class);
        final CountersReader countersReader = mock(CountersReader.class);
        final Counter counter = mock(Counter.class);
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);
        aeronContext.aeronDirectoryName("test-archive-config");
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.countersReader()).thenReturn(countersReader);
        when(countersReader.getCounterTypeId(ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .thenReturn(ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        when(counter.id()).thenReturn(ARCHIVE_CONTROL_SESSIONS_COUNTER_ID);

        context
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .controlSessionsCounter(counter)
            .archiveDir(tempDir.resolve("archive-test").toFile());
    }

    @AfterEach
    void afterEach()
    {
        context.close();
    }

    @Test
    void defaultAuthorisationServiceSupplierReturnsAnAllowAllAuthorisationService()
    {
        assertSame(AuthorisationService.ALLOW_ALL, DEFAULT_AUTHORISATION_SERVICE_SUPPLIER.get());
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
    void shouldThrowIfReplicationChannelIsNotSet()
    {
        context.replicationChannel(null);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldDeriveArchiveClientContextResponseChannelFromArchiveControlChannel()
    {
        context.controlChannel("aeron:udp?endpoint=127.0.0.2:23005");
        context.conclude();
        assertEquals("aeron:udp?endpoint=127.0.0.2:0", context.archiveClientContext().controlResponseChannel());
    }

    @Test
    void shouldThrowConfigurationExceptionIfUnableToDeriveArchiveClientContextResponseChannelDueToEndpointFormat()
    {
        context.controlChannel("aeron:udp?endpoint=some_logical_name");
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldThrowConfigurationExceptionIfUnableToDeriveArchiveClientContextResponseChannelDueToEndpointNull()
    {
        context.controlChannel("aeron:udp?control-mode=dynamic|control=192.168.0.1:12345");
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldThrowIllegalStateExceptionIfThereIsAnActiveMarkFile()
    {
        context.conclude();
        assertNotNull(context.archiveMarkFile());
        assertNotEquals(0, context.archiveMarkFile().activityTimestampVolatile());

        final Archive.Context anotherContext = TestContexts.localhostArchive()
            .archiveDir(context.archiveDir())
            .errorHandler(context.errorHandler())
            .aeron(context.aeron());

        final RuntimeException exception = assertThrowsExactly(RuntimeException.class, anotherContext::conclude);
        final Throwable cause = exception.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("active Mark file detected", cause.getMessage());
    }

    @Test
    void shouldValidateThatSessionCounterIsOfTheCorrectType()
    {
        when(context.aeron().countersReader().getCounterTypeId(ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .thenReturn(AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID);

        assertThrows(ConfigurationException.class, context::conclude);
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
