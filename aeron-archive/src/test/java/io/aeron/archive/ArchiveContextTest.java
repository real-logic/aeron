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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;

import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.*;
import static io.aeron.archive.Archive.Configuration.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArchiveContextTest
{
    private final Archive.Context context = TestContexts.localhostArchive();
    private static final int ARCHIVE_CONTROL_SESSIONS_COUNTER_ID = 928234;

    @BeforeEach
    void beforeEach(final @TempDir Path tempDir)
    {
        final Aeron aeron = mock(Aeron.class);
        final CountersReader countersReader = mock(CountersReader.class);
        final Aeron.Context aeronContext = new Aeron.Context();
        aeronContext.subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);
        aeronContext.aeronDirectoryName("test-archive-config");
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.countersReader()).thenReturn(countersReader);

        context
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .controlSessionsCounter(
                mockCounter(countersReader, ARCHIVE_CONTROL_SESSIONS_TYPE_ID, ARCHIVE_CONTROL_SESSIONS_COUNTER_ID))
            .totalWriteBytesCounter(mockCounter(countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 111))
            .totalWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 222))
            .maxWriteTimeCounter(mockCounter(countersReader, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 333))
            .totalReadBytesCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, 77))
            .totalReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, 88))
            .maxReadTimeCounter(mockCounter(countersReader, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, 99))
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

    @Test
    void markFileDirShouldReturnArchiveDirWhenNotSet(final @TempDir File archiveDir)
    {
        context.archiveDir(archiveDir);

        assertSame(archiveDir, context.markFileDir());
    }

    @Test
    void markFileDirShouldReturnExplicitlySetDirectory(final @TempDir File tempDir)
    {
        final File archiveDir = new File(tempDir, "archiveDir");
        final File markFileDir = new File(tempDir, "markFileDir");
        context.archiveDir(archiveDir);
        context.markFileDir(markFileDir);

        assertSame(markFileDir, context.markFileDir());
        assertSame(archiveDir, context.archiveDir());
    }

    @Test
    void configurationMarkFileDirReturnsNullIfPropertyNotSet()
    {
        System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        assertNull(Archive.Configuration.markFileDir());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "abc", "x/y/z" })
    void configurationMarkFileDirReturnsValueSet(final String markFileDir)
    {
        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir);
        try
        {
            assertEquals(markFileDir, Archive.Configuration.markFileDir());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getAbsolutePath());
        try
        {
            assertSame(context.archiveDir(), context.markFileDir());

            context.conclude();

            assertEquals(markFileDir, context.markFileDir());
            assertTrue(markFileDir.exists());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetDirectly(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());
        context.markFileDir(markFileDir);

        context.conclude();

        assertEquals(markFileDir, context.markFileDir());
        assertTrue(markFileDir.exists());
    }

    @Test
    void concludeCreatesTotalWriteBytesCounter()
    {
        context.totalWriteBytesCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 42);
        when(aeron.addCounter(ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, "archive-recorder total write bytes"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.totalWriteBytesCounter());
    }

    @Test
    void concludeValidatesTotalWriteBytesCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalWriteBytesCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalWriteTimeCounter()
    {
        context.totalWriteTimeCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 42);
        when(aeron.addCounter(ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, "archive-recorder total write time in ns"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.totalWriteTimeCounter());
    }

    @Test
    void concludeValidatesTotalWriteTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalWriteTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesMaxWriteTimeCounter()
    {
        context.maxWriteTimeCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 142);
        when(aeron.addCounter(ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, "archive-recorder max write time in ns"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.maxWriteTimeCounter());
    }

    @Test
    void concludeValidatesMaxWriteTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.maxWriteTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalReadBytesCounter()
    {
        context.totalReadBytesCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, 999);
        when(aeron.addCounter(ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, "archive-replayer total read bytes"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.totalReadBytesCounter());
    }

    @Test
    void concludeValidatesTotalReadBytesCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalReadBytesCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID));
    }

    @Test
    void concludeCreatesTotalReadTimeCounter()
    {
        context.totalReadTimeCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, -8);
        when(aeron.addCounter(ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, "archive-replayer total read time in ns"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.totalReadTimeCounter());
    }

    @Test
    void concludeValidatesTotalReadTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.totalReadTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID));
    }

    @Test
    void concludeCreatesMaxReadTimeCounter()
    {
        context.maxReadTimeCounter(null);

        final Aeron aeron = context.aeron();
        final Counter counter = mockCounter(aeron.countersReader(), ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, -76);
        when(aeron.addCounter(ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, "archive-replayer max read time in ns"))
            .thenReturn(counter);

        context.conclude();

        assertSame(counter, context.maxReadTimeCounter());
    }

    @Test
    void concludeValidatesMaxReadTimeCounter()
    {
        final Counter counter = mock(Counter.class);
        context.maxReadTimeCounter(counter);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().endsWith("expected=" + ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID));
    }

    private static Counter mockCounter(final CountersReader countersReader, final int typeId, final int id)
    {
        final Counter counter = mock(Counter.class);
        when(counter.id()).thenReturn(id);

        when(countersReader.getCounterTypeId(id)).thenReturn(typeId);
        return counter;
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
