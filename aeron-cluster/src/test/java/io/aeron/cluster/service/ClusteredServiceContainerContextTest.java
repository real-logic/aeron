/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;

import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_DIR_PROP_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusteredServiceContainerContextTest
{
    @TempDir
    private File clusterDir;
    private ClusteredServiceContainer.Context context;
    private final int serviceId = 1;

    @BeforeEach
    void setUp()
    {
        final RethrowingErrorHandler errorHandler = mock(RethrowingErrorHandler.class);
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.aeronDirectoryName()).thenReturn("funny");
        when(aeronContext.subscriberErrorHandler()).thenReturn(errorHandler);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(anyInt(), any(String.class))).thenAnswer(invocation -> mock(Counter.class));
        when(aeron.context()).thenReturn(aeronContext);
        final AtomicCounter errorCounter = mock(AtomicCounter.class);
        final ClusteredService clusteredService = mock(ClusteredService.class);
        context = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .errorCounter(errorCounter)
            .errorHandler(errorHandler)
            .clusteredService(clusteredService)
            .clusterDir(clusterDir);
    }

    @Test
    void throwsIllegalStateExceptionIfAnActiveMarkFileExists()
    {
        final ClusteredServiceContainer.Context anotherInstance = context.clone();

        try
        {
            context.conclude();

            final RuntimeException exception = assertThrowsExactly(RuntimeException.class, anotherInstance::conclude);
            final Throwable cause = exception.getCause();
            assertInstanceOf(IllegalStateException.class, cause);
            assertEquals("active Mark file detected", cause.getMessage());
        }
        finally
        {
            context.close();
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());
        context.serviceId(serviceId);

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getAbsolutePath());
        try
        {
            assertSame(null, context.markFileDir());

            context.conclude();

            assertEquals(markFileDir, context.markFileDir());
            assertTrue(markFileDir.exists());
            assertTrue(
                new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(context.serviceId())).exists());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
            CloseHelper.quietClose(context::close);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetDirectly(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());
        context.serviceId(serviceId).markFileDir(markFileDir);

        try
        {
            context.conclude();

            assertEquals(markFileDir, context.markFileDir());
            assertTrue(markFileDir.exists());
            assertTrue(
                new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(context.serviceId())).exists());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRemoveLinkIfMarkFileIsInClusterDir(final boolean isSet) throws IOException
    {
        final File markFileDir = isSet ? context.clusterDir() : null;

        context.serviceId(serviceId).markFileDir(markFileDir);
        final File oldLinkFile = new File(context.clusterDir(), ClusterMarkFile.linkFilenameForService(serviceId));
        assertTrue(oldLinkFile.createNewFile());
        assertTrue(oldLinkFile.exists());

        try
        {
            context.conclude();

            assertFalse(oldLinkFile.exists());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
        }
    }
}
