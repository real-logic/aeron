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
import io.aeron.RethrowingErrorHandler;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static io.aeron.cluster.codecs.mark.ClusterComponentType.BACKUP;
import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_DIR_PROP_NAME;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterBackupContextTest
{
    @TempDir
    private File clusterDir;
    private ClusterBackup.Context context;

    @BeforeEach
    void setUp()
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
        context = new ClusterBackup.Context()
            .aeron(aeron)
            .errorCounter(errorCounter)
            .errorHandler(errorHandler)
            .clusterDir(clusterDir)
            .catchupEndpoint("something");
    }

    @AfterEach
    void tearDown()
    {
        context.close();
    }

    @Test
    void throwsIllegalStateExceptionIfThereIsAnActiveMarkFile()
    {
        final ClusterBackup.Context other = context.clone();

        context.conclude();

        final RuntimeException exception = assertThrowsExactly(RuntimeException.class, other::conclude);
        final Throwable cause = exception.getCause();
        assertInstanceOf(IllegalStateException.class, cause);
        assertEquals("active Mark file detected", cause.getMessage());
    }

    @Test
    void clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet() throws IOException
    {
        context.clusterDir(clusterDir);
        context.conclude();

        assertEquals(
            new File(context.clusterDirectoryName()).getCanonicalPath(), context.clusterDir().getCanonicalPath());
    }

    @Test
    void clusterDirectoryNameShouldMatchClusterDirWhenClusterDirectoryNameSet() throws IOException
    {
        context.clusterDir(null);
        context.clusterDirectoryName(clusterDir.getAbsolutePath());
        context.conclude();

        assertEquals(
            new File(context.clusterDirectoryName()).getCanonicalPath(), context.clusterDir().getCanonicalPath());
    }

    @Test
    @Disabled
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir)
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getAbsolutePath());
        try
        {
            assertSame(null, context.markFileDir());

            context.conclude();

            assertEquals(markFileDir, context.markFileDir());
            assertTrue(markFileDir.exists());
            assertTrue(new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME).exists());
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
        assertTrue(new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME).exists());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRemoveLinkIfMarkFileIsInClusterDir(final boolean isSet) throws IOException
    {
        final File markFileDir = isSet ? context.clusterDir() : null;

        context.markFileDir(markFileDir);
        final File oldLinkFile = new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME);
        assertTrue(oldLinkFile.createNewFile());
        assertTrue(oldLinkFile.exists());

        context.conclude();

        assertFalse(oldLinkFile.exists());
    }

    @Test
    void concludeShouldCreateLinkFilePointingToTheParentDirectoryOfTheMarkFile(
        final @TempDir File clusterDir,
        final @TempDir File markFileDir,
        final @TempDir File otherDir) throws IOException
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            new File(otherDir, "abc.xyz"), BACKUP, ERROR_BUFFER_MIN_LENGTH, SystemEpochClock.INSTANCE, 4);
        context
            .clusterDir(clusterDir)
            .markFileDir(markFileDir)
            .clusterMarkFile(clusterMarkFile);

        context.conclude();

        assertEquals(clusterDir, context.clusterDir());
        assertEquals(markFileDir, context.markFileDir());
        assertEquals(otherDir, context.clusterMarkFile().parentDirectory());
        final File linkFile = new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME);
        assertTrue(linkFile.exists());
        assertEquals(otherDir.getCanonicalPath(), new String(Files.readAllBytes(linkFile.toPath()), US_ASCII));
    }
}
