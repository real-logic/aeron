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
package io.aeron.archive;

import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.test.TestContexts;
import org.agrona.MarkFile;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.io.File;
import java.nio.MappedByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArchiveMarkFileTest
{
    @Test
    void shouldUseMarkFileDirectory(final @TempDir File tempDir)
    {
        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark-file-home");
        final File markFile = new File(markFileDir, ArchiveMarkFile.FILENAME);
        assertTrue(markFileDir.mkdirs());
        assertFalse(markFile.exists());

        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.advance(12345678909876L);
        final Archive.Context ctx = TestContexts.localhostArchive()
            .aeronDirectoryName(aeronDir.getAbsolutePath())
            .controlStreamId(42)
            .localControlStreamId(-118)
            .recordingEventsStreamId(85858585)
            .errorBufferLength(4096)
            .archiveId(46238467823468L)
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .controlChannel("aeron:udp?endpoint=localhost:55555|alias=control")
            .localControlChannel(AeronArchive.Configuration.localControlChannel())
            .recordingEventsChannel("aeron:udp?endpoint=localhost:0")
            .epochClock(epochClock);
        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(ctx))
        {
            assertNotNull(archiveMarkFile);

            assertEquals(epochClock.time(), archiveMarkFile.decoder().startTimestamp());
            assertEquals(ctx.controlStreamId(), archiveMarkFile.decoder().controlStreamId());
            assertEquals(ctx.localControlStreamId(), archiveMarkFile.decoder().localControlStreamId());
            assertEquals(ctx.recordingEventsStreamId(), archiveMarkFile.decoder().eventsStreamId());
            assertEquals(ArchiveMarkFile.HEADER_LENGTH, archiveMarkFile.decoder().headerLength());
            assertEquals(ctx.errorBufferLength(), archiveMarkFile.decoder().errorBufferLength());
            assertEquals(ctx.archiveId(), archiveMarkFile.decoder().archiveId());
            assertEquals(ctx.controlChannel(), archiveMarkFile.decoder().controlChannel());
            assertEquals(ctx.localControlChannel(), archiveMarkFile.decoder().localControlChannel());
            assertEquals(ctx.recordingEventsChannel(), archiveMarkFile.decoder().eventsChannel());
            assertEquals(ctx.aeronDirectoryName(), archiveMarkFile.decoder().aeronDirectory());
        }

        assertTrue(markFile.exists());
        assertFalse(archiveDir.exists());
    }

    @Test
    @SuppressWarnings("try")
    void shouldCreateLinkFileToMarkFileDirAndRemoveLinkFileIfArchiveDirMatchesMarkFileDir(final @TempDir File tempDir)
    {
        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File markFileDir = new File(tempDir, "mark-file-home");

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath());
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .epochClock(SystemEpochClock.INSTANCE);

        try (MediaDriver driver = MediaDriver.launch(driverContext.clone());
            Archive archive = Archive.launch(archiveContext.clone()))
        {
            assertTrue(new File(markFileDir, ArchiveMarkFile.FILENAME).exists());
            assertTrue(new File(archiveDir, ArchiveMarkFile.LINK_FILENAME).exists());
            assertFalse(new File(archiveDir, ArchiveMarkFile.FILENAME).exists());
        }

        archiveContext.markFileDir(null);
        try (MediaDriver driver = MediaDriver.launch(driverContext.clone());
            Archive archive = Archive.launch(archiveContext.clone()))
        {
            assertTrue(new File(archiveDir, ArchiveMarkFile.FILENAME).exists());
            assertFalse(new File(archiveDir, ArchiveMarkFile.LINK_FILENAME).exists());
        }
    }

    @Test
    @SuppressWarnings("try")
    void anErrorOnStartupShouldNotLeaveAnUninitilisedMarkFile(final @TempDir File tempDir)
    {
        System.setProperty(CommonContext.DRIVER_TIMEOUT_PROP_NAME, "100");

        final File aeronDir = new File(tempDir, "aeron");
        final File archiveDir = new File(tempDir, "archive_dir");
        final File archiveMarkFile = new File(archiveDir, ArchiveMarkFile.FILENAME);

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath());
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(archiveMarkFile.getParentFile())
            .epochClock(SystemEpochClock.INSTANCE);

        // Force an error on startup by attempting to start an archive without a media driver.
        try (Archive ignored = Archive.launch(archiveContext.clone()))
        {
            fail("Expected archive to timeout as no media driver is running.");
        }
        catch (final DriverTimeoutException ex)
        {
            // Should be able to read the mark file and the activity timestamp should not have been set.
            try (ArchiveMarkFile testMarkFile = new ArchiveMarkFile(archiveContext.clone()))
            {
                assertEquals(0, testMarkFile.activityTimestampVolatile());
            }
        }
        finally
        {
            System.clearProperty(CommonContext.DRIVER_TIMEOUT_PROP_NAME);
        }

        // Should be able to successfully start the archive
        try (MediaDriver ignored1 = MediaDriver.launch(driverContext.clone());
            Archive archive = Archive.launch(archiveContext.clone()))
        {
            assertTrue(archiveMarkFile.exists());
            final long activityTimestamp = archive.context().archiveMarkFile().activityTimestampVolatile();
            assertTrue(activityTimestamp > 0);
        }
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldCallForceIfMarkFileIsNotClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        try (ArchiveMarkFile clusterMarkFile = new ArchiveMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verify(markFile).mappedByteBuffer();
            inOrder.verify(mappedByteBuffer).force();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldNotCallForceIfMarkFileIsClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        when(markFile.isClosed()).thenReturn(true);
        try (ArchiveMarkFile clusterMarkFile = new ArchiveMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verifyNoMoreInteractions();
        }
    }
}
