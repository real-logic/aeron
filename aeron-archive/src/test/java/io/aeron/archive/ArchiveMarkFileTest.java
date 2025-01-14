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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.test.TestContexts;
import org.agrona.IoUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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

        final Archive.Context context = TestContexts.localhostArchive()
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .aeronDirectoryName(aeronDir.getAbsolutePath());
        shouldEncodeMarkFileFromArchiveContext(context);

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

    @Test
    void shouldBeAbleToReadOldMarkFileV0(final @TempDir File tempDir) throws IOException
    {
        final int version = SemanticVersion.compose(ArchiveMarkFile.MAJOR_VERSION, 28, 16);
        final long activityTimestamp = 234783974944L;
        final int pid = -55555;
        final int controlStreamId = 1010;
        final int localControlStreamId = -9;
        final int eventsStreamId = 111;
        final String controlChannel = "aeron:udp?endpoint=localhost:8010";
        final String localControlChannel = "aeron:ipc?alias=local-control|term-length=64k";
        final String eventsChannels = "aeron:ipc?alias=events";
        final String aeronDirectory = tempDir.toPath().resolve("aeron/dir/path").toAbsolutePath().toString();

        final Path file = Files.write(
            tempDir.toPath().resolve(ArchiveMarkFile.FILENAME), new byte[1024], StandardOpenOption.CREATE_NEW);

        final MarkFile markFile = new MarkFile(
            IoUtil.mapExistingFile(file.toFile(), ArchiveMarkFile.FILENAME),
            io.aeron.archive.codecs.mark.v0.MarkFileHeaderDecoder.versionEncodingOffset(),
            io.aeron.archive.codecs.mark.v0.MarkFileHeaderDecoder.activityTimestampEncodingOffset());

        final io.aeron.archive.codecs.mark.v0.MarkFileHeaderEncoder encoder =
            new io.aeron.archive.codecs.mark.v0.MarkFileHeaderEncoder();
        encoder.wrap(markFile.buffer(), 0);

        encoder
            .version(version)
            .activityTimestamp(activityTimestamp)
            .pid(pid)
            .controlStreamId(controlStreamId)
            .localControlStreamId(localControlStreamId)
            .eventsStreamId(eventsStreamId)
            .controlChannel(controlChannel)
            .localControlChannel(localControlChannel)
            .eventsChannel(eventsChannels)
            .aeronDirectory(aeronDirectory);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(markFile))
        {
            assertEquals(version, archiveMarkFile.decoder().version());
            assertEquals(activityTimestamp, archiveMarkFile.decoder().activityTimestamp());
            assertEquals(pid, archiveMarkFile.decoder().pid());
            assertEquals(controlStreamId, archiveMarkFile.decoder().controlStreamId());
            assertEquals(localControlStreamId, archiveMarkFile.decoder().localControlStreamId());
            assertEquals(eventsStreamId, archiveMarkFile.decoder().eventsStreamId());
            assertEquals(controlChannel, archiveMarkFile.decoder().controlChannel());
            assertEquals(localControlChannel, archiveMarkFile.decoder().localControlChannel());
            assertEquals(eventsChannels, archiveMarkFile.decoder().eventsChannel());
            assertEquals(aeronDirectory, archiveMarkFile.decoder().aeronDirectory());

            assertEquals(aeronDirectory, archiveMarkFile.aeronDirectory());
            assertEquals(Aeron.NULL_VALUE, archiveMarkFile.archiveId());
        }

        final Archive.Context context = new Archive.Context()
            .archiveDir(new File(tempDir, "archive"))
            .markFileDir(file.getParent().toFile())
            .aeronDirectoryName(aeronDirectory);
        shouldEncodeMarkFileFromArchiveContext(context);
    }

    @Test
    void shouldBeAbleToReadOldMarkFileV1(final @TempDir File tempDir) throws IOException
    {
        final int version = SemanticVersion.compose(ArchiveMarkFile.MAJOR_VERSION, 4, 4);
        final long activityTimestamp = 234783974944L;
        final int pid = 1;
        final int controlStreamId = 42;
        final int localControlStreamId = 19;
        final int eventsStreamId = -87;
        final int errorBufferLength = 8192;
        final String controlChannel = "aeron:udp?endpoint=localhost:8010";
        final String localControlChannel = "aeron:ipc?alias=local-control|term-length=64k";
        final String eventsChannels = "aeron:ipc?alias=events";
        final String aeronDirectory = tempDir.toPath().resolve("aeron/dir/path").toAbsolutePath().toString();

        final Path file = Files.write(
            tempDir.toPath().resolve(ArchiveMarkFile.FILENAME), new byte[32 * 1024], StandardOpenOption.CREATE_NEW);

        final MarkFile markFile = new MarkFile(
            IoUtil.mapExistingFile(file.toFile(), ArchiveMarkFile.FILENAME),
            io.aeron.archive.codecs.mark.v1.MarkFileHeaderDecoder.versionEncodingOffset(),
            io.aeron.archive.codecs.mark.v1.MarkFileHeaderDecoder.activityTimestampEncodingOffset());

        final io.aeron.archive.codecs.mark.v1.MarkFileHeaderEncoder encoder =
            new io.aeron.archive.codecs.mark.v1.MarkFileHeaderEncoder();
        encoder.wrap(markFile.buffer(), 0);

        encoder
            .version(version)
            .activityTimestamp(activityTimestamp)
            .pid(pid)
            .controlStreamId(controlStreamId)
            .localControlStreamId(localControlStreamId)
            .eventsStreamId(eventsStreamId)
            .headerLength(ArchiveMarkFile.HEADER_LENGTH)
            .errorBufferLength(errorBufferLength)
            .controlChannel(controlChannel)
            .localControlChannel(localControlChannel)
            .eventsChannel(eventsChannels)
            .aeronDirectory(aeronDirectory);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(markFile))
        {
            assertEquals(version, archiveMarkFile.decoder().version());
            assertEquals(activityTimestamp, archiveMarkFile.decoder().activityTimestamp());
            assertEquals(pid, archiveMarkFile.decoder().pid());
            assertEquals(controlStreamId, archiveMarkFile.decoder().controlStreamId());
            assertEquals(localControlStreamId, archiveMarkFile.decoder().localControlStreamId());
            assertEquals(eventsStreamId, archiveMarkFile.decoder().eventsStreamId());
            assertEquals(ArchiveMarkFile.HEADER_LENGTH, archiveMarkFile.decoder().headerLength());
            assertEquals(errorBufferLength, archiveMarkFile.decoder().errorBufferLength());
            assertEquals(controlChannel, archiveMarkFile.decoder().controlChannel());
            assertEquals(localControlChannel, archiveMarkFile.decoder().localControlChannel());
            assertEquals(eventsChannels, archiveMarkFile.decoder().eventsChannel());
            assertEquals(aeronDirectory, archiveMarkFile.decoder().aeronDirectory());

            assertEquals(aeronDirectory, archiveMarkFile.aeronDirectory());
            assertEquals(Aeron.NULL_VALUE, archiveMarkFile.archiveId());
        }

        final Archive.Context context = new Archive.Context()
            .archiveDir(new File(tempDir, "archive"))
            .markFileDir(file.getParent().toFile())
            .aeronDirectoryName(aeronDirectory)
            .errorBufferLength(errorBufferLength);
        shouldEncodeMarkFileFromArchiveContext(context);
    }

    private static void shouldEncodeMarkFileFromArchiveContext(final Archive.Context ctx)
    {
        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.advance(12345678909876L);
        ctx
            .controlStreamId(42)
            .localControlStreamId(-118)
            .recordingEventsStreamId(85858585)
            .archiveId(46238467823468L)
            .controlChannel("aeron:udp?endpoint=localhost:55555|alias=control")
            .localControlChannel(AeronArchive.Configuration.localControlChannel())
            .recordingEventsChannel("aeron:udp?endpoint=localhost:0")
            .epochClock(epochClock);

        System.out.println(ctx);

        try (ArchiveMarkFile archiveMarkFile = new ArchiveMarkFile(ctx))
        {
            archiveMarkFile.signalReady();

            assertEquals(ArchiveMarkFile.SEMANTIC_VERSION, archiveMarkFile.decoder().version());
            assertEquals(archiveMarkFile.encoder().sbeSchemaVersion(), archiveMarkFile.decoder().codecSchemaVersion());
            assertEquals(archiveMarkFile.encoder().sbeBlockLength(), archiveMarkFile.decoder().codecBlockLength());
            assertEquals(epochClock.time(), archiveMarkFile.decoder().startTimestamp());
            assertEquals(SystemUtil.getPid(), archiveMarkFile.decoder().pid());
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

            assertInstanceOf(MarkFileHeaderDecoder.class, archiveMarkFile.decoder());
            assertInstanceOf(MarkFileHeaderEncoder.class, archiveMarkFile.encoder());
            assertEquals(ctx.archiveId(), archiveMarkFile.archiveId());

            assertEquals(ctx.aeronDirectoryName(), archiveMarkFile.aeronDirectory());
        }
    }
}
