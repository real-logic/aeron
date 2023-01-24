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
package io.aeron.archive;

import io.aeron.archive.codecs.mark.v1.MarkFileHeaderEncoder;
import io.aeron.driver.MediaDriver;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ArchiveToolSeparateMarkFileTest
{
    @Test
    void shouldDescribe(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .controlChannel("aeron:udp?endpoint=localhost:9091")
            .replicationChannel("aeron:udp?endpoint=localhost:9092");

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.describe(new PrintStream(new ByteArrayOutputStream()), markFileDir);
        }
    }

    @Test
    void shouldDescribeRecordings(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .controlChannel("aeron:udp?endpoint=localhost:9091")
            .replicationChannel("aeron:udp?endpoint=localhost:9092");

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.describeRecording(new PrintStream(new ByteArrayOutputStream()), markFileDir, 0);
        }
    }

    @Test
    void shouldSetCapacity(@TempDir final File archiveDir, @TempDir final File markFileDir)
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir)
            .controlChannel("aeron:udp?endpoint=localhost:9091")
            .replicationChannel("aeron:udp?endpoint=localhost:9092");

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.capacity(markFileDir, 1 << 16);
        }
    }

    @Test
    void shouldHandleOldVersionAndAutoMigrate(@TempDir final File archiveDir)
    {
        try (ArchiveMarkFile archiveMarkFile310 = createArchiveMarkFile3_1_0(archiveDir))
        {
            assertNull(archiveMarkFile310.archiveDirectory());
            archiveMarkFile310.updateActivityTimestamp(NULL_VALUE);
        }

        try
        {
            ArchiveTool.describe(new PrintStream(new ByteArrayOutputStream()), archiveDir);
            fail();
        }
        catch (final Exception ex)
        {
            assertEquals("Location of archive directory not available", ex.getMessage());
        }

        final MediaDriver.Context driverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .controlChannel("aeron:udp?endpoint=localhost:9091")
            .replicationChannel("aeron:udp?endpoint=localhost:9092");

        try (MediaDriver driver = MediaDriver.launch(driverContext);
            Archive archive = Archive.launch(archiveContext))
        {
            Objects.requireNonNull(driver);
            Objects.requireNonNull(archive);

            ArchiveTool.describe(new PrintStream(new ByteArrayOutputStream()), archiveDir);
            assertEquals(
                archive.context().archiveDir().getAbsolutePath(),
                archive.context().archiveMarkFile().archiveDirectory());
        }
    }

    @SuppressWarnings("checkstyle:MethodName")
    ArchiveMarkFile createArchiveMarkFile3_1_0(final File archiveDir)
    {
        final Path markFile = archiveDir.toPath().resolve(ArchiveMarkFile.FILENAME);
        final SystemEpochClock epochClock = new SystemEpochClock();

        try (FileChannel channel = FileChannel.open(markFile, CREATE_NEW, READ, WRITE, SPARSE))
        {
            final MappedByteBuffer mappedByteBuffer = channel.map(READ_WRITE, 0, 1 << 20);
            mappedByteBuffer.order(LITTLE_ENDIAN);

            final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder()
                .wrap(new UnsafeBuffer(mappedByteBuffer), 0);

            headerEncoder
                .version(SemanticVersion.compose(3, 1, 0))
                .startTimestamp(epochClock.time())
                .activityTimestamp(epochClock.time())
                .controlStreamId(1)
                .localControlStreamId(1)
                .eventsStreamId(1)
                .headerLength(8192)
                .errorBufferLength(8192)
                .controlChannel("ctx.controlChannel()")
                .localControlChannel("ctx.localControlChannel()")
                .eventsChannel("ctx.recordingEventsChannel()")
                .aeronDirectory("ctx.aeronDirectoryName()");
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }

        return new ArchiveMarkFile(
            archiveDir,
            ArchiveMarkFile.FILENAME,
            epochClock,
            TimeUnit.SECONDS.toMillis(5),
            (version) -> {},
            null);
    }
}
