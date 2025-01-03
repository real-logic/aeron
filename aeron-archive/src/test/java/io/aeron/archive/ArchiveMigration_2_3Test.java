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

import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.List;

import static io.aeron.archive.ArchiveMigrationUtils.*;
import static io.aeron.archive.MigrationUtils.createMigrationTimestampFile;
import static io.aeron.archive.MigrationUtils.migrationTimestampFileName;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.size;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.agrona.BitUtil.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ArchiveMigration_2_3Test
{
    private static final byte INVALID = (byte)0;
    private static final byte VALID = (byte)1;

    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalErr = System.err;
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final CachedEpochClock clock = new CachedEpochClock();

    private final ArchiveMigration_2_3 migration = new ArchiveMigration_2_3();

    @BeforeEach
    void before()
    {
        clock.update(1);
        System.setErr(new PrintStream(errContent));
    }

    @AfterEach
    void after()
    {
        System.setErr(originalErr);
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void minimumVersion()
    {
        assertEquals(SemanticVersion.compose(3, 0, 0), migration.minimumVersion());
    }

    @Test
    void shouldThrowExceptionIfMigrationTimestampFileAlreadyExists() throws IOException
    {
        try (ArchiveMarkFile markFile = createArchiveMarkFileInVersion2(archiveDir, clock);
            Catalog catalog = createCatalogInVersion2(archiveDir, clock, emptyList());
            FileChannel timestampFile = createMigrationTimestampFile(
                archiveDir, markFile.decoder().version(), migration.minimumVersion()))
        {
            assertNotNull(timestampFile);

            final FileAlreadyExistsException exception = assertThrows(
                FileAlreadyExistsException.class, () -> migration.migrate(System.out, markFile, catalog, archiveDir));
            assertThat(
                exception.getMessage(),
                containsString(migrationTimestampFileName(VERSION_2_1_0, migration.minimumVersion())));
        }
    }

    @Test
    void migrateEmptyCatalogFile() throws IOException
    {
        final Path catalogFile = archiveDir.toPath().resolve(Archive.Configuration.CATALOG_FILE_NAME);

        try (ArchiveMarkFile markFile = createArchiveMarkFileInVersion2(archiveDir, clock);
            Catalog catalog = createCatalogInVersion2(archiveDir, clock, emptyList()))
        {
            assertEquals(RECORDING_FRAME_LENGTH_V2, size(catalogFile));

            migration.migrate(System.out, markFile, catalog, archiveDir);

            assertEquals(migration.minimumVersion(), markFile.decoder().version());

            assertEquals(32, size(catalogFile));
            assertTrue(catalog.isClosed());

            verifyCatalogHeader(catalogFile, 0);
        }

        final String migrationTimestampFileName =
            migrationTimestampFileName(VERSION_2_1_0, migration.minimumVersion());
        assertTrue(exists(archiveDir.toPath().resolve(migrationTimestampFileName)));
    }

    @Test
    @SuppressWarnings("methodLength")
    void migrateRemovesGapsBetweenRecordings() throws IOException
    {
        final Path catalogFile = archiveDir.toPath().resolve(Archive.Configuration.CATALOG_FILE_NAME);

        final List<RecordingDescriptorV2> recordings = asList(
            new RecordingDescriptorV2(
            VALID,
            42,
            21,
            0,
            123,
            456,
            0,
            1024,
            3,
            500,
            1024,
            1408,
            8,
            55,
            "strCh",
            "aeron:udp?endpoint=localhost:9090",
            "sourceA"),
            new RecordingDescriptorV2(
            VALID,
            41,
            14,
            4,
            400,
            4000,
            44,
            444,
            4,
            44,
            44444,
            4000,
            44,
            4040,
            "ch1024",
            "aeron:udp?endpoint=localhost:44444",
            generateStringWithSuffix("source", "B", 854)),
            new RecordingDescriptorV2(
            INVALID,
            333,
            333,
            56,
            100,
            200,
            999,
            100999,
            8,
            2048,
            4096,
            9000,
            -10,
            1,
            "ch",
            "channel",
            "sourceC")
        );

        try (ArchiveMarkFile markFile = createArchiveMarkFileInVersion2(archiveDir, clock);
            Catalog catalog = createCatalogInVersion2(archiveDir, clock, recordings))
        {
            assertEquals(RECORDING_FRAME_LENGTH_V2 * 4, size(catalogFile));

            migration.migrate(System.out, markFile, catalog, archiveDir);

            assertEquals(migration.minimumVersion(), markFile.decoder().version());

            assertEquals(1440, size(catalogFile));
            assertTrue(catalog.isClosed());

            verifyCatalogHeader(catalogFile, 57);
        }

        final String migrationTimestampFileName =
            migrationTimestampFileName(VERSION_2_1_0, migration.minimumVersion());
        assertTrue(exists(archiveDir.toPath().resolve(migrationTimestampFileName)));

        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, clock, null, null))
        {
            final MutableInteger index = new MutableInteger();
            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    final RecordingDescriptorV2 recording = recordings.get(index.getAndIncrement());

                    assertNotEquals(RECORDING_FRAME_LENGTH_V2, headerDecoder.length());
                    assertEquals(recording.valid, headerDecoder.state().value());

                    assertEquals(recording.controlSessionId, descriptorDecoder.controlSessionId());
                    assertEquals(recording.correlationId, descriptorDecoder.correlationId());
                    assertEquals(recording.recordingId, descriptorDecoder.recordingId());
                    assertEquals(recording.startTimestamp, descriptorDecoder.startTimestamp());
                    assertEquals(recording.stopTimestamp, descriptorDecoder.stopTimestamp());
                    assertEquals(recording.startPosition, descriptorDecoder.startPosition());
                    assertEquals(recording.stopPosition, descriptorDecoder.stopPosition());
                    assertEquals(recording.initialTermId, descriptorDecoder.initialTermId());
                    assertEquals(recording.segmentFileLength, descriptorDecoder.segmentFileLength());
                    assertEquals(recording.termBufferLength, descriptorDecoder.termBufferLength());
                    assertEquals(recording.mtuLength, descriptorDecoder.mtuLength());
                    assertEquals(recording.sessionId, descriptorDecoder.sessionId());
                    assertEquals(recording.streamId, descriptorDecoder.streamId());
                    assertEquals(recording.strippedChannel, descriptorDecoder.strippedChannel());
                    assertEquals(recording.originalChannel, descriptorDecoder.originalChannel());
                    assertEquals(recording.sourceIdentity, descriptorDecoder.sourceIdentity());
                });

            assertEquals(recordings.size(), index.get());
        }
    }

    private void verifyCatalogHeader(final Path catalogFile, final long nextRecordingId) throws IOException
    {
        final int catalogHeaderLength = 32;
        try (FileChannel channel = FileChannel.open(catalogFile, READ))
        {
            final MappedByteBuffer mappedByteBuffer = channel.map(READ_ONLY, 0, catalogHeaderLength);
            mappedByteBuffer.order(LITTLE_ENDIAN);
            try
            {
                final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);
                int index = 0;

                // version
                assertEquals(migration.minimumVersion(), buffer.getInt(index, LITTLE_ENDIAN));
                index += SIZE_OF_INT;

                // length
                assertEquals(catalogHeaderLength, buffer.getInt(index, LITTLE_ENDIAN));
                index += SIZE_OF_INT;

                // nextRecordingId
                assertEquals(nextRecordingId, buffer.getLong(index, LITTLE_ENDIAN));
                index += SIZE_OF_LONG;

                // alignment
                assertEquals(CACHE_LINE_LENGTH, buffer.getInt(index, LITTLE_ENDIAN));
            }
            finally
            {
                BufferUtil.free(mappedByteBuffer);
            }
        }
    }
}
