/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.checksum.Checksums.crc32;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.StandardOpenOption.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class CatalogTest
{
    private static final long MAX_ENTRIES = 1024;
    private static final int TERM_LENGTH = 2 * Catalog.PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final File archiveDir = ArchiveTests.makeTestDirectory();

    private long currentTimeMs = 1;
    private final EpochClock clock = () -> currentTimeMs;

    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;

    @BeforeEach
    void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            recordingOneId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3, "channelK", "channelK?tag=f", "sourceB");
        }
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void shouldReloadExistingIndex()
    {
        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            verifyRecordingForId(catalog, recordingOneId, 6, 1, "channelG", "sourceA");
            verifyRecordingForId(catalog, recordingTwoId, 7, 2, "channelH", "sourceV");
            verifyRecordingForId(catalog, recordingThreeId, 8, 3, "channelK", "sourceB");
        }
    }

    private void verifyRecordingForId(
        final Catalog catalog,
        final long id,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        assertTrue(catalog.wrapDescriptor(id, unsafeBuffer));

        recordingDescriptorDecoder.wrap(
            unsafeBuffer,
            RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        assertEquals(id, recordingDescriptorDecoder.recordingId());
        assertEquals(sessionId, recordingDescriptorDecoder.sessionId());
        assertEquals(streamId, recordingDescriptorDecoder.streamId());
        assertEquals(strippedChannel, recordingDescriptorDecoder.strippedChannel());
        assertEquals(strippedChannel + "?tag=f", recordingDescriptorDecoder.originalChannel());
        assertEquals(sourceIdentity, recordingDescriptorDecoder.sourceIdentity());
    }

    @Test
    void shouldAppendToExistingIndex()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, () -> 3L, null, null))
        {
            newRecordingId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 4, "channelJ", "channelJ?tag=f", "sourceN");
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            verifyRecordingForId(catalog, recordingOneId, 6, 1, "channelG", "sourceA");
            verifyRecordingForId(catalog, newRecordingId, 9, 4, "channelJ", "sourceN");
        }
    }

    @Test
    void shouldAllowMultipleInstancesForSameStream()
    {
        try (Catalog ignore = new Catalog(archiveDir, clock))
        {
            final long newRecordingId = newRecording();
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }

    @Test
    void shouldIncreaseMaxEntries()
    {
        final long newMaxEntries = MAX_ENTRIES * 2;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, clock, null, null))
        {
            assertEquals(newMaxEntries, catalog.maxEntries());
        }
    }

    @Test
    void shouldNotDecreaseMaxEntries()
    {
        final long newMaxEntries = 1;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, clock, null, null))
        {
            assertEquals(MAX_ENTRIES, catalog.maxEntries());
        }
    }

    @Test
    void shouldFixTimestampForEmptyRecordingAfterFailure()
    {
        final long newRecordingId = newRecording();

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            final Catalog.CatalogEntryProcessor entryProcessor =
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                assertEquals(NULL_TIMESTAMP, descriptorDecoder.stopTimestamp());

            assertTrue(catalog.forEntry(newRecordingId, entryProcessor));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            final Catalog.CatalogEntryProcessor entryProcessor =
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                assertEquals(42L, descriptorDecoder.stopTimestamp());

            assertTrue(catalog.forEntry(newRecordingId, entryProcessor));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void shouldFixTimestampAndPositionAfterFailureSamePage() throws Exception
    {
        final long newRecordingId = newRecording();

        new File(archiveDir, segmentFileName(newRecordingId, 0)).createNewFile();
        new File(archiveDir, segmentFileName(newRecordingId, SEGMENT_LENGTH)).createNewFile();
        new File(archiveDir, segmentFileName(newRecordingId, 2 * SEGMENT_LENGTH)).createNewFile();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 3 * SEGMENT_LENGTH));

        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(1024);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, 1024);

            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, 1024 + 128);
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(NULL_TIMESTAMP, descriptorDecoder.stopTimestamp());
                    assertEquals(NULL_POSITION, descriptorDecoder.stopPosition());
                }));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(42L, descriptorDecoder.stopTimestamp());
                    assertEquals(SEGMENT_LENGTH * 3 + 1024L + 128L, descriptorDecoder.stopPosition());
                }));
        }
    }

    @Test
    void shouldThrowExceptionAfterFailureOnPageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(256);
            log.write(bb, PAGE_SIZE - 128);

            bb.clear();
            bb.put(0, (byte)0).limit(1).position(0);
            log.write(bb, PAGE_SIZE + 127);
        }

        final ArchiveException exception = assertThrows(
            ArchiveException.class,
            () ->
            {
                final Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null);
                catalog.close();
            });
        assertThat(exception.getMessage(), containsString(segmentFile.getAbsolutePath()));
    }

    @Test
    void shouldUseChecksumToVerifyLastFragmentAfterPageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(256);
            flyweight.sessionId(1025596259);
            log.write(bb, PAGE_SIZE - 128);

            bb.clear();
            bb.put(0, (byte)0).limit(1).position(0);
            log.write(bb, PAGE_SIZE + 127);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, crc32(), null))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertEquals(42L, descriptorDecoder.stopTimestamp());
                    assertEquals(PAGE_SIZE + 128, descriptorDecoder.stopPosition());
                }));
        }
    }

    private long newRecording()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            newRecordingId = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                "channelG",
                "channelG?tag=f",
                "sourceA");
        }

        return newRecordingId;
    }

    @Test
    void shouldFixTimestampAndPositionAfterFailureFullSegment() throws Exception
    {
        final long newRecordingId = newRecording();

        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(SEGMENT_LENGTH - 128);
            log.write(bb);

            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, SEGMENT_LENGTH - 128);
            log.truncate(SEGMENT_LENGTH);
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertThat(descriptorDecoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(descriptorDecoder.stopPosition(), is(NULL_POSITION));
                }));
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            assertTrue(catalog.forEntry(
                newRecordingId,
                (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    assertThat(descriptorDecoder.stopTimestamp(), is(42L));
                    assertThat(descriptorDecoder.stopPosition(), is((long)SEGMENT_LENGTH));
                }));
        }
    }

    @Test
    void shouldBeAbleToCreateMaxEntries()
    {
        after();
        final File archiveDir = ArchiveTests.makeTestDirectory();
        final long maxEntries = 2;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, maxEntries, clock, null, null))
        {
            for (int i = 0; i < maxEntries; i++)
            {
                recordingOneId = catalog.addNewRecording(
                    0L,
                    0L,
                    0,
                    SEGMENT_LENGTH,
                    TERM_LENGTH,
                    MTU_LENGTH,
                    6,
                    1,
                    "channelG",
                    "channelG?tag=f",
                    "sourceA");
            }
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(maxEntries, catalog.countEntries());
        }
    }

    @Test
    void shouldGrowCatalogWhenMaxEntriesReached()
    {
        after();
        final File archiveDir = ArchiveTests.makeTestDirectory();
        final long maxEntries = 10;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, maxEntries, clock, null, null))
        {
            for (int i = 0; i < maxEntries + 3; i++)
            {
                recordingOneId = catalog.addNewRecording(
                    0L,
                    0L,
                    0,
                    SEGMENT_LENGTH,
                    TERM_LENGTH,
                    MTU_LENGTH,
                    6,
                    1,
                    "channelG",
                    "channelG?tag=f",
                    "sourceA");
            }
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            assertEquals(maxEntries + 3, catalog.countEntries());
            assertEquals(maxEntries + (maxEntries >> 1), catalog.maxEntries());
        }
    }

    @Test
    void growCatalogThrowsExceptionIfCannotGrowToAccommodateAtLeastOneRecord()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            final long maxCatalogLength = calculateCatalogLength(MAX_ENTRIES) + 1;

            final ArchiveException exception =
                assertThrows(ArchiveException.class, () -> catalog.growCatalog(maxCatalogLength));
            assertEquals("ERROR - catalog is full, max length reached: " + maxCatalogLength, exception.getMessage());
        }
    }

    @Test
    void shouldNotThrowWhenOldRecordingLogsAreDeleted() throws IOException
    {
        final File segmentFile = new File(archiveDir, segmentFileName(recordingThreeId, SEGMENT_LENGTH * 2));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocate(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(256);
            log.write(bb);
        }

        final Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null);
        catalog.close();
    }

    @Test
    void shouldContainChannelFragment()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock, null, null))
        {
            final String originalChannel = "aeron:udp?endpoint=localhost:7777|tags=777|alias=TestString";
            final String strippedChannel = "strippedChannelUri";
            final long recordingId = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_LENGTH,
                TERM_LENGTH,
                MTU_LENGTH,
                6,
                1,
                strippedChannel,
                originalChannel,
                "sourceA");

            assertTrue(catalog.wrapDescriptor(recordingId, unsafeBuffer));

            recordingDescriptorDecoder.wrap(
                unsafeBuffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            assertTrue(Catalog.originalChannelContains(recordingDescriptorDecoder, ArrayUtil.EMPTY_BYTE_ARRAY));

            final byte[] originalChannelBytes = originalChannel.getBytes(StandardCharsets.US_ASCII);
            assertTrue(Catalog.originalChannelContains(recordingDescriptorDecoder, originalChannelBytes));

            final byte[] tagsBytes = "tags=777".getBytes(StandardCharsets.US_ASCII);
            assertTrue(Catalog.originalChannelContains(recordingDescriptorDecoder, tagsBytes));

            final byte[] testBytes = "TestString".getBytes(StandardCharsets.US_ASCII);
            assertTrue(Catalog.originalChannelContains(recordingDescriptorDecoder, testBytes));

            final byte[] wrongBytes = "wrong".getBytes(StandardCharsets.US_ASCII);
            assertFalse(Catalog.originalChannelContains(recordingDescriptorDecoder, wrongBytes));
        }
    }

    private static Stream<Arguments> pageBoundaryTestData()
    {
        return Stream.of(
            Arguments.of(0, 64, false),
            Arguments.of(100, 300, false),
            Arguments.of(0, PAGE_SIZE, false),
            Arguments.of(1, PAGE_SIZE - 1, false),
            Arguments.of(PAGE_SIZE - 1, 1, false),
            Arguments.of(PAGE_SIZE * 7 + 32, 256, false),
            Arguments.of(PAGE_SIZE * 3 + 111, PAGE_SIZE - 111, false),
            Arguments.of(0, PAGE_SIZE + 1, true),
            Arguments.of(PAGE_SIZE - 1, 2, true),
            Arguments.of(PAGE_SIZE * 4 + 11, PAGE_SIZE - 10, true));
    }

    @ParameterizedTest(name = "fragmentCrossesPageBoundary({0}, {1}, {2})")
    @MethodSource("pageBoundaryTestData")
    void detectPageBoundaryStraddle(final int fragmentOffset, final int fragmentLength, final boolean expected)
    {
        assertEquals(expected, fragmentStraddlesPageBoundary(fragmentOffset, fragmentLength));
    }
}
