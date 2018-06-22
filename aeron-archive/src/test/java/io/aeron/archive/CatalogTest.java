/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.PAGE_SIZE;
import static io.aeron.archive.Catalog.wrapDescriptorDecoder;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

public class CatalogTest
{
    private static final long MAX_ENTRIES = 1024;
    private static final int TERM_LENGTH = 2 * Catalog.PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final File archiveDir = TestUtil.makeTestDirectory();

    private long currentTimeMs = 1;
    private final EpochClock clock = () -> currentTimeMs;

    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;

    @Before
    public void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            recordingOneId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3, "channelK", "channelK?tag=f", "sourceB");
        }
    }

    @After
    public void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void shouldReloadExistingIndex()
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

        wrapDescriptorDecoder(recordingDescriptorDecoder, unsafeBuffer);

        assertEquals(id, recordingDescriptorDecoder.recordingId());
        assertEquals(sessionId, recordingDescriptorDecoder.sessionId());
        assertEquals(streamId, recordingDescriptorDecoder.streamId());
        assertEquals(strippedChannel, recordingDescriptorDecoder.strippedChannel());
        assertEquals(strippedChannel + "?tag=f", recordingDescriptorDecoder.originalChannel());
        assertEquals(sourceIdentity, recordingDescriptorDecoder.sourceIdentity());
    }

    @Test
    public void shouldAppendToExistingIndex()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, () -> 3L))
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
    public void shouldAllowMultipleInstancesForSameStream()
    {
        try (Catalog ignore = new Catalog(archiveDir, clock))
        {
            final long newRecordingId = newRecording();
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }

    @Test
    public void shouldIncreaseMaxEntries()
    {
        final long newMaxEntries = MAX_ENTRIES * 2;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, clock))
        {
            assertEquals(newMaxEntries, catalog.maxEntries());
        }
    }

    @Test
    public void shouldNotDecreaseMaxEntries()
    {
        final long newMaxEntries = 1;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, clock))
        {
            assertEquals(MAX_ENTRIES, catalog.maxEntries());
        }
    }

    @Test
    public void shouldFixTimestampForEmptyRecordingAfterFailure()
    {
        final long newRecordingId = newRecording();

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) -> assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP)), newRecordingId);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) -> assertThat(decoder.stopTimestamp(), is(42L)), newRecordingId);
        }
    }

    @Test
    public void shouldFixTimestampAndPositionAfterFailureSamePage() throws Exception
    {
        final long newRecordingId = newRecording();

        new File(archiveDir, segmentFileName(newRecordingId, 0)).createNewFile();
        new File(archiveDir, segmentFileName(newRecordingId, 1)).createNewFile();
        new File(archiveDir, segmentFileName(newRecordingId, 2)).createNewFile();
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 3));

        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
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
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(decoder.stopPosition(), is(NULL_POSITION));
                },
                newRecordingId);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(42L));
                    assertThat(decoder.stopPosition(), is(SEGMENT_LENGTH * 3 + 1024L + 128L));
                },
                newRecordingId);
        }
    }

    @Test
    public void shouldFixTimestampAndPositionAfterFailurePageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();

        createSegmentFile(newRecordingId);

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(decoder.stopPosition(), is(NULL_POSITION));
                },
                newRecordingId);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            assertTrue(catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(42L));
                    assertThat(decoder.stopPosition(), is((long)PAGE_SIZE - HEADER_LENGTH));
                },
                newRecordingId));
        }
    }

    private long newRecording()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
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
    public void shouldFixTimestampAndPositionAfterFailureFullSegment() throws Exception
    {
        final long newRecordingId = newRecording();
        final long expectedLastFrame = SEGMENT_LENGTH - 128;

        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength((int)expectedLastFrame);
            log.write(bb);
            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, expectedLastFrame);
            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, expectedLastFrame + 128);
            log.truncate(SEGMENT_LENGTH);
        }

        try (Catalog catalog = new Catalog(archiveDir, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    e.stopPosition(NULL_POSITION);
                },
                newRecordingId);
        }

        currentTimeMs = 42L;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(42L));
                    assertThat(decoder.stopPosition(), is(expectedLastFrame));
                },
                newRecordingId);
        }
    }

    @Test
    public void shouldBeAbleToCreateMaxEntries()
    {
        after();
        final File archiveDir = TestUtil.makeTestDirectory();
        final long maxEntries = 2;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, maxEntries, clock))
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

        try (Catalog catalog = new Catalog(archiveDir, null, 0, maxEntries, clock))
        {
            assertEquals(maxEntries, catalog.countEntries());
        }
    }

    @Test
    public void shouldNotThrowWhenOldRecordingLogsAreDeleted() throws IOException
    {
        createSegmentFile(recordingThreeId);

        final Path segmentFilePath = Paths.get(segmentFileName(recordingThreeId, 0));
        final boolean segmentFileExists = Files.exists(archiveDir.toPath().resolve(segmentFilePath));
        assumeThat(segmentFileExists, is(true));

        final Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock);
        catalog.close();
    }

    private void createSegmentFile(final long newRecordingId) throws IOException
    {
        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - HEADER_LENGTH);
            log.write(bb);
            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, PAGE_SIZE - HEADER_LENGTH);
            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, PAGE_SIZE - HEADER_LENGTH + 128);
        }
    }
}
