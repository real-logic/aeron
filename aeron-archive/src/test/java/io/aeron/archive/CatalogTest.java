/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CatalogTest
{
    private static final int TERM_BUFFER_LENGTH = 2 * Catalog.PAGE_SIZE;
    private static final int SEGMENT_FILE_SIZE = 2 * TERM_BUFFER_LENGTH;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final File archiveDir = TestUtil.makeTempDir();
    private final EpochClock clock = mock(EpochClock.class);
    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;

    @Before
    public void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            recordingOneId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_FILE_SIZE, TERM_BUFFER_LENGTH, 1024, 6, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_FILE_SIZE, TERM_BUFFER_LENGTH, 1024, 7, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_FILE_SIZE, TERM_BUFFER_LENGTH, 1024, 8, 3, "channelK", "channelK?tag=f", "sourceB");
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
        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
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
        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            newRecordingId = catalog.addNewRecording(
                0L, 0L, 0, SEGMENT_FILE_SIZE, TERM_BUFFER_LENGTH, 1024, 9, 4, "channelJ", "channelJ?tag=f", "sourceN");
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            verifyRecordingForId(catalog, recordingOneId, 6, 1, "channelG", "sourceA");
            verifyRecordingForId(catalog, newRecordingId, 9, 4, "channelJ", "sourceN");
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream()
    {
        try (Catalog ignore = new Catalog(archiveDir, null, 0, clock))
        {
            final long newRecordingId = newRecording();
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }

    @Test
    public void shouldFixTimestampForEmptyRecordingAfterFailure()
    {
        final long newRecordingId = newRecording();

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock, false))
        {
            catalog.forEntry(
                (he, hd, e, decoder) -> assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP)), newRecordingId);
        }

        when(clock.time()).thenReturn(42L);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
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
            final ByteBuffer bb = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
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

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock, false))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(decoder.stopPosition(), is(NULL_POSITION));
                },
                newRecordingId);
        }

        when(clock.time()).thenReturn(42L);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(42L));
                    assertThat(decoder.stopPosition(), is(SEGMENT_FILE_SIZE * 3 + 1024L + 128L));
                },
                newRecordingId);
        }
    }

    @Test
    public void shouldFixTimestampAndPositionAfterFailurePageStraddle() throws Exception
    {
        final long newRecordingId = newRecording();

        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {
            final ByteBuffer bb = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength(PAGE_SIZE - 32);
            log.write(bb);
            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, PAGE_SIZE - 32);
            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, PAGE_SIZE - 32 + 128);
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock, false))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    assertThat(decoder.stopPosition(), is(NULL_POSITION));
                },
                newRecordingId);
        }

        when(clock.time()).thenReturn(42L);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            assertTrue(catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(42L));
                    assertThat(decoder.stopPosition(), is((long)PAGE_SIZE - 32));
                },
                newRecordingId));
        }
    }

    private long newRecording()
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
        {
            newRecordingId = catalog.addNewRecording(
                0L,
                0L,
                0,
                SEGMENT_FILE_SIZE,
                TERM_BUFFER_LENGTH,
                1024,
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
        final long expectedLastFrame = SEGMENT_FILE_SIZE - 128;

        final File segmentFile = new File(archiveDir, segmentFileName(newRecordingId, 0));
        try (FileChannel log = FileChannel.open(segmentFile.toPath(), READ, WRITE, CREATE))
        {

            final ByteBuffer bb = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
            final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
            flyweight.frameLength((int)expectedLastFrame);
            log.write(bb);
            bb.clear();
            flyweight.frameLength(128);
            log.write(bb, expectedLastFrame);
            bb.clear();
            flyweight.frameLength(0);
            log.write(bb, expectedLastFrame + 128);
            log.truncate(SEGMENT_FILE_SIZE);
        }

        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock, false))
        {
            catalog.forEntry(
                (he, hd, e, decoder) ->
                {
                    assertThat(decoder.stopTimestamp(), is(NULL_TIMESTAMP));
                    e.stopPosition(NULL_POSITION);
                },
                newRecordingId);
        }

        when(clock.time()).thenReturn(42L);
        try (Catalog catalog = new Catalog(archiveDir, null, 0, clock))
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
}
