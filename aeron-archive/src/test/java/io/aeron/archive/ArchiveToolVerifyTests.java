/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.exceptions.AeronException;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.ArchiveTool.*;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class ArchiveToolVerifyTests
{
    private static final int TERM_LENGTH = 16 * PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private long record0;
    private long record1;
    private long record2;
    private long record3;
    private long record4;
    private long record5;
    private long record6;
    private long record7;
    private long record8;
    private long record9;
    private long record10;
    private long record11;
    private long record12;
    private long record13;
    private long record14;
    private long record15;
    private long record16;
    private long record17;
    private long record18;
    private long record19;
    private long record20;
    private long record21;
    private long record22;
    private long record23;
    private long record24;
    private long record25;
    private long currentTimeMillis = 0;

    private final EpochClock epochClock = () -> currentTimeMillis += 100;
    private final PrintStream out = mock(PrintStream.class);
    private File archiveDir;

    @SuppressWarnings("MethodLength")
    @BeforeEach
    void before() throws IOException
    {
        archiveDir = TestUtil.makeTestDirectory();

        try (Catalog catalog = new Catalog(archiveDir, null, 0, 128, epochClock))
        {
            record0 = catalog.addNewRecording(NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "emptyChannel", "emptyChannel?tag=X", "source1");
            record1 = catalog.addNewRecording(11, NULL_POSITION, 10, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "emptyChannel", "emptyChannel?tag=X", "source1");
            record2 = catalog.addNewRecording(22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record3 = catalog.addNewRecording(33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record4 = catalog.addNewRecording(44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record5 = catalog.addNewRecording(55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record6 = catalog.addNewRecording(66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record7 = catalog.addNewRecording(0, NULL_POSITION, 70, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record8 = catalog.addNewRecording(TERM_LENGTH + 1024, NULL_POSITION, 80, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record9 = catalog.addNewRecording(2048, NULL_POSITION, 90, NULL_TIMESTAMP, 5,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record10 = catalog.addNewRecording(0, NULL_POSITION, 100, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record11 = catalog.addNewRecording(0, NULL_POSITION, 110, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record12 = catalog.addNewRecording(0, 0, 120, 999999, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record13 = catalog.addNewRecording(1024 * 1024, SEGMENT_LENGTH * 128 + PAGE_SIZE * 3, 130, 888888, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record14 = catalog.addNewRecording(0, 14, 140, 140, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record15 = catalog.addNewRecording(PAGE_SIZE * 5 + 1024, PAGE_SIZE * 5 + 1024, 150, 160, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record16 = catalog.addNewRecording(0, NULL_POSITION, 160, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record17 = catalog.addNewRecording(212 * 1024 * 1024, NULL_POSITION, 170, NULL_TIMESTAMP, 13,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record18 = catalog.addNewRecording(8224, NULL_POSITION, 180, NULL_TIMESTAMP, 16,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record19 = catalog.addNewRecording(SEGMENT_LENGTH + 2048, NULL_POSITION, 190, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record20 = catalog.addNewRecording(0, SEGMENT_LENGTH * 10, 200, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record21 = catalog.addNewRecording(1000, 800, 210, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record22 = catalog.addNewRecording(SEGMENT_LENGTH * 5, SEGMENT_LENGTH * 20, 220, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "emptyChannel", "emptyChannel?tag=X", "source1");
            record23 = catalog
                .addNewRecording(SEGMENT_LENGTH * 3 + 1024, SEGMENT_LENGTH * 3 + 2048, 230, NULL_TIMESTAMP, 0,
                    SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record24 = catalog.addNewRecording(-123, NULL_POSITION, 240, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record25 = catalog.addNewRecording(TERM_LENGTH + 1024, NULL_POSITION, 250, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
        }

        writeToSegmentFile(
            createFile(record1 + "-0.dat"),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.streamId(Integer.MAX_VALUE);
                fileChannel.write(byteBuffer);
            });

        createFile(record2 + "-" + RECORDING_SEGMENT_SUFFIX); // ERR: no segment position
        createFile(record3 + "-" + "invalid_position" + RECORDING_SEGMENT_SUFFIX); // ERR: invalid position
        createFile(segmentFileName(record4, -111)); // ERR: negative position
        createFile(segmentFileName(record5, 0)); // ERR: empty file
        createDirectory(segmentFileName(record6, 0)); // ERR: directory

        writeToSegmentFile(
            createFile(segmentFileName(record7, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(record8, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(TERM_LENGTH + 1024);
                dataHeaderFlyweight.streamId(-1); // broken frame will not be checked due to startPosition
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(100);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer, TERM_LENGTH + 1024);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record9, SEGMENT_LENGTH)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(TERM_LENGTH + 100);
                dataHeaderFlyweight.termId(7);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termId(8);
                dataHeaderFlyweight.termOffset(128);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer, TERM_LENGTH + 128);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record10, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(PAGE_SIZE - 64);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.termOffset(PAGE_SIZE - 64);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer, PAGE_SIZE - 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record11, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(PAGE_SIZE - 64);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termOffset(PAGE_SIZE - 64);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer, PAGE_SIZE - 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record12, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(createFile(segmentFileName(record13, SEGMENT_LENGTH * 128)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(PAGE_SIZE * 3);
                dataHeaderFlyweight.termId(256);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record14, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(record15, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(111);
                dataHeaderFlyweight.streamId(-1);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record15, SEGMENT_LENGTH * 2)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(1000);
                dataHeaderFlyweight.termId(4);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record16, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(101010);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record17, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.termOffset(101010);
                dataHeaderFlyweight.termId(13);
                dataHeaderFlyweight.streamId(2);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record18, SEGMENT_LENGTH * 5)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(101010);
                dataHeaderFlyweight.streamId(2);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record19, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(record20, SEGMENT_LENGTH * 8)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(record23, SEGMENT_LENGTH * 3)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(PAGE_SIZE);
                dataHeaderFlyweight.termId(6);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.streamId(3);
                fileChannel.write(byteBuffer, 1024);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termOffset(1024 + PAGE_SIZE);
                fileChannel.write(byteBuffer, 1024 + PAGE_SIZE);
            });

        writeToSegmentFile(
            createFile(segmentFileName(record25, 0)),
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.frameLength(100);
                dataHeaderFlyweight.termOffset(-1);
                dataHeaderFlyweight.termId(-1);
                dataHeaderFlyweight.streamId(-1);
                fileChannel.write(byteBuffer, TERM_LENGTH + 1024);
            });
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void verifyCheckLastFile()
    {
        verify(out, archiveDir, false, epochClock, (file) -> file.getName().startsWith(Long.toString(record10)));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record0, INVALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record1, VALID, 11, 11, 10, 100, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record7, VALID, 0, 0, 70, 200, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record8, VALID, TERM_LENGTH + 1024, TERM_LENGTH + 1024 + 128, 80, 300, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record9, VALID, 2048, SEGMENT_LENGTH + TERM_LENGTH + 384, 90, 400, 5,
                3, "validChannel", "source3");
            assertRecording(catalog, record10, VALID, 0, PAGE_SIZE - 64, 100, 500, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record11, VALID, 0, PAGE_SIZE + 192, 110, 600, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record12, VALID, 0, 0, 120, 999999, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record13, VALID, 1024 * 1024, SEGMENT_LENGTH * 128 + PAGE_SIZE * 3, 130, 888888, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record14, VALID, 0, 0, 140, 700, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record15, VALID, PAGE_SIZE * 5 + 1024, SEGMENT_LENGTH * 2 + 1024, 150, 800, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record16, INVALID, 0, NULL_POSITION, 160, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record17, INVALID, 212 * 1024 * 1024, NULL_POSITION, 170, NULL_TIMESTAMP, 13,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record18, INVALID, 8224, NULL_POSITION, 180, NULL_TIMESTAMP, 16,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record19, INVALID, SEGMENT_LENGTH + 2048, NULL_POSITION, 190, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record20, INVALID, 0, SEGMENT_LENGTH * 10, 200, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record21, INVALID, 1000, 800, 210, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record22, VALID, SEGMENT_LENGTH * 5, SEGMENT_LENGTH * 5, 220, 900, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record23, VALID, SEGMENT_LENGTH * 3 + 1024,
                SEGMENT_LENGTH * 3 + 1024 + PAGE_SIZE + 256, 230, 1000, 0, 3, "validChannel", "source3");
            assertRecording(catalog, record24, INVALID, -123, NULL_POSITION, 240, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record25, INVALID, TERM_LENGTH + 1024, NULL_POSITION, 250, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }

        Mockito.verify(out, times(26)).println(any(String.class));
    }

    @Test
    void verifyCheckAllFiles()
    {
        verify(out, archiveDir, true, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record0, INVALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record1, VALID, 11, 11, 10, 100, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record7, VALID, 0, 0, 70, 200, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record8, VALID, TERM_LENGTH + 1024, TERM_LENGTH + 1024 + 128, 80, 300, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record9, VALID, 2048, SEGMENT_LENGTH + TERM_LENGTH + 384, 90, 400, 5,
                3, "validChannel", "source3");
            assertRecording(catalog, record10, VALID, 0, PAGE_SIZE + 64, 100, 500, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record11, VALID, 0, PAGE_SIZE + 192, 110, 600, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record12, VALID, 0, 0, 120, 999999, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record13, VALID, 1024 * 1024, SEGMENT_LENGTH * 128 + PAGE_SIZE * 3, 130, 888888, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record14, VALID, 0, 0, 140, 700, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record15, INVALID, PAGE_SIZE * 5 + 1024, PAGE_SIZE * 5 + 1024, 150, 160, 0,
                3, "validChannel", "source3");
            assertRecording(catalog, record16, INVALID, 0, NULL_POSITION, 160, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record17, INVALID, 212 * 1024 * 1024, NULL_POSITION, 170, NULL_TIMESTAMP, 13,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record18, INVALID, 8224, NULL_POSITION, 180, NULL_TIMESTAMP, 16,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record19, INVALID, SEGMENT_LENGTH + 2048, NULL_POSITION, 190, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record20, INVALID, 0, SEGMENT_LENGTH * 10, 200, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record21, INVALID, 1000, 800, 210, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record22, VALID, SEGMENT_LENGTH * 5, SEGMENT_LENGTH * 5, 220, 800, 0,
                1, "emptyChannel", "source1");
            assertRecording(catalog, record23, VALID, SEGMENT_LENGTH * 3 + 1024,
                SEGMENT_LENGTH * 3 + 1024 + PAGE_SIZE + 256, 230, 900, 0, 3, "validChannel", "source3");
            assertRecording(catalog, record24, INVALID, -123, NULL_POSITION, 240, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
            assertRecording(catalog, record25, INVALID, TERM_LENGTH + 1024, NULL_POSITION, 250, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }

        Mockito.verify(out, times(26)).println(any(String.class));
    }

    @Test
    void verifyRecordingThrowsAeronExceptionIfNoRecordingFoundWithGivenId()
    {
        assertThrows(
            AeronException.class,
            () -> verifyRecording(out, archiveDir, Long.MIN_VALUE, false, epochClock, (file) -> false));
    }

    @Test
    void verifyRecordingEmptyRecording()
    {
        verifyRecording(out, archiveDir, record1, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record1, VALID, 11, 11, 10, 100, 0,
                1, "emptyChannel", "source1");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileNameNoPositionInfo()
    {
        verifyRecording(out, archiveDir, record2, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileNameInvalidPosition()
    {
        verifyRecording(out, archiveDir, record3, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileNameNegativePosition()
    {
        verifyRecording(out, archiveDir, record4, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileEmptyFile()
    {
        verifyRecording(out, archiveDir, record5, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileDirectory()
    {
        verifyRecording(out, archiveDir, record6, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileWrongStreamId()
    {
        verifyRecording(out, archiveDir, record16, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record16, INVALID, 0, NULL_POSITION, 160, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileWrongTermOffset()
    {
        verifyRecording(out, archiveDir, record17, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record17, INVALID, 212 * 1024 * 1024, NULL_POSITION, 170, NULL_TIMESTAMP, 13,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileWrongTermId()
    {
        verifyRecording(out, archiveDir, record18, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record18, INVALID, 8224, NULL_POSITION, 180, NULL_TIMESTAMP, 16,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingValidateAllFiles()
    {
        verifyRecording(out, archiveDir, record15, true, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record15, INVALID, PAGE_SIZE * 5 + 1024, PAGE_SIZE * 5 + 1024, 150, 160, 0,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingValidateLastFiles()
    {
        verifyRecording(out, archiveDir, record15, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record15, VALID, PAGE_SIZE * 5 + 1024, SEGMENT_LENGTH * 2 + 1024, 150, 100, 0,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingTruncateFileOnPageStraddle()
    {
        verifyRecording(out, archiveDir, record10, false, epochClock, (file) -> true);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record10, VALID, 0, PAGE_SIZE - 64, 100, 100, 0,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingDoNotTruncateFileOnPageStraddle()
    {
        verifyRecording(out, archiveDir, record10, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record10, VALID, 0, PAGE_SIZE + 64, 100, 100, 0,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingInvalidStartPositionNull()
    {
        verifyRecording(out, archiveDir, record0, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record0, INVALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                1, "emptyChannel", "source1");
        }
    }

    @Test
    void verifyRecordingInvalidStartPositionNegative()
    {
        verifyRecording(out, archiveDir, record24, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record24, INVALID, -123, NULL_POSITION, 240, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidStartPositionBeyondMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, record19, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record19, INVALID, SEGMENT_LENGTH + 2048, NULL_POSITION, 190, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidStopPositionBeyondMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, record20, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record20, INVALID, 0, SEGMENT_LENGTH * 10, 200, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingInvalidStopPositionBeforeStartPosition()
    {
        verifyRecording(out, archiveDir, record21, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record21, INVALID, 1000, 800, 210, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @Test
    void verifyRecordingEmptyRecordingWithBothStartPositionAndStopPositionAssigned()
    {
        verifyRecording(out, archiveDir, record22, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record22, VALID, SEGMENT_LENGTH * 5, SEGMENT_LENGTH * 5, 220, 100, 0,
                1, "emptyChannel", "source1");
        }
    }

    @Test
    void verifyRecordingNonEmptyRecordingWithNonZeroStartPosition()
    {
        verifyRecording(out, archiveDir, record8, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record8, VALID, TERM_LENGTH + 1024, TERM_LENGTH + 1024 + 128, 80, 100, 0,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingNonEmptyRecordingWithNonZeroStartPositionBeforeMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, record9, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record9, VALID, 2048, SEGMENT_LENGTH + TERM_LENGTH + 384, 90, 100, 5,
                3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingNonEmptyRecordingWithNonZeroStartPositionWithinMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, record23, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record23, VALID, SEGMENT_LENGTH * 3 + 1024,
                SEGMENT_LENGTH * 3 + 1024 + PAGE_SIZE + 256, 230, 100, 0, 3, "validChannel", "source3");
        }
    }

    @Test
    void verifyRecordingInvalidSegmentFileWrongHeaderDefinitionNonZeroStartPosition()
    {
        verifyRecording(out, archiveDir, record25, false, epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, record25, INVALID, TERM_LENGTH + 1024, NULL_POSITION, 250, NULL_TIMESTAMP, 0,
                2, "invalidChannel", "source2");
        }
    }

    @FunctionalInterface
    interface SegmentWriter
    {
        void write(ByteBuffer bb, DataHeaderFlyweight flyweight, FileChannel channel) throws IOException;
    }

    private File createFile(final String name) throws IOException
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.createNewFile());
        return file;
    }

    private void createDirectory(final String name)
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.mkdir());
    }

    private void writeToSegmentFile(final File file, final SegmentWriter segmentWriter) throws IOException
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(HEADER_LENGTH);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(byteBuffer);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            byteBuffer.clear();
            segmentWriter.write(byteBuffer, flyweight, channel);
        }
    }

    private void assertRecording(
        final Catalog catalog,
        final long recordingId,
        final byte valid,
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimeStamp,
        final int initialTermId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        assertTrue(catalog.forEntry(
            recordingId,
            (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                assertEquals(valid, headerDecoder.valid());
                assertEquals(startPosition, descriptorDecoder.startPosition());
                assertEquals(stopPosition, descriptorDecoder.stopPosition());
                assertEquals(startTimestamp, descriptorDecoder.startTimestamp());
                assertEquals(stopTimeStamp, descriptorDecoder.stopTimestamp());
                assertEquals(initialTermId, descriptorDecoder.initialTermId());
                assertEquals(MTU_LENGTH, descriptorDecoder.mtuLength());
                assertEquals(SEGMENT_LENGTH, descriptorDecoder.segmentFileLength());
                assertEquals(streamId, descriptorDecoder.streamId());
                assertEquals(strippedChannel, descriptorDecoder.strippedChannel());
                assertNotNull(descriptorDecoder.originalChannel());
                assertEquals(sourceIdentity, descriptorDecoder.sourceIdentity());
            }));
    }
}
