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

import io.aeron.logbuffer.LogBufferDescriptor;
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
import java.util.EnumSet;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.ArchiveTool.*;
import static io.aeron.archive.ArchiveTool.VerifyOption.APPLY_CRC;
import static io.aeron.archive.ArchiveTool.VerifyOption.VALIDATE_ALL_SEGMENT_FILES;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class ArchiveToolTests
{
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;
    private static final int MTU_LENGTH = 1024;

    private long invalidRecording0;
    private long invalidRecording1;
    private long invalidRecording2;
    private long invalidRecording3;
    private long invalidRecording4;
    private long invalidRecording5;
    private long invalidRecording6;
    private long invalidRecording7;
    private long invalidRecording8;
    private long invalidRecording9;
    private long invalidRecording10;
    private long invalidRecording11;
    private long invalidRecording12;
    private long invalidRecording13;
    private long invalidRecording14;
    private long validRecording0;
    private long validRecording1;
    private long validRecording2;
    private long validRecording3;
    private long validRecording4;
    private long validRecording5;
    private long validRecording6;

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
            invalidRecording0 = catalog.addNewRecording(NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording1 = catalog.addNewRecording(FRAME_ALIGNMENT - 7, NULL_POSITION, 2, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording2 = catalog.addNewRecording(1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording3 = catalog.addNewRecording(0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording4 = catalog.addNewRecording(SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording5 = catalog.addNewRecording(0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording6 = catalog.addNewRecording(0, NULL_POSITION, 7, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording7 = catalog.addNewRecording(0, NULL_POSITION, 8, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording8 = catalog.addNewRecording(0, NULL_POSITION, 9, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording9 = catalog.addNewRecording(0, NULL_POSITION, 10, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording10 = catalog.addNewRecording(128, NULL_POSITION, 11, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording11 = catalog.addNewRecording(0, NULL_POSITION, 12, NULL_TIMESTAMP, 5,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording12 = catalog.addNewRecording(0, NULL_POSITION, 13, NULL_TIMESTAMP, 9,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 6, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording13 = catalog.addNewRecording(0, NULL_POSITION, 14, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 13, "ch1", "ch1?tag=ERR", "src1");
            invalidRecording14 = catalog.addNewRecording(128, NULL_POSITION, -14, 41, -14,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 0, "ch1", "ch1?tag=ERR", "src1");
            validRecording0 = catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording1 = catalog.addNewRecording(1024, NULL_POSITION, 16, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording2 = catalog.addNewRecording(3 * TERM_LENGTH + 96, NULL_POSITION, 17, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            validRecording3 = catalog.addNewRecording(
                7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128, 18, NULL_TIMESTAMP, 7,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 999, 13, "ch2", "ch2?tag=OK", "src2");
            validRecording4 = catalog.addNewRecording(
                21 * TERM_LENGTH + (TERM_LENGTH - 64), 22 * TERM_LENGTH + 992, 19, 1, -25,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, -999, 7, "ch2", "ch2?tag=OK", "src2");
            validRecording5 = catalog.addNewRecording(0, 64 + PAGE_SIZE, 20, 777, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, -1, 20, "ch2", "ch2?tag=OK", "src2");
            validRecording6 = catalog.addNewRecording(352, NULL_POSITION, 21, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 10, 6, "ch2", "ch2?tag=OK", "src2");
        }

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording4, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording5, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        createFile(invalidRecording6 + "-" + RECORDING_SEGMENT_SUFFIX);

        createFile(invalidRecording7 + "-this-will-not-parse" + RECORDING_SEGMENT_SUFFIX);

        createFile(segmentFileName(invalidRecording8, -1024));

        createDirectory(segmentFileName(invalidRecording9, 0));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording10, 0)),
            150,
            (byteBuffer, dataHeaderFlyweight, fileChannel) -> fileChannel.write(byteBuffer));

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording11, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(1);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(5);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(40);
                dataHeaderFlyweight.termId(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording12, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(6);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(9);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(50);
                dataHeaderFlyweight.termOffset(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording13, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(0);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(60);
                dataHeaderFlyweight.streamId(-1);
                fileChannel.write(byteBuffer, 64);
            });

        writeToSegmentFile(
            createFile(segmentFileName(invalidRecording14, 0)),
            228,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(100);
                dataHeaderFlyweight.streamId(0);
                dataHeaderFlyweight.termOffset(128);
                dataHeaderFlyweight.termId(-14);
                fileChannel.write(byteBuffer, 128);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording0, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                for (int i = 0, termOffset = 0; i < TERM_LENGTH / MTU_LENGTH - 1; i++, termOffset += MTU_LENGTH)
                {
                    byteBuffer.clear().limit(MTU_LENGTH);
                    dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                    dataHeaderFlyweight.streamId(2);
                    dataHeaderFlyweight.frameLength(MTU_LENGTH);
                    dataHeaderFlyweight.sessionId(790663674);
                    dataHeaderFlyweight.termOffset(termOffset);
                    fileChannel.write(byteBuffer, termOffset);
                }
                byteBuffer.clear().limit(256);
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.streamId(2);
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - MTU_LENGTH);
                dataHeaderFlyweight.sessionId(1025596259);
                fileChannel.write(byteBuffer, TERM_LENGTH - MTU_LENGTH);
                byteBuffer.clear().limit(MTU_LENGTH - 256);
                dataHeaderFlyweight.headerType(HDR_TYPE_PAD);
                dataHeaderFlyweight.streamId(2);
                dataHeaderFlyweight.sessionId(-1); // should not be checked
                dataHeaderFlyweight.frameLength(MTU_LENGTH - 256);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - MTU_LENGTH + 256);
                fileChannel.write(byteBuffer, TERM_LENGTH - MTU_LENGTH + 256);
                byteBuffer.clear().limit(64);
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.streamId(2);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.termOffset(0);
                dataHeaderFlyweight.termId(1);
                dataHeaderFlyweight.sessionId(420107693);
                fileChannel.write(byteBuffer, TERM_LENGTH);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording2, 3 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(0);
                fileChannel.write(byteBuffer, 96);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording3, 7 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(1024);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termId(14);
                dataHeaderFlyweight.termOffset(96);
                fileChannel.write(byteBuffer, 96);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(TERM_LENGTH);
                dataHeaderFlyweight.termOffset(1120);
                fileChannel.write(byteBuffer, 1120);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.termId(15);
                dataHeaderFlyweight.termOffset(1120);
                fileChannel.write(byteBuffer, 1120 + TERM_LENGTH);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(256);
                dataHeaderFlyweight.termId(15);
                dataHeaderFlyweight.termOffset(1248);
                fileChannel.write(byteBuffer, 1248 + TERM_LENGTH);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(SEGMENT_LENGTH - (1504 + TERM_LENGTH));
                dataHeaderFlyweight.termId(15);
                dataHeaderFlyweight.termOffset(1504);
                fileChannel.write(byteBuffer, 1504 + TERM_LENGTH);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording3, 11 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(180);
                dataHeaderFlyweight.streamId(13);
                dataHeaderFlyweight.termId(18);
                dataHeaderFlyweight.termOffset(0);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(128);
                dataHeaderFlyweight.termId(18);
                dataHeaderFlyweight.termOffset(192);
                fileChannel.write(byteBuffer, 192);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording4, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(Integer.MAX_VALUE);
                fileChannel.write(byteBuffer);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording4, 21 * TERM_LENGTH)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_PAD);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(7);
                dataHeaderFlyweight.termId(-4);
                dataHeaderFlyweight.termOffset(TERM_LENGTH - 64);
                fileChannel.write(byteBuffer, TERM_LENGTH - 64);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(992);
                dataHeaderFlyweight.termId(-3);
                dataHeaderFlyweight.termOffset(0);
                fileChannel.write(byteBuffer, TERM_LENGTH);
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording5, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(20);
                dataHeaderFlyweight.sessionId(420107693);
                fileChannel.write(byteBuffer);
                for (int i = 0; i < PAGE_SIZE / MTU_LENGTH; i++)
                {
                    byteBuffer.clear();
                    dataHeaderFlyweight.frameLength(MTU_LENGTH);
                    dataHeaderFlyweight.termOffset(64 + i * MTU_LENGTH);
                    dataHeaderFlyweight.sessionId(790663674);
                    fileChannel.write(byteBuffer, dataHeaderFlyweight.termOffset());
                }
            });

        writeToSegmentFile(
            createFile(segmentFileName(validRecording6, 0)),
            SEGMENT_LENGTH,
            (byteBuffer, dataHeaderFlyweight, fileChannel) ->
            {
                dataHeaderFlyweight.headerType(HDR_TYPE_DATA);
                dataHeaderFlyweight.frameLength(64);
                dataHeaderFlyweight.streamId(6);
                dataHeaderFlyweight.termOffset(352);
                dataHeaderFlyweight.sessionId(-1960800604); // CRC
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 20, (byte)1);
                fileChannel.write(byteBuffer, 352);
                byteBuffer.clear();
                dataHeaderFlyweight.frameLength(544);
                dataHeaderFlyweight.termOffset(416);
                dataHeaderFlyweight.sessionId(-327206874); // CRC
                dataHeaderFlyweight.setMemory(HEADER_LENGTH, 256, (byte)11);
                dataHeaderFlyweight.setMemory(HEADER_LENGTH + 256, 256, (byte)-20);
                fileChannel.write(byteBuffer, 416);
            });
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsNegative()
    {
        verifyRecording(out, archiveDir, invalidRecording0, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsNotFrameAligned()
    {
        verifyRecording(out, archiveDir, invalidRecording1, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording1, INVALID, FRAME_ALIGNMENT - 7, NULL_POSITION, 2, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsBeforeStartPosition()
    {
        verifyRecording(out, archiveDir, invalidRecording2, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording2, INVALID, 1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsNotFrameAligned()
    {
        verifyRecording(out, archiveDir, invalidRecording3, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording3, INVALID, 0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStartPositionIsOutOfRangeForTheMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, invalidRecording4, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording4, INVALID, SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfStopPositionIsOutOfRangeForTheMaxSegmentFile()
    {
        verifyRecording(out, archiveDir, invalidRecording5, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording5, INVALID, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNoPositionInformation()
    {
        verifyRecording(out, archiveDir, invalidRecording6, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording6, INVALID, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNonIntegerPositionInformation()
    {
        verifyRecording(out, archiveDir, invalidRecording7, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording7, INVALID, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileNameContainsNegativePositionInformation()
    {
        verifyRecording(out, archiveDir, invalidRecording8, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording8, INVALID, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfCannotReadFromSegmentFile()
    {
        verifyRecording(out, archiveDir, invalidRecording9, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording9, INVALID, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfCannotReadFrameFromSegmentFileAtGivenOffset()
    {
        verifyRecording(out, archiveDir, invalidRecording10, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording10, INVALID, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongTermId()
    {
        verifyRecording(out, archiveDir, invalidRecording11, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording11, INVALID, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongTermOffset()
    {
        verifyRecording(out, archiveDir, invalidRecording12, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording12, INVALID, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsAFrameWithWrongStreamId()
    {
        verifyRecording(out, archiveDir, invalidRecording13, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording13, INVALID, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingShouldMarkRecordingAsInvalidIfSegmentFileContainsIncompleteFrame()
    {
        verifyRecording(out, archiveDir, invalidRecording14, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording14, INVALID, 128, NULL_POSITION, -14, 41, -14, 0, "ch1", "src1");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldComputeStopPositionFromZeroStartPosition()
    {
        verifyRecording(out, archiveDir, validRecording0, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording0, VALID, 0, TERM_LENGTH + 64, 15, 100, 0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldUseStartPositionIfNoSegmentFilesExist()
    {
        verifyRecording(out, archiveDir, validRecording1, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording1, VALID, 1024, 1024, 16, 100, 0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldUseStartPositionWhenNoDataInTheMaxSegmentFileAtAGivenOffset()
    {
        verifyRecording(out, archiveDir, validRecording2, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording2, VALID, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96, 17, 100,
                0, 2, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldComputeStopPositionWhenStartingAtALaterSegment()
    {
        verifyRecording(out, archiveDir, validRecording3, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingShouldNotUpdateStopPositionIfAlreadyCorrect()
    {
        verifyRecording(out, archiveDir, validRecording4, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording4, VALID, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingValidateAllSegmentFiles()
    {
        verifyRecording(out, archiveDir, validRecording3, EnumSet
            .of(VALIDATE_ALL_SEGMENT_FILES), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, VALID, 7 * TERM_LENGTH + 96, 11 * TERM_LENGTH + 320,
                18, 100, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingInvalidRecordingValidateAllSegmentFiles()
    {
        verifyRecording(out, archiveDir, validRecording4, EnumSet
            .of(VALIDATE_ALL_SEGMENT_FILES), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording4, INVALID, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingTruncateSegmentFileOnPageStraddle()
    {
        verifyRecording(out, archiveDir, validRecording5, emptySet(), epochClock, (file) -> true);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording5, VALID, 0, 64 + PAGE_SIZE - MTU_LENGTH, 20, 100,
                0, 20, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingDoNotTruncateSegmentFileOnPageStraddle()
    {
        verifyRecording(out, archiveDir, validRecording5, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording5, VALID, 0, 64 + PAGE_SIZE, 20, 777, 0, 20, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingValidRecordingPerformCRC()
    {
        verifyRecording(out, archiveDir, validRecording6, EnumSet.of(APPLY_CRC), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording6, VALID, 352, 960, 21, 100, 0, 6, "ch2", "src2");
        }
    }

    @Test
    void verifyRecordingInvalidRecordingPerformCRC()
    {
        verifyRecording(out, archiveDir, validRecording3, EnumSet.of(APPLY_CRC), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, validRecording3, INVALID, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
        }
    }

    @Test
    void verifyNoOptionsDoNotTruncateFileOnPageStraddle()
    {
        verify(out, archiveDir, emptySet(), epochClock, (file) -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording1, INVALID, FRAME_ALIGNMENT - 7, NULL_POSITION, 2, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording2, INVALID, 1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording3, INVALID, 0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording4, INVALID, SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording5, INVALID, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording6, INVALID, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording7, INVALID, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording8, INVALID, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording9, INVALID, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording10, INVALID, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording11, INVALID, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording12, INVALID, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
            assertRecording(catalog, invalidRecording13, INVALID, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
            assertRecording(catalog, invalidRecording14, INVALID, 128, NULL_POSITION, -14, 41, -14, 0, "ch1", "src1");
            assertRecording(catalog, validRecording0, VALID, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, VALID, 7 * TERM_LENGTH + 96,
                11 * TERM_LENGTH + 320, 18, 400, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, VALID, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording5, VALID, 0, 64 + PAGE_SIZE, 20, 777,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, 352, 960, 21, 500, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(22)).println(any(String.class));
    }

    @Test
    void verifyAllOptionsTruncateFileOnPageStraddle()
    {
        verify(out, archiveDir, EnumSet.allOf(VerifyOption.class), epochClock, (file) -> true);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            assertRecording(catalog, invalidRecording0, INVALID, NULL_POSITION, NULL_POSITION, 1, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording1, INVALID, FRAME_ALIGNMENT - 7, NULL_POSITION, 2, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording2, INVALID, 1024, FRAME_ALIGNMENT * 2, 3, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording3, INVALID, 0, FRAME_ALIGNMENT * 5 + 11, 4, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording4, INVALID, SEGMENT_LENGTH, NULL_POSITION, 5, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording5, INVALID, 0, SEGMENT_LENGTH, 6, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording6, INVALID, 0, NULL_POSITION, 7, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording7, INVALID, 0, NULL_POSITION, 8, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording8, INVALID, 0, NULL_POSITION, 9, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording9, INVALID, 0, NULL_POSITION, 10, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording10, INVALID, 128, NULL_POSITION, 11, NULL_TIMESTAMP,
                0, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording11, INVALID, 0, NULL_POSITION, 12, NULL_TIMESTAMP,
                5, 1, "ch1", "src1");
            assertRecording(catalog, invalidRecording12, INVALID, 0, NULL_POSITION, 13, NULL_TIMESTAMP,
                9, 6, "ch1", "src1");
            assertRecording(catalog, invalidRecording13, INVALID, 0, NULL_POSITION, 14, NULL_TIMESTAMP,
                0, 13, "ch1", "src1");
            assertRecording(catalog, invalidRecording14, INVALID, 128, NULL_POSITION, -14, 41, -14, 0, "ch1", "src1");
            assertRecording(catalog, validRecording0, VALID, 0, TERM_LENGTH + 64, 15, 100,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording1, VALID, 1024, 1024, 16, 200,
                0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording2, VALID, TERM_LENGTH * 3 + 96, TERM_LENGTH * 3 + 96,
                17, 300, 0, 2, "ch2", "src2");
            assertRecording(catalog, validRecording3, INVALID, 7 * TERM_LENGTH + 96, 7 * TERM_LENGTH + 128,
                18, NULL_TIMESTAMP, 7, 13, "ch2", "src2");
            assertRecording(catalog, validRecording4, INVALID, 21 * TERM_LENGTH + (TERM_LENGTH - 64),
                22 * TERM_LENGTH + 992, 19, 1, -25, 7, "ch2", "src2");
            assertRecording(catalog, validRecording5, VALID, 0, 64 + PAGE_SIZE - MTU_LENGTH, 20, 400,
                0, 20, "ch2", "src2");
            assertRecording(catalog, validRecording6, VALID, 352, 960, 21, 500, 0, 6, "ch2", "src2");
        }

        Mockito.verify(out, times(22)).println(any(String.class));
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

    private void writeToSegmentFile(
        final File file, final int fileLength, final SegmentWriter segmentWriter) throws IOException
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(MTU_LENGTH);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(byteBuffer);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            segmentWriter.write(byteBuffer, flyweight, channel);
            channel.truncate(fileLength);
            byteBuffer.put(0, (byte)0).limit(1).position(0);
            channel.write(byteBuffer, fileLength - 1);
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
