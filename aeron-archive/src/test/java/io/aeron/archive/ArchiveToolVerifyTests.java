package io.aeron.archive;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.ArchiveTool.openCatalogReadOnly;
import static io.aeron.archive.ArchiveTool.verify;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class ArchiveToolVerifyTests
{
    private static final int TERM_LENGTH = 2 * PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private File archiveDir;
    private long currentTimeMillis = 0;
    private EpochClock epochClock = () -> currentTimeMillis += 100;
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

    @BeforeEach
    void setup() throws IOException
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
            record9 = catalog.addNewRecording(2048, NULL_POSITION, 90, NULL_TIMESTAMP, 0,
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
            record15 = catalog.addNewRecording(PAGE_SIZE * 5 + 1024, 150, 150, 160, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
        }
        createFile(record2 + "-" + RECORDING_SEGMENT_SUFFIX); // ERR: no segment position
        createFile(record3 + "-" + "invalid_position" + RECORDING_SEGMENT_SUFFIX); // ERR: invalid position
        createFile(segmentFileName(record4, -111)); // ERR: negative position
        createFile(segmentFileName(record5, 0)); // ERR: empty file
        createDirectory(segmentFileName(record6, 0)); // ERR: directory
        writeToSegmentFile(createFile(segmentFileName(record7, 0)), (bb, fl, ch) -> ch.write(bb));
        writeToSegmentFile(createFile(segmentFileName(record8, 0)), (bb, fl, ch) -> ch.write(bb));
        writeToSegmentFile(createFile(segmentFileName(record9, SEGMENT_LENGTH)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(100);
                flyweight.streamId(3);
                ch.write(bb);
            });
        writeToSegmentFile(createFile(segmentFileName(record10, 0)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(PAGE_SIZE - 64);
                flyweight.streamId(3);
                ch.write(bb);
                bb.clear();
                flyweight.frameLength(128);
                flyweight.streamId(3);
                ch.write(bb, PAGE_SIZE - 64);
            });
        writeToSegmentFile(createFile(segmentFileName(record11, 0)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(PAGE_SIZE - 64);
                flyweight.streamId(3);
                ch.write(bb);
                bb.clear();
                flyweight.frameLength(256);
                flyweight.streamId(3);
                ch.write(bb, PAGE_SIZE - 64);
            });
        writeToSegmentFile(createFile(segmentFileName(record12, 0)), (bb, fl, ch) -> ch.write(bb));
        writeToSegmentFile(createFile(segmentFileName(record13, SEGMENT_LENGTH * 128)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(PAGE_SIZE * 3);
                flyweight.streamId(3);
                ch.write(bb);
            });
        writeToSegmentFile(createFile(segmentFileName(record14, 0)), (bb, fl, ch) -> ch.write(bb));
        writeToSegmentFile(createFile(segmentFileName(record15, 0)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(111);
                flyweight.streamId(-1);
                ch.write(bb);
            });
        writeToSegmentFile(createFile(segmentFileName(record15, SEGMENT_LENGTH * 2)),
            (bb, flyweight, ch) ->
            {
                flyweight.frameLength(1000);
                flyweight.streamId(3);
                ch.write(bb);
            });
    }

    private File createFile(final String name) throws IOException
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.createNewFile());
        return file;
    }

    private File createDirectory(final String name)
    {
        final File file = new File(archiveDir, name);
        assertTrue(file.mkdir());
        return file;
    }

    @FunctionalInterface
    private interface SegmentWriter
    {
        void write(ByteBuffer bb, DataHeaderFlyweight flyweight, FileChannel channel) throws IOException;
    }

    private void writeToSegmentFile(final File file, final SegmentWriter segmentWriter) throws IOException
    {
        final ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(bb);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            bb.clear();
            segmentWriter.write(bb, flyweight, channel);
        }
    }

    @AfterEach
    void teardown()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void verifyCheckLastFile()
    {
        verify(System.out, archiveDir, false, epochClock, file -> file.getName().startsWith("" + record10));

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            verifyRecording(catalog, record0, VALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                1, 1, "emptyChannel", "source1");
            verifyRecording(catalog, record1, VALID, 11, 11, 10, 100, 0,
                1, 1, "emptyChannel", "source1");
            verifyRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record7, VALID, 0, 0, 70, 200, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record8, VALID, TERM_LENGTH + 1024, -TERM_LENGTH, 80, 300, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record9, VALID, 2048, SEGMENT_LENGTH + 128, 90, 400, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record10, VALID, 0, PAGE_SIZE - 64, 100, 500, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record11, VALID, 0, PAGE_SIZE + 192, 110, 600, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record12, VALID, 0, 0, 120, 999999, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record13, VALID, 1024 * 1024, SEGMENT_LENGTH * 128 + PAGE_SIZE * 3, 130, 888888, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record14, VALID, 0, 0, 140, 700, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record15, VALID, PAGE_SIZE * 5 + 1024, SEGMENT_LENGTH * 2 + 1024, 150, 800, 0,
                3, 3, "validChannel", "source3");
        }
    }

    @Test
    void verifyCheckAllFiles()
    {
        verify(System.out, archiveDir, true, epochClock, file -> false);

        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            verifyRecording(catalog, record0, VALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                1, 1, "emptyChannel", "source1");
            verifyRecording(catalog, record1, VALID, 11, 11, 10, 100, 0,
                1, 1, "emptyChannel", "source1");
            verifyRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0,
                2, 2, "invalidChannel", "source2");
            verifyRecording(catalog, record7, VALID, 0, 0, 70, 200, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record8, VALID, TERM_LENGTH + 1024, -TERM_LENGTH, 80, 300, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record9, VALID, 2048, SEGMENT_LENGTH + 128, 90, 400, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record10, VALID, 0, PAGE_SIZE + 64, 100, 500, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record11, VALID, 0, PAGE_SIZE + 192, 110, 600, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record12, VALID, 0, 0, 120, 999999, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record13, VALID, 1024 * 1024, SEGMENT_LENGTH * 128 + PAGE_SIZE * 3, 130, 888888, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record14, VALID, 0, 0, 140, 700, 0,
                3, 3, "validChannel", "source3");
            verifyRecording(catalog, record15, INVALID, PAGE_SIZE * 5 + 1024, 150, 150, 160, 0,
                3, 3, "validChannel", "source3");
        }
    }

    private void verifyRecording(
        final Catalog catalog,
        final long recordingId,
        final byte valid,
        final long startPosition,
        final long stopPosition,
        final long startTimestamp,
        final long stopTimeStamp,
        final int initialTermId,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        assertTrue(
            catalog.forEntry(recordingId, (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                assertEquals(valid, headerDecoder.valid());
                assertEquals(startPosition, descriptorDecoder.startPosition());
                assertEquals(stopPosition, descriptorDecoder.stopPosition());
                assertEquals(startTimestamp, descriptorDecoder.startTimestamp());
                assertEquals(stopTimeStamp, descriptorDecoder.stopTimestamp());
                assertEquals(initialTermId, descriptorDecoder.initialTermId());
                assertEquals(MTU_LENGTH, descriptorDecoder.mtuLength());
                assertEquals(SEGMENT_LENGTH, descriptorDecoder.segmentFileLength());
                assertEquals(sessionId, descriptorDecoder.sessionId());
                assertEquals(streamId, descriptorDecoder.streamId());
                assertEquals(strippedChannel, descriptorDecoder.strippedChannel());
                assertNotNull(descriptorDecoder.originalChannel());
                assertEquals(sourceIdentity, descriptorDecoder.sourceIdentity());
            })
        );
    }
}