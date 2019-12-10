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
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BitUtil.align;
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

    @BeforeEach
    void setup() throws IOException
    {
        archiveDir = TestUtil.makeTestDirectory();
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 128, epochClock))
        {
            record0 = catalog.addNewRecording(NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1, "emptyChannel", "emptyChannel?tag=X", "source1");
            record1 = catalog.addNewRecording(11, NULL_POSITION, 10, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 1, 1, "emptyChannel", "emptyChannel?tag=X", "source1");
            record2 = catalog.addNewRecording(22, NULL_POSITION, 20, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record3 = catalog.addNewRecording(33, NULL_POSITION, 30, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record4 = catalog.addNewRecording(44, NULL_POSITION, 40, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record5 = catalog.addNewRecording(55, NULL_POSITION, 50, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record6 = catalog.addNewRecording(66, NULL_POSITION, 60, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 2, 2, "invalidChannel", "invalidChannel?tag=Y", "source2");
            record7 = catalog.addNewRecording(0, NULL_POSITION, 70, NULL_TIMESTAMP, 0, SEGMENT_LENGTH, TERM_LENGTH,
                MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record8 = catalog.addNewRecording(TERM_LENGTH, NULL_POSITION, 80, NULL_TIMESTAMP, 0, SEGMENT_LENGTH,
                TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record9 = catalog.addNewRecording(0, NULL_POSITION, 90, NULL_TIMESTAMP, 0, SEGMENT_LENGTH,
                TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
            record10 = catalog.addNewRecording(0, 1001, 100, NULL_TIMESTAMP, 0, SEGMENT_LENGTH,
                TERM_LENGTH, MTU_LENGTH, 3, 3, "validChannel", "validChannel?tag=Z", "source3");
        }
        createFile(record2 + "-" + RECORDING_SEGMENT_SUFFIX); // ERR: no segment position
        createFile(record3 + "-" + "invalid_position" + RECORDING_SEGMENT_SUFFIX); // ERR: invalid position
        createFile(segmentFileName(record4, -111)); // ERR: negative position
        createFile(segmentFileName(record5, 0)); // ERR: empty file
        createDirectory(segmentFileName(record6, 0)); // ERR: directory
        writeToSegmentFile(createFile(segmentFileName(record7, 555)), (bb, fl, ch) -> ch.write(bb));
        writeToSegmentFile(createFile(segmentFileName(record8, 1000)), (bb, flyweight, ch) ->
        {
            flyweight.frameLength(123);
            flyweight.sessionId(3);
            flyweight.streamId(3);
            ch.write(bb);
            bb.clear();
            flyweight.frameLength(0);
            ch.write(bb, align(123, FRAME_ALIGNMENT));
        });
        writeToSegmentFile(createFile(segmentFileName(record9, 0)), (bb, flyweight, ch) ->
        {
            flyweight.frameLength(PAGE_SIZE - HEADER_LENGTH);
            flyweight.sessionId(3);
            flyweight.streamId(3);
            ch.write(bb);
            bb.clear();
            flyweight.frameLength(128);
            flyweight.sessionId(3);
            flyweight.streamId(3);
            ch.write(bb, PAGE_SIZE - HEADER_LENGTH);
            bb.clear();
            flyweight.frameLength(0);
            ch.write(bb, PAGE_SIZE - HEADER_LENGTH + 128);
        });
        writeToSegmentFile(createFile(segmentFileName(record10, 1001)), (bb, fl, ch) -> ch.write(bb));
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
    void verifyAllRecordingsCheckLastFileOnly()
    {
        ArchiveTool.verify(System.out, archiveDir, epochClock);

        try (Catalog catalog = ArchiveTool.openCatalogReadOnly(archiveDir, epochClock))
        {
            verifyRecording(catalog, record0, VALID, NULL_POSITION, NULL_POSITION, NULL_TIMESTAMP, 100, 0, 1, 1,
                "emptyChannel", "source1");
            verifyRecording(catalog, record1, VALID, 11, 11, 10, 200, 0, 1, 1, "emptyChannel", "source1");
            verifyRecording(catalog, record2, INVALID, 22, NULL_POSITION, 20, NULL_TIMESTAMP, 0, 2, 2, "invalidChannel",
                "source2");
            verifyRecording(catalog, record3, INVALID, 33, NULL_POSITION, 30, NULL_TIMESTAMP, 0, 2, 2, "invalidChannel",
                "source2");
            verifyRecording(catalog, record4, INVALID, 44, NULL_POSITION, 40, NULL_TIMESTAMP, 0, 2, 2, "invalidChannel",
                "source2");
            verifyRecording(catalog, record5, INVALID, 55, NULL_POSITION, 50, NULL_TIMESTAMP, 0, 2, 2, "invalidChannel",
                "source2");
            verifyRecording(catalog, record6, INVALID, 66, NULL_POSITION, 60, NULL_TIMESTAMP, 0, 2, 2, "invalidChannel",
                "source2");
            verifyRecording(catalog, record7, VALID, 0, 555, 70, 300, 0, 3, 3, "validChannel",
                "source3");
            verifyRecording(catalog, record8, VALID, TERM_LENGTH, TERM_LENGTH + 1128, 80, 400, 0, 3, 3, "validChannel",
                "source3");
//            verifyRecording(catalog, record9, VALID, 0, PAGE_SIZE - HEADER_LENGTH, 90, 500, 0, 3, 3, "validChannel",
//                "source3");
            verifyRecording(catalog, record10, VALID, 0, 1001, 100, NULL_TIMESTAMP, 0, 3, 3, "validChannel",
                "source3");
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