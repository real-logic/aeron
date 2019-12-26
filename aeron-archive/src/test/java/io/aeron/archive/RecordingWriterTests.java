package io.aeron.archive;

import io.aeron.Image;
import io.aeron.archive.Archive.Context;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.ArchiveException.GENERIC;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.fill;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.Checksums.crc32;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordingWriterTests
{
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;

    private File archiveDir;

    @BeforeEach
    void before()
    {
        archiveDir = TestUtil.makeTestDirectory();
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void initShouldOpenAFileChannel() throws IOException
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        final File segmentFile = segmentFile(1, 0);
        assertFalse(segmentFile.exists());

        try
        {
            recordingWriter.init();
            assertTrue(segmentFile.exists());
        }
        finally
        {
            recordingWriter.close();
        }
    }

    @Test
    void initThrowsIOExceptionIfItCannotOpenAFileChannel() throws IOException
    {
        final File notADirectory = new File(archiveDir, "dummy.txt");
        assertTrue(notADirectory.createNewFile());
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(notADirectory), null, null);

        assertThrows(IOException.class, recordingWriter::init);
        assertTrue(recordingWriter.isClosed());
    }

    @Test
    void closeShouldCloseTheUnderlyingFile() throws IOException
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        recordingWriter.init();
        assertFalse(recordingWriter.isClosed());

        recordingWriter.close();

        assertTrue(recordingWriter.isClosed());
        final File segmentFile = segmentFile(1, 0);
        assertTrue(segmentFile.exists());
        delete(segmentFile.toPath()); // can delete since the underlying FileChannel was already closed
    }

    @Test
    void onBlockThrowsNullPointerExceptionIfInitWasNotCalled()
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);

        assertThrows(NullPointerException.class, () ->
            recordingWriter.onBlock(new UnsafeBuffer(allocate(32)), 0, 10, 5, 8));
        assertTrue(recordingWriter.isClosed());
    }

    @Test
    void onBlockThrowsArchiveExceptionIfCurrentThreadWasInterrupted() throws IOException
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        recordingWriter.init();

        assertFalse(Thread.interrupted());
        try
        {
            Thread.currentThread().interrupt();
            final ArchiveException exception = assertThrows(ArchiveException.class, () ->
                recordingWriter.onBlock(new UnsafeBuffer(allocate(32)), 0, 10, 5, 8));
            assertEquals(GENERIC, exception.errorCode());
            assertEquals("file closed by interrupt, recording aborted", exception.getMessage());
            assertTrue(recordingWriter.isClosed());
        }
        finally
        {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    void onBlockShouldWriteHeaderAndContentsOfTheNonPaddingFrame() throws IOException
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        recordingWriter.init();
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocate(128));
        frameType(termBuffer, 0, HDR_TYPE_DATA);
        frameLengthOrdered(termBuffer, 0, 128);
        final byte[] data = new byte[96];
        fill(data, (byte)7);
        termBuffer.putBytes(HEADER_LENGTH, data);

        recordingWriter.onBlock(termBuffer, 0, 128, -1, -1);

        recordingWriter.close();
        final File segmentFile = segmentFile(1, 0);
        assertTrue(segmentFile.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile.length());
        final UnsafeBuffer fileBuffer = new UnsafeBuffer();
        fileBuffer.wrap(readAllBytes(segmentFile.toPath()));
        assertEquals(HDR_TYPE_DATA, frameType(fileBuffer, 0));
        assertEquals(128, frameLength(fileBuffer, 0));
        assertEquals(0, frameSessionId(fileBuffer, 0));
        final byte[] fileBytes = new byte[96];
        fileBuffer.getBytes(HEADER_LENGTH, fileBytes, 0, 96);
        assertArrayEquals(data, fileBytes);
    }

    @Test
    void onBlockShouldWriteHeaderOfThePaddingFrameAndAdvanceFilePositionByThePaddingLength() throws IOException
    {
        final int segmentOffset = 96;
        final long startPosition = 7 * TERM_LENGTH + segmentOffset;
        final Image image = mockImage(startPosition);
        final RecordingWriter recordingWriter = new RecordingWriter(
            5, startPosition, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        recordingWriter.init();
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocate(1024));
        frameType(termBuffer, 0, HDR_TYPE_PAD);
        frameLengthOrdered(termBuffer, 0, 1024);
        frameSessionId(termBuffer, 0, 111);
        final byte[] data = new byte[992];
        fill(data, (byte)-1);
        termBuffer.putBytes(HEADER_LENGTH, data);

        recordingWriter.onBlock(termBuffer, 0, 1024, -1, -1);

        recordingWriter.close();
        final File segmentFile = segmentFile(5, startPosition);
        assertTrue(segmentFile.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile.length());
        final UnsafeBuffer fileBuffer = new UnsafeBuffer();
        fileBuffer.wrap(readAllBytes(segmentFile.toPath()));
        final byte[] preamble = new byte[segmentOffset];
        fileBuffer.getBytes(0, preamble, 0, segmentOffset);
        assertArrayEquals(new byte[segmentOffset], preamble);
        assertEquals(HDR_TYPE_PAD, frameType(fileBuffer, segmentOffset));
        assertEquals(1024, frameLength(fileBuffer, segmentOffset));
        assertEquals(111, frameSessionId(fileBuffer, segmentOffset));
        final byte[] fileBytes = new byte[992];
        fileBuffer.getBytes(segmentOffset + HEADER_LENGTH, fileBytes, 0, 992);
        assertArrayEquals(new byte[992], fileBytes);
    }

    @Test
    void onBlockShouldRollOverToTheNextSegmentFile() throws IOException
    {
        final Image image = mockImage(0L);
        final RecordingWriter recordingWriter = new RecordingWriter(
            13, 0, SEGMENT_LENGTH, image, new Context().archiveDir(archiveDir), null, null);
        recordingWriter.init();
        final byte[] data1 = new byte[992];
        fill(data1, (byte)13);
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocate(TERM_LENGTH));
        frameType(termBuffer, 0, HDR_TYPE_DATA);
        frameLengthOrdered(termBuffer, 0, 1024);
        termBuffer.putBytes(HEADER_LENGTH, data1);
        for (int i = 0; i < SEGMENT_LENGTH / 1024; i++)
        {
            recordingWriter.onBlock(termBuffer, 0, 1024, -1, -1);
        }
        frameType(termBuffer, 0, HDR_TYPE_DATA);
        frameLengthOrdered(termBuffer, 0, 192);
        final byte[] data2 = new byte[160];
        fill(data2, (byte)22);
        termBuffer.putBytes(HEADER_LENGTH, data2);

        recordingWriter.onBlock(termBuffer, 0, 192, -1, -1);

        recordingWriter.close();
        final File segmentFile1 = segmentFile(13, 0);
        assertTrue(segmentFile1.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile1.length());
        final UnsafeBuffer fileBuffer = new UnsafeBuffer();
        fileBuffer.wrap(readAllBytes(segmentFile1.toPath()));
        assertEquals(HDR_TYPE_DATA, frameType(fileBuffer, 0));
        assertEquals(1024, frameLength(fileBuffer, 0));
        byte[] fileBytes = new byte[992];
        fileBuffer.getBytes(HEADER_LENGTH, fileBytes, 0, 992);
        assertArrayEquals(data1, fileBytes);
        final File segmentFile2 = segmentFile(13, SEGMENT_LENGTH);
        assertTrue(segmentFile2.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile2.length());
        fileBuffer.wrap(readAllBytes(segmentFile2.toPath()));
        assertEquals(HDR_TYPE_DATA, frameType(fileBuffer, 0));
        assertEquals(192, frameLength(fileBuffer, 0));
        fileBytes = new byte[160];
        fileBuffer.getBytes(HEADER_LENGTH, fileBytes, 0, 160);
        assertArrayEquals(data2, fileBytes);
    }

    @Test
    void onBlockThrowNullPointerExceptionIfRecordingBufferIsNullButCrcIsEnabled() throws IOException
    {
        final Image image = mockImage(0L);
        final Context ctx = new Context().archiveDir(archiveDir).recordingCrcEnabled(true);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, ctx, null, null);
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(512, 64));
        frameType(termBuffer, 0, HDR_TYPE_DATA);
        frameLengthOrdered(termBuffer, 0, 1024);
        recordingWriter.init();
        try
        {
            assertThrows(NullPointerException.class,
                () -> recordingWriter.onBlock(termBuffer, 0, 1024, -1, -1));
        }
        finally
        {
            recordingWriter.close();
        }
    }

    @Test
    void onBlockShouldComputeCrcUsingTheRecordingBuffer() throws IOException
    {
        final Image image = mockImage(0L);
        final Context ctx = new Context().archiveDir(archiveDir).recordingCrcEnabled(true);
        final UnsafeBuffer recordingBuffer = new UnsafeBuffer(allocateDirectAligned(512, 64));
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, ctx, null, recordingBuffer);
        recordingWriter.init();
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(512, 64));
        frameType(termBuffer, 96, HDR_TYPE_DATA);
        frameTermId(termBuffer, 96, 96);
        frameLengthOrdered(termBuffer, 96, 64);
        frameSessionId(termBuffer, 96, 96);
        termBuffer.setMemory(96 + HEADER_LENGTH, 32, (byte)96);
        frameType(termBuffer, 160, HDR_TYPE_DATA);
        frameTermId(termBuffer, 160, 160);
        frameLengthOrdered(termBuffer, 160, 288);
        frameSessionId(termBuffer, 160, 160);
        termBuffer.setMemory(160 + HEADER_LENGTH, 256, (byte)160);

        recordingWriter.onBlock(termBuffer, 96, 352, -1, -1);

        recordingWriter.close();
        final File segmentFile = segmentFile(1, 0);
        assertTrue(segmentFile.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile.length());
        final UnsafeBuffer fileBuffer = new UnsafeBuffer();
        fileBuffer.wrap(readAllBytes(segmentFile.toPath()));
        assertEquals(HDR_TYPE_DATA, frameType(fileBuffer, 0));
        assertEquals(96, frameTermId(fileBuffer, 0));
        assertEquals(64, frameLength(fileBuffer, 0));
        assertEquals(
            crc32(0, termBuffer.addressOffset(), 96 + HEADER_LENGTH, 32),
            frameSessionId(fileBuffer, 0)
        );
        assertEquals(HDR_TYPE_DATA, frameType(fileBuffer, 64));
        assertEquals(160, frameTermId(fileBuffer, 64));
        assertEquals(288, frameLength(fileBuffer, 64));
        assertEquals(
            crc32(0, termBuffer.addressOffset(), 160 + HEADER_LENGTH, 256),
            frameSessionId(fileBuffer, 64)
        );
        // Ensure that the source buffer was not modified
        assertEquals(96, frameSessionId(termBuffer, 96));
        assertEquals(160, frameSessionId(termBuffer, 160));
    }

    @Test
    void onBlockShouldNotComputeCrcForThePaddingFrame() throws IOException
    {

        final Image image = mockImage(0L);
        final Context ctx = new Context().archiveDir(archiveDir).recordingCrcEnabled(true);
        final RecordingWriter recordingWriter = new RecordingWriter(
            1, 0, SEGMENT_LENGTH, image, ctx, null, null);
        recordingWriter.init();
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocate(512));
        final int length = 128;
        final byte[] data = new byte[length - HEADER_LENGTH];
        fill(data, (byte)99);
        frameType(termBuffer, 0, HDR_TYPE_PAD);
        frameTermId(termBuffer, 0, 18);
        frameLengthOrdered(termBuffer, 0, length);
        frameSessionId(termBuffer, 0, 5);
        termBuffer.putBytes(HEADER_LENGTH, data);

        recordingWriter.onBlock(termBuffer, 0, length, -1, -1);

        recordingWriter.close();
        final File segmentFile = segmentFile(1, 0);
        assertTrue(segmentFile.exists());
        assertEquals(SEGMENT_LENGTH, segmentFile.length());
        final UnsafeBuffer fileBuffer = new UnsafeBuffer();
        fileBuffer.wrap(readAllBytes(segmentFile.toPath()));
        assertEquals(HDR_TYPE_PAD, frameType(fileBuffer, 0));
        assertEquals(18, frameTermId(fileBuffer, 0));
        assertEquals(length, frameLength(fileBuffer, 0));
        assertEquals(5, frameSessionId(fileBuffer, 0));
    }

    private Image mockImage(final long joinPosition)
    {
        final Image image = mock(Image.class);
        when(image.termBufferLength()).thenReturn(TERM_LENGTH);
        when(image.joinPosition()).thenReturn(joinPosition);
        return image;
    }

    private File segmentFile(final int recordingId, final long position)
    {
        final long segmentBasePosition = position - (position & (TERM_LENGTH - 1));
        return new File(archiveDir, segmentFileName(recordingId, segmentBasePosition));
    }
}