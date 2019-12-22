package io.aeron.archive;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.*;
import static java.util.Arrays.*;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.*;

class FrameCRCTests
{

    private FrameCRC crc = new FrameCRC();

    @Test
    void computeThrowsNullPointerExceptionIfTermBufferIsNull()
    {
        assertThrows(NullPointerException.class,
            () -> crc.forEach(null, 0, 10, (buffer, offset, length, checksum) -> {}));
    }

    @Test
    void computeThrowsNullPointerExceptionIfTheUnderlyingByteBufferIsNull()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer();
        assertNull(termBuffer.byteBuffer());

        assertThrows(NullPointerException.class,
            () -> crc.forEach(termBuffer, 0, 10, (buffer, offset, length, checksum) -> {}));
    }

    @Test
    void computeThrowsIndexOutOfBoundsExceptionIfOffsetIsNegative()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));

        assertThrows(IndexOutOfBoundsException.class,
            () -> crc.forEach(termBuffer, -1, 10, (buffer, offset, length, checksum) -> {}));
    }

    @Test
    void computeIsANoOpIfOffsetIsGreaterThanTheLength()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(2));
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 5, 2, (buffer, offset, length, checksum) -> counter.incrementAndGet());

        assertEquals(0, counter.get());
    }

    @Test
    void computeIsANoOpIfBufferIsOffsetIsEqualToLength()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(0));
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 4, 4, (buffer, offset, length, checksum) -> counter.incrementAndGet());

        assertEquals(0, counter.get());
    }

    @Test
    void computeIsANoOpIfBufferStartsWithAnEmptyFrame()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(HEADER_LENGTH));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 0);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(
            termBuffer, 0, HEADER_LENGTH, (buffer, offset, length, checksum) -> counter.incrementAndGet());

        assertEquals(0, counter.get());
    }

    @Test
    void computeIsANoOpIfFrameIsNotADataFrame()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(HEADER_LENGTH));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_SETUP);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(
            termBuffer, 0, HEADER_LENGTH, (buffer, offset, length, checksum) -> counter.incrementAndGet());

        assertEquals(0, counter.get());
    }

    @Test
    void computeCrcFromThePayloadAligned()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 128);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final byte[] payload = new byte[128 - HEADER_LENGTH];
        final Random r = new Random(1234);
        r.nextBytes(payload);
        final int expectedChecksum = computeCrc(payload);
        termBuffer.putBytes(HEADER_LENGTH, payload);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 0, 128,
            (buffer, offset, length, checksum) ->
            {
                counter.incrementAndGet();
                assertEquals(expectedChecksum, checksum);
            });

        assertEquals(1, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 128);
    }

    @Test
    void computeCrcFromThePayloadUnaligned()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(256));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 230);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final byte[] payload = new byte[230 - HEADER_LENGTH];
        final Random r = new Random(1234);
        r.nextBytes(payload);
        final int expectedChecksum = computeCrc(copyOf(payload, align(230 - HEADER_LENGTH, FRAME_ALIGNMENT)));
        termBuffer.putBytes(HEADER_LENGTH, payload);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 0, 256,
            (buffer, offset, length, checksum) ->
            {
                counter.incrementAndGet();
                assertEquals(expectedChecksum, checksum);
            });

        assertEquals(1, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 256);
    }

    @Test
    void computeStopsOnAFirstEmptyFrame()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocate(192));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        termBuffer.putInt(64 + FRAME_LENGTH_FIELD_OFFSET, 0);
        termBuffer.putInt(64 + TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 0, 128, (buffer, offset, length, checksum) -> counter.incrementAndGet());

        assertEquals(1, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 192);
    }

    @Test
    void computeCrcDirectByteBuffer()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final byte[] payload = new byte[32];
        fill(payload, (byte)11);
        final int expectedChecksum = computeCrc(payload);
        termBuffer.putBytes(HEADER_LENGTH, payload);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 0, 64,
            (buffer, offset, length, checksum) ->
            {
                counter.incrementAndGet();
                assertEquals(expectedChecksum, checksum);
            });

        assertEquals(1, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 64);
    }

    @Test
    void computeCrcAtTheGivenOffset()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final byte[] payload = new byte[32];
        fill(payload, (byte)3);
        final int expectedChecksum = computeCrc(payload);
        termBuffer.putInt(64 + FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(64 + TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        termBuffer.putBytes(64 + HEADER_LENGTH, payload);
        final AtomicInteger counter = new AtomicInteger();

        crc.forEach(termBuffer, 64, 128,
            (buffer, offset, length, checksum) ->
            {
                counter.incrementAndGet();
                assertEquals(expectedChecksum, checksum);
            });

        assertEquals(1, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 128);
    }

    @Test
    void computeCrcForEachDataFrameInTheRange()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        termBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        final byte[] payload = new byte[32];
        fill(payload, (byte)-1);
        final int crc1 = computeCrc(payload);
        termBuffer.putInt(64 + FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(64 + TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        termBuffer.putBytes(64 + HEADER_LENGTH, payload);
        termBuffer.putInt(128 + FRAME_LENGTH_FIELD_OFFSET, 32);
        termBuffer.putInt(128 + TYPE_FIELD_OFFSET, HDR_TYPE_PAD);
        termBuffer.putInt(160 + FRAME_LENGTH_FIELD_OFFSET, 64);
        termBuffer.putInt(160 + TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
        fill(payload, (byte)7);
        final int crc2 = computeCrc(payload);
        termBuffer.putBytes(160 + HEADER_LENGTH, payload);
        final AtomicInteger counter = new AtomicInteger();
        final List<Integer> results = new ArrayList<>();

        crc.forEach(termBuffer, 64, 224,
            (buffer, offset, length, checksum) ->
            {
                counter.incrementAndGet();
                results.add(checksum);
            });

        assertEquals(2, counter.get());
        assertEquals(termBuffer.byteBuffer().position(), 0);
        assertEquals(termBuffer.byteBuffer().limit(), 256);
        assertEquals(asList(crc1, crc2), results);
    }

    private int computeCrc(final byte[] payload)
    {
        final CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return (int)crc32.getValue();
    }
}