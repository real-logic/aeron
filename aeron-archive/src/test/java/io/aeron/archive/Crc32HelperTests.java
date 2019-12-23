package io.aeron.archive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static io.aeron.archive.Crc32Helper.crc32;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Crc32HelperTests
{

    private final CRC32 crc32 = new CRC32();

    @BeforeEach
    void before()
    {
        crc32.reset();
    }

    @Test
    void crc32ThrowsNullPointerExceptionIfStateIsNull()
    {
        assertThrows(NullPointerException.class, () -> crc32(null, allocate(32), 0, 32));
    }

    @Test
    void crc32ThrowsNullPointerExceptionIfByteBufferIsNull()
    {
        assertThrows(NullPointerException.class, () -> crc32(crc32, null, 0, 32));
    }

    @Test
    void crc32ThrowsIllegalArgumentExceptionIfFrameOffsetPlusLengthIsGreaterThanBufferCapacity()
    {
        assertThrows(IllegalArgumentException.class, () -> crc32(crc32, allocate(32), 1, 32));
    }

    @Test
    void crc32ThrowsIllegalArgumentExceptionIfFrameOffsetPlusLengthIsNegative()
    {
        assertThrows(IllegalArgumentException.class, () -> crc32(crc32, allocate(32), 0, -1));
    }

    @Test
    void crc32ByteBuffer()
    {
        final byte[] data = new byte[20];
        fill(data, (byte)3);
        final int expectedCrc32 = computeCrc(copyOf(data, 32));
        final ByteBuffer byteBuffer = allocate(32);
        byteBuffer.put(data).flip();

        assertEquals(expectedCrc32, crc32(crc32, byteBuffer, 0, 32));
        assertEquals(0, byteBuffer.position());
        assertEquals(20, byteBuffer.limit());
    }

    @Test
    void crc32DirectByteBuffer()
    {
        final byte[] data = new byte[32];
        fill(data, (byte)-1);
        final int expectedCrc32 = computeCrc(data);
        final ByteBuffer byteBuffer = allocateDirect(36);
        byteBuffer.position(4);
        byteBuffer.put(data);
        byteBuffer.position(8).limit(13);

        assertEquals(expectedCrc32, crc32(crc32, byteBuffer, 4, 32));
        assertEquals(8, byteBuffer.position());
        assertEquals(13, byteBuffer.limit());
    }

    @Test
    void crc32ResetStateBetweenTheCalls()
    {
        final byte[] data = new byte[32];
        fill(data, (byte)-1);
        final ByteBuffer byteBuffer = allocateDirect(64);
        byteBuffer.position(32);
        byteBuffer.put(data);
        assertEquals(computeCrc(data), crc32(crc32, byteBuffer, 32, 32));

        fill(data, (byte)5);
        byteBuffer.clear().position(11);
        byteBuffer.put(data);
        assertEquals(computeCrc(data), crc32(crc32, byteBuffer, 11, 32));
    }

    private int computeCrc(final byte[] payload)
    {
        final CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return (int)crc32.getValue();
    }
}