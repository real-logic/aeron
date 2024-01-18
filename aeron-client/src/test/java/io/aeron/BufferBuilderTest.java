/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron;

import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.BufferBuilder.INIT_MIN_CAPACITY;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.EOS_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.*;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class BufferBuilderTest
{
    private final BufferBuilder bufferBuilder = new BufferBuilder();

    @ParameterizedTest
    @CsvSource({
        "0,0,4096", "4095,1,4096", "4096,7,4096", "100,200,4096", "101,5000,6144", "5000,8080,11250",
        "6666,11111,14998", "0,2147483639,2147483639", "2048,2147483647,2147483639", "5,1000000000000,2147483639" })
    void shouldFindSuitableCapacity(final int capacity, final long requiredCapacity, final int expected)
    {
        assertEquals(expected, BufferBuilder.findSuitableCapacity(capacity, requiredCapacity));
    }

    @Test
    void shouldInitialiseToDefaultValues()
    {
        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertEquals(0, bufferBuilder.buffer().capacity());
        assertNull(bufferBuilder.buffer().byteBuffer());
        assertArrayEquals(ArrayUtil.EMPTY_BYTE_ARRAY, bufferBuilder.buffer().byteArray());
    }

    @Test
    void shouldGrowDirectBuffer()
    {
        final BufferBuilder builder = new BufferBuilder(0, true);
        assertEquals(0, builder.limit());
        assertEquals(0, builder.capacity());
        assertEquals(0, builder.buffer().capacity());

        final int appendedLength = 10;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[appendedLength]);
        builder.append(srcBuffer, 0, srcBuffer.capacity());

        assertEquals(INIT_MIN_CAPACITY, builder.capacity());
        assertEquals(INIT_MIN_CAPACITY, builder.buffer().capacity());
        assertEquals(appendedLength, builder.limit());
        assertNotNull(builder.buffer().byteBuffer());
        assertTrue(builder.buffer().byteBuffer().isDirect());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldCreateHaderBuffer(final boolean isDirect)
    {
        final BufferBuilder builder = new BufferBuilder(0, isDirect);
        assertNotNull(builder.buffer());
        assertNotNull(builder.headerBuffer());
        assertNotSame(builder.headerBuffer(), builder.buffer());

        assertEquals(0, builder.capacity());
        assertEquals(0, builder.limit());
        assertEquals(0, builder.buffer().capacity());

        assertEquals(HEADER_LENGTH, builder.headerBuffer().capacity());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, Integer.MAX_VALUE, Integer.MAX_VALUE - 7 })
    void shouldThrowIllegalArgumentExceptionIfInitialCapacityIsOutOfRange(final int initialCapacity)
    {
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> new BufferBuilder(initialCapacity));
        assertEquals(
            "initialCapacity outside range 0 - 2147483639: initialCapacity=" + initialCapacity, exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 65, 1000000 })
    void shouldThrowIllegalArgumentExceptionIfLimitIsOutOfRange(final int limit)
    {
        final BufferBuilder builder = new BufferBuilder(0);
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> builder.limit(limit));
        assertEquals("limit outside range: capacity=" + builder.capacity() + " limit=" + limit,
            exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MAX_VALUE, Integer.MAX_VALUE - 23 })
    void shouldThrowIllegalStateExceptionIfDataExceedsMaxCapacity(final int length)
    {
        final UnsafeBuffer src = new UnsafeBuffer(new byte[16]);
        src.putLong(0, Long.MAX_VALUE);
        src.putLong(SIZE_OF_LONG, Long.MIN_VALUE);

        final BufferBuilder builder = new BufferBuilder(64);
        builder.append(src, 0, src.capacity());
        assertEquals(src.capacity(), builder.limit());

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, () -> builder.append(src, 0, length));
        assertEquals(
            "insufficient capacity: maxCapacity=2147483639 limit=" + builder.limit() +
            " additionalLength=" + length, exception.getMessage());
    }

    @Test
    void shouldAppendNothingForZeroLength()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, 0);

        assertEquals(0, bufferBuilder.limit());
        assertEquals(0, bufferBuilder.capacity());
    }

    @Test
    void shouldGrowToMultipleOfInitialCapacity()
    {
        final int srcCapacity = INIT_MIN_CAPACITY * 5;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[srcCapacity]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());

        assertEquals(srcCapacity, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(srcCapacity));
    }

    @Test
    void shouldAppendThenReset()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());

        assertEquals(srcBuffer.capacity(), bufferBuilder.limit());

        bufferBuilder.reset();

        assertEquals(0, bufferBuilder.limit());
    }

    @Test
    void shouldAppendOneBufferWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);
        final byte[] bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertEquals(bytes.length, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertArrayEquals(temp, bytes);
    }

    @Test
    void shouldAppendTwoBuffersWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[INIT_MIN_CAPACITY]);
        final byte[] bytes = "1111111122222222".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length / 2);
        bufferBuilder.append(srcBuffer, bytes.length / 2, bytes.length / 2);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertEquals(bytes.length, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertArrayEquals(temp, bytes);
    }

    @Test
    void shouldFillBufferWithoutResizing()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);
        final int initialCapacity = bufferBuilder.capacity();

        bufferBuilder.append(srcBuffer, 0, bufferLength);

        final byte[] temp = new byte[bufferLength];
        bufferBuilder.buffer().getBytes(0, temp, 0, bufferLength);

        assertEquals(bufferLength, bufferBuilder.limit());
        assertEquals(initialCapacity, bufferBuilder.capacity());
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldResizeWhenBufferJustDoesNotFit()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength + 1];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

        bufferBuilder.append(srcBuffer, 0, buffer.length);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, buffer.length);

        assertEquals(buffer.length, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThan(bufferLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldAppendTwoBuffersAndResize()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        final int firstLength = buffer.length / 4;
        final int secondLength = buffer.length / 2;
        Arrays.fill(buffer, 0, firstLength + secondLength, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength / 2);

        bufferBuilder.append(srcBuffer, 0, firstLength);
        bufferBuilder.append(srcBuffer, firstLength, secondLength);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, secondLength + firstLength);

        assertEquals(firstLength + secondLength, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(firstLength + secondLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    void shouldCompactBufferToLowerLimit()
    {
        final int bufferLength = INIT_MIN_CAPACITY / 2;
        final byte[] buffer = new byte[bufferLength];
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder();

        final int bufferCount = 5;
        for (int i = 0; i < bufferCount; i++)
        {
            bufferBuilder.append(srcBuffer, 0, buffer.length);
        }

        final int expectedLimit = buffer.length * bufferCount;
        assertEquals(expectedLimit, bufferBuilder.limit());
        final int expandedCapacity = bufferBuilder.capacity();
        assertThat(expandedCapacity, greaterThan(expectedLimit));

        bufferBuilder.reset();

        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);

        bufferBuilder.compact();

        assertEquals(buffer.length * 3, bufferBuilder.limit());
        assertThat(bufferBuilder.capacity(), lessThan(expandedCapacity));
        assertEquals(bufferBuilder.limit(), bufferBuilder.capacity());
    }

    @Test
    void captureFirstHeader()
    {
        final long reservedValue = Long.MAX_VALUE - 117;
        final int offset = 40;
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[100], offset, 50);
        headerFlyweight.frameLength(512);
        headerFlyweight.version((short)0xA);
        headerFlyweight.flags((short)0b1111_1111);
        headerFlyweight.headerType(HDR_TYPE_RTTM);
        headerFlyweight.termOffset(124);
        headerFlyweight.sessionId(-890);
        headerFlyweight.streamId(555);
        headerFlyweight.termId(42);
        headerFlyweight.reservedValue(reservedValue);

        final Header header = new Header(5, 3);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());
        final DirectBuffer originalHeaderBuffer = header.buffer();

        assertSame(bufferBuilder, bufferBuilder.captureFirstHeader(header));

        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertSame(originalHeaderBuffer, header.buffer());
        assertNotSame(bufferBuilder.buffer(), header.buffer());
        assertNotSame(bufferBuilder.headerBuffer(), header.buffer());

        headerFlyweight.wrap(bufferBuilder.headerBuffer(), 0, HEADER_LENGTH);
        assertEquals(0, headerFlyweight.frameLength());
        assertEquals(0, headerFlyweight.version());
        assertEquals(0, headerFlyweight.flags());
        assertEquals(0, headerFlyweight.headerType());
        assertEquals(0, headerFlyweight.termOffset());
        assertEquals(0, headerFlyweight.sessionId());
        assertEquals(0, headerFlyweight.streamId());
        assertEquals(0, headerFlyweight.termId());
        assertEquals(reservedValue, headerFlyweight.reservedValue());
    }

    @Test
    void shouldPrepareCompleteHeader()
    {
        final UnsafeBuffer data = new UnsafeBuffer(new byte[999]);
        bufferBuilder.append(data, 0, data.capacity());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertEquals(data.capacity(), bufferBuilder.limit());

        final long reservedValue = 0xCAFE_BABE_DEAD_BEEFL;
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[44], 1, 39);
        headerFlyweight.frameLength(1);
        headerFlyweight.version((short)1);
        headerFlyweight.flags(UNFRAGMENTED);
        headerFlyweight.headerType(1);
        headerFlyweight.termOffset(1);
        headerFlyweight.sessionId(1);
        headerFlyweight.streamId(1);
        headerFlyweight.termId(1);
        headerFlyweight.reservedValue(reservedValue);

        final Header header = new Header(2, 3);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());

        assertSame(bufferBuilder, bufferBuilder.captureFirstHeader(header));

        final int flags = 0b0101_0101;
        headerFlyweight.wrap(new byte[88], 19, 42);
        headerFlyweight.frameLength(256);
        headerFlyweight.version((short)0xC);
        headerFlyweight.flags((short)flags);
        headerFlyweight.headerType(HDR_TYPE_ATS_SETUP);
        headerFlyweight.termOffset(48);
        headerFlyweight.sessionId(999);
        headerFlyweight.streamId(4);
        headerFlyweight.termId(39);
        headerFlyweight.reservedValue(Long.MIN_VALUE);
        header.buffer(new UnsafeBuffer(headerFlyweight.byteArray()));
        header.offset(headerFlyweight.wrapAdjustment());
        final DirectBuffer originalHeaderBuffer = header.buffer();

        assertSame(header, bufferBuilder.prepareCompleteHeader(header));

        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertEquals(data.capacity(), bufferBuilder.limit());
        assertNotSame(originalHeaderBuffer, header.buffer());
        assertSame(bufferBuilder.headerBuffer(), header.buffer());
        assertNotSame(bufferBuilder.headerBuffer(), bufferBuilder.buffer());
        assertEquals(0, header.offset());
        assertEquals(2, header.initialTermId());
        assertEquals(3, header.positionBitsToShift());
        assertEquals(bufferBuilder.limit() + HEADER_LENGTH, header.frameLength());
        assertEquals((byte)(flags | BEGIN_FRAG_FLAG), header.flags());
        assertNotEquals(0, header.flags() & BEGIN_FRAG_FLAG);
        assertEquals(48, header.termOffset());
        assertEquals(999, header.sessionId());
        assertEquals(4, header.streamId());
        assertEquals(39, header.termId());
        assertEquals(reservedValue, header.reservedValue());
        assertEquals((byte)0xC, bufferBuilder.headerBuffer().getByte(VERSION_FIELD_OFFSET));
        assertEquals((short)HDR_TYPE_ATS_SETUP, bufferBuilder.headerBuffer().getShort(TYPE_FIELD_OFFSET));
    }

    @Test
    void shouldThrowAnExceptionIfPrepareCompleteHeaderIsCalledWithNonLastFragment()
    {
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[64], 32, 32);
        headerFlyweight.flags((byte)0b1011_1111);
        final Header header = new Header(0, 16);
        header.buffer(headerFlyweight);

        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> bufferBuilder.prepareCompleteHeader(header));
        assertEquals("invalid frame, i.e. the (01000000) bit must be set in flags, got: 10111111",
            exception.getMessage());
    }

    @Test
    void shouldThrowAnExceptionIfCaptureFirstHeaderIsCalledWithInvalidFlags()
    {
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(new byte[64], 32, 32);
        headerFlyweight.flags((short)(END_FRAG_FLAG | EOS_FLAG | 3));
        final Header header = new Header(0, 16);
        header.buffer(headerFlyweight);

        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> bufferBuilder.captureFirstHeader(header));
        assertEquals("invalid frame, i.e. the (10000000) bit must be set in flags, got: 01100011",
            exception.getMessage());
    }

    @Test
    void compactEmptyBufferIsAnOp()
    {
        assertSame(bufferBuilder, bufferBuilder.compact());

        assertEquals(0, bufferBuilder.capacity());
        assertEquals(0, bufferBuilder.limit());
        assertEquals(0, bufferBuilder.buffer().capacity());
    }

    @Test
    void compactIsANoOpIfTheCapacityIsNotDecreasing()
    {
        assertSame(bufferBuilder, bufferBuilder.append(new UnsafeBuffer(new byte[5]), 0, 3));
        assertEquals(3, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        final MutableDirectBuffer originalBuffer = bufferBuilder.buffer();
        assertEquals(INIT_MIN_CAPACITY, originalBuffer.capacity());

        assertSame(bufferBuilder, bufferBuilder.compact());

        assertEquals(3, bufferBuilder.limit());
        assertEquals(INIT_MIN_CAPACITY, bufferBuilder.capacity());
        assertSame(originalBuffer, bufferBuilder.buffer());
        assertEquals(INIT_MIN_CAPACITY, originalBuffer.capacity());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldNotChangeHeaderBufferUponResize(final boolean isDirect)
    {
        final BufferBuilder builder = new BufferBuilder(0, isDirect);
        final UnsafeBuffer headerBuffer = builder.headerBuffer();
        final ByteBuffer headerByteBuffer = headerBuffer.byteBuffer();
        final byte[] headerByteArray = headerBuffer.byteArray();
        headerBuffer.setMemory(0, headerBuffer.capacity(), (byte)0xFA);

        final UnsafeBuffer src = new UnsafeBuffer(new byte[100]);
        ThreadLocalRandom.current().nextBytes(src.byteArray());
        builder.append(src, 0, src.capacity());

        assertSame(headerBuffer, builder.headerBuffer());
        assertSame(headerByteBuffer, builder.headerBuffer().byteBuffer());
        assertSame(headerByteArray, builder.headerBuffer().byteArray());
        for (int i = 0; i < headerBuffer.capacity(); i++)
        {
            assertEquals((byte)0xFA, headerBuffer.getByte(i));
        }
    }
}
