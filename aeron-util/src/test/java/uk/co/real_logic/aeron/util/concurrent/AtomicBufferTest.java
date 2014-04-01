/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util.concurrent;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.util.BitUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class AtomicBufferTest
{
    private static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();
    private static final int BUFFER_CAPACITY = 4096;
    private static final int INDEX = 8;

    private static final byte BYTE_VALUE = 1;
    private static final short SHORT_VALUE = Byte.MAX_VALUE + 2;
    private static final int INT_VALUE = Short.MAX_VALUE + 3;
    private static final float FLOAT_VALUE = Short.MAX_VALUE + 4.0f;
    private static final long LONG_VALUE = Integer.MAX_VALUE + 5L;
    private static final double DOUBLE_VALUE = Integer.MAX_VALUE + 7.0d;

    @DataPoint
    public static final AtomicBuffer BYTE_ARRAY_BACKED = new AtomicBuffer(new byte[BUFFER_CAPACITY]);

    @DataPoint
    public static final AtomicBuffer HEAP_BYTE_BUFFER = new AtomicBuffer(ByteBuffer.allocate(BUFFER_CAPACITY));

    @DataPoint
    public static final AtomicBuffer DIRECT_BYTE_BUFFER = new AtomicBuffer(ByteBuffer.allocateDirect(BUFFER_CAPACITY));

    @DataPoint
    public static final AtomicBuffer HEAP_BYTE_BUFFER_SLICE =
        new AtomicBuffer(((ByteBuffer)(ByteBuffer.allocate(BUFFER_CAPACITY * 2).position(BUFFER_CAPACITY))).slice());

    @Theory
    public void shouldGetCapacity(final AtomicBuffer buffer)
    {
        assertThat(valueOf(buffer.capacity()), is(valueOf(BUFFER_CAPACITY)));
    }

    @Theory
    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldThrowExceptionForLimitAboveCapacity(final AtomicBuffer buffer)
    {
        final int position = BUFFER_CAPACITY + 1;
        buffer.checkLimit(position);
    }

    @Theory
    public void shouldCopyMemory(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "xxxxxxxxxxx".getBytes();

        buffer.setMemory(0, testBytes.length, (byte)'x');

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldGetLongFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(buffer.getLong(INDEX, BYTE_ORDER)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldPutLongToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putLong(INDEX, LONG_VALUE, BYTE_ORDER);

        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldGetLongFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(buffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldPutLongToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLong(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldGetLongVolatileFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(buffer.getLongVolatile(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldPutLongVolatileToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLongVolatile(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldPutLongOrderedToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLongOrdered(INDEX, LONG_VALUE);

        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE)));
    }

    @Theory
    public void shouldCompareAndSetLongToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertTrue(buffer.compareAndSetLong(INDEX, LONG_VALUE, LONG_VALUE + 1));

        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE + 1)));
    }

    @Theory
    public void shouldGetAndSetLongToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        final long afterValue = 1;
        final long beforeValue = buffer.getAndSetLong(INDEX, afterValue);

        assertThat(Long.valueOf(beforeValue), is(Long.valueOf(LONG_VALUE)));
        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(afterValue)));
    }

    @Theory
    public void shouldGetAndAddLongToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        final long delta = 1;
        final long beforeValue = buffer.getAndAddLong(INDEX, delta);

        assertThat(Long.valueOf(beforeValue), is(Long.valueOf(LONG_VALUE)));
        assertThat(Long.valueOf(duplicateBuffer.getLong(INDEX)), is(Long.valueOf(LONG_VALUE + delta)));
    }

    @Theory
    public void shouldGetIntFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(valueOf(buffer.getInt(INDEX, BYTE_ORDER)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldPutIntToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putInt(INDEX, INT_VALUE);

        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldGetIntFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(valueOf(buffer.getInt(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldPutIntToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putInt(INDEX, INT_VALUE, BYTE_ORDER);

        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldGetIntVolatileFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(valueOf(buffer.getIntVolatile(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldPutIntVolatileToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putIntVolatile(INDEX, INT_VALUE);

        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldPutIntOrderedToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putIntOrdered(INDEX, INT_VALUE);

        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE)));
    }

    @Theory
    public void shouldCompareAndSetIntToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertTrue(buffer.compareAndSetInt(INDEX, INT_VALUE, INT_VALUE + 1));

        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE + 1)));
    }

    @Theory
    public void shouldGetAndSetIntToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        final int afterValue = 1;
        final int beforeValue = buffer.getAndSetInt(INDEX, afterValue);

        assertThat(valueOf(beforeValue), is(valueOf(INT_VALUE)));
        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(afterValue)));
    }

    @Theory
    public void shouldGetAndAddIntToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        final int delta = 1;
        final int beforeValue = buffer.getAndAddInt(INDEX, delta);

        assertThat(valueOf(beforeValue), is(valueOf(INT_VALUE)));
        assertThat(valueOf(duplicateBuffer.getInt(INDEX)), is(valueOf(INT_VALUE + delta)));
    }

    @Theory
    public void shouldGetShortFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(Short.valueOf(buffer.getShort(INDEX, BYTE_ORDER)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldPutShortToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putShort(INDEX, SHORT_VALUE, BYTE_ORDER);

        assertThat(Short.valueOf(duplicateBuffer.getShort(INDEX)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldGetShortFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(Short.valueOf(buffer.getShort(INDEX)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldPutShortToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putShort(INDEX, SHORT_VALUE);

        assertThat(Short.valueOf(duplicateBuffer.getShort(INDEX)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldGetShortVolatileFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(Short.valueOf(buffer.getShortVolatile(INDEX)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldPutShortVolatileToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putShortVolatile(INDEX, SHORT_VALUE);

        assertThat(Short.valueOf(duplicateBuffer.getShort(INDEX)), is(Short.valueOf(SHORT_VALUE)));
    }

    @Theory
    public void shouldGetDoubleFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(Double.valueOf(buffer.getDouble(INDEX, BYTE_ORDER)), is(Double.valueOf(DOUBLE_VALUE)));
    }

    @Theory
    public void shouldPutDoubleToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putDouble(INDEX, DOUBLE_VALUE, BYTE_ORDER);

        assertThat(Double.valueOf(duplicateBuffer.getDouble(INDEX)), is(Double.valueOf(DOUBLE_VALUE)));
    }

    @Theory
    public void shouldGetDoubleFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(Double.valueOf(buffer.getDouble(INDEX)), is(Double.valueOf(DOUBLE_VALUE)));
    }

    @Theory
    public void shouldPutDoubleToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(Double.valueOf(duplicateBuffer.getDouble(INDEX)), is(Double.valueOf(DOUBLE_VALUE)));
    }

    @Theory
    public void shouldGetFloatFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(Float.valueOf(buffer.getFloat(INDEX, BYTE_ORDER)), is(Float.valueOf(FLOAT_VALUE)));
    }

    @Theory
    public void shouldPutFloatToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putFloat(INDEX, FLOAT_VALUE, BYTE_ORDER);

        assertThat(Float.valueOf(duplicateBuffer.getFloat(INDEX)), is(Float.valueOf(FLOAT_VALUE)));
    }

    @Theory
    public void shouldGetFloatFromNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(Float.valueOf(buffer.getFloat(INDEX)), is(Float.valueOf(FLOAT_VALUE)));
    }

    @Theory
    public void shouldPutFloatToNativeBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(Float.valueOf(duplicateBuffer.getFloat(INDEX)), is(Float.valueOf(FLOAT_VALUE)));
    }

    @Theory
    public void shouldGetByteFromBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.put(INDEX, BYTE_VALUE);

        assertThat(Byte.valueOf(buffer.getByte(INDEX)), is(Byte.valueOf(BYTE_VALUE)));
    }

    @Theory
    public void shouldPutByteToBuffer(final AtomicBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putByte(INDEX, BYTE_VALUE);

        assertThat(Byte.valueOf(duplicateBuffer.get(INDEX)), is(Byte.valueOf(BYTE_VALUE)));
    }

    @Theory
    public void shouldGetByteArrayFromBuffer(final AtomicBuffer buffer)
    {
        final byte[] testArray = {'H', 'e', 'l', 'l', 'o'};

        int i = INDEX;
        for (final byte v : testArray)
        {
            buffer.putByte(i, v);
            i += BitUtil.SIZE_OF_BYTE;
        }

        final byte[] result = new byte[testArray.length];
        buffer.getBytes(INDEX, result);

        assertThat(result, is(testArray));
    }

    @Theory
    public void shouldGetBytesFromBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final byte[] buff = new byte[testBytes.length];
        buffer.getBytes(INDEX, buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldGetBytesFromBufferToBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final ByteBuffer dstBuffer = ByteBuffer.allocate(testBytes.length);
        buffer.getBytes(INDEX, dstBuffer, testBytes.length);

        assertThat(dstBuffer.array(), is(testBytes));
    }

    @Theory
    public void shouldGetBytesFromBufferToAtomicBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final ByteBuffer dstBuffer = ByteBuffer.allocateDirect(testBytes.length);
        buffer.getBytes(INDEX, dstBuffer, testBytes.length);

        dstBuffer.flip();
        byte[] result = new byte[testBytes.length];
        dstBuffer.get(result);

        assertThat(result, is(testBytes));
    }

    @Theory
    public void shouldGetBytesFromBufferToSlice(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final ByteBuffer dstBuffer = ((ByteBuffer)ByteBuffer.allocate(testBytes.length * 2).position(testBytes.length))
                                                            .slice();

        buffer.getBytes(INDEX, dstBuffer, testBytes.length);

        dstBuffer.flip();
        byte[] result = new byte[testBytes.length];
        dstBuffer.get(result);

        assertThat(result, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        buffer.putBytes(INDEX, testBytes);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToBufferFromBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ByteBuffer.wrap(testBytes);

        buffer.putBytes(INDEX, srcBuffer, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToBufferFromAtomicBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ByteBuffer.allocateDirect(testBytes.length);
        srcBuffer.put(testBytes).flip();

        buffer.putBytes(INDEX, srcBuffer, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToBufferFromSlice(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ((ByteBuffer)ByteBuffer.allocate(testBytes.length * 2).position(testBytes.length))
                                                            .slice();
        srcBuffer.put(testBytes).flip();

        buffer.putBytes(INDEX, srcBuffer, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToAtomicBufferFromAtomicBuffer(final AtomicBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ByteBuffer.allocateDirect(testBytes.length);
        srcBuffer.put(testBytes).flip();

        final AtomicBuffer srcAtomicBuffer = new AtomicBuffer(srcBuffer);

        buffer.putBytes(INDEX, srcAtomicBuffer, 0, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }
}
