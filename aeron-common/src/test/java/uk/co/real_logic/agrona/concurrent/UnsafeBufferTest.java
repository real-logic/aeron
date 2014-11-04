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
package uk.co.real_logic.agrona.concurrent;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.agrona.BitUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class UnsafeBufferTest
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
    public static final UnsafeBuffer BYTE_ARRAY_BACKED = new UnsafeBuffer(new byte[BUFFER_CAPACITY]);

    @DataPoint
    public static final UnsafeBuffer HEAP_BYTE_BUFFER = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_CAPACITY));

    @DataPoint
    public static final UnsafeBuffer DIRECT_BYTE_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_CAPACITY));

    @DataPoint
    public static final UnsafeBuffer HEAP_BYTE_BUFFER_SLICE = new UnsafeBuffer(
        ((ByteBuffer)(ByteBuffer.allocate(BUFFER_CAPACITY * 2).position(BUFFER_CAPACITY))).slice());

    @Theory
    public void shouldGetCapacity(final UnsafeBuffer buffer)
    {
        assertThat(buffer.capacity(), is(BUFFER_CAPACITY));
    }

    @Theory
    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldThrowExceptionForLimitAboveCapacity(final UnsafeBuffer buffer)
    {
        final int position = BUFFER_CAPACITY + 1;
        buffer.checkLimit(position);
    }

    @Theory
    public void shouldCopyMemory(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "xxxxxxxxxxx".getBytes();

        buffer.setMemory(0, testBytes.length, (byte)'x');

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldGetLongFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(buffer.getLong(INDEX, BYTE_ORDER), is(LONG_VALUE));
    }

    @Theory
    public void shouldPutLongToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putLong(INDEX, LONG_VALUE, BYTE_ORDER);

        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldGetLongFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(buffer.getLong(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldPutLongToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLong(INDEX, LONG_VALUE);

        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldGetLongVolatileFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertThat(buffer.getLongVolatile(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldPutLongVolatileToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLongVolatile(INDEX, LONG_VALUE);

        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldPutLongOrderedToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putLongOrdered(INDEX, LONG_VALUE);

        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE));
    }

    @Theory
    public void shouldAddLongOrderedToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        final long initialValue = Integer.MAX_VALUE + 7L;
        final long increment = 9L;
        buffer.putLongOrdered(INDEX, initialValue);
        buffer.addLongOrdered(INDEX, increment);

        assertThat(duplicateBuffer.getLong(INDEX), is(initialValue + increment));
    }

    @Theory
    public void shouldCompareAndSetLongToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        assertTrue(buffer.compareAndSetLong(INDEX, LONG_VALUE, LONG_VALUE + 1));

        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE + 1));
    }

    @Theory
    public void shouldGetAndSetLongToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        final long afterValue = 1;
        final long beforeValue = buffer.getAndSetLong(INDEX, afterValue);

        assertThat(beforeValue, is(LONG_VALUE));
        assertThat(duplicateBuffer.getLong(INDEX), is(afterValue));
    }

    @Theory
    public void shouldGetAndAddLongToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putLong(INDEX, LONG_VALUE);

        final long delta = 1;
        final long beforeValue = buffer.getAndAddLong(INDEX, delta);

        assertThat(beforeValue, is(LONG_VALUE));
        assertThat(duplicateBuffer.getLong(INDEX), is(LONG_VALUE + delta));
    }

    @Theory
    public void shouldGetIntFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(buffer.getInt(INDEX, BYTE_ORDER), is(INT_VALUE));
    }

    @Theory
    public void shouldPutIntToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putInt(INDEX, INT_VALUE);

        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldGetIntFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(buffer.getInt(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldPutIntToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putInt(INDEX, INT_VALUE, BYTE_ORDER);

        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldGetIntVolatileFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertThat(buffer.getIntVolatile(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldPutIntVolatileToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putIntVolatile(INDEX, INT_VALUE);

        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldPutIntOrderedToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putIntOrdered(INDEX, INT_VALUE);

        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE));
    }

    @Theory
    public void shouldAddIntOrderedToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        final int initialValue = 7;
        final int increment = 9;
        buffer.putIntOrdered(INDEX, initialValue);
        buffer.addIntOrdered(INDEX, increment);

        assertThat(duplicateBuffer.getInt(INDEX), is(initialValue + increment));
    }

    @Theory
    public void shouldCompareAndSetIntToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        assertTrue(buffer.compareAndSetInt(INDEX, INT_VALUE, INT_VALUE + 1));

        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE + 1));
    }

    @Theory
    public void shouldGetAndSetIntToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        final int afterValue = 1;
        final int beforeValue = buffer.getAndSetInt(INDEX, afterValue);

        assertThat(beforeValue, is(INT_VALUE));
        assertThat(duplicateBuffer.getInt(INDEX), is(afterValue));
    }

    @Theory
    public void shouldGetAndAddIntToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putInt(INDEX, INT_VALUE);

        final int delta = 1;
        final int beforeValue = buffer.getAndAddInt(INDEX, delta);

        assertThat(beforeValue, is(INT_VALUE));
        assertThat(duplicateBuffer.getInt(INDEX), is(INT_VALUE + delta));
    }

    @Theory
    public void shouldGetShortFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(buffer.getShort(INDEX, BYTE_ORDER), is(SHORT_VALUE));
    }

    @Theory
    public void shouldPutShortToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putShort(INDEX, SHORT_VALUE, BYTE_ORDER);

        assertThat(duplicateBuffer.getShort(INDEX), is(SHORT_VALUE));
    }

    @Theory
    public void shouldGetShortFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(buffer.getShort(INDEX), is(SHORT_VALUE));
    }

    @Theory
    public void shouldPutShortToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putShort(INDEX, SHORT_VALUE);

        assertThat(duplicateBuffer.getShort(INDEX), is(SHORT_VALUE));
    }

    @Theory
    public void shouldGetShortVolatileFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putShort(INDEX, SHORT_VALUE);

        assertThat(buffer.getShortVolatile(INDEX), is(SHORT_VALUE));
    }

    @Theory
    public void shouldPutShortVolatileToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putShortVolatile(INDEX, SHORT_VALUE);

        assertThat(duplicateBuffer.getShort(INDEX), is(SHORT_VALUE));
    }

    @Theory
    public void shouldGetDoubleFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(buffer.getDouble(INDEX, BYTE_ORDER), is(DOUBLE_VALUE));
    }

    @Theory
    public void shouldPutDoubleToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putDouble(INDEX, DOUBLE_VALUE, BYTE_ORDER);

        assertThat(duplicateBuffer.getDouble(INDEX), is(DOUBLE_VALUE));
    }

    @Theory
    public void shouldGetDoubleFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(buffer.getDouble(INDEX), is(DOUBLE_VALUE));
    }

    @Theory
    public void shouldPutDoubleToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putDouble(INDEX, DOUBLE_VALUE);

        assertThat(duplicateBuffer.getDouble(INDEX), is(DOUBLE_VALUE));
    }

    @Theory
    public void shouldGetFloatFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(buffer.getFloat(INDEX, BYTE_ORDER), is(FLOAT_VALUE));
    }

    @Theory
    public void shouldPutFloatToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putFloat(INDEX, FLOAT_VALUE, BYTE_ORDER);

        assertThat(duplicateBuffer.getFloat(INDEX), is(FLOAT_VALUE));
    }

    @Theory
    public void shouldGetFloatFromNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        duplicateBuffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(buffer.getFloat(INDEX), is(FLOAT_VALUE));
    }

    @Theory
    public void shouldPutFloatToNativeBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(ByteOrder.nativeOrder());

        buffer.putFloat(INDEX, FLOAT_VALUE);

        assertThat(duplicateBuffer.getFloat(INDEX), is(FLOAT_VALUE));
    }

    @Theory
    public void shouldGetByteFromBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        duplicateBuffer.put(INDEX, BYTE_VALUE);

        assertThat(buffer.getByte(INDEX), is(BYTE_VALUE));
    }

    @Theory
    public void shouldPutByteToBuffer(final UnsafeBuffer buffer)
    {
        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);

        buffer.putByte(INDEX, BYTE_VALUE);

        assertThat(duplicateBuffer.get(INDEX), is(BYTE_VALUE));
    }

    @Theory
    public void shouldGetByteArrayFromBuffer(final UnsafeBuffer buffer)
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
    public void shouldGetBytesFromBuffer(final UnsafeBuffer buffer)
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
    public void shouldGetBytesFromBufferToBuffer(final UnsafeBuffer buffer)
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
    public void shouldGetBytesFromBufferToAtomicBuffer(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final ByteBuffer dstBuffer = ByteBuffer.allocateDirect(testBytes.length);
        buffer.getBytes(INDEX, dstBuffer, testBytes.length);

        dstBuffer.flip();
        final byte[] result = new byte[testBytes.length];
        dstBuffer.get(result);

        assertThat(result, is(testBytes));
    }

    @Theory
    public void shouldGetBytesFromBufferToSlice(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);
        duplicateBuffer.put(testBytes);

        final ByteBuffer dstBuffer =
            ((ByteBuffer)ByteBuffer.allocate(testBytes.length * 2).position(testBytes.length)).slice();

        buffer.getBytes(INDEX, dstBuffer, testBytes.length);

        dstBuffer.flip();
        final byte[] result = new byte[testBytes.length];
        dstBuffer.get(result);

        assertThat(result, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToBuffer(final UnsafeBuffer buffer)
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
    public void shouldPutBytesToBufferFromBuffer(final UnsafeBuffer buffer)
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
    public void shouldPutBytesToBufferFromAtomicBuffer(final UnsafeBuffer buffer)
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
    public void shouldPutBytesToBufferFromSlice(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ((ByteBuffer)ByteBuffer.allocate(testBytes.length * 2).position(testBytes.length)).slice();
        srcBuffer.put(testBytes).flip();

        buffer.putBytes(INDEX, srcBuffer, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldPutBytesToAtomicBufferFromAtomicBuffer(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ByteBuffer.allocateDirect(testBytes.length);
        srcBuffer.put(testBytes).flip();

        final UnsafeBuffer srcUnsafeBuffer = new UnsafeBuffer(srcBuffer);

        buffer.putBytes(INDEX, srcUnsafeBuffer, 0, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }

    @Theory
    public void shouldGetBytesIntoAtomicBufferFromAtomicBuffer(final UnsafeBuffer buffer)
    {
        final byte[] testBytes = "Hello World".getBytes();
        final ByteBuffer srcBuffer = ByteBuffer.allocateDirect(testBytes.length);
        srcBuffer.put(testBytes).flip();

        final UnsafeBuffer srcUnsafeBuffer = new UnsafeBuffer(srcBuffer);

        srcUnsafeBuffer.getBytes(0, buffer, INDEX, testBytes.length);

        final ByteBuffer duplicateBuffer = buffer.duplicateByteBuffer().order(BYTE_ORDER);
        duplicateBuffer.position(INDEX);

        final byte[] buff = new byte[testBytes.length];
        duplicateBuffer.get(buff);

        assertThat(buff, is(testBytes));
    }
}
