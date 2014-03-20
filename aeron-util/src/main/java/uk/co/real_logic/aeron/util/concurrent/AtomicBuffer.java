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

import sun.misc.Unsafe;
import uk.co.real_logic.aeron.util.UnsafeAccess;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Supports regular, byte ordered, and atomic (memory ordered) access to an underlying buffer.
 * The buffer can be a byte[] or one of the various {@link ByteBuffer} implementations.
 */
public class AtomicBuffer
{
    private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
    private static final long ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private byte[] byteArray;
    private ByteBuffer byteBuffer;
    private long addressOffset;
    private int capacity;

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    public AtomicBuffer(final byte[] buffer)
    {
        wrap(buffer);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public AtomicBuffer(final ByteBuffer buffer)
    {
        wrap(buffer);
    }

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    public void wrap(final byte[] buffer)
    {
        addressOffset = ARRAY_BASE_OFFSET;
        capacity = buffer.length;
        byteArray = buffer;
        byteBuffer = null;
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public void wrap(final ByteBuffer buffer)
    {
        byteBuffer = buffer;

        if (buffer.hasArray())
        {
            byteArray = buffer.array();
            addressOffset = ARRAY_BASE_OFFSET + buffer.arrayOffset();
        }
        else
        {
            byteArray = null;
            addressOffset = ((sun.nio.ch.DirectBuffer)buffer).address();
        }

        capacity = buffer.capacity();
    }

    /**
     * Set a region of memory to a given byte value.
     *
     * @param index at which to start.
     * @param length of the run of bytes to set.
     * @param value the memory will be set to.
     */
    public void setMemory(final int index, final int length, final byte value)
    {
        UNSAFE.setMemory(byteArray, addressOffset + index, length, value);
    }

    /**
     * Get the capacity of the underlying buffer.
     *
     * @return the capacity of the underlying buffer in bytes.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * Check that a given limit is not greater than the capacity of a buffer from a given offset.
     *
     * Can be overridden in a DirectBuffer subclass to enable an extensible buffer or handle retry after a flush.
     *
     * @param limit up to which access is required.
     * @throws IndexOutOfBoundsException if limit is beyond buffer capacity.
     */
    public void checkLimit(final int limit)
    {
        if (limit > capacity)
        {
            final String msg = String.format("limit=%d is beyond capacity=%d",
                                             Integer.valueOf(limit),
                                             Integer.valueOf(capacity));

            throw new IndexOutOfBoundsException(msg);
        }
    }

    /**
     * Create a duplicate {@link ByteBuffer} for the view in native byte order.
     * The duplicate {@link ByteBuffer} shares the underlying memory so all changes are reflected.
     * If no {@link ByteBuffer} is attached then one will be created.
     *
     * @return a duplicate of the underlying {@link ByteBuffer}
     */
    public ByteBuffer duplicateByteBuffer()
    {
        if (null == byteBuffer)
        {
            return ByteBuffer.wrap(byteArray);
        }
        else
        {
            final ByteBuffer duplicate = byteBuffer.duplicate();
            duplicate.clear();

            return duplicate;
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value for at a given index
     */
    public long getLong(final int index, final ByteOrder byteOrder)
    {
        long bits = UNSAFE.getLong(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Long.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     * @param byteOrder of the value when written
     */
    public void putLong(final int index, final long value, final ByteOrder byteOrder)
    {
        long bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLong(byteArray, addressOffset + index, bits);
    }

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.

     * @return the value for at a given index
     */
    public long getLong(final int index)
    {
        return UNSAFE.getLong(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putLong(final int index, final long value)
    {
        UNSAFE.putLong(byteArray, addressOffset + index, value);
    }

    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index in bytes from which to get.
     * @return the value for at a given index
     */
    public long getLongVolatile(final int index)
    {
        return UNSAFE.getLongVolatile(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putLongVolatile(final int index, final long value)
    {
        UNSAFE.putLongVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * Put a value to a given index with ordered semantics.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putLongOrdered(final int index, final long value)
    {
        UNSAFE.putOrderedLong(byteArray, addressOffset + index, value);
    }

    /**
     * Atomic compare and set of a long given an expected value.
     *
     * @param index in bytes for where to put.
     * @param expectedValue at to be compared
     * @param updateValue to be exchanged
     */
    public boolean compareAndSetLong(final int index, final long expectedValue, final long updateValue)
    {
        return UNSAFE.compareAndSwapLong(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    /**
     * Atomically exchange a value at a location returning the previous contents.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public long getAndSetLong(final int index, final long value)
    {
        return UNSAFE.getAndSetLong(byteArray, addressOffset + index, value);
    }

    /**
     * Atomically add a delta to a value at a location returning the previous contents.
     * To decrement a negative delta can be provided.
     *
     * @param index in bytes for where to put.
     * @param delta to be added to the value at the index
     */
    public long getAndAddLong(final int index, final long delta)
    {
        return UNSAFE.getAndAddLong(byteArray, addressOffset + index, delta);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public int getInt(final int index, final ByteOrder byteOrder)
    {
        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     * @param byteOrder of the value when written
     */
    public void putInt(final int index, final int value, final ByteOrder byteOrder)
    {
        int bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
    }

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.

     * @return the value for at a given index
     */
    public int getInt(final int index)
    {
        return UNSAFE.getInt(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putInt(final int index, final int value)
    {
        UNSAFE.putInt(byteArray, addressOffset + index, value);
    }

    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index in bytes from which to get.
     * @return the value for at a given index
     */
    public int getIntVolatile(final int index)
    {
        return UNSAFE.getIntVolatile(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putIntVolatile(final int index, final int value)
    {
        UNSAFE.putIntVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * Put a value to a given index with ordered semantics.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putIntOrdered(final int index, final int value)
    {
        UNSAFE.putOrderedInt(byteArray, addressOffset + index, value);
    }

    /**
     * Atomic compare and set of a int given an expected value.
     *
     * @param index in bytes for where to put.
     * @param expectedValue at to be compared
     * @param updateValue to be exchanged
     */
    public boolean compareAndSetInt(final int index, final int expectedValue, final int updateValue)
    {
        return UNSAFE.compareAndSwapInt(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    /**
     * Atomically exchange a value at a location returning the previous contents.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public int getAndSetInt(final int index, final int value)
    {
        return UNSAFE.getAndSetInt(byteArray, addressOffset + index, value);
    }

    /**
     * Atomically add a delta to a value at a location returning the previous contents.
     * To decrement a negative delta can be provided.
     *
     * @param index in bytes for where to put.
     * @param delta to be added to the value at the index
     */
    public int getAndAddInt(final int index, final int delta)
    {
        return UNSAFE.getAndAddInt(byteArray, addressOffset + index, delta);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public double getDouble(final int index, final ByteOrder byteOrder)
    {
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            long bits = UNSAFE.getLong(byteArray, addressOffset + index);
            return Double.longBitsToDouble(Long.reverseBytes(bits));
        }
        else
        {
            return UNSAFE.getDouble(byteArray, addressOffset + index);
        }
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     * @param byteOrder of the value when written.
     */
    public void putDouble(final int index, final double value, final ByteOrder byteOrder)
    {
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            long bits = Long.reverseBytes(Double.doubleToRawLongBits(value));
            UNSAFE.putLong(byteArray, addressOffset + index, bits);
        }
        else
        {
            UNSAFE.putDouble(byteArray, addressOffset + index, value);
        }
    }

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    public double getDouble(final int index)
    {
        return UNSAFE.getDouble(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     */
    public void putDouble(final int index, final double value)
    {
        UNSAFE.putDouble(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public float getFloat(final int index, final ByteOrder byteOrder)
    {
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            int bits = UNSAFE.getInt(byteArray, addressOffset + index);
            return Float.intBitsToFloat(Integer.reverseBytes(bits));
        }
        else
        {
            return UNSAFE.getFloat(byteArray, addressOffset + index);
        }
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     * @param byteOrder of the value when written.
     */
    public void putFloat(final int index, final float value, final ByteOrder byteOrder)
    {
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            int bits = Integer.reverseBytes(Float.floatToRawIntBits(value));
            UNSAFE.putLong(byteArray, addressOffset + index, bits);
        }
        else
        {
            UNSAFE.putFloat(byteArray, addressOffset + index, value);
        }
    }

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    public float getFloat(final int index)
    {
        return UNSAFE.getFloat(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     */
    public void putFloat(final int index, final float value)
    {
        UNSAFE.putFloat(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public short getShort(final int index, final ByteOrder byteOrder)
    {
        short bits = UNSAFE.getShort(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Short.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     * @param byteOrder of the value when written.
     */
    public void putShort(final int index, final short value, final ByteOrder byteOrder)
    {
        short bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Short.reverseBytes(bits);
        }

        UNSAFE.putShort(byteArray, addressOffset + index, bits);
    }

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    public short getShort(final int index)
    {
        return UNSAFE.getShort(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     */
    public void putShort(final int index, final short value)
    {
        UNSAFE.putShort(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index in bytes from which to get.
     * @return the value at a given index.
     */
    public byte getByte(final int index)
    {
        return UNSAFE.getByte(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index.
     *
     * @param index in bytes for where to put.
     * @param value to be written
     */
    public void putByte(final int index, final byte value)
    {
        UNSAFE.putByte(byteArray, addressOffset + index, value);
    }

    /**
     * Get from the underlying buffer into a supplied byte array.
     * This method will try to fill the supplied byte array.
     *
     * @param index in the underlying buffer to start from.
     * @param dst   into which the dst will be copied.
     * @return count of bytes copied.
     */
    public int getBytes(final int index, final byte[] dst)
    {
        return getBytes(index, dst, 0, dst.length);
    }

    /**
     * Get bytes from the underlying buffer into a supplied byte array.
     *
     * @param index  in the underlying buffer to start from.
     * @param dst    into which the bytes will be copied.
     * @param offset in the supplied buffer to start the copy
     * @param length of the supplied buffer to use.
     * @return count of bytes copied.
     */
    public int getBytes(final int index, final byte[] dst, final int offset, final int length)
    {
        final int count = Math.min(length, capacity - index);
        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET + offset, count);

        return count;
    }

    /**
     * Get from the underlying buffer into a supplied {@link ByteBuffer}.
     *
     * @param index     in the underlying buffer to start from.
     * @param dstBuffer into which the bytes will be copied.
     * @param length    of the supplied buffer to use.
     * @return count of bytes copied.
     */
    public int getBytes(final int index, final ByteBuffer dstBuffer, final int length)
    {
        int count = Math.min(dstBuffer.remaining(), capacity - index);
        count = Math.min(count, length);

        final int dstOffset = dstBuffer.position();
        final byte[] dstByteArray;
        final long dstBaseOffset;
        if (dstBuffer.hasArray())
        {
            dstByteArray = dstBuffer.array();
            dstBaseOffset = ARRAY_BASE_OFFSET + dstBuffer.arrayOffset();
        }
        else
        {
            dstByteArray = null;
            dstBaseOffset = ((sun.nio.ch.DirectBuffer)dstBuffer).address();
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dstByteArray, dstBaseOffset + dstOffset, count);
        dstBuffer.position(dstBuffer.position() + count);

        return count;
    }

    /**
     * Put an array of src into the underlying buffer.
     *
     * @param index in the underlying buffer to start from.
     * @param src   to be copied to the underlying buffer.
     * @return count of bytes copied.
     */
    public int putBytes(final int index, final byte[] src)
    {
        return putBytes(index, src, 0, src.length);
    }

    /**
     * Put an array into the underlying buffer.
     *
     * @param index  in the underlying buffer to start from.
     * @param src    to be copied to the underlying buffer.
     * @param offset in the supplied buffer to begin the copy.
     * @param length of the supplied buffer to copy.
     * @return count of bytes copied.
     */
    public int putBytes(final int index, final byte[] src, final int offset, final int length)
    {
        final int count = Math.min(length, capacity - index);
        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray, addressOffset + index,  count);

        return count;
    }

    /**
     * Put bytes into the underlying buffer for the view.  Bytes will be copied from current
     * {@link java.nio.ByteBuffer#position()} to {@link java.nio.ByteBuffer#limit()}.
     *
     * @param index     in the underlying buffer to start from.
     * @param srcBuffer to copy the bytes from.
     * @return count of bytes copied.
     */
    public int putBytes(final int index, final ByteBuffer srcBuffer, final int length)
    {
        int count = Math.min(srcBuffer.remaining(), capacity - index);
        count = Math.min(count, length);

        final int srcOffset = srcBuffer.position();
        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (srcBuffer.hasArray())
        {
            srcByteArray = srcBuffer.array();
            srcBaseOffset = ARRAY_BASE_OFFSET + srcBuffer.arrayOffset();
        }
        else
        {
            srcByteArray = null;
            srcBaseOffset = ((sun.nio.ch.DirectBuffer)srcBuffer).address();
        }

        UNSAFE.copyMemory(srcByteArray, srcBaseOffset + srcOffset, byteArray, addressOffset + index,  count);
        srcBuffer.position(srcBuffer.position() + count);

        return count;
    }

    /**
     * Put bytes from a source {@link AtomicBuffer} into this {@link AtomicBuffer} at given indices.
     *
     * @param index in this buffer to begin putting the bytes.
     * @param srcBuffer from which the bytes will be copied.
     * @param srcIndex in the source buffer from which the byte copy will begin.
     * @param length of the bytes to be copied.
     */
    public void putBytes(final int index, final AtomicBuffer srcBuffer, final int srcIndex, final int length)
    {
        UNSAFE.copyMemory(srcBuffer.byteArray,
                          srcBuffer.addressOffset + srcIndex,
                          byteArray,
                          addressOffset + index,
                          length);
    }
}
