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
package uk.co.real_logic.aeron.common.concurrent;

import sun.misc.Unsafe;
import uk.co.real_logic.aeron.common.UnsafeAccess;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static uk.co.real_logic.aeron.common.BitUtil.*;

/**
 * Supports regular, byte ordered, and atomic (memory ordered) access to an underlying buffer.
 * The buffer can be a byte[] or one of the various {@link ByteBuffer} implementations.
 */
public class AtomicBuffer
{
    public static final String DISABLE_BOUNDS_CHECKS_PROP_NAME = "aeron.disable.bounds.checks";
    public static final boolean DISABLE_BOUNDS_CHECKS = Boolean.getBoolean(DISABLE_BOUNDS_CHECKS_PROP_NAME);

    private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.UTF_8);
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
     * Attach a view to an off-heap memory region by address.
     *
     * @param address  where the memory begins off-heap
     * @param capacity of the buffer from the given address
     */
    public AtomicBuffer(final long address, final int capacity)
    {
        wrap(address, capacity);
    }

    /**
     * Attach a view to an existing {@link AtomicBuffer}
     *
     * @param buffer to which the view is attached.
     */
    public AtomicBuffer(final AtomicBuffer buffer)
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
     * Attach a view to an off-heap memory region by address.
     *
     * @param address  where the memory begins off-heap
     * @param capacity of the buffer from the given address
     */
    public void wrap(final long address, final int capacity)
    {
        addressOffset = address;
        this.capacity = capacity;
        byteArray = null;
        byteBuffer = null;
    }

    /**
     * Attach a view to an existing {@link AtomicBuffer}
     *
     * @param buffer to which the view is attached.
     */
    public void wrap(final AtomicBuffer buffer)
    {
        addressOffset = buffer.addressOffset;
        capacity = buffer.capacity;
        byteArray = buffer.byteArray;
        byteBuffer = buffer.byteBuffer;
    }

    /**
     * Set a region of memory to a given byte value.
     *
     * @param index  at which to start.
     * @param length of the run of bytes to set.
     * @param value  the memory will be set to.
     */
    public void setMemory(final int index, final int length, final byte value)
    {
        boundsCheck(index, length);

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
            final String msg = String.format("limit=%d is beyond capacity=%d", limit, capacity);
            throw new IndexOutOfBoundsException(msg);
        }
    }

    /**
     * Reads the underlying offset to to the memory address.
     *
     * @return the underlying offset to to the memory address.
     */
    public long addressOffset()
    {
        return addressOffset;
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

    /**
     * Return the underlying {@link ByteBuffer} if one is attached.
     *
     * @return the underlying {@link ByteBuffer} if one is attached.
     */
    public ByteBuffer byteBuffer()
    {
        return byteBuffer;
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index     in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value for at a given index
     */
    public long getLong(final int index, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_LONG);

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
     * @param index     in bytes for where to put.
     * @param value     for at a given index
     * @param byteOrder of the value when written
     */
    public void putLong(final int index, final long value, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_LONG);

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
        boundsCheck(index, SIZE_OF_LONG);

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
        boundsCheck(index, SIZE_OF_LONG);

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
        boundsCheck(index, SIZE_OF_LONG);

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
        boundsCheck(index, SIZE_OF_LONG);

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
        boundsCheck(index, SIZE_OF_LONG);

        UNSAFE.putOrderedLong(byteArray, addressOffset + index, value);
    }

    /**
     * Atomic compare and set of a long given an expected value.
     *
     * @param index         in bytes for where to put.
     * @param expectedValue at to be compared
     * @param updateValue   to be exchanged
     * @return set successful or not
     */
    public boolean compareAndSetLong(final int index, final long expectedValue, final long updateValue)
    {
        boundsCheck(index, SIZE_OF_LONG);

        return UNSAFE.compareAndSwapLong(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    /**
     * Atomically exchange a value at a location returning the previous contents.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     * @return previous value
     */
    public long getAndSetLong(final int index, final long value)
    {
        boundsCheck(index, SIZE_OF_LONG);

        return UNSAFE.getAndSetLong(byteArray, addressOffset + index, value);
    }

    /**
     * Atomically add a delta to a value at a location returning the previous contents.
     * To decrement a negative delta can be provided.
     *
     * @param index in bytes for where to put.
     * @param delta to be added to the value at the index
     * @return previous value
     */
    public long getAndAddLong(final int index, final long delta)
    {
        boundsCheck(index, SIZE_OF_LONG);

        return UNSAFE.getAndAddLong(byteArray, addressOffset + index, delta);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index     in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public int getInt(final int index, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_INT);

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
     * @param index     in bytes for where to put.
     * @param value     to be written
     * @param byteOrder of the value when written
     */
    public void putInt(final int index, final int value, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_INT);

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
        boundsCheck(index, SIZE_OF_INT);

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
        boundsCheck(index, SIZE_OF_INT);

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
        boundsCheck(index, SIZE_OF_INT);

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
        boundsCheck(index, SIZE_OF_INT);

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
        boundsCheck(index, SIZE_OF_INT);

        UNSAFE.putOrderedInt(byteArray, addressOffset + index, value);
    }

    /**
     * Atomic compare and set of a int given an expected value.
     *
     * @param index         in bytes for where to put.
     * @param expectedValue at to be compared
     * @param updateValue   to be exchanged
     * @return successful or not
     */
    public boolean compareAndSetInt(final int index, final int expectedValue, final int updateValue)
    {
        boundsCheck(index, SIZE_OF_INT);

        return UNSAFE.compareAndSwapInt(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    /**
     * Atomically exchange a value at a location returning the previous contents.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     * @return previous value
     */
    public int getAndSetInt(final int index, final int value)
    {
        boundsCheck(index, SIZE_OF_INT);

        return UNSAFE.getAndSetInt(byteArray, addressOffset + index, value);
    }

    /**
     * Atomically add a delta to a value at a location returning the previous contents.
     * To decrement a negative delta can be provided.
     *
     * @param index in bytes for where to put.
     * @param delta to be added to the value at the index
     * @return previous value
     */
    public int getAndAddInt(final int index, final int delta)
    {
        boundsCheck(index, SIZE_OF_INT);

        return UNSAFE.getAndAddInt(byteArray, addressOffset + index, delta);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index     in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public double getDouble(final int index, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_DOUBLE);

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
     * @param index     in bytes for where to put.
     * @param value     to be written
     * @param byteOrder of the value when written.
     */
    public void putDouble(final int index, final double value, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_DOUBLE);

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
        boundsCheck(index, SIZE_OF_DOUBLE);

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
        boundsCheck(index, SIZE_OF_DOUBLE);

        UNSAFE.putDouble(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index     in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public float getFloat(final int index, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_FLOAT);

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
     * @param index     in bytes for where to put.
     * @param value     to be written
     * @param byteOrder of the value when written.
     */
    public void putFloat(final int index, final float value, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_FLOAT);

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
        boundsCheck(index, SIZE_OF_FLOAT);

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
        boundsCheck(index, SIZE_OF_FLOAT);

        UNSAFE.putFloat(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the value at a given index.
     *
     * @param index     in bytes from which to get.
     * @param byteOrder of the value to be read.
     * @return the value at a given index.
     */
    public short getShort(final int index, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_SHORT);

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
     * @param index     in bytes for where to put.
     * @param value     to be written
     * @param byteOrder of the value when written.
     */
    public void putShort(final int index, final short value, final ByteOrder byteOrder)
    {
        boundsCheck(index, SIZE_OF_SHORT);

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
        boundsCheck(index, SIZE_OF_SHORT);

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
        boundsCheck(index, SIZE_OF_SHORT);

        UNSAFE.putShort(byteArray, addressOffset + index, value);
    }

    /**
     * Get the value at a given index with volatile semantics.
     *
     * @param index in bytes from which to get.
     * @return the value for at a given index
     */
    public short getShortVolatile(final int index)
    {
        boundsCheck(index, SIZE_OF_SHORT);

        return UNSAFE.getShortVolatile(byteArray, addressOffset + index);
    }

    /**
     * Put a value to a given index with volatile semantics.
     *
     * @param index in bytes for where to put.
     * @param value for at a given index
     */
    public void putShortVolatile(final int index, final short value)
    {
        boundsCheck(index, SIZE_OF_SHORT);

        UNSAFE.putShortVolatile(byteArray, addressOffset + index, value);
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
        boundsCheck(index, SIZE_OF_BYTE);

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
        boundsCheck(index, SIZE_OF_BYTE);

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
        int count = Math.min(length, capacity - index);
        count = Math.min(count, dst.length);

        boundsCheck(index, count);

        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET + offset, count);

        return count;
    }

    /**
     * Get bytes from this {@link AtomicBuffer} into the provided {@link AtomicBuffer} at given indices.
     *
     * @param index     in this buffer to begin getting the bytes.
     * @param dstBuffer to which the bytes will be copied.
     * @param dstIndex  in the channel buffer to which the byte copy will begin.
     * @param length    of the bytes to be copied.
     */
    public void getBytes(final int index, final AtomicBuffer dstBuffer, final int dstIndex, final int length)
    {
        boundsCheck(index, length);

        dstBuffer.putBytes(dstIndex, this, index, length);
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

        boundsCheck(index, count);

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
        int count = Math.min(length, capacity - index);
        count = Math.min(count, src.length);

        boundsCheck(index, count);

        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray, addressOffset + index, count);

        return count;
    }

    /**
     * Put bytes into the underlying buffer for the view.  Bytes will be copied from current
     * {@link java.nio.ByteBuffer#position()} to {@link java.nio.ByteBuffer#limit()}.
     *
     * @param index     in the underlying buffer to start from.
     * @param srcBuffer to copy the bytes from.
     * @param length    of the supplied buffer to copy.
     * @return count of bytes copied.
     */
    public int putBytes(final int index, final ByteBuffer srcBuffer, final int length)
    {
        int count = Math.min(srcBuffer.remaining(), length);
        count = Math.min(count, capacity - index);

        boundsCheck(index, count);

        count = putBytes(index, srcBuffer, srcBuffer.position(), count);
        srcBuffer.position(srcBuffer.position() + count);

        return count;
    }

    /**
     * Put bytes into the underlying buffer for the view. Bytes will be copied from the buffer index to
     * the buffer index + length.
     *
     * @param index     in the underlying buffer to start from.
     * @param srcBuffer to copy the bytes from (does not change position).
     * @param srcIndex  in the source buffer from which the copy will begin.
     * @param length    of the bytes to be copied.
     * @return count of bytes copied.
     */
    public int putBytes(final int index, final ByteBuffer srcBuffer, final int srcIndex, final int length)
    {
        int count = Math.min(length, capacity - index);
        count = Math.min(count, srcBuffer.capacity() - srcIndex);

        boundsCheck(index, count);

        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (srcBuffer.hasArray())
        {
            srcByteArray = srcBuffer.array();
            srcBaseOffset = ARRAY_BASE_OFFSET + srcBuffer.arrayOffset() + srcIndex;
        }
        else
        {
            srcByteArray = null;
            srcBaseOffset = ((sun.nio.ch.DirectBuffer)srcBuffer).address();
        }

        UNSAFE.copyMemory(srcByteArray, srcBaseOffset + srcIndex, byteArray, addressOffset + index, count);

        return count;
    }

    /**
     * Put bytes from a source {@link AtomicBuffer} into this {@link AtomicBuffer} at given indices.
     *
     * @param index     in this buffer to begin putting the bytes.
     * @param srcBuffer from which the bytes will be copied.
     * @param srcIndex  in the source buffer from which the byte copy will begin.
     * @param length    of the bytes to be copied.
     */
    public void putBytes(final int index, final AtomicBuffer srcBuffer, final int srcIndex, final int length)
    {
        boundsCheck(index, length);
        srcBuffer.boundsCheck(srcIndex, length);

        UNSAFE.copyMemory(
            srcBuffer.byteArray,
            srcBuffer.addressOffset + srcIndex,
            byteArray,
            addressOffset + index,
            length);
    }

    public String getString(final int offset, final ByteOrder byteOrder)
    {
        final int length = getInt(offset, byteOrder);

        return getString(offset, length);
    }

    public String getString(final int offset, final int length)
    {
        final byte[] stringInBytes = new byte[length];
        getBytes(offset + SIZE_OF_INT, stringInBytes);

        return new String(stringInBytes, StandardCharsets.UTF_8);
    }

    public int putString(final int offset, final String value, final ByteOrder byteOrder)
    {
        return putString(offset, value, Integer.MAX_VALUE, byteOrder);
    }

    public int putString(final int offset, final String value, final int maximumEncodedSize, final ByteOrder byteOrder)
    {
        final byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : NULL_BYTES;
        if (bytes.length > maximumEncodedSize)
        {
            throw new IllegalArgumentException("Encoded string larger than maximum size: " + maximumEncodedSize);
        }

        putInt(offset, bytes.length, byteOrder);

        return SIZE_OF_INT + putBytes(offset + SIZE_OF_INT, bytes);
    }

    public String getStringWithoutLength(final int offset, final int length)
    {
        final byte[] stringInBytes = new byte[length];
        getBytes(offset, stringInBytes);

        return new String(stringInBytes, StandardCharsets.UTF_8);
    }

    public int putStringWithoutLength(final int offset, final String value)
    {
        final byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : NULL_BYTES;

        return putBytes(offset, bytes);
    }

    private void boundsCheck(final int index, final int length)
    {
        if (DISABLE_BOUNDS_CHECKS)
        {
            return;
        }

        if (index < 0 || length < 0 || (index + length) > capacity)
        {
            throw new IndexOutOfBoundsException(String.format("index=%d, length=%d, capacity=%d", index, length, capacity));
        }
    }
}
