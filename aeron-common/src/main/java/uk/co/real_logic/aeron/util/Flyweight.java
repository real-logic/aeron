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
package uk.co.real_logic.aeron.util;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 * Parent class for flyweight implementations both in the messaging
 * protocol and also the control protocol.
 */
public class Flyweight
{
    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final AtomicBuffer atomicBuffer = new AtomicBuffer(EMPTY_BUFFER);
    private int offset;

    public Flyweight wrap(final byte[] buffer)
    {
        atomicBuffer.wrap(buffer);
        offset = 0;
        return this;
    }

    public Flyweight wrap(final ByteBuffer buffer)
    {
        return wrap(buffer, 0);
    }

    public Flyweight wrap(final ByteBuffer buffer, final int offset)
    {
        atomicBuffer.wrap(buffer);
        this.offset = offset;
        return this;
    }

    public Flyweight wrap(final AtomicBuffer buffer, final int offset)
    {
        atomicBuffer.wrap(buffer);
        this.offset = offset;
        return this;
    }

    public AtomicBuffer atomicBuffer()
    {
        return atomicBuffer;
    }

    public int offset()
    {
        return offset;
    }

    protected void copyFlyweight(final Flyweight srcFlyweight, final int index, final int length)
    {
        atomicBuffer.putBytes(index, srcFlyweight.atomicBuffer, srcFlyweight.offset, length);
    }

    protected boolean uint8GetChoice(final int offset, final int bitIndex)
    {
        return 0 != (atomicBuffer.getByte(offset) & (1 << bitIndex));
    }

    protected void uint8PutChoice(final int offset, final int bitIndex, final boolean switchOn)
    {
        byte bits = atomicBuffer.getByte(offset);
        bits = (byte)((switchOn ? bits | (1 << bitIndex) : bits & ~(1 << bitIndex)));
        atomicBuffer.putByte(offset, bits);
    }

    protected short uint8Get(final int offset)
    {
        return (short)(atomicBuffer.getByte(offset) & 0xFF);
    }

    protected void uint8Put(final int offset, final short value)
    {
        atomicBuffer.putByte(offset, (byte)value);
    }

    protected int uint16Get(final int offset, final ByteOrder byteOrder)
    {
        return atomicBuffer.getShort(offset, byteOrder) & 0xFFFF;
    }

    protected void uint16Put(final int offset, final int value, final ByteOrder byteOrder)
    {
        atomicBuffer.putShort(offset, (short)value, byteOrder);
    }

    protected long uint32Get(final int offset, final ByteOrder byteOrder)
    {
        return atomicBuffer.getInt(offset, byteOrder) & 0xFFFF_FFFFL;
    }

    protected void uint32Put(final int offset, final long value, final ByteOrder byteOrder)
    {
        atomicBuffer.putInt(offset, (int)value, byteOrder);
    }

    protected long[] uint32ArrayGet(final int offset, final ByteOrder byteOrder)
    {
        final int length = atomicBuffer.getInt(offset);
        final long[] values = new long[length];
        int location = offset + SIZE_OF_INT;

        for (int i = 0; i < length; i++)
        {
            values[i] = uint32Get(location, byteOrder);
            location += SIZE_OF_LONG;
        }

        return values;
    }

    protected int uint32ArrayPut(final int offset, final long[] values, final ByteOrder byteOrder)
    {
        final int length = values.length;
        atomicBuffer.putInt(offset, length, byteOrder);
        int location = offset + SIZE_OF_INT;

        for (final long value : values)
        {
            uint32Put(location, value, byteOrder);
            location += SIZE_OF_LONG;
        }

        return SIZE_OF_INT + (length * BitUtil.SIZE_OF_LONG);
    }

    public String stringGet(final int offset, final ByteOrder byteOrder)
    {
        return atomicBuffer.getString(offset, byteOrder);
    }

    public int stringPut(final int offset, final String value, final ByteOrder byteOrder)
    {
        return atomicBuffer.putString(offset, value, byteOrder);
    }
}
