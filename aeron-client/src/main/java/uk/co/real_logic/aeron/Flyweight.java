/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Parent class for flyweight implementations both in the messaging
 * protocol and also the control protocol.
 */
public class Flyweight
{
    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final MutableDirectBuffer buffer = new UnsafeBuffer(EMPTY_BUFFER);
    private int offset;

    public Flyweight wrap(final byte[] buffer)
    {
        this.buffer.wrap(buffer);
        offset = 0;

        return this;
    }

    public Flyweight wrap(final ByteBuffer buffer)
    {
        return wrap(buffer, 0);
    }

    public Flyweight wrap(final ByteBuffer buffer, final int offset)
    {
        this.buffer.wrap(buffer);
        this.offset = offset;

        return this;
    }

    public Flyweight wrap(final MutableDirectBuffer buffer)
    {
        return wrap(buffer, 0);
    }

    public Flyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer.wrap(buffer);
        this.offset = offset;

        return this;
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public void offset(final int offset)
    {
        this.offset = offset;
    }

    protected void copyFlyweight(final Flyweight srcFlyweight, final int index, final int length)
    {
        buffer.putBytes(index, srcFlyweight.buffer, srcFlyweight.offset, length);
    }

    protected short uint8Get(final int offset)
    {
        return (short)(buffer.getByte(offset) & 0xFF);
    }

    protected void uint8Put(final int offset, final short value)
    {
        buffer.putByte(offset, (byte)value);
    }

    protected int uint16Get(final int offset, final ByteOrder byteOrder)
    {
        return buffer.getShort(offset, byteOrder) & 0xFFFF;
    }

    protected void uint16Put(final int offset, final int value, final ByteOrder byteOrder)
    {
        buffer.putShort(offset, (short)value, byteOrder);
    }

    public String stringGet(final int offset, final ByteOrder byteOrder)
    {
        return buffer.getStringUtf8(offset, byteOrder);
    }

    public int stringPut(final int offset, final String value, final ByteOrder byteOrder)
    {
        return buffer.putStringUtf8(offset, value, byteOrder);
    }
}
