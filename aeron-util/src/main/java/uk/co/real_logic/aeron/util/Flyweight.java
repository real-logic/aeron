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
import java.nio.charset.StandardCharsets;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;

/**
 * Parent class for flyweight implementations both in the messaging
 * protocol and also the control protocol.
 */
public class Flyweight
{
    protected AtomicBuffer atomicBuffer;
    protected int offset;

    public Flyweight()
    {
    }

    public Flyweight reset(final ByteBuffer buffer)
    {
        return reset(buffer, 0);
    }

    public Flyweight reset(final ByteBuffer buffer, final int offset)
    {
        return reset(new AtomicBuffer(buffer), offset);
    }

    public Flyweight reset(final AtomicBuffer buffer, final int offset)
    {
        this.atomicBuffer = buffer;
        this.offset = offset;
        return this;
    }

    public short uint8Get(final int offset)
    {
        return (short)(atomicBuffer.getByte(offset) & 0xFF);
    }

    public void uint8Put(final int offset, final short value)
    {
        atomicBuffer.putByte(offset, (byte)value);
    }

    public int uint16Get(final int offset, final ByteOrder byteOrder)
    {
        return (atomicBuffer.getShort(offset, byteOrder) & 0xFFFF);
    }

    public void uint16Put(final int offset,
                          final int value,
                          final ByteOrder byteOrder)
    {
        atomicBuffer.putShort(offset, (short)value, byteOrder);
    }

    public long uint32Get(final int offset, final ByteOrder byteOrder)
    {
        return (atomicBuffer.getInt(offset, byteOrder) & 0xFFFFFFFFL);
    }

    public void uint32Put(final int offset,
                          final long value,
                          final ByteOrder byteOrder)
    {
        atomicBuffer.putInt(offset, (int)value, byteOrder);
    }

    // TODO: consider efficiency for String encoding/decoding
    // TODO: is there a sensible error handling for getBytes/putBytes not reading/writing the current amount of data
    public String stringGet(final int offset, ByteOrder byteOrder)
    {
        int length = atomicBuffer.getInt(offset);
        byte[] stringInBytes = new byte[length];
        atomicBuffer.getBytes(offset + SIZE_OF_INT, stringInBytes);
        return new String(stringInBytes, StandardCharsets.UTF_8);
    }

    public int stringPut(final int offset, String value, ByteOrder byteOrder)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        atomicBuffer.putInt(offset, bytes.length);
        return SIZE_OF_INT + atomicBuffer.putBytes(offset + SIZE_OF_INT, bytes);
    }

}
