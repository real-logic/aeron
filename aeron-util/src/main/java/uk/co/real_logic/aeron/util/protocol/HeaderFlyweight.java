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
package uk.co.real_logic.aeron.util.protocol;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Flyweight for general Aeron header
 *
 * Fill Usage: (Fluent)
 *
 * flywt.reset(...).version(0);
 *
 * Parse Usage:
 *
 * flwt.reset(...)
 * val = flwt.version();
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers | Flags |    Type       |        Frame Length           |
 * +-------+-+-+-+-+---------------+-------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                       Depends on Type                        ...
 *
 */
public class HeaderFlyweight
{
    /** header type DATA */
    public static final short HDR_TYPE_DATA = 0x00;
    /** header type CONN */
    public static final short HDR_TYPE_CONN = 0x01;
    /** header type NAK */
    public static final short HDR_TYPE_NAK = 0x02;
    /** header type SM */
    public static final short HDR_TYPE_SM = 0x03;
    /** header type Add Channel */
    public static final short HDR_TYPE_ADD_CHANNEL = 0x04;
    /** header type Remove Channel */
    public static final short HDR_TYPE_REMOVE_CHANNEL = 0x05;
    /** header type Remove Receiver */
    public static final short HDR_TYPE_REMOVE_RECEIVER = 0x06;
    /** header type EXT */
    public static final short HDR_TYPE_EXT = 0xFF;

    /** default version */
    public static final byte CURRENT_VERSION = 0x0;

    public static final int SIZE_OF_INT = 4;

    public static final int VERS_FIELD_OFFSET = 0;
    public static final int TYPE_FIELD_OFFSET = 1;
    public static final int FRAME_LENGTH_FIELD_OFFSET = 2;
    public static final int SESSION_ID_FIELD_OFFSET = 4;

    protected AtomicBuffer atomicBuffer;
    protected int offset;

    public HeaderFlyweight()
    {
    }

    public HeaderFlyweight reset(final ByteBuffer buffer)
    {
        return reset(new AtomicBuffer(buffer), 0);
    }

    public HeaderFlyweight reset(final ByteBuffer buffer, final int offset)
    {
        return reset(new AtomicBuffer(buffer), offset);
    }

    public HeaderFlyweight reset(final AtomicBuffer buffer, final int offset)
    {
        this.atomicBuffer = buffer;
        this.offset = offset;
        return this;
    }

    public static short uint8Get(final AtomicBuffer buffer, final int offset)
    {
        return (short)(buffer.getByte(offset) & 0xFF);
    }

    public static void uint8Put(final AtomicBuffer buffer, final int offset, final short value)
    {
        buffer.putByte(offset, (byte)value);
    }

    public static int uint16Get(final AtomicBuffer buffer, final int offset, final ByteOrder byteOrder)
    {
        return (buffer.getShort(offset, byteOrder) & 0xFFFF);
    }

    public static void uint16Put(final AtomicBuffer buffer, final int offset, final int value, final ByteOrder byteOrder)
    {
        buffer.putShort(offset, (short)value, byteOrder);
    }

    public static long uint32Get(final AtomicBuffer buffer, final int offset, final ByteOrder byteOrder)
    {
        return (buffer.getInt(offset, byteOrder) & 0xFFFFFFFFL);
    }

    public static void uint32Put(final AtomicBuffer buffer, final int offset, final long value, final ByteOrder byteOrder)
    {
        buffer.putInt(offset, (int)value, byteOrder);
    }

    // TODO: consider efficiency for String encoding/decoding
    // TODO: is there a sensible error handling for getBytes/putBytes not reading/writing the current amount of data
    public static String stringGet(AtomicBuffer buffer, final int offset, ByteOrder byteOrder)
    {
        int length = buffer.getInt(offset);
        byte[] stringInBytes = new byte[length];
        buffer.getBytes(offset + SIZE_OF_INT, stringInBytes);
        return new String(stringInBytes);
    }

    public static void stringPut(AtomicBuffer buffer, final int offset, String value, ByteOrder byteOrder)
    {
        byte[] bytes = value.getBytes();
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + SIZE_OF_INT, bytes);
    }

    /**
     * return version field value
     * @return ver field value
     */
    public byte version()
    {
        final int versAndFlags = uint8Get(atomicBuffer, offset + VERS_FIELD_OFFSET);

        return (byte)(versAndFlags >> 4);
    }

    /**
     * set version field value
     * @param ver field value
     * @return flyweight
     */
    public HeaderFlyweight version(final byte ver)
    {
        uint8Put(atomicBuffer, offset + VERS_FIELD_OFFSET, (byte)(ver << 4));
        return this;
    }

    public HeaderFlyweight currentVersion()
    {
        return version(CURRENT_VERSION);
    }

    /**
     * return header type field
     * @return type field value
     */
    public short headerType()
    {
        return uint8Get(atomicBuffer, offset + TYPE_FIELD_OFFSET);
    }

    /**
     * set header type field
     * @param type field value
     * @return flyweight
     */
    public HeaderFlyweight headerType(final short type)
    {
        uint8Put(atomicBuffer, offset + TYPE_FIELD_OFFSET, (byte)type);
        return this;
    }

    /**
     * return frame length field
     * @return frame length field
     */
    public int frameLength()
    {
        return uint16Get(atomicBuffer, offset + FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     * @param length field value
     * @return flyweight
     */
    public HeaderFlyweight frameLength(final int length)
    {
        uint16Put(atomicBuffer, offset + FRAME_LENGTH_FIELD_OFFSET, (short)length, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(atomicBuffer, offset + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public HeaderFlyweight sessionId(final long sessionId)
    {
        uint32Put(atomicBuffer, offset + SESSION_ID_FIELD_OFFSET, (int)sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}
