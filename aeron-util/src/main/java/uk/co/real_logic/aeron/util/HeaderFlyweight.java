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

import uk.co.real_logic.sbe.codec.java.CodecUtil;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

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
    public static final int HDR_TYPE_DATA = 0x00;
    /** header type CONN */
    public static final int HDR_TYPE_CONN = 0x01;
    /** header type NAK */
    public static final int HDR_TYPE_NAK = 0x02;
    /** header type FCR */
    public static final int HDR_TYPE_FCR = 0x03;
    /** header type EXT */
    public static final int HDR_TYPE_EXT = 0xFF;

    /** default version */
    public static final int CURRENT_VERSION = 0x0;

    private static final int VERS_FIELD_OFFSET = 0;
    private static final int TYPE_FIELD_OFFSET = 1;
    private static final int FRAME_LENGTH_FIELD_OFFSET = 2;
    private static final int SESSION_ID_FIELD_OFFSET = 4;

    protected DirectBuffer directBuffer;
    protected int offset;

    public HeaderFlyweight()
    {
    }

    public HeaderFlyweight reset(final ByteBuffer buffer, final int offset)
    {
        this.directBuffer = new DirectBuffer(buffer);
        this.offset = offset;
        return this;
    }

    public HeaderFlyweight reset(final DirectBuffer buffer, final int offset)
    {
        this.directBuffer = buffer;
        this.offset = offset;
        return this;
    }

    /**
     * return version field value
     * @return ver field value
     */
    public byte version()
    {
        final int versAndFlags = CodecUtil.uint8Get(directBuffer, offset + VERS_FIELD_OFFSET);

        return (byte)(versAndFlags >> 4);
    }

    /**
     * set version field value
     * @param ver field value
     * @return flyweight
     */
    public HeaderFlyweight version(final byte ver)
    {
        CodecUtil.uint8Put(directBuffer, offset + VERS_FIELD_OFFSET, (short)(ver << 4));
        return this;
    }

    /**
     * return header type field
     * @return type field value
     */
    public short headerType()
    {
        return CodecUtil.uint8Get(directBuffer, offset + TYPE_FIELD_OFFSET);
    }

    /**
     * set header type field
     * @param type field value
     * @return flyweight
     */
    public HeaderFlyweight headerType(final short type)
    {
        CodecUtil.uint8Put(directBuffer, offset + TYPE_FIELD_OFFSET, type);
        return this;
    }

    /**
     * return frame length field
     * @return frame length field
     */
    public int frameLength()
    {
        return CodecUtil.uint16Get(directBuffer, offset + FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     * @param length field value
     * @return flyweight
     */
    public HeaderFlyweight frameLength(final int length)
    {
        CodecUtil.uint16Put(directBuffer, offset + FRAME_LENGTH_FIELD_OFFSET, length, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return CodecUtil.uint32Get(directBuffer, offset + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public HeaderFlyweight sessionId(final long sessionId)
    {
        CodecUtil.uint32Put(directBuffer, offset + SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}
