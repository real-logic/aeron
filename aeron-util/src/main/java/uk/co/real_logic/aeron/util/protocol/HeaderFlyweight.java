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

import uk.co.real_logic.aeron.util.Flyweight;

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
public class HeaderFlyweight extends Flyweight
{
    /** header type DATA */
    public static final short HDR_TYPE_DATA = 0x00;
    /** header type NAK */
    public static final short HDR_TYPE_NAK = 0x01;
    /** header type SM */
    public static final short HDR_TYPE_SM = 0x02;
    /** header type EXT */
    public static final short HDR_TYPE_EXT = 0xFF;

    /** default version */
    public static final byte CURRENT_VERSION = 0x0;

    public static final int VERS_FIELD_OFFSET = 0;
    public static final int TYPE_FIELD_OFFSET = 1;
    public static final int FRAME_LENGTH_FIELD_OFFSET = 2;
    public static final int SESSION_ID_FIELD_OFFSET = 4;

    /**
     * return version field value
     * @return ver field value
     */
    public byte version()
    {
        final int versAndFlags = uint8Get(offset + VERS_FIELD_OFFSET);

        return (byte)(versAndFlags >> 4);
    }

    /**
     * set version field value
     * @param ver field value
     * @return flyweight
     */
    public HeaderFlyweight version(final byte ver)
    {
        uint8Put(offset + VERS_FIELD_OFFSET, (byte)(ver << 4));
        return this;
    }

    /**
     * return header type field
     * @return type field value
     */
    public short headerType()
    {
        return uint8Get(offset + TYPE_FIELD_OFFSET);
    }

    /**
     * set header type field
     * @param type field value
     * @return flyweight
     */
    public HeaderFlyweight headerType(final short type)
    {
        uint8Put(offset + TYPE_FIELD_OFFSET, (byte)type);
        return this;
    }

    /**
     * return frame length field
     * @return frame length field
     */
    public int frameLength()
    {
        return uint16Get(offset + FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     * @param length field value
     * @return flyweight
     */
    public HeaderFlyweight frameLength(final int length)
    {
        uint16Put(offset + FRAME_LENGTH_FIELD_OFFSET, (short)length, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public HeaderFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset + SESSION_ID_FIELD_OFFSET, (int)sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

}
