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
package uk.co.real_logic.aeron.common.protocol;

import uk.co.real_logic.aeron.common.Flyweight;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Flyweight for general Aeron header
 *
 * Fill Usage: (Fluent)
 *
 * flyweight.wrap(...).version(0);
 *
 * Parse Usage:
 *
 * flyweight.wrap(...)
 * val = flyweight.version();
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Version    |     Flags     |               Type              |
 * +-------------+---------------+---------------------------------+
 * |                        Frame Length                           |
 * +---------------------------------------------------------------+
 * |                       Depends on Type                        ...
 *
 */
public class HeaderFlyweight extends Flyweight
{
    /** header type PAD */
    public static final int HDR_TYPE_PAD = 0x00;
    /** header type DATA */
    public static final int HDR_TYPE_DATA = 0x01;
    /** header type NAK */
    public static final int HDR_TYPE_NAK = 0x02;
    /** header type SM */
    public static final int HDR_TYPE_SM = 0x03;
    /** header type ERR */
    public static final int HDR_TYPE_ERR = 0x04;
    /** header type SETUP */
    public static final int HDR_TYPE_SETUP = 0x05;
    /** header type EXT */
    public static final int HDR_TYPE_EXT = 0xFFFF;

    /** default version */
    public static final byte CURRENT_VERSION = 0x0;

    public static final int VERSION_FIELD_OFFSET = 0;
    public static final int FLAGS_FIELD_OFFSET = 1;
    public static final int TYPE_FIELD_OFFSET = 2;
    public static final int FRAME_LENGTH_FIELD_OFFSET = 4;
    public static final int HEADER_LENGTH = FRAME_LENGTH_FIELD_OFFSET + SIZE_OF_INT;

    /**
     * return version field value
     *
     * @return ver field value
     */
    public short version()
    {
        return uint8Get(offset() + VERSION_FIELD_OFFSET);
    }

    /**
     * set version field value
     *
     * @param ver field value
     * @return flyweight
     */
    public HeaderFlyweight version(final short ver)
    {
        uint8Put(offset() + VERSION_FIELD_OFFSET, ver);

        return this;
    }

    /**
     * return flags field value
     *
     * @return flags field value
     */
    public short flags()
    {
        return uint8Get(offset() + FLAGS_FIELD_OFFSET);
    }

    /**
     * set the flags field value
     *
     * @param flags field value
     * @return flyweight
     */
    public HeaderFlyweight flags(final short flags)
    {
        uint8Put(offset() + FLAGS_FIELD_OFFSET, flags);

        return this;
    }

    /**
     * return header type field
     *
     * @return type field value
     */
    public int headerType()
    {
        return uint16Get(offset() + TYPE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set header type field
     *
     * @param type field value
     * @return flyweight
     */
    public HeaderFlyweight headerType(final int type)
    {
        uint16Put(offset() + TYPE_FIELD_OFFSET, (short)type, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return frame length field
     *
     * @return frame length field
     */
    public int frameLength()
    {
        return (int)uint32Get(offset() + FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     *
     * @param length field value
     * @return flyweight
     */
    public HeaderFlyweight frameLength(final int length)
    {
        uint32Put(offset() + FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);

        return this;
    }
}
