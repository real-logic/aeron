/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.protocol;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

/**
 * Flyweight for general Aeron network protocol header of a message frame.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Frame Length                           |
 *  +---------------------------------------------------------------+
 *  |  Version    |     Flags     |               Type              |
 *  +-------------+---------------+---------------------------------+
 *  |                       Depends on Type                        ...
 * </pre>
 */
public class HeaderFlyweight extends UnsafeBuffer
{
    /**
     * header type PAD
     */
    public static final int HDR_TYPE_PAD = 0x00;

    /**
     * header type DATA
     */
    public static final int HDR_TYPE_DATA = 0x01;

    /**
     * header type NAK
     */
    public static final int HDR_TYPE_NAK = 0x02;

    /**
     * header type SM
     */
    public static final int HDR_TYPE_SM = 0x03;

    /**
     * header type ERR
     */
    public static final int HDR_TYPE_ERR = 0x04;

    /**
     * header type SETUP
     */
    public static final int HDR_TYPE_SETUP = 0x05;
    /**
     * header type RTT Measurement
     */
    public static final int HDR_TYPE_RTTM = 0x06;

    /**
     * header type EXT
     */
    public static final int HDR_TYPE_EXT = 0xFFFF;

    /**
     * default version
     */
    public static final byte CURRENT_VERSION = 0x0;

    public static final int FRAME_LENGTH_FIELD_OFFSET = 0;
    public static final int VERSION_FIELD_OFFSET = 4;
    public static final int FLAGS_FIELD_OFFSET = 5;
    public static final int TYPE_FIELD_OFFSET = 6;
    public static final int HEADER_LENGTH = TYPE_FIELD_OFFSET + SIZE_OF_SHORT;

    public HeaderFlyweight()
    {
    }

    public HeaderFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    public HeaderFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * return version field value
     *
     * @return ver field value
     */
    public short version()
    {
        return (short)(getByte(VERSION_FIELD_OFFSET) & 0xFF);
    }

    /**
     * set version field value
     *
     * @param version field value
     * @return flyweight
     */
    public HeaderFlyweight version(final short version)
    {
        putByte(VERSION_FIELD_OFFSET, (byte)version);

        return this;
    }

    /**
     * return flags field value
     *
     * @return flags field value
     */
    public short flags()
    {
        return (short)(getByte(FLAGS_FIELD_OFFSET) & 0xFF);
    }

    /**
     * set the flags field value
     *
     * @param flags field value
     * @return flyweight
     */
    public HeaderFlyweight flags(final short flags)
    {
        putByte(FLAGS_FIELD_OFFSET, (byte)flags);

        return this;
    }

    /**
     * return header type field
     *
     * @return type field value
     */
    public int headerType()
    {
        return getShort(TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * set header type field
     *
     * @param type field value
     * @return flyweight
     */
    public HeaderFlyweight headerType(final int type)
    {
        putShort(TYPE_FIELD_OFFSET, (short)type, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return frame length field
     *
     * @return frame length field
     */
    public int frameLength()
    {
        return getInt(FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set frame length field
     *
     * @param length field value
     * @return flyweight
     */
    public HeaderFlyweight frameLength(final int length)
    {
        putInt(FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);

        return this;
    }
}
