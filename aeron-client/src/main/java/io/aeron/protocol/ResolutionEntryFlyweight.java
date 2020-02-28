/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.protocol;

import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.lang.Integer.toHexString;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;

/**
 * Flyweight for Resolution Entry header.
 * <p>
 * <a target="_blank"
 *    href="https://github.com/real-logic/aeron/wiki/Transport-Protocol-Specification">Protocol Specification</a>
 * wiki page.
 */
public class ResolutionEntryFlyweight extends HeaderFlyweight
{
    public static final byte RES_TYPE_NAME_TO_IP4_MD = 0x01;
    public static final byte RES_TYPE_NAME_TO_IP6_MD = 0x02;

    public static final int ADDRESS_LENGTH_IP4 = 4;
    public static final int ADDRESS_LENGTH_IP6 = 16;

    /**
     * (S) - Self flag
     */
    public static final short SELF_FLAG = 0x80;

    public static final int RES_TYPE_FIELD_OFFSET = 0;
    public static final int RES_FLAGS_FIELD_OFFSET = 1;
    public static final int UDP_PORT_FIELD_OFFSET = 2;
    public static final int AGE_IN_MS_FIELD_OFFSET = 4;
    public static final int ADDRESS_FIELD_OFFSET = 8;

    public static final int MAX_NAME_LENGTH = 512;

    public ResolutionEntryFlyweight()
    {
    }

    public ResolutionEntryFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    public ResolutionEntryFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    public ResolutionEntryFlyweight resType(final byte type)
    {
        putByte(RES_TYPE_FIELD_OFFSET, type);
        return this;
    }

    public byte resType()
    {
        return getByte(RES_TYPE_FIELD_OFFSET);
    }

    public ResolutionEntryFlyweight flags(final short flags)
    {
        putByte(RES_FLAGS_FIELD_OFFSET, (byte)flags);
        return this;
    }

    public short flags()
    {
        return (short)(getByte(RES_FLAGS_FIELD_OFFSET) & 0xFF);
    }

    public ResolutionEntryFlyweight udpPort(final int udpPort)
    {
        putShort(UDP_PORT_FIELD_OFFSET, (short)udpPort, LITTLE_ENDIAN);
        return this;
    }

    public int udpPort()
    {
        return getShort(UDP_PORT_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
    }

    public ResolutionEntryFlyweight ageInMs(final int ageInMs)
    {
        putInt(AGE_IN_MS_FIELD_OFFSET, ageInMs, LITTLE_ENDIAN);
        return this;
    }

    public int ageInMs()
    {
        return getInt(AGE_IN_MS_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    public ResolutionEntryFlyweight putAddress(final byte[] address)
    {
        switch (resType())
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                putBytes(ADDRESS_FIELD_OFFSET, address, 0, 4);
                return this;

            case RES_TYPE_NAME_TO_IP6_MD:
                putBytes(ADDRESS_FIELD_OFFSET, address, 0, 16);
                return this;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + resType());
    }

    public int getAddress(final byte[] address)
    {
        switch (resType())
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                getBytes(ADDRESS_FIELD_OFFSET, address, 0, 4);
                return 4;

            case RES_TYPE_NAME_TO_IP6_MD:
                getBytes(ADDRESS_FIELD_OFFSET, address, 0, 16);
                return 16;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + resType());
    }

    public void appendAddress(final Appendable appendable)
    {
        try
        {
            switch (resType())
            {
                case RES_TYPE_NAME_TO_IP4_MD:
                {
                    final int i = ADDRESS_FIELD_OFFSET;
                    appendable
                        .append(String.valueOf(getByte(i) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 1) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 2) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 3) & 0xFF));
                    return;
                }

                case RES_TYPE_NAME_TO_IP6_MD:
                {
                    final int i = ADDRESS_FIELD_OFFSET;
                    appendable
                        .append(toHexString(((getByte(i) << 8) & 0xFF00) | getByte(i + 1) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 2) << 8) & 0xFF00) | getByte(i + 3) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 4) << 8) & 0xFF00) | getByte(i + 5) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 6) << 8) & 0xFF00) | getByte(i + 7) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 8) << 8) & 0xFF00) | getByte(i + 9) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 10) << 8) & 0xFF00) | getByte(i + 11) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 12) << 8) & 0xFF00) | getByte(i + 13) & 0xFF))
                        .append(':')
                        .append(toHexString(((getByte(i + 14) << 8) & 0xFF00) | getByte(i + 15) & 0xFF));
                    return;
                }
            }

            appendable
                .append("unknown RES_TYPE=")
                .append(String.valueOf(resType()));
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public ResolutionEntryFlyweight putName(final byte[] name)
    {
        final int nameOffset = nameOffset(resType());

        putShort(nameOffset, (short)name.length, LITTLE_ENDIAN);
        putBytes(nameOffset + SIZE_OF_SHORT, name);
        return this;
    }

    public int getName(final byte[] name)
    {
        final int nameOffset = nameOffset(resType());
        final short nameLength = getShort(nameOffset, LITTLE_ENDIAN);

        getBytes(nameOffset + SIZE_OF_SHORT, name, 0, nameLength);
        return nameLength;
    }

    public void appendName(final Appendable appendable)
    {
        final int nameOffset = nameOffset(resType());
        final short nameLength = getShort(nameOffset, LITTLE_ENDIAN);

        getStringWithoutLengthAscii(nameOffset + SIZE_OF_SHORT, nameLength, appendable);
    }

    public int entryLength()
    {
        final int nameOffset = nameOffset(resType());

        return align(nameOffset + SIZE_OF_SHORT + getShort(nameOffset, LITTLE_ENDIAN), SIZE_OF_LONG);
    }

    public static int nameOffset(final byte type)
    {
        switch (type)
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                return ADDRESS_FIELD_OFFSET + 4;

            case RES_TYPE_NAME_TO_IP6_MD:
                return ADDRESS_FIELD_OFFSET + 16;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + type);
    }

    public static int entryLengthRequired(final byte type, final int nameLength)
    {
        return align(nameOffset(type) + SIZE_OF_SHORT + nameLength, SIZE_OF_LONG);
    }

    public static int addressLength(final byte type)
    {
        switch (type)
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                return 4;

            case RES_TYPE_NAME_TO_IP6_MD:
                return 16;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + type);
    }

    public static boolean isIp4Wildcard(final byte[] address, final int addressLength)
    {
        if (addressLength == ADDRESS_LENGTH_IP4)
        {
            return 0 == address[0] && 0 == address[1] && 0 == address[2] && 0 == address[3];
        }

        return false;
    }
}
