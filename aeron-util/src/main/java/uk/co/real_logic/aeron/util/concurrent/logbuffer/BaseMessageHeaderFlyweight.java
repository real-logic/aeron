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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.Flyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for manipulating the base set of fields all message headers must support.
 *
 * Description of the structure for message framing in a log buffer.
 * All messages are logged in frames that have a minimum header layout as follows plus a reserve then
 * the encoded message follows:
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |  Version      |B|E| Flags     |             Type              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |R|                       Frame Length                          |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Sequence Number                         |
 *  +-+-------------------------------------------------------------+
 *  |                      Additional Fields                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                        Encoded Message                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * The (B)egin and (E)nd flags are used for message fragmentation. R is for reserved bit.
 * Both are set for a message that does not span frames.
 */
public class BaseMessageHeaderFlyweight extends Flyweight
{
    /** Length in bytes for the base header fields. */
    public static final int BASE_HEADER_LENGTH = 12;

    public static final int VERSION_OFFSET = 0;
    public static final int FLAGS_OFFSET = 1;
    public static final int TYPE_OFFSET = 2;
    public static final int LENGTH_OFFSET = 4;
    public static final int SEQ_NUMBER_OFFSET = 8;

    public BaseMessageHeaderFlyweight wrap(final AtomicBuffer atomicBuffer, final int offset)
    {
        super.wrap(atomicBuffer, offset);
        return this;
    }

    public static int minHeaderLength()
    {
        return BASE_HEADER_LENGTH;
    }

    public BaseMessageHeaderFlyweight version(final short version)
    {
        uint8Put(VERSION_OFFSET, version);
        return this;
    }

    public short version()
    {
        return uint8Get(0);
    }

    public BaseMessageHeaderFlyweight beginFragment(final boolean value)
    {
        uint8PutChoice(FLAGS_OFFSET, 8, value);
        return this;
    }

    public boolean beginFragment()
    {
        return uint8GetChoice(1, 8);
    }

    public BaseMessageHeaderFlyweight endFragment(final boolean value)
    {
        uint8PutChoice(FLAGS_OFFSET, 7, value);
        return this;
    }

    public boolean endFragment()
    {
        return uint8GetChoice(FLAGS_OFFSET, 8);
    }

    public BaseMessageHeaderFlyweight type(final int type)
    {
        uint16Put(TYPE_OFFSET, type, LITTLE_ENDIAN);
        return this;
    }

    public int type()
    {
        return uint16Get(TYPE_OFFSET, LITTLE_ENDIAN);
    }

    public BaseMessageHeaderFlyweight length(final int value)
    {
        atomicBuffer().putInt(LENGTH_OFFSET, value, LITTLE_ENDIAN);
        return this;
    }

    public int length()
    {
        return atomicBuffer().getInt(LENGTH_OFFSET, LITTLE_ENDIAN);
    }

    public BaseMessageHeaderFlyweight putLengthOrdered(int value)
    {
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            value = Integer.reverseBytes(value);
        }

        atomicBuffer().putIntOrdered(LENGTH_OFFSET, value);

        return this;
    }

    public int getLengthVolatile()
    {
        int bits = atomicBuffer().getIntVolatile(LENGTH_OFFSET);
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    public BaseMessageHeaderFlyweight sequenceNumber(final int value)
    {
        atomicBuffer().putInt(SEQ_NUMBER_OFFSET, value, LITTLE_ENDIAN);
        return this;
    }

    public int sequenceNumber()
    {
        return atomicBuffer().getInt(SEQ_NUMBER_OFFSET, LITTLE_ENDIAN);
    }
}
