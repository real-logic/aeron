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

import uk.co.real_logic.aeron.util.BitUtil;

import static java.lang.Integer.valueOf;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;

/**
 * Description of the structure for message framing in a log buffer.
 * All messages are logged in frames that have a minimum header layout as follows plus a reserve then
 * the encoded message follows:
 *
 * <pre>
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |  Vers |B|E|       Flags       |             Type              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |R|                       Frame Length                          |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Sequence Number                         |
 *  +-+-------------------------------------------------------------+
 *  |                            Reserve                           ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                        Encoded Message                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * The B (Begin) and E (End) flags are used for message fragmentation.
 * Both are set for a message that does not span frames.
 */
public class FrameDescriptor
{
    /** Alignment as a multiple of bytes for each frame. */
    public static final int FRAME_ALIGNMENT = BitUtil.CACHE_LINE_SIZE;

    /** Word alignment for fields. */
    public static final int WORD_ALIGNMENT = BitUtil.SIZE_OF_LONG;

    /** Offset from the beginning of the frame at which the length field begins. */
    public static final int LENGTH_FIELD_OFFSET = 4;

    /** Offset from the beginning of the frame at which the length field begins. */
    public static final int SEQ_NUMBER_OFFSET = 8;

    /** Length in bytes for the fixed part of the header before the reserve */
    public static final int FIXED_HEADER_LENGTH = 12;

    /** Start fragment flag of a message. */
    public static final byte START_FLAG = 0b0000_1000;

    /** End fragment flag of a message. */
    public static final byte END_FLAG = 0b0000_0100;

    /** An unfragmented message has both flags set. */
    public static final byte UNFRAGMENTED = START_FLAG & END_FLAG;

    /**
     * Offset to the byte containing the fragment flags.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset to the byte containing the fragment flags.
     */
    public static int fragmentFlagsOffset(final int frameOffset)
    {
        return frameOffset;
    }

    /**
     * Offset to the length field from the beginning of the frame.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset to the length from the beginning of the frame.
     */
    public static int lengthOffset(final int frameOffset)
    {
        return frameOffset + LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset to the sequence number field from the beginning of the frame.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset to the sequence number field from the beginning of the frame.
     */
    public static int seqNumberOffset(final int frameOffset)
    {
        return frameOffset + SEQ_NUMBER_OFFSET;
    }

    /**
     * Offset to the encoded message from the beginning of the frame.
     *
     * @param frameOffset at which the frame begins.
     * @param headerReserve before the message begins.
     * @return the offset to the length from the beginning of the frame.
     */
    public static int messageOffset(final int frameOffset, final int headerReserve)
    {
        return frameOffset + FIXED_HEADER_LENGTH + headerReserve;
    }

    /**
     * Calculate the maximum supported message length for a buffer of given capacity.
     *
     * @param capacity of the log buffer.
     * @return the maximum supported size for a message.
     */
    public static int calculateMaxMessageLength(final int capacity)
    {
        return Math.min(capacity / 4, 1 << 16);
    }

    /**
     * Check the frame header reserve is aligned on an word boundary.
     *
     * @param length to be applied to all logged frames.
     * @throws IllegalStateException if the record header length is invalid
     */
    public static void checkHeaderReserve(final int length)
    {
        if (length % SIZE_OF_INT != 0)
        {
            final String s = String.format("Header reserve must be a multiple of %d, length=%d",
                                           valueOf(SIZE_OF_INT), valueOf(length));
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check the frame header reserve is aligned on an word boundary.
     *
     * @param length to be applied to all logged frames.
     * @throws IllegalStateException if the record header length is invalid
     */
    public static void checkMaxFrameLength(final int length)
    {
        if (length % WORD_ALIGNMENT != 0)
        {
            final String s = String.format("Max frame length must be a multiple of %d, length=%d",
                                           valueOf(WORD_ALIGNMENT), valueOf(length));
            throw new IllegalStateException(s);
        }
    }
}
