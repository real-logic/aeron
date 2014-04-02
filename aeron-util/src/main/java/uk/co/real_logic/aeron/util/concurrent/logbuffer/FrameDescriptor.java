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

/**
 * Description of the structure for message framing in a log buffer.
 * All messages are logged in frames that have a minimum header layout as follows plus a reserve then
 * the encoded message follows:
 *
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers |S|E|       Flags       |             Type              |
 * +-------+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 * |R|                       Frame Length                          |
 * +-------------------------------+-------------------------------+
 * |                            Reserve                          ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 * |                        Encoded Message                      ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 * </pre>
 *
 * The S (Start) and E (End) flags are used for message fragmentation.
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

    /** Length in bytes for the fixed part of the header before the reserve */
    public static final int FIXED_HEADER_LENGTH = 8;

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
        if (length % WORD_ALIGNMENT != 0)
        {
            final String s = String.format("Header reserve must be a multiple of %d, length=%d",
                                           valueOf(WORD_ALIGNMENT), valueOf(length));
            throw new IllegalStateException(s);
        }
    }
}
