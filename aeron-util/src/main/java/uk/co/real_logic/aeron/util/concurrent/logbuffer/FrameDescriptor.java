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
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BaseMessageHeaderFlyweight.BASE_HEADER_LENGTH;

/**
 * Description of the structure for message framing in a log buffer.
 *
 * All messages frames consist of a header and a body. The header is described by sub-class of
 * {@link BaseMessageHeaderFlyweight} that is followed by the encoded message in the body.
 *
 */
public class FrameDescriptor
{
    /** Alignment as a multiple of bytes for each frame. */
    public static final int FRAME_ALIGNMENT = BitUtil.CACHE_LINE_SIZE;

    /** Word alignment for fields. */
    public static final int WORD_ALIGNMENT = BitUtil.SIZE_OF_LONG;

    /**
     * Calculate the maximum supported message length for a buffer of given capacity.
     *
     * @param capacity of the log buffer.
     * @return the maximum supported size for a message.
     */
    public static int calculateMaxMessageLength(final int capacity)
    {
        return Math.min(capacity / 8, 1 << 16);
    }

    /**
     * Check the the default header is greater than or equal in size to
     * {@link BaseMessageHeaderFlyweight#BASE_HEADER_LENGTH} and a multiple of {@link #WORD_ALIGNMENT}.
     *
     * @param defaultHeader to check.
     * @throws IllegalStateException if the default header is invalid.
     */
    public static void checkDefaultHeader(final byte[] defaultHeader)
    {
        final int length = defaultHeader.length;

        if (length < BASE_HEADER_LENGTH)
        {
            final String s = String.format("Frame frame length must not be less than %d, length=%d",
                                           valueOf(BASE_HEADER_LENGTH), valueOf(length));
            throw new IllegalStateException(s);
        }

        if (length % WORD_ALIGNMENT != 0)
        {
            final String s = String.format("Frame frame length must be a multiple of %d, length=%d",
                                           valueOf(WORD_ALIGNMENT), valueOf(length));
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check the max frame length is a multiple of {@link #FRAME_ALIGNMENT}
     *
     * @param length to be applied to all logged frames.
     * @throws IllegalStateException if not a multiple of {@link #FRAME_ALIGNMENT}
     */
    public static void checkMaxFrameLength(final int length)
    {
        if (length % FRAME_ALIGNMENT != 0)
        {
            final String s = String.format("Max frame length must be a multiple of %d, length=%d",
                                           valueOf(FRAME_ALIGNMENT), valueOf(length));
            throw new IllegalStateException(s);
        }
    }
}
