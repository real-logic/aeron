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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.lang.Integer.valueOf;
import static uk.co.real_logic.aeron.util.BitUtil.CACHE_LINE_SIZE;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

/**
 * Layout description for the log and state buffers.
 */
public class BufferDescriptor
{
    /** Offset within the trailer where the tail value is stored. */
    public static final int TAIL_COUNTER_OFFSET;

    /** Offset within the trailer where the high water mark is stored. */
    public static final int HIGH_WATER_MARK_OFFSET;

    /** Total length of the state buffer in bytes. */
    public static final int STATE_BUFFER_LENGTH;

    static
    {
        HIGH_WATER_MARK_OFFSET = 0;
        TAIL_COUNTER_OFFSET = SIZE_OF_INT;

        STATE_BUFFER_LENGTH = CACHE_LINE_SIZE;
    }

    /** Minimum buffer size for the log */
    public static final int LOG_MIN_SIZE = FrameDescriptor.FRAME_ALIGNMENT * 256;

    /** Padding frame type to indicate end of the log is not in use. */
    public static final int PADDING_FRAME_TYPE = 0;

    /**
     * Check that log buffer is the correct size and alignment.
     *
     * @param buffer to be checked.
     * @throws IllegalStateException if the buffer is not as expected.
     */
    public static void checkLogBuffer(final AtomicBuffer buffer)
    {
        final int capacity = buffer.capacity();
        if (capacity < LOG_MIN_SIZE)
        {
            final String s = String.format("Log buffer capacity less than min size of %d, capacity=%d",
                                           valueOf(LOG_MIN_SIZE), valueOf(capacity));
            throw new IllegalStateException(s);
        }

        if (capacity % FRAME_ALIGNMENT != 0)
        {
            final String s = String.format("Log buffer capacity not a multiple of %d, capacity=%d",
                                           valueOf(FRAME_ALIGNMENT), valueOf(capacity));
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check that state buffer is of sufficient size.
     *
     * @param buffer to be checked.
     * @throws IllegalStateException if the buffer is not as expected.
     */
    public static void checkStateBuffer(final AtomicBuffer buffer)
    {
        final int capacity = buffer.capacity();
        if (capacity < STATE_BUFFER_LENGTH)
        {
            final String s = String.format("State buffer capacity less than min size of %d, capacity=%d",
                                           valueOf(STATE_BUFFER_LENGTH), valueOf(capacity));
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check that the offset is is not greater than the tail.
     *
     * @param offset to check.
     * @param tail   current value fo the tail.
     */
    public static void checkOffset(final int offset, final int tail)
    {
        if (offset > tail)
        {
            throw new IllegalStateException("Cannot seek to " + offset + ", the tail is only " + tail);
        }
    }
}
