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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;

/**
 * Description of the record structure for event framing in the a {@link RingBuffer}.
 */
public class RecordDescriptor
{
    /**
     * Header length made up of fields for record length, event length, event type, and reserved,
     * and then the encoded event.
     * <p>
     * Writing of the record length signals the event recording is complete.
     * <p>
     * <pre>
     *   0        4        8        12       16 -byte position
     *   +--------+--------+--------+--------+------------------------+
     *   |rec len |evt len |evt type|reserve |encoded event...........|
     *   +--------+--------+--------+--------+------------------------+
     * </pre>
     */
    public static final int HEADER_LENGTH = SIZE_OF_INT * 4;

    /**
     * Alignment as a multiple of bytes for each record.
     */
    public static final int ALIGNMENT = 32;

    /**
     * The offset from the beginning of a record at which the length field begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the length field begins.
     */
    public static int lengthOffset(final int recordOffset)
    {
        return recordOffset;
    }

    /**
     * The offset from the beginning of a record at which the event length field begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the type field begins.
     */
    public static int eventLengthOffset(final int recordOffset)
    {
        return recordOffset + SIZE_OF_INT;
    }

    /**
     * The offset from the beginning of a record at which the event type field begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the type field begins.
     */
    public static int eventTypeOffset(final int recordOffset)
    {
        return recordOffset + SIZE_OF_INT + SIZE_OF_INT;
    }

    /**
     * The offset from the beginning of a record at which the encoded event begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the encoded event begins.
     */
    public static int encodedEventOffset(final int recordOffset)
    {
        return recordOffset + HEADER_LENGTH;
    }

    /**
     * Check that and event id is in the valid range.
     *
     * @param eventTypeId to be checked.
     * @throws IllegalArgumentException if the id is not in the valid range.
     */
    public static void checkEventTypeId(final int eventTypeId)
    {
        if (eventTypeId < 1)
        {
            final String msg = String.format("event type id must be greater than zero, eventTypeId=%d",
                                             Integer.valueOf(eventTypeId));

            throw new IllegalArgumentException(msg);
        }
    }
}
