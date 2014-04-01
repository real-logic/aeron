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
 * Description of the record structure for message framing in a log buffer.
 */
public class RecordDescriptor
{
    /** Alignment as a multiple of bytes for each record. */
    public static final int RECORD_ALIGNMENT = BitUtil.CACHE_LINE_SIZE;

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
        return capacity / 4;
    }

    /**
     * Check the record header is sufficient size and aligned on an 8 byte boundary.
     *
     * @param recordHeaderLength to be applied to all logged records.
     * @throws IllegalStateException if the record header length is invalid
     */
    public static void checkRecordHeaderLength(final int recordHeaderLength)
    {
        if (recordHeaderLength < WORD_ALIGNMENT)
        {
            final String s = String.format("Record header is less than min length of %d, length=%d",
                                           valueOf(WORD_ALIGNMENT), valueOf(recordHeaderLength));
            throw new IllegalStateException(s);
        }

        if (recordHeaderLength % WORD_ALIGNMENT != 0)
        {
            final String s = String.format("Record header length must be a multiple of %d, length=%d",
                                           valueOf(WORD_ALIGNMENT), valueOf(recordHeaderLength));
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check the record header field is within header at appropriate boundary.
     *
     * @param recordHeaderLength header length in which field is located.
     * @param recordHeaderLengthFieldOffset for the beginning of the field.
     * @throws IndexOutOfBoundsException if the offset is out of range within the header.
     */
    public static void checkRecordHeaderLengthFieldOffset(final int recordHeaderLength,
                                                          final int recordHeaderLengthFieldOffset)
    {
        final int upperBound = recordHeaderLength - 1 - SIZE_OF_INT;
        if (recordHeaderLengthFieldOffset < 0 || recordHeaderLengthFieldOffset > upperBound)
        {
            final String s = String.format("Invalid offset for header of length %d, beginning offset=%d",
                                           valueOf(recordHeaderLength), valueOf(recordHeaderLengthFieldOffset));
            throw new IndexOutOfBoundsException(s);
        }
    }
}
