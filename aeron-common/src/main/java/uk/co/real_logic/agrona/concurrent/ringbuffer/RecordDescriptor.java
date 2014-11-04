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
package uk.co.real_logic.agrona.concurrent.ringbuffer;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Description of the record structure for message framing in the a {@link RingBuffer}.
 */
public class RecordDescriptor
{
    /**
     * Header length made up of fields for record length, message length, message type, and reserved,
     * and then the encoded message.
     * <p>
     * Writing of the record length signals the message recording is complete.
     * <pre>
     *   0        4        8        12       16 -byte position
     *   +--------+--------+--------+--------+------------------------+
     *   |rec len |msg len |msg type|reserve |encoded message.........|
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
     * The offset from the beginning of a record at which the message length field begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the type field begins.
     */
    public static int msgLengthOffset(final int recordOffset)
    {
        return recordOffset + SIZE_OF_INT;
    }

    /**
     * The offset from the beginning of a record at which the message type field begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the type field begins.
     */
    public static int msgTypeOffset(final int recordOffset)
    {
        return recordOffset + SIZE_OF_INT + SIZE_OF_INT;
    }

    /**
     * The offset from the beginning of a record at which the encoded message begins.
     *
     * @param recordOffset beginning index of the record.
     * @return offset from the beginning of a record at which the encoded message begins.
     */
    public static int encodedMsgOffset(final int recordOffset)
    {
        return recordOffset + HEADER_LENGTH;
    }

    /**
     * Check that and message id is in the valid range.
     *
     * @param msgTypeId to be checked.
     * @throws IllegalArgumentException if the id is not in the valid range.
     */
    public static void checkMsgTypeId(final int msgTypeId)
    {
        if (msgTypeId < 1)
        {
            final String msg = String.format("Message type id must be greater than zero, msgTypeId=%d", msgTypeId);
            throw new IllegalArgumentException(msg);
        }
    }
}
