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
package uk.co.real_logic.aeron.util.protocol;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for a Nak Packet
 *
 * <p>
 * @see <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-recovery-via-retransmit-request">Data Recovery</a>
 */
public class NakFlyweight extends HeaderFlyweight
{
    public static final int MAX_SEQUENCE_NUMBER_RANGES = 16;

    private static final int SEQUENCE_NUMBER_RANGE_SIZE = 16;
    private static final int START_TERM_ID_RELATIVE_OFFSET = 0;
    private static final int START_SEQUENCE_NUMBER_RELATIVE_OFFSET = 4;
    private static final int END_TERM_ID_RELATIVE_OFFSET = 8;
    private static final int END_SEQUENCE_NUMBER_RELATIVE_OFFSET = 12;

    private static final int SESSION_ID_FIELD_OFFSET = 8;
    private static final int CHANNEL_ID_FIELD_OFFSET = 12;
    private static final int SEQUENCE_RANGES_FIELDS_OFFSET = 16;

    public NakFlyweight countOfSequenceNumberRanges(int countOfSequenceNumberRanges)
    {
        if (countOfSequenceNumberRanges > MAX_SEQUENCE_NUMBER_RANGES)
        {
            String message = String.format("You may request up to %d sequence number ranges, not %d",
                                           MAX_SEQUENCE_NUMBER_RANGES,
                                           countOfSequenceNumberRanges);
            throw new IllegalArgumentException(message);
        }

        frameLength(SEQUENCE_RANGES_FIELDS_OFFSET + countOfSequenceNumberRanges * SEQUENCE_NUMBER_RANGE_SIZE);
        return this;
    }

    public int countOfSequenceNumberRanges()
    {
        return (frameLength() - SEQUENCE_RANGES_FIELDS_OFFSET) / SEQUENCE_NUMBER_RANGE_SIZE;
    }

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset() + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public NakFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset() + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return channel id field
     *
     * @return channel id field
     */
    public long channelId()
    {
        return uint32Get(offset() + CHANNEL_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public NakFlyweight channelId(final long channelId)
    {
        uint32Put(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * set a sequence range
     *
     * @return flyweight
     */
    public NakFlyweight sequenceRange(final long startTermId,
                                      final long startSequenceNumber,
                                      final long endTermId,
                                      final long endSequenceNumber,
                                      final int index)
    {
        final int offset = sequenceRangeOffset(index);
        uint32Put(offset + START_TERM_ID_RELATIVE_OFFSET, startTermId, LITTLE_ENDIAN);
        uint32Put(offset + START_SEQUENCE_NUMBER_RELATIVE_OFFSET, startSequenceNumber, LITTLE_ENDIAN);
        uint32Put(offset + END_TERM_ID_RELATIVE_OFFSET, endTermId, LITTLE_ENDIAN);
        uint32Put(offset + END_SEQUENCE_NUMBER_RELATIVE_OFFSET, endSequenceNumber, LITTLE_ENDIAN);
        return this;
    }

    public long startTermId(final int index)
    {
        return uint32Get(sequenceRangeOffset(index) + START_TERM_ID_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long startSequenceNumber(final int index)
    {
        return uint32Get(sequenceRangeOffset(index) + START_SEQUENCE_NUMBER_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long endTermId(final int index)
    {
        return uint32Get(sequenceRangeOffset(index) + END_TERM_ID_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long endSequenceNumber(final int index)
    {
        return uint32Get(sequenceRangeOffset(index) + END_SEQUENCE_NUMBER_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    private int sequenceRangeOffset(final int index)
    {
        return offset() + SEQUENCE_RANGES_FIELDS_OFFSET + (SEQUENCE_NUMBER_RANGE_SIZE * index);
    }

}
