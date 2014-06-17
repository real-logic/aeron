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
public class    NakFlyweight extends HeaderFlyweight
{
    public static final int MAX_RANGES = 16;
    public static final int HEADER_LENGTH_SINGLE_RANGE = length(1);

    private static final int RANGE_SIZE = 16;
    private static final int START_TERM_ID_RELATIVE_OFFSET = 0;
    private static final int START_TERM_OFFSET_RELATIVE_OFFSET = 4;
    private static final int END_TERM_ID_RELATIVE_OFFSET = 8;
    private static final int END_TERM_OFFSET_RELATIVE_OFFSET = 12;

    private static final int SESSION_ID_FIELD_OFFSET = 8;
    private static final int CHANNEL_ID_FIELD_OFFSET = 12;
    private static final int RANGES_FIELDS_OFFSET = 16;

    public NakFlyweight countOfRanges(int countOfRanges)
    {
        if (countOfRanges > MAX_RANGES)
        {
            String message = String.format("You may request up to %d sequence number ranges, not %d",
                    MAX_RANGES, countOfRanges);
            throw new IllegalArgumentException(message);
        }

        frameLength(RANGES_FIELDS_OFFSET + countOfRanges * RANGE_SIZE);
        return this;
    }

    public int countOfRanges()
    {
        return (frameLength() - RANGES_FIELDS_OFFSET) / RANGE_SIZE;
    }

    public static int length(final int countOfRanges)
    {
        return RANGES_FIELDS_OFFSET + (countOfRanges * RANGE_SIZE);
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
     * set a loss range
     *
     * @return flyweight
     */
    public NakFlyweight range(final long startTermId,
                              final long startTermOffset,
                              final long endTermId,
                              final long endTermOffset,
                              final int index)
    {
        final int offset = rangeOffset(index);
        uint32Put(offset + START_TERM_ID_RELATIVE_OFFSET, startTermId, LITTLE_ENDIAN);
        uint32Put(offset + START_TERM_OFFSET_RELATIVE_OFFSET, startTermOffset, LITTLE_ENDIAN);
        uint32Put(offset + END_TERM_ID_RELATIVE_OFFSET, endTermId, LITTLE_ENDIAN);
        uint32Put(offset + END_TERM_OFFSET_RELATIVE_OFFSET, endTermOffset, LITTLE_ENDIAN);
        return this;
    }

    public long startTermId(final int index)
    {
        return uint32Get(rangeOffset(index) + START_TERM_ID_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long startTermOffset(final int index)
    {
        return uint32Get(rangeOffset(index) + START_TERM_OFFSET_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long endTermId(final int index)
    {
        return uint32Get(rangeOffset(index) + END_TERM_ID_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    public long endTermOffset(final int index)
    {
        return uint32Get(rangeOffset(index) + END_TERM_OFFSET_RELATIVE_OFFSET, LITTLE_ENDIAN);
    }

    private int rangeOffset(final int index)
    {
        return offset() + RANGES_FIELDS_OFFSET + (RANGE_SIZE * index);
    }

}
