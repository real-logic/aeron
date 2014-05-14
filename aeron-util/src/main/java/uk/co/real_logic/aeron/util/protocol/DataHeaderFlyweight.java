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
 * HeaderFlyweight for Data Header
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Version    |S|E|   Flags   |             Type (=0x01)        |
 * +-------------+-+-+-+-+-+-+-+-+---------------------------------+
 * |                         Frame Length                          |
 * +---------------------------------------------------------------+
 * |                         Term Offset                           |
 * +---------------------------------------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                            Term ID                            |
 * +---------------------------------------------------------------+
 * |                             Data                             ...
 * ...                                                              |
 * +---------------------------------------------------------------+
 */
public class DataHeaderFlyweight extends HeaderFlyweight
{
    /** Size of the Data Header */
    public static final int HEADER_LENGTH = 24;

    /** Begin Flag */
    public static final short BEGIN_FLAG = 0x80;

    /** End Flag */
    public static final short END_FLAG = 0x40;

    /** Begin and End Flags */
    public static final short BEGIN_AND_END_FLAGS = (BEGIN_FLAG | END_FLAG);

    private static final int TERM_OFFSET_FIELD_OFFSET = 8;
    private static final int SESSION_ID_FIELD_OFFSET = 12;
    private static final int CHANNEL_ID_FIELD_OFFSET = 16;
    private static final int TERM_ID_FIELD_OFFSET = 20;
    private static final int DATA_OFFSET = 24;

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
    public DataHeaderFlyweight sessionId(final long sessionId)
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
    public DataHeaderFlyweight channelId(final long channelId)
    {
        uint32Put(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return term id field
     *
     * @return term id field
     */
    public long termId()
    {
        return uint32Get(offset() + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public DataHeaderFlyweight termId(final long termId)
    {
        uint32Put(offset() + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return term offset field
     *
     * @return term offset field
     */
    public long termOffset()
    {
        return uint32Get(offset() + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term offset field
     *
     * @param termOffset field value
     * @return flyweight
     */
    public DataHeaderFlyweight termOffset(final long termOffset)
    {
        uint32Put(offset() + TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Return offset in buffer for data
     *
     * @return offset of data in the buffer
     */
    public int dataOffset()
    {
        return offset() + DATA_OFFSET;
    }

}
