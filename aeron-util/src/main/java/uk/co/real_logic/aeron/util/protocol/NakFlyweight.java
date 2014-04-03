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

import java.nio.ByteOrder;

/**
 * Flyweight for a Nak Packet
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers |S|E|     Flags         |             Type (=0x00)      |
 * +-------+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 * |                         Frame Length                          |
 * +-------------------------------+-------------------------------+
 * |                        Sequence Number                        |
 * +---------------------------------------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                            Term ID                            |
 * +---------------------------------------------------------------+
 */
public class NakFlyweight extends HeaderFlyweight
{
    /** Size of the Nak Packet */
    public static final int LENGTH = 24;

    private static final int SEQUENCE_NUMBER_FIELD_OFFSET = 8;
    private static final int SESSION_ID_FIELD_OFFSET = 12;
    private static final int CHANNEL_ID_FIELD_OFFSET = 16;
    private static final int TERM_ID_FIELD_OFFSET = 20;

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public NakFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset + SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return channel id field
     *
     * @return channel id field
     */
    public long channelId()
    {
        return uint32Get(offset + CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public NakFlyweight channelId(final long channelId)
    {
        uint32Put(offset + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return sequence number field
     *
     * @return sequence number field
     */
    public long sequenceNumber()
    {
        return uint32Get(offset + SEQUENCE_NUMBER_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set sequence number field
     *
     * @param sequenceNumber field value
     * @return flyweight
     */
    public NakFlyweight sequenceNumber(final long sequenceNumber)
    {
        uint32Put(offset + SEQUENCE_NUMBER_FIELD_OFFSET, sequenceNumber, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return term id field
     *
     * @return term id field
     */
    public long termId()
    {
        return uint32Get(offset + TERM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public NakFlyweight termId(final long termId)
    {
        uint32Put(offset + TERM_ID_FIELD_OFFSET, termId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

}
