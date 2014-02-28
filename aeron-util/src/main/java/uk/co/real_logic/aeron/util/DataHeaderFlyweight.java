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
package uk.co.real_logic.aeron.util;

import uk.co.real_logic.sbe.codec.java.CodecUtil;

import java.nio.ByteOrder;

/**
 * HeaderFlyweight for Data Header
 *
 *      0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers |S|E|H|R| Type (=0x00)  |   Frame Length (=data + 20)   |
 * +-------+-+-+-+-+---------------+-------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                            Term ID                            |
 * +---------------------------------------------------------------+
 * |                        Sequence Number                        |
 * +---------------------------------------------------------------+
 * |                             Data                             ...
 *...                                                              |
 * +---------------------------------------------------------------+
 */
public class DataHeaderFlyweight extends HeaderFlyweight
{
    private static final int CHANNEL_ID_FIELD_OFFSET = 8;
    private static final int TERM_ID_FIELD_OFFSET = 12;
    private static final int SEQUENCE_NUMBER_FIELD_OFFSET = 16;
    private static final int DATA_OFFSET = 20;

    /**
     * return channel id field
     * @return channel id field
     */
    public long channelId()
    {
        return CodecUtil.uint32Get(directBuffer, offset + CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     * @param channelId field value
     * @return flyweight
     */
    public DataHeaderFlyweight channelId(final long channelId)
    {
        CodecUtil.uint32Put(directBuffer, offset + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return term id field
     * @return term id field
     */
    public long termId()
    {
        return CodecUtil.uint32Get(directBuffer, offset + TERM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set term id field
     * @param termId field value
     * @return flyweight
     */
    public DataHeaderFlyweight termId(final long termId)
    {
        CodecUtil.uint32Put(directBuffer, offset + TERM_ID_FIELD_OFFSET, termId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return sequence number field
     * @return sequence number field
     */
    public long sequenceNumber()
    {
        return CodecUtil.uint32Get(directBuffer, offset + SEQUENCE_NUMBER_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set sequence number field
     * @param sequenceNumber field value
     * @return flyweight
     */
    public DataHeaderFlyweight sequenceNumber(final long sequenceNumber)
    {
        CodecUtil.uint32Put(directBuffer, offset + SEQUENCE_NUMBER_FIELD_OFFSET, sequenceNumber, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Return offset in buffer for data
     * @return offset of data in the buffer
     */
    public int dataOffset()
    {
        return offset + DATA_OFFSET;
    }
}
