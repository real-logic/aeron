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

    /**
     * return session id field
     * @return session id field
     */
    public long channelId()
    {
        return CodecUtil.uint32Get(directBuffer, CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     * @param channelId field value
     * @return flyweight
     */
    public HeaderFlyweight channelId(final long channelId)
    {
        CodecUtil.uint32Put(directBuffer, CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

}
