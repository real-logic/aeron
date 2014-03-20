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
package uk.co.real_logic.aeron.util.control;

import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteOrder;

/**
 * Control message for adding and removing a channel.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers |S|E|H|R| Type (=0x00)  |   Frame Length (=data + 20)   |
 * +-------+-+-+-+-+---------------+-------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                           Term ID                             |
 * +---------------------------------------------------------------+
 */
public class RequestTermFlyweight extends HeaderFlyweight
{
    private static final int CHANNEL_ID_FIELD_OFFSET = 8;
    private static final int TERM_ID_FIELD_OFFSET = 12;

    /**
     * return channel id field
     *
     * @return channel id field
     */
    public long channelId()
    {
        return uint32Get(atomicBuffer, offset + CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public RequestTermFlyweight channelId(final long channelId)
    {
        uint32Put(atomicBuffer, offset + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return termId field
     *
     * @return termId field
     */
    public long termId()
    {
        return uint32Get(atomicBuffer, offset + TERM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set termId field
     *
     * @param termId field value
     * @return flyweight
     */
    public RequestTermFlyweight termId(final long termId)
    {
        uint32Put(atomicBuffer, offset + TERM_ID_FIELD_OFFSET, termId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

}
