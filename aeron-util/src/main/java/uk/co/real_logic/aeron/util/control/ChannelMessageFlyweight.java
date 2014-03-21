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
 * |      Destination Length       |   Destination               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class ChannelMessageFlyweight extends HeaderFlyweight
{
    private static final int CHANNEL_ID_FIELD_OFFSET = 8;
    private static final int DESTINATION_OFFSET = 12;

    private int lengthOfDestination;

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
    public ChannelMessageFlyweight channelId(final long channelId)
    {
        uint32Put(atomicBuffer, offset + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return destination field
     *
     * @return destination field
     */
    public String destination()
    {
        return stringGet(atomicBuffer, offset + DESTINATION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set destination field
     *
     * @param destination field value
     * @return flyweight
     */
    public ChannelMessageFlyweight destination(final String destination)
    {
        lengthOfDestination = stringPut(atomicBuffer,
                                        offset + DESTINATION_OFFSET,
                                        destination,
                                        ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public int length()
    {
        return DESTINATION_OFFSET + lengthOfDestination;
    }

}
