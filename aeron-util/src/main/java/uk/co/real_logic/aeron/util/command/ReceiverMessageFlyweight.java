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
package uk.co.real_logic.aeron.util.command;

import uk.co.real_logic.aeron.util.Flyweight;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a receiver.
 *
 * Must write channels ids before destination.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      Channel Length           |   Channel Ids               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |      Destination Length       |   Destination               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class ReceiverMessageFlyweight extends Flyweight
{
    private static final int CHANNEL_IDS_OFFSET = 0;

    private int lengthOfChannelIds;
    private int lengthOfDestination;

    /**
     * get the channel id list
     *
     * @return the channel id list
     */
    public long[] channelIds()
    {
        return uint32ArrayGet(offset + CHANNEL_IDS_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set the channel id list
     *
     * @param value the channel id list
     */
    public void channelIds(long[] value)
    {
        lengthOfChannelIds = uint32ArrayPut(offset + CHANNEL_IDS_OFFSET, value, LITTLE_ENDIAN);
    }

    /**
     * return destination field
     *
     * @return destination field
     */
    public String destination()
    {
        // destination comes after channels
        final int destinationOffset = CHANNEL_IDS_OFFSET
                                    + SIZE_OF_INT
                                    + atomicBuffer.getInt(offset + CHANNEL_IDS_OFFSET, LITTLE_ENDIAN) * SIZE_OF_LONG;

        return stringGet(offset + destinationOffset, LITTLE_ENDIAN);
    }

    /**
     * set destination field
     *
     * Assumes the channel has already been written
     *
     * @param destination field value
     * @return flyweight
     */
    public ReceiverMessageFlyweight destination(final String destination)
    {
        lengthOfDestination = stringPut(offset + lengthOfChannelIds,
                                        destination,
                                        LITTLE_ENDIAN);
        return this;
    }

    public int length()
    {
        return lengthOfChannelIds + lengthOfDestination;
    }



}
