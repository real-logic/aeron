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
package uk.co.real_logic.aeron.common.command;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a subscription.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Correlation ID                        |
 * +---------------------------------------------------------------+
 * |                           Channel Id                          |
 * +---------------------------------------------------------------+
 * |      Destination Length       |   Destination               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class SubscriptionMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int CHANNEL_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int DESTINATION_OFFSET = CHANNEL_ID_OFFSET + SIZE_OF_LONG;

    private int lengthOfDestination;

    /**
     * return the channel id
     *
     * @return the channel id
     */
    public long channelId()
    {
        return uint32Get(offset() + CHANNEL_ID_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the channel id
     *
     * @param value the channel id
     */
    public SubscriptionMessageFlyweight channelId(long value)
    {
        uint32Put(offset() + CHANNEL_ID_OFFSET, value, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return the destination field
     *
     * @return destination field
     */
    public String destination()
    {
        return stringGet(offset() + DESTINATION_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set destination field
     *
     * @param destination field value
     * @return flyweight
     */
    public SubscriptionMessageFlyweight destination(final String destination)
    {
        lengthOfDestination = stringPut(offset() + DESTINATION_OFFSET, destination, LITTLE_ENDIAN);
        return this;
    }

    public int length()
    {
        return SIZE_OF_LONG + SIZE_OF_LONG + lengthOfDestination;
    }
}
