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
package uk.co.real_logic.aeron.command;

import uk.co.real_logic.aeron.util.command.TripletMessageFlyweight;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;

/**
 * Notification that data was written.
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                           Term ID                             |
 * +---------------------------------------------------------------+
 * |                            Amount                             |
 * +---------------------------------------------------------------+
 * |                          Current Time                         |
 * +---------------------------------------------------------------+
 */
public class DataWrittenFlyweight extends TripletMessageFlyweight
{
    private static final int AMOUNT_OFFSET = 12;
    private static final int CURRENT_TIME_OFFSET = 16;

    public long amount()
    {
        return uint32Get(offset + AMOUNT_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    public TripletMessageFlyweight amount(final long channelId)
    {
        uint32Put(offset + AMOUNT_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public long currentTime()
    {
        return uint32Get(offset + CURRENT_TIME_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    public TripletMessageFlyweight currentTime(final long channelId)
    {
        uint32Put(offset + CURRENT_TIME_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int length()
    {
        return CURRENT_TIME_OFFSET + SIZE_OF_INT;
    }

}
