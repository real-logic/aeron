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

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a publication
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Correlation ID                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |      Destination Length       |   Destination               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class PublicationMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int SESSION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int CHANNEL_ID_FIELD_OFFSET = SESSION_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int DESTINATION_OFFSET = CHANNEL_ID_FIELD_OFFSET + SIZE_OF_INT;

    private int lengthOfDestination;

    /**
     * Get the session id field
     *
     * @return session id field
     */
    public int sessionId()
    {
        return atomicBuffer().getInt(offset() + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set session id field
     *
     * @param sessionId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight sessionId(final int sessionId)
    {
        atomicBuffer().putInt(offset() + SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Get the channel id field
     *
     * @return channel id field
     */
    public int channelId()
    {
        return atomicBuffer().getInt(offset() + CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set the channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight channelId(final int channelId)
    {
        atomicBuffer().putInt(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Get the destination field
     *
     * @return destination field
     */
    public String destination()
    {
        return stringGet(offset() + DESTINATION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set the destination field
     *
     * @param destination field value
     * @return flyweight
     */
    public PublicationMessageFlyweight destination(final String destination)
    {
        lengthOfDestination = stringPut(offset() + DESTINATION_OFFSET, destination, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Get the length of the current message
     *
     * NB: must be called after the data is written in order to be accurate.
     *
     * @return the length of the current message
     */
    public int length()
    {
        return DESTINATION_OFFSET + lengthOfDestination;
    }
}
