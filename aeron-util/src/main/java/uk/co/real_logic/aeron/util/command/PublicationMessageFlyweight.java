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

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding and removing a publication
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
public class PublicationMessageFlyweight extends Flyweight
{
    private static final int CORRELATION_ID_FIELD_OFFSET = 0;
    private static final int SESSION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int CHANNEL_ID_FIELD_OFFSET = SESSION_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int DESTINATION_OFFSET = CHANNEL_ID_FIELD_OFFSET + SIZE_OF_INT;

    private int lengthOfDestination;

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset() + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset() + SESSION_ID_FIELD_OFFSET, (int)sessionId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return atomicBuffer().getLong(offset() + CORRELATION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight correlationId(final long correlationId)
    {
        atomicBuffer().putLong(offset() + CORRELATION_ID_FIELD_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return channel id field
     *
     * @return channel id field
     */
    public long channelId()
    {
        return uint32Get(offset() + CHANNEL_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight channelId(final long channelId)
    {
        uint32Put(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * return destination field
     *
     * @return destination field
     */
    public String destination()
    {
        return stringGet(offset() + DESTINATION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set destination field
     *
     * @param destination field value
     * @return flyweight
     */
    public PublicationMessageFlyweight destination(final String destination)
    {
        lengthOfDestination = stringPut(offset() + DESTINATION_OFFSET,
                                        destination,
                                        ByteOrder.LITTLE_ENDIAN);
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
