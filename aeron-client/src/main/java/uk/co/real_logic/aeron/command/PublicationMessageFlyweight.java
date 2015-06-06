/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import java.nio.ByteOrder;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

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
 * |                          Stream ID                            |
 * +---------------------------------------------------------------+
 * |                        Channel Length                         |
 * +---------------------------------------------------------------+
 * |                           Channel                            ...
 *...                                                              |
 * +---------------------------------------------------------------+
 */
public class PublicationMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int SESSION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int CHANNEL_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;

    private int lengthOfChannel;

    /**
     * Get the session id field
     *
     * @return session id field
     */
    public int sessionId()
    {
        return buffer().getInt(offset() + SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set session id field
     *
     * @param sessionId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight sessionId(final int sessionId)
    {
        buffer().putInt(offset() + SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the stream id field
     *
     * @return stream id field
     */
    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set the stream id field
     *
     * @param streamId field value
     * @return flyweight
     */
    public PublicationMessageFlyweight streamId(final int streamId)
    {
        buffer().putInt(offset() + STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the channel field
     *
     * @return channel field
     */
    public String channel()
    {
        return stringGet(offset() + CHANNEL_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set the channel field
     *
     * @param channel field value
     * @return flyweight
     */
    public PublicationMessageFlyweight channel(final String channel)
    {
        lengthOfChannel = stringPut(offset() + CHANNEL_OFFSET, channel, ByteOrder.LITTLE_ENDIAN);

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
        return CHANNEL_OFFSET + lengthOfChannel;
    }
}
