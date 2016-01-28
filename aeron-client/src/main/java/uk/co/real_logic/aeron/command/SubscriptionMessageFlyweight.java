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
 * Control message for adding or removing a subscription.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Command Correlation ID                     |
 * +---------------------------------------------------------------+
 * |                  Registration Correlation ID                  |
 * +---------------------------------------------------------------+
 * |                           Stream Id                           |
 * +---------------------------------------------------------------+
 * |      Channel Length         |   Channel                     ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class SubscriptionMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_OFFSET = REGISTRATION_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;

    private int lengthOfChannel;

    /**
     * return correlation id used in registration field
     *
     * @return correlation id field
     */
    public long registrationCorrelationId()
    {
        return buffer.getLong(offset + REGISTRATION_CORRELATION_ID_OFFSET);
    }

    /**
     * set registration correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public SubscriptionMessageFlyweight registrationCorrelationId(final long correlationId)
    {
        buffer.putLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * return the stream id
     *
     * @return the stream id
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_OFFSET);
    }

    /**
     * Set the stream id
     *
     * @param streamId the channel id
     * @return flyweight
     */
    public SubscriptionMessageFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_OFFSET, streamId);

        return this;
    }

    /**
     * return the channel field
     *
     * @return channel field
     */
    public String channel()
    {
        return buffer.getStringUtf8(offset + CHANNEL_OFFSET, ByteOrder.nativeOrder());
    }

    /**
     * Set channel field
     *
     * @param channel field value
     * @return flyweight
     */
    public SubscriptionMessageFlyweight channel(final String channel)
    {
        lengthOfChannel = buffer.putStringUtf8(offset + CHANNEL_OFFSET, channel, ByteOrder.nativeOrder());

        return this;
    }

    public int length()
    {
        return CHANNEL_OFFSET + lengthOfChannel;
    }
}
