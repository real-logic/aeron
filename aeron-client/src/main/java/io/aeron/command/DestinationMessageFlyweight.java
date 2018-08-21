/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.command;

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a destination for a Publication in multi-destination-cast or a Subscription
 * in multi-destination Subscription.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Client ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Command Correlation ID                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Registration Correlation ID                  |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Channel Length                          |
 *  +---------------------------------------------------------------+
 *  |                       Channel (ASCII)                        ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class DestinationMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;
    private static final int CHANNEL_OFFSET = REGISTRATION_CORRELATION_ID_OFFSET + SIZE_OF_LONG;

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
    public DestinationMessageFlyweight registrationCorrelationId(final long correlationId)
    {
        buffer.putLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * Get the channel field in ASCII
     *
     * @return channel field
     */
    public String channel()
    {
        return buffer.getStringAscii(offset + CHANNEL_OFFSET);
    }

    /**
     * Set channel field in ASCII
     *
     * @param channel field value
     * @return flyweight
     */
    public DestinationMessageFlyweight channel(final String channel)
    {
        lengthOfChannel = buffer.putStringAscii(offset + CHANNEL_OFFSET, channel);

        return this;
    }

    public int length()
    {
        return CHANNEL_OFFSET + lengthOfChannel;
    }
}
