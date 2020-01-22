/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.command;

import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote that a Subscription has been successfully set up.
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Channel Status Indicator ID                  |
 *  +---------------------------------------------------------------+
 *  |                            Stream ID                          |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * @since 0.1.0 (CnC)
 */
public class SubscriptionReadyFlyweightV1
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int CHANNEL_STATUS_INDICATOR_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_OFFSET = CHANNEL_STATUS_INDICATOR_ID_OFFSET + SIZE_OF_INT;

    public static final int LENGTH = STREAM_ID_OFFSET + SIZE_OF_INT;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return for fluent API
     */
    public final SubscriptionReadyFlyweightV1 wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Get the correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_OFFSET);
    }

    /**
     * Set the correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public SubscriptionReadyFlyweightV1 correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * The channel status counter id.
     *
     * @return channel status counter id.
     */
    public int channelStatusCounterId()
    {
        return buffer.getInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET);
    }

    /**
     * Set channel status counter id field
     *
     * @param counterId field value
     * @return flyweight
     */
    public SubscriptionReadyFlyweightV1 channelStatusCounterId(final int counterId)
    {
        buffer.putInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET, counterId);

        return this;
    }

    /**
     * The stream id that will of been parsed out of an incoming addSubscription that included it in the channel uri.
     *
     * @return stream id.
     * @see ChannelMessageFlyweight
     */
    public int streamId()
    {
        return buffer.getInt(STREAM_ID_OFFSET);
    }

    /**
     * Set the stream id.
     *
     * @param streamId field value
     * @return flyweight.
     */
    public SubscriptionReadyFlyweightV1 streamId(final int streamId)
    {
        buffer.putInt(STREAM_ID_OFFSET, streamId);

        return this;
    }
}
