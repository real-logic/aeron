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

import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Control message flyweight for any message that needs to represent a connection
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Correlation ID                         |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          Stream ID                            |
 * +---------------------------------------------------------------+
 * |                        Channel Length                         |
 * +---------------------------------------------------------------+
 * |                           Channel                           ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 */
public class ImageMessageFlyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int STREAM_ID_FIELD_OFFSET = 8;
    private static final int CHANNEL_OFFSET = 12;

    private MutableDirectBuffer buffer;
    private int offset;
    private int lengthOfChannel;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return for fluent API
     */
    public final ImageMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * return correlation id field
     * @return correlation id field
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_OFFSET);
    }

    /**
     * set correlation id field
     * @param correlationId field value
     * @return flyweight
     */
    public ImageMessageFlyweight correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * return stream id field
     *
     * @return stream id field
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_FIELD_OFFSET);
    }

    /**
     * set stream id field
     *
     * @param streamId field value
     * @return flyweight
     */
    public ImageMessageFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId);

        return this;
    }

    /**
     * return channel field
     *
     * @return channel field
     */
    public String channel()
    {
        final int length = buffer.getInt(offset + CHANNEL_OFFSET);
        lengthOfChannel = SIZE_OF_INT + length;

        return buffer.getStringUtf8(offset + CHANNEL_OFFSET, length);
    }

    /**
     * set channel field
     *
     * @param channel field value
     * @return flyweight
     */
    public ImageMessageFlyweight channel(final String channel)
    {
        lengthOfChannel = buffer.putStringUtf8(offset + CHANNEL_OFFSET, channel, ByteOrder.nativeOrder());

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
