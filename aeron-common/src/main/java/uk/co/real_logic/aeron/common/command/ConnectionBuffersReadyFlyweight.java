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
package uk.co.real_logic.aeron.common.command;

import uk.co.real_logic.aeron.common.Flyweight;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote that new buffers have been added for a subscription.
 *
 * @see ControlProtocolEvents
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Correlation ID                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                        Joining Position                       |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                           Stream ID                           |
 * +---------------------------------------------------------------+
 * |                   Position Indicators Count                   |
 * +---------------------------------------------------------------+
 * |                         Channel Length                        |
 * +---------------------------------------------------------------+
 * |                            Channel                          ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 * |                         Log File Length                       |
 * +---------------------------------------------------------------+
 * |                          Log File Name                      ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 * |                         source info Length                    |
 * +---------------------------------------------------------------+
 * |                         source info Name                    ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 * |                     Position Indicator Id 0                   |
 * +---------------------------------------------------------------+
 * |                         Registration Id 0                     |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                     Position Indicator Id 1                   |
 * +---------------------------------------------------------------+
 * |                         Registration Id 1                     |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                                                             ...
 * Up to "Position Indicators Count" entries of this form
 */
public class ConnectionBuffersReadyFlyweight extends Flyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int JOINING_POSITION_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int SESSION_ID_OFFSET = JOINING_POSITION_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int POSITION_INDICATOR_COUNT_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int CHANNEL_FIELD_OFFSET = POSITION_INDICATOR_COUNT_OFFSET + SIZE_OF_INT;

    private static final int POSITION_INDICATOR_FIELD_SIZE = SIZE_OF_LONG + SIZE_OF_INT;

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return buffer().getLong(offset() + CORRELATION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight correlationId(final long correlationId)
    {
        buffer().putLong(offset() + CORRELATION_ID_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);

        return this;
    }

    /**
     * The joining position value
     *
     * @return joining position value
     */
    public long joiningPosition()
    {
        return buffer().getLong(offset() + JOINING_POSITION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set joining position field
     *
     * @param joiningPosition field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight joiningPosition(final long joiningPosition)
    {
        buffer().putLong(offset() + JOINING_POSITION_OFFSET, joiningPosition, ByteOrder.LITTLE_ENDIAN);

        return this;
    }

    /**
     * return session id field
     *
     * @return session id field
     */
    public int sessionId()
    {
        return buffer().getInt(offset() + SESSION_ID_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight sessionId(final int sessionId)
    {
        buffer().putInt(offset() + SESSION_ID_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return stream id field
     *
     * @return stream id field
     */
    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set stream id field
     *
     * @param streamId field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight streamId(final int streamId)
    {
        buffer().putInt(offset() + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return the number of position indicators
     *
     * @return the number of position indicators
     */
    public int positionIndicatorCount()
    {
        return buffer().getInt(offset() + POSITION_INDICATOR_COUNT_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set the number of position indicators
     *
     * @param value the number of position indicators
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight positionIndicatorCount(final int value)
    {
        buffer().putInt(offset() + POSITION_INDICATOR_COUNT_OFFSET, value, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return channel field
     *
     * @return channel field
     */
    public String channel()
    {
        return buffer().getStringUtf8(offset() + CHANNEL_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set channel field
     *
     * @param channel field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight channel(final String channel)
    {
        buffer().putStringUtf8(offset() + CHANNEL_FIELD_OFFSET, channel, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public String logFileName()
    {
        return buffer().getStringUtf8(logFileNameOffset(), LITTLE_ENDIAN);
    }

    public ConnectionBuffersReadyFlyweight logFileName(final String logFileName)
    {
        buffer().putStringUtf8(logFileNameOffset(), logFileName, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    private int logFileNameOffset()
    {
        final int channelStart = offset() + CHANNEL_FIELD_OFFSET;
        return buffer().getInt(channelStart) + channelStart + SIZE_OF_INT;
    }

    public String sourceInfo()
    {
        return buffer().getStringUtf8(sourceInfoOffset(), LITTLE_ENDIAN);
    }

    public ConnectionBuffersReadyFlyweight sourceInfo(final String value)
    {
        buffer().putStringUtf8(sourceInfoOffset(), value, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    private int sourceInfoOffset()
    {
        final int logFileNameOffset = logFileNameOffset();
        return buffer().getInt(logFileNameOffset) + logFileNameOffset + SIZE_OF_INT;
    }

    public ConnectionBuffersReadyFlyweight positionIndicatorCounterId(final int index, final int id)
    {
        buffer().putInt(positionIndicatorOffset(index), id);

        return this;
    }

    public int positionIndicatorCounterId(final int index)
    {
        return buffer().getInt(positionIndicatorOffset(index));
    }

    public ConnectionBuffersReadyFlyweight positionIndicatorRegistrationId(final int index, final long id)
    {
        buffer().putLong(positionIndicatorOffset(index) + SIZE_OF_INT, id);

        return this;
    }

    public long positionIndicatorRegistrationId(final int index)
    {
        return buffer().getLong(positionIndicatorOffset(index) + SIZE_OF_INT);
    }

    private int positionIndicatorOffset(final int index)
    {
        final int sourceInfoOffset = sourceInfoOffset();
        final int endOfSourceInfo = buffer().getInt(sourceInfoOffset) + sourceInfoOffset + SIZE_OF_INT;
        return endOfSourceInfo + index * POSITION_INDICATOR_FIELD_SIZE;
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
        return positionIndicatorOffset(positionIndicatorCount() - 1) + POSITION_INDICATOR_FIELD_SIZE;
    }
}
