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

import uk.co.real_logic.aeron.Flyweight;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote that new buffers have been added for a subscription.
 *
 * NOTE: Layout should be SBE compliant
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
 * |                Subscriber Position Block Length               |
 * +---------------------------------------------------------------+
 * |                   Subscriber Position Count                   |
 * +---------------------------------------------------------------+
 * |                      Subscriber Position Id 0                 |
 * +---------------------------------------------------------------+
 * |                         Registration Id 0                     |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                     Subscriber Position Id 1                  |
 * +---------------------------------------------------------------+
 * |                         Registration Id 1                     |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                                                              ...
 *...     Up to "Position Indicators Count" entries of this form
 * +---------------------------------------------------------------+
 * |                         Log File Length                       |
 * +---------------------------------------------------------------+
 * |                          Log File Name                       ...
 *...                                                              |
 * +---------------------------------------------------------------+
 * |                     Source identity Length                    |
 * +---------------------------------------------------------------+
 * |                     Source identity Name                     ...
 *...                                                              |
 * +---------------------------------------------------------------+
 */
public class ConnectionBuffersReadyFlyweight extends Flyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int JOINING_POSITION_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int SESSION_ID_OFFSET = JOINING_POSITION_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int SUBSCRIBER_POSITION_COUNT_OFFSET = SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET + SIZE_OF_INT;
    private static final int SUBSCRIBER_POSITIONS_OFFSET = SUBSCRIBER_POSITION_COUNT_OFFSET + SIZE_OF_INT;

    private static final int SUBSCRIBER_POSITION_BLOCK_LENGTH = SIZE_OF_LONG + SIZE_OF_INT;

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
    public int subscriberPositionCount()
    {
        return buffer().getInt(offset() + SUBSCRIBER_POSITION_COUNT_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set the number of position indicators
     *
     * @param value the number of position indicators
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight subscriberPositionCount(final int value)
    {
        buffer().putInt(offset() + SUBSCRIBER_POSITION_BLOCK_LENGTH_OFFSET, SUBSCRIBER_POSITION_BLOCK_LENGTH, LITTLE_ENDIAN);
        buffer().putInt(offset() + SUBSCRIBER_POSITION_COUNT_OFFSET, value, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Set the position Id for the subscriber
     *
     * @param index for the subscriber position
     * @param id for the subscriber position
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight subscriberPositionId(final int index, final int id)
    {
        buffer().putInt(subscriberPositionOffset(index), id);

        return this;
    }

    /**
     * Return the position Id for the subscriber
     *
     * @param index for the subscriber position
     * @return position Id for the subscriber
     */
    public int subscriberPositionId(final int index)
    {
        return buffer().getInt(subscriberPositionOffset(index));
    }

    /**
     * Set the registration Id for the subscriber position
     *
     * @param index for the subscriber position
     * @param id for the subscriber position
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight positionIndicatorRegistrationId(final int index, final long id)
    {
        buffer().putLong(subscriberPositionOffset(index) + SIZE_OF_INT, id);

        return this;
    }

    /**
     * Return the registration Id for the subscriber position
     *
     * @param index for the subscriber position
     * @return registration Id for the subscriver position
     */
    public long positionIndicatorRegistrationId(final int index)
    {
        return buffer().getLong(subscriberPositionOffset(index) + SIZE_OF_INT);
    }

    /**
     * Return the Log Filename
     *
     * @return log filename
     */
    public String logFileName()
    {
        return buffer().getStringUtf8(logFileNameOffset(), LITTLE_ENDIAN);
    }

    /**
     * Set the log filename
     *
     * @param logFileName for the connection
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight logFileName(final String logFileName)
    {
        buffer().putStringUtf8(logFileNameOffset(), logFileName, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Return the source identity string
     *
     * @return source identity string
     */
    public String sourceIdentity()
    {
        return buffer().getStringUtf8(sourceIdentityOffset(), LITTLE_ENDIAN);
    }

    /**
     * Set the source identity string
     *
     * @param value for the source identity
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight sourceIdentity(final String value)
    {
        buffer().putStringUtf8(sourceIdentityOffset(), value, ByteOrder.LITTLE_ENDIAN);
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
        final int sourceIdentityOffset = sourceIdentityOffset();
        return sourceIdentityOffset + buffer().getInt(sourceIdentityOffset, LITTLE_ENDIAN) + SIZE_OF_INT;
    }

    private int subscriberPositionOffset(final int index)
    {
        return offset() + SUBSCRIBER_POSITIONS_OFFSET + (index * SUBSCRIBER_POSITION_BLOCK_LENGTH);
    }

    private int logFileNameOffset()
    {
        return subscriberPositionOffset(subscriberPositionCount());
    }

    private int sourceIdentityOffset()
    {
        final int logFileNameOffset = logFileNameOffset();
        return logFileNameOffset + buffer().getInt(logFileNameOffset, LITTLE_ENDIAN) + SIZE_OF_INT;
    }
}
