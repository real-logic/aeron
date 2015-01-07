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

import uk.co.real_logic.aeron.common.Flyweight;
import uk.co.real_logic.aeron.common.TermHelper;

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
 * |                           Term ID                             |
 * +---------------------------------------------------------------+
 * |                   Position Indicators Count                   |
 * +---------------------------------------------------------------+
 * |                          File Offset 0                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          File Offset 1                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          File Offset 2                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          File Offset 3                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          File Offset 4                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          File Offset 5                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                             Length 0                          |
 * +---------------------------------------------------------------+
 * |                             Length 1                          |
 * +---------------------------------------------------------------+
 * |                             Length 2                          |
 * +---------------------------------------------------------------+
 * |                             Length 3                          |
 * +---------------------------------------------------------------+
 * |                             Length 4                          |
 * +---------------------------------------------------------------+
 * |                             Length 5                          |
 * +---------------------------------------------------------------+
 * |                          Location 1 Start                     |
 * +---------------------------------------------------------------+
 * |                          Location 2 Start                     |
 * +---------------------------------------------------------------+
 * |                          Location 3 Start                     |
 * +---------------------------------------------------------------+
 * |                          Location 4 Start                     |
 * +---------------------------------------------------------------+
 * |                          Location 5 Start                     |
 * +---------------------------------------------------------------+
 * |                     Source Information Start                  |
 * +---------------------------------------------------------------+
 * |                           Channel Start                       |
 * +---------------------------------------------------------------+
 * |                           Channel End                         |
 * +---------------------------------------------------------------+
 * |                            Location 0                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Location 1                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Location 2                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Location 3                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Location 4                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Location 5                       ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                            Channel                          ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                     Position Indicator Id 0                 ...
 * +---------------------------------------------------------------+
 * |                         Registration Id 0                   ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                     Position Indicator Id 1                 ...
 * +---------------------------------------------------------------+
 * |                         Registration Id 1                   ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 * |                                                             ...
 * Up to "Position Indicators Count" entries of this form
 */
public class ConnectionBuffersReadyFlyweight extends Flyweight implements BuffersReadyFlyweight
{
    private static final int NUM_FILES = 6;

    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int JOINING_POSITION_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int SESSION_ID_OFFSET = JOINING_POSITION_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int TERM_ID_FIELD_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int POSITION_INDICATOR_COUNT_OFFSET = TERM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int FILE_OFFSETS_FIELDS_OFFSET = POSITION_INDICATOR_COUNT_OFFSET + SIZE_OF_INT;
    private static final int BUFFER_LENGTHS_FIELDS_OFFSET = FILE_OFFSETS_FIELDS_OFFSET + (NUM_FILES * SIZE_OF_LONG);
    private static final int LOCATION_POINTER_FIELDS_OFFSET = BUFFER_LENGTHS_FIELDS_OFFSET + (NUM_FILES * SIZE_OF_INT);
    private static final int LOCATION_0_FIELD_OFFSET = LOCATION_POINTER_FIELDS_OFFSET + (9 * SIZE_OF_INT);
    private static final int POSITION_INDICATOR_FIELD_SIZE = SIZE_OF_LONG + SIZE_OF_INT;

    /**
     * Contains both term buffers and state buffers
     */
    public static final int PAYLOAD_BUFFER_COUNT = TermHelper.BUFFER_COUNT * 2;

    /**
     * The Source Information sits after the location strings, but before the Channel
     */
    public static final int SOURCE_INFORMATION_INDEX = PAYLOAD_BUFFER_COUNT;

    /**
     * The Channel sits after the source name and location strings for both the term and state buffers.
     */
    private static final int CHANNEL_INDEX = SOURCE_INFORMATION_INDEX + 1;

    public long bufferOffset(final int index)
    {
        return buffer().getLong(FILE_OFFSETS_FIELDS_OFFSET + (index * SIZE_OF_LONG), ByteOrder.LITTLE_ENDIAN);
    }

    public ConnectionBuffersReadyFlyweight bufferOffset(final int index, final long value)
    {
        buffer().putLong(FILE_OFFSETS_FIELDS_OFFSET + (index * SIZE_OF_LONG), value, ByteOrder.LITTLE_ENDIAN);

        return this;
    }

    public int bufferLength(final int index)
    {
        return relativeIntField(index, BUFFER_LENGTHS_FIELDS_OFFSET);
    }

    public ConnectionBuffersReadyFlyweight bufferLength(final int index, final int value)
    {
        return relativeIntField(index, value, BUFFER_LENGTHS_FIELDS_OFFSET);
    }

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
     * return termId field
     *
     * @return termId field
     */
    public int termId()
    {
        return buffer().getInt(offset() + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set termId field
     *
     * @param termId field value
     * @return flyweight
     */
    public ConnectionBuffersReadyFlyweight termId(final int termId)
    {
        buffer().putInt(offset() + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

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

    private int relativeIntField(final int index, final int fieldOffset)
    {
        return buffer().getInt(relativeOffset(index, fieldOffset), LITTLE_ENDIAN);
    }

    private ConnectionBuffersReadyFlyweight relativeIntField(final int index, final int value, final int fieldOffset)
    {
        buffer().putInt(relativeOffset(index, fieldOffset), value, LITTLE_ENDIAN);

        return this;
    }

    private int relativeOffset(final int index, final int fieldOffset)
    {
        return offset() + fieldOffset + index * SIZE_OF_INT;
    }

    private int locationPointer(final int index)
    {
        if (index == 0)
        {
            return LOCATION_0_FIELD_OFFSET;
        }

        return relativeIntField(index, LOCATION_POINTER_FIELDS_OFFSET);
    }

    private ConnectionBuffersReadyFlyweight locationPointer(final int index, final int value)
    {
        return relativeIntField(index, value, LOCATION_POINTER_FIELDS_OFFSET);
    }

    public String bufferLocation(final int index)
    {
        final int start = locationPointer(index);
        final int length = locationPointer(index + 1) - start;

        return buffer().getStringWithoutLengthUtf8(offset() + start, length);
    }

    public ConnectionBuffersReadyFlyweight bufferLocation(final int index, final String value)
    {
        final int start = locationPointer(index);
        if (start == 0)
        {
            throw new IllegalStateException("Previous location been hasn't been set yet at index " + index);
        }

        final int length = buffer().putStringWithoutLengthUtf8(offset() + start, value);
        locationPointer(index + 1, start + length);

        return this;
    }

    public String sourceInfo()
    {
        return bufferLocation(SOURCE_INFORMATION_INDEX);
    }

    public ConnectionBuffersReadyFlyweight sourceInfo(final String value)
    {
        return bufferLocation(SOURCE_INFORMATION_INDEX, value);
    }

    public String channel()
    {
        return bufferLocation(CHANNEL_INDEX);
    }

    public ConnectionBuffersReadyFlyweight channel(final String value)
    {
        return bufferLocation(CHANNEL_INDEX, value);
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
        return endOfChannel() + index * POSITION_INDICATOR_FIELD_SIZE;
    }

    private int endOfChannel()
    {
        return locationPointer(CHANNEL_INDEX + 1);
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
        return positionIndicatorOffset(positionIndicatorCount() + 1);
    }
}
