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

import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.Flyweight;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;

/**
 * Message to denote that new buffers have been added for a subscription.
 *
 * @see uk.co.real_logic.aeron.util.command.ControlProtocolEvents
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                          Channel ID                           |
 * +---------------------------------------------------------------+
 * |                           Term ID                             |
 * +---------------------------------------------------------------+
 * |                          File Offset 0                        |
 * +---------------------------------------------------------------+
 * |                          File Offset 1                        |
 * +---------------------------------------------------------------+
 * |                          File Offset 2                        |
 * +---------------------------------------------------------------+
 * |                          File Offset 3                        |
 * +---------------------------------------------------------------+
 * |                          File Offset 4                        |
 * +---------------------------------------------------------------+
 * |                          File Offset 5                        |
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
 * |                          Destination Start                    |
 * +---------------------------------------------------------------+
 * |                          Destination End                      |
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
 * |                            Destination                      ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class NewBufferMessageFlyweight extends Flyweight
{
    private static final int SESSION_ID_OFFSET = 0;
    private static final int CHANNEL_ID_FIELD_OFFSET = 4;
    private static final int TERM_ID_FIELD_OFFSET = 8;
    private static final int FILE_OFFSETS_FIELDS_OFFSET = 12;
    private static final int BUFFER_LENGTHS_FIELDS_OFFSET = 36;
    private static final int LOCATION_POINTER_FIELDS_OFFSET = 60;
    private static final int LOCATION_0_FIELD_OFFSET = 92;

    /**
     * Contains both log buffers and state buffers
     */
    public static final int PAYLOAD_BUFFER_COUNT = BufferRotationDescriptor.BUFFER_COUNT * 2;

    /**
     * The Destination sits at the end of the message, after the location strings for both the
     * log and state buffers.
     */
    private static final int DESTINATION_INDEX = PAYLOAD_BUFFER_COUNT;

    public int bufferOffset(final int index)
    {
        return relativeIntField(index, FILE_OFFSETS_FIELDS_OFFSET);
    }

    public NewBufferMessageFlyweight bufferOffset(final int index, final int value)
    {
        return relativeIntField(index, value, FILE_OFFSETS_FIELDS_OFFSET);
    }

    public int bufferLength(final int index)
    {
        return relativeIntField(index, BUFFER_LENGTHS_FIELDS_OFFSET);
    }

    public NewBufferMessageFlyweight bufferLength(final int index, final int value)
    {
        return relativeIntField(index, value, BUFFER_LENGTHS_FIELDS_OFFSET);
    }

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset() + SESSION_ID_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public NewBufferMessageFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset() + SESSION_ID_OFFSET, (int)sessionId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return channel id field
     *
     * @return channel id field
     */
    public long channelId()
    {
        return uint32Get(offset() + CHANNEL_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set channel id field
     *
     * @param channelId field value
     * @return flyweight
     */
    public NewBufferMessageFlyweight channelId(final long channelId)
    {
        uint32Put(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return termId field
     *
     * @return termId field
     */
    public long termId()
    {
        return uint32Get(offset() + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set termId field
     *
     * @param termId field value
     * @return flyweight
     */
    public NewBufferMessageFlyweight termId(final long termId)
    {
        uint32Put(offset() + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);
        return this;
    }

    private int relativeIntField(final int index, final int fieldOffset)
    {
        return atomicBuffer().getInt(relativeOffset(index, fieldOffset), LITTLE_ENDIAN);
    }

    private NewBufferMessageFlyweight relativeIntField(final int index, final int value, final int fieldOffset)
    {
        atomicBuffer().putInt(relativeOffset(index, fieldOffset), value, LITTLE_ENDIAN);
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

    private NewBufferMessageFlyweight locationPointer(final int index, final int value)
    {
        return relativeIntField(index, value, LOCATION_POINTER_FIELDS_OFFSET);
    }

    public String location(final int index)
    {
        final int start = locationPointer(index);
        final int length = locationPointer(index + 1) - start;
        return atomicBuffer().getStringWithoutLength(offset() + start, length);
    }

    public NewBufferMessageFlyweight location(final int index, final String value)
    {
        final int start = locationPointer(index);
        if (start == 0)
        {
            throw new IllegalStateException("Previous location been hasn't been set yet at index " + index);
        }
        final int length = atomicBuffer().putStringWithoutLength(offset() + start, value);
        locationPointer(index + 1, start + length);
        return this;
    }

    public String destination()
    {
        return location(DESTINATION_INDEX);
    }

    public NewBufferMessageFlyweight destination(final String value)
    {
        return location(DESTINATION_INDEX, value);
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
        return locationPointer(DESTINATION_INDEX + 1);
    }
}
