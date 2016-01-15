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
package uk.co.real_logic.aeron.protocol;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * HeaderFlyweight for Data Header
 *
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame">Data Frame</a>
 */
public class DataHeaderFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Data Header
     */
    public static final int HEADER_LENGTH = 24;

    /**
     * Begin Flag
     */
    public static final short BEGIN_FLAG = 0x80;

    /**
     * End Flag
     */
    public static final short END_FLAG = 0x40;

    /**
     * Begin and End Flags
     */
    public static final short BEGIN_AND_END_FLAGS = BEGIN_FLAG | END_FLAG;

    public static final int TERM_OFFSET_FIELD_OFFSET = 8;
    public static final int SESSION_ID_FIELD_OFFSET = 12;
    public static final int STREAM_ID_FIELD_OFFSET = 16;
    public static final int TERM_ID_FIELD_OFFSET = 20;
    public static final int DATA_OFFSET = 24;

    public DataHeaderFlyweight()
    {
    }

    public DataHeaderFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    public DataHeaderFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * return session id field
     *
     * @return session id field
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     *
     * @param sessionId field value
     * @return flyweight
     */
    public DataHeaderFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return stream id field
     *
     * @return stream id field
     */
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set stream id field
     *
     * @param streamId field value
     * @return flyweight
     */
    public DataHeaderFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return term id field
     *
     * @return term id field
     */
    public int termId()
    {
        return getInt(TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public DataHeaderFlyweight termId(final int termId)
    {
        putInt(TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return term offset field
     *
     * @return term offset field
     */
    public int termOffset()
    {
        return getInt(TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term offset field
     *
     * @param termOffset field value
     * @return flyweight
     */
    public DataHeaderFlyweight termOffset(final int termOffset)
    {
        putInt(TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return offset in buffer for data
     *
     * @return offset of data in the buffer
     */
    public int dataOffset()
    {
        return DATA_OFFSET;
    }

    /**
     * Return an initialised default Data Frame Header.
     *
     * @param sessionId for the header
     * @param streamId  for the header
     * @param termId    for the header
     * @return byte array containing the header
     */
    public static UnsafeBuffer createDefaultHeader(final int sessionId, final int streamId, final int termId)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[HEADER_LENGTH]);

        buffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        buffer.putByte(FLAGS_FIELD_OFFSET, (byte)BEGIN_AND_END_FLAGS);
        buffer.putShort(TYPE_FIELD_OFFSET, (short)HDR_TYPE_DATA, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(TERM_ID_FIELD_OFFSET, termId, ByteOrder.LITTLE_ENDIAN);

        return buffer;
    }

    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        final String formattedFlags = String.format("%1$8s", Integer.toBinaryString(flags())).replace(' ', '0');

        sb.append("Data Header{")
            .append("frame_length=").append(frameLength())
            .append(" version=").append(version())
            .append(" flags=").append(formattedFlags)
            .append(" type=").append(headerType())
            .append(" frame_length=").append(frameLength())
            .append(" term_offset=").append(termOffset())
            .append(" session_id=").append(sessionId())
            .append(" stream_id=").append(streamId())
            .append(" term_id=").append(termId())
            .append("}");

        return sb.toString();
    }
}
