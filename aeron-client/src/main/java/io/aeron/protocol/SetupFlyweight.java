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
package io.aeron.protocol;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * HeaderFlyweight for Setup Message Frames.
 * <p>
 * <a target="_blank" href="https://github.com/real-logic/aeron/wiki/Protocol-Specification#stream-setup">
 *     Stream Setup</a> wiki page.
 */
public class SetupFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Setup Message Frame
     */
    public static final int HEADER_LENGTH = 40;

    private static final int TERM_OFFSET_FIELD_OFFSET = 8;
    private static final int SESSION_ID_FIELD_OFFSET = 12;
    private static final int STREAM_ID_FIELD_OFFSET = 16;
    private static final int INITIAL_TERM_ID_FIELD_OFFSET = 20;
    private static final int ACTIVE_TERM_ID_FIELD_OFFSET = 24;
    private static final int TERM_LENGTH_FIELD_OFFSET = 28;
    private static final int MTU_LENGTH_FIELD_OFFSET = 32;
    private static final int TTL_FIELD_OFFSET = 36;

    public SetupFlyweight()
    {
    }

    public SetupFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    public SetupFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
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
    public SetupFlyweight termOffset(final int termOffset)
    {
        putInt(TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
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
    public SetupFlyweight sessionId(final int sessionId)
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
    public SetupFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return initial term id field
     *
     * @return initial term id field
     */
    public int initialTermId()
    {
        return getInt(INITIAL_TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set initial term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public SetupFlyweight initialTermId(final int termId)
    {
        putInt(INITIAL_TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return active term id field
     *
     * @return term id field
     */
    public int activeTermId()
    {
        return getInt(ACTIVE_TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set active term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public SetupFlyweight activeTermId(final int termId)
    {
        putInt(ACTIVE_TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return term length field
     *
     * @return term length field value
     */
    public int termLength()
    {
        return getInt(TERM_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term length field
     *
     * @param termLength field value
     * @return flyweight
     */
    public SetupFlyweight termLength(final int termLength)
    {
        putInt(TERM_LENGTH_FIELD_OFFSET, termLength, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return MTU length field
     *
     * @return MTU length field value
     */
    public int mtuLength()
    {
        return getInt(MTU_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set MTU length field
     *
     * @param mtuLength field value
     * @return flyweight
     */
    public SetupFlyweight mtuLength(final int mtuLength)
    {
        putInt(MTU_LENGTH_FIELD_OFFSET, mtuLength, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return the TTL field
     *
     * @return TTL field value
     */
    public int ttl()
    {
        return getInt(TTL_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the TTL field
     *
     * @param ttl field value
     * @return flyweight
     */
    public SetupFlyweight ttl(final int ttl)
    {
        putInt(TTL_FIELD_OFFSET, ttl, LITTLE_ENDIAN);

        return this;
    }

    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        final String formattedFlags = String.format("%1$8s", Integer.toBinaryString(flags())).replace(' ', '0');

        sb.append("SETUP Message{")
            .append("frame_length=").append(frameLength())
            .append(" version=").append(version())
            .append(" flags=").append(formattedFlags)
            .append(" type=").append(headerType())
            .append(" term_offset=").append(termOffset())
            .append(" session_id=").append(sessionId())
            .append(" stream_id=").append(streamId())
            .append(" initial_term_id=").append(initialTermId())
            .append(" active_term_id=").append(activeTermId())
            .append(" term_length=").append(termLength())
            .append(" mtu_length=").append(mtuLength())
            .append(" ttl=").append(ttl())
            .append("}");

        return sb.toString();
    }
}
