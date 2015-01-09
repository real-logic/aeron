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
package uk.co.real_logic.aeron.common.protocol;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * HeaderFlyweight for Setup Frames
 * <p>
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#stream-setup">Stream Setup</a>
 */
public class SetupFlyweight extends HeaderFlyweight
{
    /** Size of the Setup Header */
    public static final int HEADER_LENGTH = 32;

    private static final int TERM_OFFSET_FIELD_OFFSET = 8;
    private static final int SESSION_ID_FIELD_OFFSET = 12;
    private static final int STREAM_ID_FIELD_OFFSET = 16;
    private static final int TERM_ID_FIELD_OFFSET = 20;
    private static final int TERM_SIZE_FIELD_OFFSET = 24;
    private static final int MTU_LENGTH_FIELD_OFFSET = 28;

    /**
     * return term offset field
     *
     * @return term offset field
     */
    public int termOffset()
    {
        return buffer().getInt(offset() + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term offset field
     *
     * @param termOffset field value
     * @return flyweight
     */
    public SetupFlyweight termOffset(final int termOffset)
    {
        buffer().putInt(offset() + TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return session id field
     * @return session id field
     */
    public int sessionId()
    {
        return buffer().getInt(offset() + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public SetupFlyweight sessionId(final int sessionId)
    {
        buffer().putInt(offset() + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

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
    public SetupFlyweight streamId(final int streamId)
    {
        buffer().putInt(offset() + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return term id field
     *
     * @return term id field
     */
    public int termId()
    {
        return buffer().getInt(offset() + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public SetupFlyweight termId(final int termId)
    {
        buffer().putInt(offset() + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return term size field
     *
     * @return term size field value
     */
    public int termSize()
    {
        return buffer().getInt(offset() + TERM_SIZE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term size field
     *
     * @param termSize field value
     * @return flyweight
     */
    public SetupFlyweight termSize(final int termSize)
    {
        buffer().putInt(offset() + TERM_SIZE_FIELD_OFFSET, termSize, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return MTU length field
     *
     * @return MTU length field value
     */
    public int mtuLength()
    {
        return buffer().getInt(offset() + MTU_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set MTU length field
     *
     * @param mtuLength field value
     * @return flyweight
     */
    public SetupFlyweight mtuLength(final int mtuLength)
    {
        buffer().putInt(offset() + MTU_LENGTH_FIELD_OFFSET, mtuLength, LITTLE_ENDIAN);

        return this;
    }
}
