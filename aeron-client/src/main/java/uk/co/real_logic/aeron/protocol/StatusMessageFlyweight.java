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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for a Status Message Packet
 *
 * @see <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#status-messages">Status Message</a>
 */
public class StatusMessageFlyweight extends HeaderFlyweight
{
    /** Length of the Status Message Packet */
    public static final int HEADER_LENGTH = 28;

    /** Publisher should send SETUP frame */
    public static final short SEND_SETUP_FLAG = 0x80;

    private static final int SESSION_ID_FIELD_OFFSET = 8;
    private static final int STREAM_ID_FIELD_OFFSET = 12;
    private static final int CONSUMPTION_TERM_ID_FIELD_OFFSET = 16;
    private static final int CONSUMPTION_TERM_OFFSET_FIELD_OFFSET = 20;
    private static final int RECEIVER_WINDOW_FIELD_OFFSET = 24;

    public StatusMessageFlyweight()
    {
    }

    public StatusMessageFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    public StatusMessageFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * return session id field
     * @return session id field
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public StatusMessageFlyweight sessionId(final int sessionId)
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
    public StatusMessageFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return highest consumption term offset field
     *
     * @return highest consumption term offset field
     */
    public int consumptionTermOffset()
    {
        return getInt(CONSUMPTION_TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set highest consumption term offset field
     *
     * @param termOffset field value
     * @return flyweight
     */
    public StatusMessageFlyweight consumptionTermOffset(final int termOffset)
    {
        putInt(CONSUMPTION_TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return highest consumption term id field
     *
     * @return highest consumption term id field
     */
    public int consumptionTermId()
    {
        return getInt(CONSUMPTION_TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set highest consumption term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public StatusMessageFlyweight consumptionTermId(final int termId)
    {
        putInt(CONSUMPTION_TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return receiver window field
     *
     * @return receiver window field
     */
    public int receiverWindowLength()
    {
        return getInt(RECEIVER_WINDOW_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set receiver window field
     *
     * @param receiverWindowLength field value
     * @return flyweight
     */
    public StatusMessageFlyweight receiverWindowLength(final int receiverWindowLength)
    {
        putInt(RECEIVER_WINDOW_FIELD_OFFSET, receiverWindowLength, LITTLE_ENDIAN);

        return this;
    }
}
