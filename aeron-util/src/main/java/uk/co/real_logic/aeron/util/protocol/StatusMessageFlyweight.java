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
package uk.co.real_logic.aeron.util.protocol;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for a Status Message Packet
 *
 * @see <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#status-messages">Status Message</a>
 */
public class StatusMessageFlyweight extends HeaderFlyweight
{
    /** Size of the Status Message Packet */
    public static final int HEADER_LENGTH = 28;

    private static final int SESSION_ID_FIELD_OFFSET = 8;
    private static final int CHANNEL_ID_FIELD_OFFSET = 12;
    private static final int TERM_ID_FIELD_OFFSET = 16;
    private static final int CONTIGUOUS_SEQUENCE_NUMBER_FIELD_OFFSET = 20;
    private static final int RECEIVER_WINDOW_FIELD_OFFSET = 24;

    /**
     * return session id field
     * @return session id field
     */
    public long sessionId()
    {
        return uint32Get(offset() + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     * @param sessionId field value
     * @return flyweight
     */
    public StatusMessageFlyweight sessionId(final long sessionId)
    {
        uint32Put(offset() + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
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
    public StatusMessageFlyweight channelId(final long channelId)
    {
        uint32Put(offset() + CHANNEL_ID_FIELD_OFFSET, channelId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return highest contiguous sequence number field
     *
     * @return highest contiguous sequence number field
     */
    public long highestContiguousSequenceNumber()
    {
        return uint32Get(offset() + CONTIGUOUS_SEQUENCE_NUMBER_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set highest contiguous sequence number field
     *
     * @param sequenceNumber field value
     * @return flyweight
     */
    public StatusMessageFlyweight highestContiguousSequenceNumber(final long sequenceNumber)
    {
        uint32Put(offset() + CONTIGUOUS_SEQUENCE_NUMBER_FIELD_OFFSET, sequenceNumber, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return term id field
     *
     * @return term id field
     */
    public long termId()
    {
        return uint32Get(offset() + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set term id field
     *
     * @param termId field value
     * @return flyweight
     */
    public StatusMessageFlyweight termId(final long termId)
    {
        uint32Put(offset() + TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);
        return this;
    }

    /**
     * return receiver window field
     *
     * @return receiver window field
     */
    public long receiverWindow()
    {
        return uint32Get(offset() + RECEIVER_WINDOW_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set receiver window field
     *
     * @param receiverWindow field value
     * @return flyweight
     */
    public StatusMessageFlyweight receiverWindow(final long receiverWindow)
    {
        uint32Put(offset() + RECEIVER_WINDOW_FIELD_OFFSET, receiverWindow, LITTLE_ENDIAN);
        return this;
    }

}
