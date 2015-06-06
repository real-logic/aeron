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
 * Message to denote that new buffers have been setup for a publication.
 *
 * @see ControlProtocolEvents
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Correlation ID                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                          Session ID                           |
 * +---------------------------------------------------------------+
 * |                           Stream ID                           |
 * +---------------------------------------------------------------+
 * |                    Publication Limit Offset                   |
 * +---------------------------------------------------------------+
 * |                         Log File Length                       |
 * +---------------------------------------------------------------+
 * |                          Log File Name                      ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 */
public class PublicationBuffersReadyFlyweight extends Flyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int PUBLICATION_LIMIT_COUNTER_ID_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int LOGFILE_FIELD_OFFSET = PUBLICATION_LIMIT_COUNTER_ID_OFFSET + SIZE_OF_INT;

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
    public PublicationBuffersReadyFlyweight correlationId(final long correlationId)
    {
        buffer().putLong(offset() + CORRELATION_ID_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);

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
     *
     * @param sessionId field value
     * @return flyweight
     */
    public PublicationBuffersReadyFlyweight sessionId(final int sessionId)
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
    public PublicationBuffersReadyFlyweight streamId(final int streamId)
    {
        buffer().putInt(offset() + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The publication limit counter id.
     *
     * @return publication limit counter id.
     */
    public int publicationLimitCounterId()
    {
        return buffer().getInt(offset() + PUBLICATION_LIMIT_COUNTER_ID_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set position counter id field
     *
     * @param positionCounterId field value
     * @return flyweight
     */
    public PublicationBuffersReadyFlyweight publicationLimitCounterId(final int positionCounterId)
    {
        buffer().putInt(offset() + PUBLICATION_LIMIT_COUNTER_ID_OFFSET, positionCounterId, LITTLE_ENDIAN);

        return this;
    }

    public String logFileName()
    {
        return buffer().getStringUtf8(offset() + LOGFILE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    public PublicationBuffersReadyFlyweight logFileName(final String logFileName)
    {
        buffer().putStringUtf8(offset() + LOGFILE_FIELD_OFFSET, logFileName, ByteOrder.LITTLE_ENDIAN);
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
        return buffer().getInt(offset() + LOGFILE_FIELD_OFFSET) + LOGFILE_FIELD_OFFSET + SIZE_OF_INT;
    }
}
