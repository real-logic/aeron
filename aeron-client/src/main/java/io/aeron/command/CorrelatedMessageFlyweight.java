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
package io.aeron.command;

import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Base flyweight that can be extended to track a client request.
 *
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            Client ID                          |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                         Correlation ID                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 */
public class CorrelatedMessageFlyweight
{
    public static final int CLIENT_ID_FIELD_OFFSET = 0;
    public static final int CORRELATION_ID_FIELD_OFFSET = CLIENT_ID_FIELD_OFFSET + SIZE_OF_LONG;
    public static final int LENGTH = 2 * SIZE_OF_LONG;

    protected MutableDirectBuffer buffer;
    protected int offset;

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap
     * @param offset at which the message begins.
     * @return for fluent API
     */
    public final CorrelatedMessageFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * return client id field
     *
     * @return client id field
     */
    public long clientId()
    {
        return buffer.getLong(offset + CLIENT_ID_FIELD_OFFSET);
    }

    /**
     * set client id field
     *
     * @param clientId field value
     * @return for fluent API
     */
    public CorrelatedMessageFlyweight clientId(final long clientId)
    {
        buffer.putLong(offset + CLIENT_ID_FIELD_OFFSET, clientId);

        return this;
    }

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_FIELD_OFFSET);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return for fluent API
     */
    public CorrelatedMessageFlyweight correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_FIELD_OFFSET, correlationId);

        return this;
    }
}
