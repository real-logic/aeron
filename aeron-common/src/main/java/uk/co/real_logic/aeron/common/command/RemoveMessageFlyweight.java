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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;

/**
 * Control message for adding or removing a subscription.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            Client ID                          |
 * +---------------------------------------------------------------+
 * |                    Command Correlation ID                     |
 * +---------------------------------------------------------------+
 * |                  Registration Correlation ID                  |
 * +---------------------------------------------------------------+
 */
public class RemoveMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

    /**
     * return correlation id used in registration field
     *
     * @return correlation id field
     */
    public long registrationCorrelationId()
    {
        return atomicBuffer().getLong(offset() + REGISTRATION_CORRELATION_ID_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set registration correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public RemoveMessageFlyweight registrationCorrelationId(final long correlationId)
    {
        atomicBuffer().putLong(offset() + REGISTRATION_CORRELATION_ID_OFFSET, correlationId, LITTLE_ENDIAN);

        return this;
    }

    public static int length()
    {
        return CorrelatedMessageFlyweight.LENGTH + SIZE_OF_LONG;
    }
}
