/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Control message for removing a Publication or Subscription.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Client ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Command Correlation ID                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Registration ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RemoveMessageFlyweight extends CorrelatedMessageFlyweight
{
    private static final int REGISTRATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

    /**
     * Get the registration id field
     *
     * @return registration id field
     */
    public long registrationId()
    {
        return buffer.getLong(offset + REGISTRATION_ID_OFFSET);
    }

    /**
     * Set registration id field
     *
     * @param registrationId field value
     * @return flyweight
     */
    public RemoveMessageFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_OFFSET, registrationId);

        return this;
    }

    public static int length()
    {
        return LENGTH + SIZE_OF_LONG;
    }
}
