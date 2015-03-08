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
package uk.co.real_logic.aeron.common;

/**
 * Error codes between media driver and library and the on-wire protocol.
 */
public enum ErrorCode
{
    GENERIC_ERROR(0),
    INVALID_CHANNEL(1),
    UNKNOWN_SUBSCRIPTION(2),
    GENERIC_ERROR_MESSAGE(3),
    GENERIC_ERROR_SUBSCRIPTION_MESSAGE(4),
    UNKNOWN_PUBLICATION(5);

    private final short value;

    ErrorCode(final int value)
    {
        this.value = (short) value;
    }

    public short value()
    {
        return value;
    }

    public static ErrorCode get(final short value)
    {
        if (value > Singleton.VALUES.length)
        {
            throw new IllegalArgumentException("no ErrorCode for value: " + value);
        }

        return Singleton.VALUES[value];
    }

    static class Singleton
    {
        public static final ErrorCode[] VALUES = ErrorCode.values();
    }
}
