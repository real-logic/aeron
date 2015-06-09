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
package uk.co.real_logic.aeron;

/**
 * Error codes between media driver and client and the on-wire protocol.
 */
public enum ErrorCode
{
    /** Aeron encountered an error condition. */
    GENERIC_ERROR(0),
    /** A failure occurred creating a new channel or parsing the channel string. */
    INVALID_CHANNEL(1),
    /** Attempted to remove a subscription, but it was not found */
    UNKNOWN_SUBSCRIPTION(2),
    /** Reserved for future use. */
    GENERIC_ERROR_MESSAGE(3),
    /** Reserved for future use. */
    GENERIC_ERROR_SUBSCRIPTION_MESSAGE(4),
    /** Attempted to remove a publication, but it was not found. */
    UNKNOWN_PUBLICATION(5);

    private final short value;

    ErrorCode(final int value)
    {
        this.value = (short) value;
    }

    /**
     * Get the value of this ErrorCode.
     * @return The value.
     */
    public short value()
    {
        return value;
    }

    /**
     * Get the ErrorCode that corresponds to the given value.
     * @param value Of the ErrorCode
     * @return ErrorCode
     */
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
