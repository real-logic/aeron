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
package io.aeron;

import io.aeron.exceptions.AeronException;

/**
 * Error codes between media driver and client and the on-wire protocol.
 */
public enum ErrorCode
{
    /**
     * Aeron encountered an error condition.
     */
    GENERIC_ERROR(0),

    /**
     * A failure occurred creating a new channel or parsing the channel string.
     */
    INVALID_CHANNEL(1),

    /**
     * Attempted to remove a subscription, but it was not found
     */
    UNKNOWN_SUBSCRIPTION(2),

    /**
     * Attempted to remove a publication, but it was not found.
     */
    UNKNOWN_PUBLICATION(3),

    /**
     * Channel Endpoint could not be successfully opened.
     */
    CHANNEL_ENDPOINT_ERROR(4),

    /**
     * Attempted to remove a counter, but it was not found.
     */
    UNKNOWN_COUNTER(5),

    // *** Insert new codes above here.

    /**
     * A code value returned was not known.
     */
    UNKNOWN_CODE_VALUE(-1);

    static final ErrorCode[] ERROR_CODES;

    static
    {
        final ErrorCode[] errorCodes = values();
        ERROR_CODES = new ErrorCode[errorCodes.length];
        for (final ErrorCode errorCode : errorCodes)
        {
            final int value = errorCode.value();

            if (value == UNKNOWN_CODE_VALUE.value())
            {
                continue;
            }

            if (null != ERROR_CODES[value])
            {
                throw new AeronException("value already in use: " + value);
            }

            ERROR_CODES[value] = errorCode;
        }
    }

    private final int value;

    ErrorCode(final int value)
    {
        this.value = value;
    }

    /**
     * Get the value of this ErrorCode.
     *
     * @return The value.
     */
    public int value()
    {
        return value;
    }

    /**
     * Get the ErrorCode that corresponds to the given value.
     *
     * @param value of the ErrorCode
     * @return ErrorCode
     */
    public static ErrorCode get(final int value)
    {
        if (value >= 0 && value < (ERROR_CODES.length - 2))
        {
            return ERROR_CODES[value];
        }

        return UNKNOWN_CODE_VALUE;
    }
}
