/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.exceptions;

import io.aeron.*;

/**
 * Caused when a error occurs during addition, modification, or release of client resources such as
 * {@link Publication}s, {@link Subscription}s, or {@link Counter}s.
 */
public class RegistrationException extends AeronException
{
    /**
     * The correlation id of the command to register the resource action.
     */
    private final long correlationId;

    /**
     * Value of the {@link #errorCode()} encoded as an int.
     */
    private final int errorCodeValue;

    /**
     * The {@link ErrorCode} for the specific exception.
     */
    private final ErrorCode errorCode;

    /**
     * Construct a exception to represent an error which occurred during registration of a resource such as a
     * Publication, Subscription, or Counter.
     *
     * @param correlationId  of the command to register the resource.
     * @param errorCodeValue in case the {@link ErrorCode} is unknown to the client version.
     * @param errorCode      indicating type of error experienced by the media driver.
     * @param msg            proving more detail.
     */
    public RegistrationException(
        final long correlationId, final int errorCodeValue, final ErrorCode errorCode, final String msg)
    {
        super(
            stripCategoryName(msg) + ", errorCodeValue=" + errorCodeValue,
            ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == errorCode ? Category.WARN : Category.ERROR);
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.errorCodeValue = errorCodeValue;
    }

    /**
     * Get the correlation id of the command to register the resource action.
     *
     * @return the correlation id of the command to register the resource action.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Get the {@link ErrorCode} for the specific exception.
     *
     * @return the {@link ErrorCode} for the specific exception.
     */
    public ErrorCode errorCode()
    {
        return errorCode;
    }

    /**
     * Value of the {@link #errorCode()} encoded as an int. This can provide additional information when a
     * {@link ErrorCode#UNKNOWN_CODE_VALUE} is returned.
     *
     * @return value of the errorCode encoded as an int.
     */
    public int errorCodeValue()
    {
        return errorCodeValue;
    }

    private static String stripCategoryName(final String msg)
    {
        if (null != msg)
        {
            final int i = msg.indexOf('-');
            final int length = msg.length();

            if (i < length)
            {
                if (msg.startsWith(Category.FATAL.name()) && 6 == i)
                {
                    return msg.substring(i + 2);
                }

                if (msg.startsWith(Category.ERROR.name()) && 6 == i)
                {
                    return msg.substring(i + 2);
                }

                if (msg.startsWith(Category.WARN.name()) && 5 == i)
                {
                    return msg.substring(i + 2);
                }
            }
        }

        return msg;
    }
}
