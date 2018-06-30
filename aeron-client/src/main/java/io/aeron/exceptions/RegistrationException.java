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
package io.aeron.exceptions;

import io.aeron.*;

/**
 * Caused when a error occurs during addition or release of {@link Publication}s or {@link Subscription}s
 */
public class RegistrationException extends AeronException
{
    private final ErrorCode code;
    private final int value;

    public RegistrationException(final ErrorCode code, final int value, final String msg)
    {
        super(msg);
        this.code = code;
        this.value = value;
    }

    /**
     * Get the {@link ErrorCode} for the specific exception.
     *
     * @return the {@link ErrorCode} for the specific exception.
     */
    public ErrorCode errorCode()
    {
        return code;
    }

    /**
     * Value of the code encoded. This can provide additional information when a {@link ErrorCode#UNKNOWN_CODE_VALUE} is
     * returned.
     *
     * @return of the code encoded.
     */
    public int value()
    {
        return value;
    }

    public String getMessage()
    {
        return "value=" + value + " " + super.getMessage();
    }
}
