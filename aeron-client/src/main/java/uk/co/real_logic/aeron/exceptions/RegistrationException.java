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
package uk.co.real_logic.aeron.exceptions;

import uk.co.real_logic.aeron.ErrorCode;

/**
 * Caused when a error occurs during addition or release of {@link uk.co.real_logic.aeron.Publication}s
 * or {@link uk.co.real_logic.aeron.Subscription}s
 */
public class RegistrationException extends RuntimeException
{
    private final ErrorCode code;

    public RegistrationException(final ErrorCode code, final String msg)
    {
        super(msg);
        this.code = code;
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
}
