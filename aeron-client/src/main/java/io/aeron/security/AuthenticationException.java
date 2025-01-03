/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.security;

import io.aeron.exceptions.AeronException;

/**
 * Used to indicate a failed authentication attempt when connecting to a system.
 */
public class AuthenticationException extends AeronException
{
    private static final long serialVersionUID = -3205449285259494699L;

    /**
     * Default exception as {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     */
    public AuthenticationException()
    {
        super();
    }

    /**
     * Authentication exception with provided message and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public AuthenticationException(final String message)
    {
        super(message);
    }

    /**
     * Authentication exception with provided cause and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param cause of the error.
     */
    public AuthenticationException(final Throwable cause)
    {
        super(cause);
    }

    /**
     * Authentication exception with a detailed message and cause.
     *
     * @param message providing detail on the error.
     * @param cause   of the error.
     */
    public AuthenticationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Constructs a new Authentication exception with a detail message, cause, suppression enabled or disabled,
     * and writable stack trace enabled or disabled, in the category
     * {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message            providing detail on the error.
     * @param cause              of the error.
     * @param enableSuppression  is suppression enabled or not.
     * @param writableStackTrace is the stack trace writable or not.
     */
    protected AuthenticationException(
        final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
