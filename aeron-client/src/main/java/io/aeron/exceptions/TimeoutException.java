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
package io.aeron.exceptions;

/**
 * Generic timeout has occurred while waiting on some action or event.
 */
public class TimeoutException extends AeronException
{
    private static final long serialVersionUID = 339608646881678251L;

    /**
     * Default timeout exception as {@link AeronException.Category#ERROR}.
     */
    public TimeoutException()
    {
    }

    /**
     * Timeout exception with provided message and {@link AeronException.Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public TimeoutException(final String message)
    {
        super(message);
    }

    /**
     * Timeout Aeron exception with provided {@link AeronException.Category}.
     *
     * @param category of this exception.
     */
    public TimeoutException(final Category category)
    {
        super(category);
    }

    /**
     * Timeout exception with a detailed message and provided {@link AeronException.Category}.
     *
     * @param message  providing detail on the error.
     * @param category of the exception.
     */
    public TimeoutException(final String message, final Category category)
    {
        super(message, category);
    }

    /**
     * AerTimeouton exception with cause and provided {@link AeronException.Category}.
     *
     * @param cause    of the error.
     * @param category of the exception.
     */
    public TimeoutException(final Throwable cause, final Category category)
    {
        super(cause, category);
    }

    /**
     * Timeout exception with a detailed message, cause, and {@link AeronException.Category}.
     *
     * @param message  providing detail on the error.
     * @param cause    of the error.
     * @param category of the exception.
     */
    public TimeoutException(final String message, final Throwable cause, final Category category)
    {
        super(message, cause, category);
    }

    /**
     * Constructs a new timeout exception with the detail message, cause, suppression enabled or disabled,
     * writable stack trace enabled or disabled, an {@link AeronException.Category}.
     *
     * @param message            providing detail on the error.
     * @param cause              of the error.
     * @param enableSuppression  is suppression enabled or not.
     * @param writableStackTrace is the stack trace writable or not.
     * @param category           of the exception.
     */
    protected TimeoutException(
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace,
        final Category category)
    {
        super(message, cause, enableSuppression, writableStackTrace, category);
    }
}
