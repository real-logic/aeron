/*
 * Copyright 2014-2020 Real Logic Limited.
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
 * Base Aeron exception for catching all Aeron specific errors.
 */
public class AeronException extends RuntimeException
{
    /**
     * Category of {@link Exception}.
     */
    public enum Category
    {
        /**
         * Exception indicates a fatal condition. Recommendation is to terminate process immediately to avoid
         * state corruption.
         */
        FATAL,

        /**
         * Exception is an error. Corrective action is recommended if understood, otherwise treat as fatal.
         */
        ERROR,

        /**
         * Exception is a warning. Action has been, or will be, taken to handle the condition.
         * Additional corrective action by the application may be needed.
         */
        WARN
    }

    private final Category category;

    /**
     * Default Aeron exception of {@link Category#ERROR}.
     */
    public AeronException()
    {
        this.category = Category.ERROR;
    }

    /**
     * Default Aeron exception with provided {@link Category}.
     *
     * @param category of this exception.
     */
    public AeronException(final Category category)
    {
        this.category = category;
    }

    /**
     * Aeron exception with provided message and {@link Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public AeronException(final String message)
    {
        super(message);
        this.category = Category.ERROR;
    }

    /**
     * Aeron exception with provided cause and {@link Category#ERROR}.
     *
     * @param cause of the error.
     */
    public AeronException(final Throwable cause)
    {
        super(cause);
        this.category = Category.ERROR;
    }

    /**
     * Aeron exception with a detailed message and provided {@link Category}.
     *
     * @param message  providing detail on the error.
     * @param category of the exception.
     */
    public AeronException(final String message, final Category category)
    {
        super(message);
        this.category = category;
    }

    /**
     * Aeron exception with a detailed message and cause.
     *
     * @param message providing detail on the error.
     * @param cause   of the error.
     */
    public AeronException(final String message, final Throwable cause)
    {
        super(message, cause);
        this.category = Category.ERROR;
    }

    /**
     * Aeron exception with cause and provided {@link Category}.
     *
     * @param cause    of the error.
     * @param category of the exception.
     */
    public AeronException(final Throwable cause, final Category category)
    {
        super(cause);
        this.category = category;
    }

    /**
     * Aeron exception with a detailed message, cause, and {@link Category}.
     *
     * @param message  providing detail on the error.
     * @param cause    of the error.
     * @param category of the exception.
     */
    public AeronException(final String message, final Throwable cause, final Category category)
    {
        super(message, cause);
        this.category = category;
    }

    /**
     * Constructs a new Aeron exception with the a detail message, cause, suppression enabled or disabled,
     * and writable stack trace enabled or disabled, in the category {@link Category#ERROR}.
     *
     * @param message            providing detail on the error.
     * @param cause              of the error.
     * @param enableSuppression  is suppression enabled or not.
     * @param writableStackTrace is the stack trace writable or not.
     */
    public AeronException(
        final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.category = Category.ERROR;
    }

    /**
     * Constructs a new Aeron exception with the a detail message, cause, suppression enabled or disabled,
     * writable stack trace enabled or disabled, an {@link Category}.
     *
     * @param message            providing detail on the error.
     * @param cause              of the error.
     * @param enableSuppression  is suppression enabled or not.
     * @param writableStackTrace is the stack trace writable or not.
     * @param category           of the exception.
     */
    public AeronException(
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace,
        final Category category)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.category = category;
    }

    /**
     * {@link Category} of exception for determining what follow up action can be taken.
     *
     * @return {@link Category} of exception for determining what follow up action can be taken.
     */
    public Category category()
    {
        return category;
    }

    /**
     * Add the {@link #category()} name to the beginning of the {@link Throwable#getMessage()}.
     *
     * {@inheritDoc}
     */
    public String getMessage()
    {
        return category.name() + " - " + super.getMessage();
    }
}
