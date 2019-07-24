/*
 * Copyright 2014-2019 Real Logic Ltd.
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
     * Type of exception.
     */
    public enum Type
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
        WARNING
    }

    private final Type type;

    public AeronException()
    {
        this.type = Type.ERROR;
    }

    public AeronException(final Type type)
    {
        this.type = type;
    }

    public AeronException(final String message)
    {
        super(message);
        this.type = Type.ERROR;
    }

    public AeronException(final String message, final Type type)
    {
        super(message);
        this.type = type;
    }

    public AeronException(final String message, final Throwable cause)
    {
        super(message, cause);
        this.type = Type.ERROR;
    }

    public AeronException(final String message, final Throwable cause, final Type type)
    {
        super(message, cause);
        this.type = type;
    }

    public AeronException(final Throwable cause)
    {
        super(cause);
        this.type = Type.ERROR;
    }

    public AeronException(final Throwable cause, final Type type)
    {
        super(cause);
        this.type = type;
    }

    public AeronException(
        final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.type = Type.ERROR;
    }

    public AeronException(
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace,
        final Type type)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.type = type;
    }

    public Type type()
    {
        return type;
    }
}
