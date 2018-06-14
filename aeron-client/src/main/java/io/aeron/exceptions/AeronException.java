/*
 * Copyright 2018 Real Logic Ltd.
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

/**
 * Base Aeron exception for catching all Aeron specific errors.
 */
public class AeronException extends RuntimeException
{
    public AeronException()
    {
    }

    public AeronException(final String message)
    {
        super(message);
    }

    public AeronException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public AeronException(final Throwable cause)
    {
        super(cause);
    }

    public AeronException(
        final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
