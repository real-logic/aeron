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
package io.aeron.cluster.client;

import io.aeron.exceptions.AeronException;

/**
 * Exceptions specific to Cluster operation.
 */
public class ClusterException extends AeronException
{
    private static final long serialVersionUID = -2688245045186545277L;

    /**
     * Default Cluster exception as {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     */
    public ClusterException()
    {
    }

    /**
     * Cluster exception with provided message and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public ClusterException(final String message)
    {
        super(message);
    }

    /**
     * Cluster exception with a detailed message and provided {@link io.aeron.exceptions.AeronException.Category}.
     *
     * @param message  providing detail on the error.
     * @param category of the exception.
     */
    public ClusterException(final String message, final Category category)
    {
        super(message, category);
    }

    /**
     * Cluster exception with a detailed message, cause, and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message providing detail on the error.
     * @param cause   of the error.
     */
    public ClusterException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Cluster exception with provided cause and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param cause of the error.
     */
    public ClusterException(final Throwable cause)
    {
        super(cause);
    }

    /**
     * Cluster exception with cause and provided {@link io.aeron.exceptions.AeronException.Category}.
     *
     * @param cause    of the error.
     * @param category of the exception.
     */
    public ClusterException(final Throwable cause, final Category category)
    {
        super(cause, category);
    }

    /**
     * Cluster exception with a detailed message, cause, and {@link io.aeron.exceptions.AeronException.Category}.
     *
     * @param message  providing detail on the error.
     * @param cause    of the error.
     * @param category of the exception.
     */
    public ClusterException(final String message, final Throwable cause, final Category category)
    {
        super(message, cause, category);
    }

    /**
     * Constructs a new Cluster exception with a detail message, cause, suppression enabled or disabled,
     * and writable stack trace enabled or disabled, in the category
     * {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message            providing detail on the error.
     * @param cause              of the error.
     * @param enableSuppression  is suppression enabled or not.
     * @param writableStackTrace is the stack trace writable or not.
     */
    public ClusterException(
        final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
