/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.driver.exceptions;

import io.aeron.ErrorCode;
import io.aeron.exceptions.ControlProtocolException;

/**
 * Indicates an invalid URI for a channel has been received by the driver from a client.
 */
public class InvalidChannelException extends ControlProtocolException
{
    private static final long serialVersionUID = 8395688431913848255L;

    /**
     * Exception with provided message and {@link ErrorCode#INVALID_CHANNEL}.
     *
     * @param message to detail the exception.
     */
    public InvalidChannelException(final String message)
    {
        super(ErrorCode.INVALID_CHANNEL, message);
    }

    /**
     * Exception with provided cause and {@link ErrorCode#INVALID_CHANNEL}.
     *
     * @param cause of the exception.
     */
    public InvalidChannelException(final Exception cause)
    {
        super(ErrorCode.INVALID_CHANNEL, cause);
    }

    /**
     * Exception with provided message, cause and {@link ErrorCode#INVALID_CHANNEL}.
     *
     * @param message to detail the exception.
     * @param cause of the exception.
     */
    public InvalidChannelException(final String message, final Exception cause)
    {
        super(ErrorCode.INVALID_CHANNEL, message, cause);
    }
}
