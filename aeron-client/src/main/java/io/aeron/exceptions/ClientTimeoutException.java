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
 * Client timeout event received from the driver for this client.
 * <p>
 * This is likely to happen as a result of a GC pause that is longer than the {@code aeron.client.liveness.timeout}
 * setting.
 */
public class ClientTimeoutException extends TimeoutException
{
    private static final long serialVersionUID = 4085394356371474876L;

    /**
     * Construct the client timeout exception with detail message.
     *
     * @param message detail for the exception.
     */
    public ClientTimeoutException(final String message)
    {
        super(message, Category.FATAL);
    }
}
