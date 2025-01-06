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
 * A timeout has occurred between service calls for the client conductor.
 * <p>
 * This is likely to occur due to GC or resource starvation where the client conductor thread has not being able to
 * run within the {@code aeron.client.liveness.timeout} property set on the media driver.
 */
public class ConductorServiceTimeoutException extends TimeoutException
{
    private static final long serialVersionUID = 4289404220974757441L;

    /**
     * Construct the exception for the service interval timeout with detailed message.
     *
     * @param message detail for the exception.
     */
    public ConductorServiceTimeoutException(final String message)
    {
        super(message, Category.FATAL);
    }
}
