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
 * A timeout has occurred while waiting on the media driver responding to an operation.
 */
public class DriverTimeoutException extends TimeoutException
{
    private static final long serialVersionUID = -334819963402642904L;

    /**
     * Construct the exception for driver timeout due to lack of heartbeat.
     *
     * @param message detail for the exception.
     */
    public DriverTimeoutException(final String message)
    {
        super(message, Category.FATAL);
    }
}
