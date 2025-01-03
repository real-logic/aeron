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
package io.aeron.driver.exceptions;

import io.aeron.exceptions.AeronException;

/**
 * Indicates a currently active driver has been detected in {@code aeron.dir} during startup.
 */
public class ActiveDriverException extends AeronException
{
    private static final long serialVersionUID = 2539364003811295552L;

    /**
     * Exception with provided message and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public ActiveDriverException(final String message)
    {
        super(message);
    }
}
