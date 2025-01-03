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
 * Indicates an invalid configuration option has been provided.
 */
public class ConfigurationException extends AeronException
{
    private static final long serialVersionUID = 2545086690221965112L;

    /**
     * Construct an exception with detail for the configuration error.
     *
     * @param message detail for the configuration error.
     */
    public ConfigurationException(final String message)
    {
        super(message);
    }
}
