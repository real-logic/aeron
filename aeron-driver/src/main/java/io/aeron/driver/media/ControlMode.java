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
package io.aeron.driver.media;

/**
 * Represents the control mode specified on a channel URI.
 */
public enum ControlMode
{
    /**
     * The default when no control mode is specified. Will mean that the stream is a normal one without MDC or similar.
     */
    NONE,
    /**
     * The stream should use dynamic MDC.
     */
    DYNAMIC,
    /**
     * The stream should use manual MDC.
     */
    MANUAL,
    /**
     * The stream is being used as part of a response channel.
     */
    RESPONSE;

    /**
     * Indicates if this is a multi-destination control mode.
     *
     * @return <code>true</code> if this is multi-destination <code>false</code> otherwise.
     */
    public boolean isMultiDestination()
    {
        return this == DYNAMIC || this == MANUAL;
    }
}
