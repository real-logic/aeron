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
 * Indicates an error occurred when setting up the channel for either a {@link io.aeron.Publication} or
 * {@link io.aeron.Subscription}.
 */
public class ChannelEndpointException extends AeronException
{
    private static final long serialVersionUID = 6810249167217382358L;

    /**
     * Counter id for the status indicator of the channel.
     */
    private final int statusIndicatorId;

    /**
     * Construct an exception with a given status indicator counter id and detail message.
     *
     * @param statusIndicatorId counter id for the channel.
     * @param message           for the exception.
     */
    public ChannelEndpointException(final int statusIndicatorId, final String message)
    {
        super(message);
        this.statusIndicatorId = statusIndicatorId;
    }

    /**
     * Return the id for the counter associated with the channel endpoint.
     *
     * @return counter id associated with the channel endpoint
     */
    public int statusIndicatorId()
    {
        return statusIndicatorId;
    }
}
