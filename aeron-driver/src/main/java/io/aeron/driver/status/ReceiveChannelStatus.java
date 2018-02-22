/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.status;

import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * The status of a receive channel endpoint represented as a counter value.
 */
public class ReceiveChannelStatus
{
    /**
     * Type id of a receive channel status indicator.
     */
    public static final int RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "rcv-channel";

    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer, final CountersManager countersManager, final String channel)
    {
        return ChannelEndpointStatus.allocate(
            tempBuffer, NAME, RECEIVE_CHANNEL_STATUS_TYPE_ID, countersManager, channel);
    }
}
