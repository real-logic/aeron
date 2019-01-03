/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.UnsafeBufferPosition;

/**
 * The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have multiple
 * Subscribers on the same machine tracked by a {@link io.aeron.driver.MediaDriver}.
 */
public class SubscriberPos
{
    /**
     * Type id of a subscriber position counter.
     */
    public static final int SUBSCRIBER_POSITION_TYPE_ID = 4;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "sub-pos";

    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel,
        final long joinPosition)
    {
        return StreamPositionCounter.allocate(
            tempBuffer,
            NAME,
            SUBSCRIBER_POSITION_TYPE_ID,
            countersManager,
            registrationId,
            sessionId,
            streamId,
            channel,
            joinPosition);
    }
}
