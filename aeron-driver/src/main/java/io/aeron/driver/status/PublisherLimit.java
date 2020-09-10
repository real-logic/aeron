/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.driver.status;

import io.aeron.AeronCounters;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.UnsafeBufferPosition;

/**
 * The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will experience
 * back pressure when this position is passed as a means of flow control.
 */
public class PublisherLimit
{
    /**
     * Type id of a publisher limit counter.
     */
    public static final int PUBLISHER_LIMIT_TYPE_ID = AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "pub-lmt";

    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        return StreamCounter.allocate(
            tempBuffer, NAME, PUBLISHER_LIMIT_TYPE_ID, countersManager, registrationId, sessionId, streamId, channel);
    }
}
