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
 * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
 * It is possible the stream is not complete to this point if the stream has experienced loss.
 */
public class ReceiverHwm
{
    /**
     * Type id of a receiver high-water-mark position counter.
     */
    public static final int RECEIVER_HWM_TYPE_ID = 3;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "rcv-hwm";

    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        return StreamCounter.allocate(
            tempBuffer, NAME, RECEIVER_HWM_TYPE_ID, countersManager, registrationId, sessionId, streamId, channel);
    }
}
