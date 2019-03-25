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
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Count of back-pressure events (BPE)s a sender has experienced on a stream. This is a per-stream event count for
 * that which is aggregated in {@link SystemCounterDescriptor#SENDER_FLOW_CONTROL_LIMITS}.
 */
public class SenderBpe
{
    /**
     * Type id of a sender back-pressure event counter.
     */
    public static final int SENDER_BPE_TYPE_ID = 13;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "snd-bpe";

    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final int counterId = StreamCounter.allocateCounterId(
            tempBuffer, NAME, SENDER_BPE_TYPE_ID, countersManager, registrationId, sessionId, streamId, channel);

        return new AtomicCounter(countersManager.valuesBuffer(), counterId, countersManager);
    }
}
