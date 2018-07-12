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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Counter representing the heartbeat timestamp from a service.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Service ID                            |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class ServiceHeartbeat
{
    /**
     * Type id of a service heartbeat counter.
     */
    public static final int SERVICE_HEARTBEAT_TYPE_ID = 206;

    public static final int SERVICE_ID_OFFSET = 0;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "service-heartbeat: serviceId=";

    public static final int KEY_LENGTH = SERVICE_ID_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the heartbeat of a clustered service.
     *
     * @param aeron      to allocate the counter.
     * @param tempBuffer to use for building the key and label without allocation.
     * @param serviceId  of the service heartbeat.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(final Aeron aeron, final MutableDirectBuffer tempBuffer, final int serviceId)
    {
        tempBuffer.putInt(SERVICE_ID_OFFSET, serviceId);

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, serviceId);

        return aeron.addCounter(
            SERVICE_HEARTBEAT_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
    }

    /**
     * Find the active counter id for heartbeat of a given service id.
     *
     * @param counters  to search within.
     * @param serviceId to search for.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterId(final CountersReader counters, final int serviceId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SERVICE_HEARTBEAT_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + SERVICE_ID_OFFSET) == serviceId)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }
}
