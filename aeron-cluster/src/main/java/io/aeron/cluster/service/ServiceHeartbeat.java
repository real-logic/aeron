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
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.Aeron.NULL_VALUE;
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
 *  |                      Cluster Member ID                        |
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
    public static final int MEMBER_ID_OFFSET = SERVICE_ID_OFFSET + SIZE_OF_INT;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "service-heartbeat: serviceId=";

    public static final int KEY_LENGTH = MEMBER_ID_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the heartbeat of a clustered service.
     *
     * @param aeron           to allocate the counter.
     * @param tempBuffer      to use for building the key and label without allocation.
     * @param serviceId       of the service heartbeat.
     * @param clusterMemberId the service will be associated with.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final int serviceId,
        final int clusterMemberId)
    {
        tempBuffer.putInt(SERVICE_ID_OFFSET, serviceId);
        tempBuffer.putInt(MEMBER_ID_OFFSET, clusterMemberId);

        final int labelOffset = BitUtil.align(KEY_LENGTH, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, NAME);
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, serviceId);

        return aeron.addCounter(
            SERVICE_HEARTBEAT_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, labelOffset, labelLength);
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

    /**
     * Get the cluster member id this service is associated with.
     *
     * @param counters  to search within.
     * @param counterId for the active service heartbeat counter.
     * @return if found otherwise {@link Aeron#NULL_VALUE}.
     */
    public static int getClusterMemberId(final CountersReader counters, final int counterId)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == SERVICE_HEARTBEAT_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + MEMBER_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }
}
