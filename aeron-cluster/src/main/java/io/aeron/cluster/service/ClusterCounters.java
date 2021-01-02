/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * For allocating and finding cluster associated counters identified by
 * {@link ClusteredServiceContainer.Context#clusterId()}.
 */
public final class ClusterCounters
{
    private ClusterCounters()
    {
    }

    /**
     * Allocate a counter to represent the a component state within a cluster.
     *
     * @param aeron     to allocate the counter.
     * @param name      of the counter for the label.
     * @param typeId    for the counter.
     * @param clusterId to which the allocated counter belongs.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(final Aeron aeron, final String name, final int typeId, final int clusterId)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        int index = 0;
        buffer.putInt(index, clusterId);
        index += SIZE_OF_INT;

        index += buffer.putStringWithoutLengthAscii(index, name);
        index += buffer.putStringWithoutLengthAscii(index, " - clusterId=");
        index += buffer.putIntAscii(index, clusterId);

        return aeron.addCounter(typeId, buffer, 0, SIZE_OF_INT, buffer, SIZE_OF_INT, index - SIZE_OF_INT);
    }

    /**
     * Find the counter id for a type of counter in a cluster.
     *
     * @param counters  to search within.
     * @param typeId    of the counter.
     * @param clusterId to which the allocated counter belongs.
     * @return the matching counter id or {@link Aeron#NULL_VALUE} if not found.
     */
    public static int find(final CountersReader counters, final int typeId, final int clusterId)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            final int counterState = counters.getCounterState(i);

            if (counterState == RECORD_ALLOCATED)
            {
                if (counters.getCounterTypeId(i) == typeId &&
                    buffer.getInt(CountersReader.metaDataOffset(i) + KEY_OFFSET) == clusterId)
                {
                    return i;
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return Aeron.NULL_VALUE;
    }
}
