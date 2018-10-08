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
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.TYPE_ID_OFFSET;

/**
 * Counter representing the commit position that can consumed by a state machine on a stream, it is the consensus
 * position reached by the cluster.
 */
public class CommitPos
{
    /**
     * Type id of a commit position counter.
     */
    public static final int COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "cluster-commit-pos";


    /**
     * Allocate a counter to represent the commit position on stream for the current leadership term.
     *
     * @param aeron to allocate the counter.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(final Aeron aeron)
    {
        return aeron.addCounter(COMMIT_POSITION_TYPE_ID, NAME);
    }

    /**
     * Find the active counter id for a cluster commit position
     *
     * @param counters to search within.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterId(final CountersReader counters)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }
}
