/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * For allocating and finding Archive associated counters identified by the {@link Aeron#clientId()}.
 */
final class ArchiveCounters
{
    private ArchiveCounters()
    {
    }

    /**
     * Allocate a counter to represent state within an Archive. The {@code archiveId} is assumed to be
     * {@link Aeron#clientId()}.
     *
     * @param aeron      from {@link Archive} instance to allocate the counter.
     * @param tempBuffer temporary storage to create label and metadata.
     * @param typeId     for the counter.
     * @param name       of the counter for the label.
     * @param archiveId  to which the allocated counter belongs.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final int typeId,
        final String name,
        final long archiveId)
    {
        int index = 0;
        tempBuffer.putLong(index, archiveId);
        index += SIZE_OF_LONG;

        index += tempBuffer.putStringWithoutLengthAscii(index, name);
        index += tempBuffer.putStringWithoutLengthAscii(index, " - archiveId=");
        index += tempBuffer.putLongAscii(index, archiveId);

        return aeron.addCounter(typeId, tempBuffer, 0, SIZE_OF_LONG, tempBuffer, SIZE_OF_LONG, index - SIZE_OF_LONG);
    }

    /**
     * Find the counter id for a type of counter in an Archive.
     *
     * @param counters  to search within.
     * @param typeId    of the counter.
     * @param archiveId to which the allocated counter belongs.
     * @return the matching counter id or {@link Aeron#NULL_VALUE} if not found.
     */
    public static int find(final CountersReader counters, final int typeId, final long archiveId)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            final int counterState = counters.getCounterState(i);

            if (RECORD_ALLOCATED == counterState)
            {
                if (counters.getCounterTypeId(i) == typeId &&
                    buffer.getLong(CountersReader.metaDataOffset(i) + KEY_OFFSET) == archiveId)
                {
                    return i;
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_VALUE;
    }
}
